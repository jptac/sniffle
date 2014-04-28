-module(sniffle_img_vnode).
-behaviour(riak_core_vnode).
-behaviour(riak_core_aae_vnode).
-include("sniffle.hrl").
-include_lib("riak_core/include/riak_core_vnode.hrl").
-include("bitcask.hrl").

-export([
         repair/4,
         get/3,
         create/4,
         delete/3
        ]).

-export([
         start_vnode/1,
         init/1,
         terminate/2,
         handle_command/3,
         is_empty/1,
         delete/1,
         handle_handoff_command/3,
         handoff_starting/2,
         handoff_cancelled/1,
         handoff_finished/2,
         handle_handoff_data/2,
         encode_handoff_item/2,
         handle_coverage/4,
         handle_exit/3,
         handle_info/2
        ]).

-export([
         master/0,
         aae_repair/2,
         hash_object/2
        ]).

-ignore_xref([
              create/4,
              delete/3,
              get/3,
              list/2,
              list/3,
              repair/4,
              start_vnode/1,
              handle_info/2
             ]).

-define(SERVICE, sniffle_img).

-define(MASTER, sniffle_img_vnode_master).

%%%===================================================================
%%% AAE
%%%===================================================================

master() ->
    ?MASTER.

hash_object(BKey, RObj) ->
    lager:debug("Hashing Key: ~p", [BKey]),
    list_to_binary(integer_to_list(erlang:phash2({BKey, RObj}))).

aae_repair(_, Key) ->
    lager:debug("AAE Repair: ~p", [Key]),
    sniffle_img:get(Key).

%%%===================================================================
%%% API
%%%===================================================================

start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

repair(IdxNode, Img, VClock, Obj) ->
    riak_core_vnode_master:command([IdxNode],
                                   {repair, Img, VClock, Obj},
                                   ?MASTER).

%%%===================================================================
%%% API - reads
%%%===================================================================

get(Preflist, ReqID, {Img, Idx}) ->
    get(Preflist, ReqID, <<Img/binary, Idx:32>>);

get(Preflist, ReqID, ImgAndIdx) ->
    riak_core_vnode_master:command(Preflist,
                                   {get, ReqID, ImgAndIdx},
                                   {fsm, undefined, self()},
                                   ?MASTER).

%%%===================================================================
%%% API - writes
%%%===================================================================

create(Preflist, ReqID, {Img, Idx}, Data) ->
    riak_core_vnode_master:command(Preflist,
                                   {create, ReqID, {Img, Idx}, Data},
                                   {fsm, undefined, self()},
                                   ?MASTER).

delete(Preflist, ReqID, {Img, Idx}) ->
    riak_core_vnode_master:command(Preflist,
                                   {delete, ReqID, {Img, Idx}},
                                   {fsm, undefined, self()},
                                   ?MASTER);

delete(Preflist, ReqID, Img) ->
    riak_core_vnode_master:command(Preflist,
                                   {delete, ReqID, Img},
                                   {fsm, undefined, self()},
                                   ?MASTER).

%%%===================================================================
%%% VNode
%%%===================================================================

init([Partition]) ->
    PartStr = integer_to_list(Partition),
    {ok, DBLoc} = application:get_env(fifo_db, db_path),
    DB = bitcask:open(DBLoc ++ "/" ++ PartStr ++ ".img", [read_write]),
    HT = riak_core_aae_vnode:maybe_create_hashtrees(?SERVICE, Partition,
                                                    ?MODULE, undefined),
    {ok, #vstate{db = DB, hashtrees = HT, partition = Partition, node = node()}}.

handle_command(ping, _Sender, State) ->
    {reply, {pong, State#vstate.partition}, State};

handle_command({repair, <<Img:36/binary, Idx:32/integer>>, VClock, Obj}, Sender, State) ->
    handle_command({repair, {Img, Idx}, VClock, Obj}, Sender, State);
handle_command({repair, {Img, Idx}, VClock, Obj}, _Sender, State) ->
    lager:warning("Repair of img: ~s~p", [Img, Idx]),
    case get(State#vstate.db, <<Img/binary, Idx:32>>) of
        {ok, #sniffle_obj{vclock = VC1}} when VC1 =:= VClock ->
            put(State, Img, Obj);
        not_found ->
            put(State, Img,  Obj);
        _ ->
            lager:error("[imgs] Read repair failed, data was updated too recent.")
    end,
    {noreply, State};

%%%===================================================================
%%% AAE
%%%===================================================================

handle_command({hashtree_pid, Node}, _, State=#vstate{hashtrees=HT}) ->
    %% Handle riak_core request forwarding during ownership handoff.
    %% Following is necessary in cases where anti-entropy was enabled
    %% after the vnode was already running
    case {node(), HT} of
        {Node, undefined} ->
            HT1 =  riak_core_aae_vnode:maybe_create_hashtrees(
                     ?SERVICE, State#vstate.partition, ?MODULE, HT),
            {reply, {ok, HT1}, State#vstate{hashtrees = HT1}};
        {Node, _} ->
            {reply, {ok, HT}, State};
        _ ->
            {reply, {error, wrong_node}, State}
    end;

handle_command({rehash, {_Bucket, Key}}, _, State=#vstate{db=DB}) ->
    case bitcask:get(DB, Key) of
        {ok, Term} ->
            riak_core_aae_vnode:update_hashtree(<<"img">>, Key,
                                                term_to_binary(Term),
                                                State#vstate.hashtrees);
        _ ->
            %% Make sure hashtree isn't tracking deleted data
            riak_core_index_hashtree:delete({<<"img">>, Key},
                                            State#vstate.hashtrees)
    end,
    {noreply, State};

handle_command(?FOLD_REQ{foldfun=Fun, acc0=Acc0}, _Sender, State) ->
    lager:debug("Fold on ~p", [State#vstate.partition]),
    Acc = bitcask:fold(State#vstate.db,
                       fun(K, V, O) ->
                               Fun({<<"img">>, K}, V, O)
                       end, Acc0),
    {reply, Acc, State};

%%%===================================================================
%%% General
%%%===================================================================

handle_command({get, ReqID, ImgAndIdx}, _Sender, State) ->
    Res = case get(State#vstate.db, ImgAndIdx) of
              {ok, R = #sniffle_obj{val=V}} ->
                  case statebox:is_statebox(V) of
                      true ->
                          R1 = R#sniffle_obj{val=statebox:value(V)},
                          put(State#vstate.db, ImgAndIdx, R1),
                          R1;
                      false  ->
                          R
                  end;
              not_found ->
                  not_found
          end,
    NodeIdx = {State#vstate.partition, State#vstate.node},
    {reply, {ok, ReqID, NodeIdx, Res}, State};

handle_command({create, {ReqID, Coordinator}, {Img, Idx}, Data},
               _Sender, State) ->
    VC0 = vclock:fresh(),
    VC = vclock:increment(Coordinator, VC0),
    HObject = #sniffle_obj{val=Data, vclock=VC},
    put(State, <<Img/binary, Idx:32>>, HObject),
    {reply, {ok, ReqID}, State};

handle_command({delete, {ReqID, _Coordinator}, {Img, Idx}}, _Sender, State) ->
    bitcask:delete(State#vstate.db, <<Img/binary, Idx:32>>),
    {reply, {ok, ReqID}, State};

handle_command(_Message, _Sender, State) ->
    {noreply, State}.

handle_handoff_command(?FOLD_REQ{foldfun=Fun, acc0=Acc0}, _Sender, State) ->
    Acc = bitcask:fold(State#vstate.db, Fun, Acc0),
    {reply, Acc, State};

handle_handoff_command({get, _ReqID, _Vm} = Req, Sender, State) ->
    handle_command(Req, Sender, State);

handle_handoff_command(Req, Sender, State) ->
    S1 = case handle_command(Req, Sender, State) of
             {noreply, NewState} ->
                 NewState;
             {reply, _, NewState} ->
                 NewState
         end,
    {forward, S1}.

handoff_starting(_TargetNode, State) ->
    {true, State}.

handoff_cancelled(State) ->
    {ok, State}.

handoff_finished(_TargetNode, State) ->
    {ok, State}.

handle_handoff_data(Data, State) ->
    {Img, HObject} = binary_to_term(Data),
    put(State, Img, HObject),
    {reply, ok, State}.

encode_handoff_item(Img, Data) ->
    term_to_binary({Img, Data}).

is_empty(State) ->
    bitcask:fold_keys(State#vstate.db,
                      fun (_, _) ->
                              {false, State}
                      end, {true, State}).

delete(State) ->
    bitcask:fold_keys(State#vstate.db,
                      fun (#bitcask_entry{key=K}, _A) ->
                              bitcask:delete(State#vstate.db, K)
                      end, ok),
    {ok, State}.

handle_coverage(list, _KeySpaces, {_, ReqID, _}, State) ->
    List = bitcask:fold_keys(State#vstate.db,
                             fun (#bitcask_entry{key=K}, []) ->
                                     S = byte_size(K) - 4,
                                     <<K1:S/binary, _:32>> = K,
                                     [K1];
                                 (#bitcask_entry{key=K}, [F|R]) ->
                                     S = byte_size(K) - 4,
                                     case K of
                                         <<K1:S/binary, _:32>>
                                           when K1 =:= F ->
                                             [F|R];
                                         <<K1:S/binary, _:32>> ->
                                             [K1, F | R]
                                     end
                             end, []),
    {reply,
     {ok, ReqID, {State#vstate.partition,State#vstate.node}, List},
     State};

handle_coverage({list, Img}, _KeySpaces, {_, ReqID, _}, State) ->
    S = byte_size(Img),
    List = bitcask:fold_keys(State#vstate.db,
                             fun (#bitcask_entry{key = <<Img1:S/binary, Idx:32>>}, L) when Img1 =:= Img ->
                                     [Idx|L];
                                 (_, L) ->
                                     L
                             end, []),
    {reply,
     {ok, ReqID, {State#vstate.partition,State#vstate.node}, List},
     State};

handle_coverage({list, Img, true}, _KeySpaces, {_, ReqID, _}, State) ->
    S = byte_size(Img),
    Acc = bitcask:fold(
            State#vstate.db,
            fun(#bitcask_entry{key = K = <<Img1:S/binary, _>>}, V, L)
                  when Img1 =:= Img ->
                    O = term_to_binary(V),
                    [O#sniffle_obj{val={K, O#sniffle_obj.val}} | L];
               (_, _, L) ->
                    L
            end, []),
    {reply,
     {ok, ReqID, {State#vstate.partition, State#vstate.node}, Acc},
     State};

handle_coverage(_Req, _KeySpaces, _Sender, State) ->
    {stop, not_implemented, State}.

handle_exit(_Pid, _Reason, State) ->
    {noreply, State}.

terminate(_Reason,  State) ->
    bitcask:close(State#vstate.db),
    ok.

put(State, Key, Value) ->
    DB = State#vstate.db,
    Bin = term_to_binary(Value),
    R = bitcask:put(DB, Key, Bin),
    riak_core_aae_vnode:update_hashtree(<<"img">>, Key, Bin,
                                        State#vstate.hashtrees),
    %% This is a very ugly hack, but since we don't have
    %% much opperations on the image server we need to
    %% trigger the GC manually.
    erlang:garbage_collect(),
    case erlang:get(DB) of
        {bc_state,_,{filestate,_,_,_,P1,P2,_,_},_,_,_,_,_} ->
            erlang:garbage_collect(P1),
            erlang:garbage_collect(P2),
            R;
        _ ->
            R
    end.

get(DB, Key) ->
    case bitcask:get(DB, Key) of
        {ok, V} ->
            {ok, binary_to_term(V)};
        E ->
            E
    end.

%%%===================================================================
%%% AAE
%%%===================================================================

handle_info(retry_create_hashtree, State=#vstate{
                                            hashtrees=undefined,
                                            partition=Idx
                                           }) ->
    lager:debug("~p/~p retrying to create a hash tree.", [?SERVICE, Idx]),
    HT = riak_core_aae_vnode:maybe_create_hashtrees(?SERVICE,
                                                    State#vstate.partition,
                                                    ?MODULE,  undefined),
    {ok, State#vstate{hashtrees = HT}};
handle_info(retry_create_hashtree, State) ->
    {ok, State};
handle_info({'DOWN', _, _, Pid, _}, State=#vstate{
                                             hashtrees=Pid,
                                             partition=Idx
                                            }) ->
    lager:debug("~p/~p hashtree ~p went down.", [?SERVICE, Idx, Pid]),
    erlang:send_after(1000, self(), retry_create_hashtree),
    {ok, State#vstate{hashtrees = undefined}};
handle_info({'DOWN', _, _, _, _}, State) ->
    {ok, State};
handle_info(_, State) ->
    {ok, State}.
