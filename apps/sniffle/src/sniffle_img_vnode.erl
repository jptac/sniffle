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
         hashtree_pid/1,
         rehash/3,
         hash_object/2,
         request_hashtree_pid/1
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

-record(state, {db, partition, node, hashtrees}).

-type state() :: #state{}.

-define(DEFAULT_HASHTREE_TOKENS, 90).

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

hashtree_pid(Partition) ->
    riak_core_vnode_master:sync_command({Partition, node()},
                                        {hashtree_pid, node()},
                                        ?MASTER,
                                        infinity).

%% Asynchronous version of {@link hashtree_pid/1} that sends a message back to
%% the calling process. Used by the {@link riak_core_entropy_manager}.
request_hashtree_pid(Partition) ->
    ReqId = {hashtree_pid, Partition},
    riak_core_vnode_master:command({Partition, node()},
                                   {hashtree_pid, node()},
                                   {raw, ReqId, self()},
                                   ?MASTER).

%% Used by {@link riak_core_exchange_fsm} to force a vnode to update the hashtree
%% for repaired keys. Typically, repairing keys will trigger read repair that
%% will update the AAE hash in the write path. However, if the AAE tree is
%% divergent from the KV data, it is possible that AAE will try to repair keys
%% that do not have divergent KV replicas. In that case, read repair is never
%% triggered. Always rehashing keys after any attempt at repair ensures that
%% AAE does not try to repair the same non-divergent keys over and over.
rehash(Preflist, _, Key) ->
    riak_core_vnode_master:command(Preflist,
                                   {rehash, Key},
                                   ignore,
                                   ?MASTER).

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
    {ok, #state{
            db = DB,
            partition = Partition,
            node = node()}}.

handle_command(ping, _Sender, State) ->
    {reply, {pong, State#state.partition}, State};

handle_command({repair, {Img, Idx}, VClock, Obj}, _Sender, State) ->
    case get(State#state.db, <<Img/binary, Idx:32>>) of
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

handle_command({hashtree_pid, Node}, _, State=#state{hashtrees=HT}) ->
    %% Handle riak_core request forwarding during ownership handoff.
    case node() of
        Node ->
            %% Following is necessary in cases where anti-entropy was enabled
            %% after the vnode was already running
            case HT of
                undefined ->
                    State2 = maybe_create_hashtrees(State),
                    {reply, {ok, State2#state.hashtrees}, State2};
                _ ->
                    {reply, {ok, HT}, State}
            end;
        _ ->
            {reply, {error, wrong_node}, State}
    end;

handle_command({rehash, Key}, _, State=#state{db=DB}) ->
    case fifo_db:get(DB, <<"img">>, Key) of
        {ok, Term} ->
            update_hashtree(Key, term_to_binary(Term), State);
        _ ->
            %% Make sure hashtree isn't tracking deleted data
            riak_core_index_hashtree:delete({<<"img">>, Key}, State#state.hashtrees)
    end,
    {noreply, State};

handle_command(?FOLD_REQ{foldfun=Fun, acc0=Acc0}, _Sender, State) ->
    lager:debug("Fold on ~p", [State#state.partition]),
    Acc = fifo_db:fold(State#state.db, <<"img">>,
                       fun(K, V, O) ->
                               Fun({<<"img">>, K}, V, O)
                       end, Acc0),
    {reply, Acc, State};

%%%===================================================================
%%% General
%%%===================================================================

handle_command({get, ReqID, ImgAndIdx}, _Sender, State) ->
    Res = case get(State#state.db, ImgAndIdx) of
              {ok, R} ->
                  R;
              not_found ->
                  not_found
          end,
    NodeIdx = {State#state.partition, State#state.node},
    {reply, {ok, ReqID, NodeIdx, Res}, State};

handle_command({create, {ReqID, Coordinator}, {Img, Idx}, Data},
               _Sender, State) ->
    I0 = statebox:new(fun sniffle_img_state:new/0),
    I1 = statebox:modify({fun sniffle_img_state:data/2, [Data]}, I0),
    I2 = statebox:truncate(0, I1),
    VC0 = vclock:fresh(),
    VC = vclock:increment(Coordinator, VC0),
    HObject = #sniffle_obj{val=I2, vclock=VC},
    put(State, <<Img/binary, Idx:32>>, HObject),
    {reply, {ok, ReqID}, State};

handle_command({delete, {ReqID, _Coordinator}, {Img, Idx}}, _Sender, State) ->
    bitcask:delete(State#state.db, <<Img/binary, Idx:32>>),
    {reply, {ok, ReqID}, State};

handle_command(_Message, _Sender, State) ->
    {noreply, State}.

handle_handoff_command(?FOLD_REQ{foldfun=Fun, acc0=Acc0}, _Sender, State) ->
    Acc = bitcask:fold(State#state.db, Fun, Acc0),
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
    bitcask:fold_keys(State#state.db,
                      fun (_, _) ->
                              {false, State}
                      end, {true, State}).

delete(State) ->
    bitcask:fold_keys(State#state.db,
                      fun (#bitcask_entry{key=K}, _A) ->
                              bitcask:delete(State#state.db, K)
                      end, ok),
    {ok, State}.

handle_coverage(list, _KeySpaces, {_, ReqID, _}, State) ->
    List = bitcask:fold_keys(State#state.db,
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
     {ok, ReqID, {State#state.partition,State#state.node}, List},
     State};

handle_coverage({list, Img}, _KeySpaces, {_, ReqID, _}, State) ->
    S = byte_size(Img),
    List = bitcask:fold_keys(State#state.db,
                             fun (#bitcask_entry{key = <<Img1:S/binary, Idx:32>>}, L) when Img1 =:= Img ->
                                     [Idx|L];
                                 (_, L) ->
                                     L
                             end, []),
    {reply,
     {ok, ReqID, {State#state.partition,State#state.node}, List},
     State};

handle_coverage(_Req, _KeySpaces, _Sender, State) ->
    {stop, not_implemented, State}.

handle_exit(_Pid, _Reason, State) ->
    {noreply, State}.

terminate(_Reason,  State) ->
    bitcask:close(State#state.db),
    ok.

put(State, Key, Value) ->
    DB = State#state.db,
    Bin = term_to_binary(Value),
    R = bitcask:put(DB, Key, Bin),
    update_hashtree(Key, Bin, State),

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

handle_info(retry_create_hashtree, State=#state{hashtrees=undefined}) ->
    State2 = maybe_create_hashtrees(State),
    case State2#state.hashtrees of
        undefined ->
            ok;
        _ ->
            lager:info("riak_core/~p: successfully started index_hashtree on retry",
                       [State#state.partition])
    end,
    {ok, State2};
handle_info(retry_create_hashtree, State) ->
    {ok, State};
handle_info({'DOWN', _, _, Pid, _}, State=#state{hashtrees=Pid}) ->
    State2 = State#state{hashtrees=undefined},
    State3 = maybe_create_hashtrees(State2),
    {ok, State3};
handle_info({'DOWN', _, _, _, _}, State) ->
    {ok, State};
handle_info(_, State) ->
    {ok, State}.

-spec maybe_create_hashtrees(state()) -> state().
maybe_create_hashtrees(State) ->
    maybe_create_hashtrees(riak_core_entropy_manager:enabled(), State).

-spec maybe_create_hashtrees(boolean(), state()) -> state().
maybe_create_hashtrees(false, State) ->
    lager:info("sniffle_img: Hashtree not enabled."),
    State;

maybe_create_hashtrees(true, State=#state{partition=Index}) ->
    %% Only maintain a hashtree if a primary vnode
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    lager:debug("sniffle_img/~p: creating hashtree.", [Index]),
    case riak_core_ring:vnode_type(Ring, Index) of
        primary ->
            RP = riak_core_util:responsible_preflists(Index),
            case riak_core_index_hashtree:start(sniffle_img, Index, RP, self(),
                                                ?MODULE) of
                {ok, Trees} ->
                    lager:debug("sniffle_img/~p: hashtree created: ~p.", [Index, Trees]),
                    monitor(process, Trees),
                    State#state{hashtrees=Trees};
                Error ->
                    lager:info("sniffle_img/~p: unable to start index_hashtree: ~p",
                               [Index, Error]),
                    erlang:send_after(1000, self(), retry_create_hashtree),
                    State#state{hashtrees=undefined}
            end;
        _ ->
            lager:debug("sniffle_img/~p: not primary", [Index]),
            State
    end.

-spec update_hashtree(binary(), binary(), state()) -> ok.
update_hashtree(Key, Val, #state{hashtrees=Trees}) ->
    case get_hashtree_token() of
        true ->
            riak_core_index_hashtree:async_insert_object({<<"img">>, Key}, Val, Trees),
            ok;
        false ->
            riak_core_index_hashtree:insert_object({<<"img">>, Key}, Val, Trees),
            put(hashtree_tokens, max_hashtree_tokens()),
            ok
    end.

get_hashtree_token() ->
    Tokens = get(hashtree_tokens),
    case Tokens of
        undefined ->
            put(hashtree_tokens, max_hashtree_tokens() - 1),
            true;
        N when N > 0 ->
            put(hashtree_tokens, Tokens - 1),
            true;
        _ ->
            false
    end.

-spec max_hashtree_tokens() -> pos_integer().
max_hashtree_tokens() ->
    app_helper:get_env(riak_core,
                       anti_entropy_max_async,
                       ?DEFAULT_HASHTREE_TOKENS).
