-module(sniffle_img_vnode).
-behaviour(riak_core_vnode).
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
         handle_exit/3
        ]).

-record(state, {
          db,
          partition,
          node
         }).

-ignore_xref([
              create/4,
              delete/3,
              get/3,
              list/2,
              list/3,
              repair/4,
              start_vnode/1
             ]).

-define(MASTER, sniffle_img_vnode_master).

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
    riak_core_vnode_master:command(Preflist,
                                   {get, ReqID, {Img, Idx}},
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
    {ok, DBLoc} = application:get_env(sniffle, db_path),
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
            put(State#state.db, Img, Obj);
        not_found ->
            put(State#state.db, Img,  Obj);
        _ ->
            lager:error("[imgs] Read repair failed, data was updated too recent.")
    end,
    {noreply, State};

handle_command({get, ReqID, {Img, Idx}}, _Sender, State) ->
    Res = case get(State#state.db, <<Img/binary, Idx:32>>) of
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
    put(State#state.db, <<Img/binary, Idx:32>>, HObject),
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
    put(State#state.db, Img, HObject),
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

put(DB, Key, Value) ->
    R = bitcask:put(DB, Key, term_to_binary(Value)),
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
