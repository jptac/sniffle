-module(sniffle_img_vnode).
-behaviour(riak_core_vnode).
-include("sniffle.hrl").
-include_lib("riak_core/include/riak_core_vnode.hrl").

-export([
         repair/4,
         get/3,
         list/2,
         list/3,
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
%%% API - coverage
%%%===================================================================

list(Preflist, ReqID) ->
    riak_core_vnode_master:coverage(
      {list, ReqID},
      Preflist,
      all,
      {fsm, undefined, self()},
      ?MASTER).

list(Preflist, ReqID, Img) ->
    riak_core_vnode_master:coverage(
      {list, ReqID, Img},
      Preflist,
      all,
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
                                   ?MASTER).

%%%===================================================================
%%% VNode
%%%===================================================================

init([Partition]) ->
    DB = list_to_atom(integer_to_list(Partition)),
    sniffle_db:start(DB),
    {ok, #state{
       db = DB,
       partition = Partition,
       node = node()}}.

handle_command(ping, _Sender, State) ->
    {reply, {pong, State#state.partition}, State};

handle_command({repair, {Img, Idx}, VClock, Obj}, _Sender, State) ->
    case sniffle_db:get(State#state.db, <<"img">>, <<Img/binary, Idx:32>>) of
        {ok, #sniffle_obj{vclock = VC1}} when VC1 =:= VClock ->
            estatsd:increment("sniffle.imgs.readrepair.success"),
            sniffle_db:put(State#state.db, <<"img">>, Img, Obj);
        not_found ->
            estatsd:increment("sniffle.imgs.readrepair.success"),
            sniffle_db:put(State#state.db, <<"img">>, Img, Obj);
        _ ->
            estatsd:increment("sniffle.imgs.readrepair.failed"),
            lager:error("[imgs] Read repair failed, data was updated too recent.")
    end,
    {noreply, State};

handle_command({get, ReqID, {Img, Idx}}, _Sender, State) ->
    Res = case sniffle_db:get(State#state.db, <<"img">>, <<Img/binary, Idx:32>>) of
              {ok, R} ->
                  estatsd:increment("sniffle.imgs.read.success"),
                  R;
              not_found ->
                  estatsd:increment("sniffle.imgs.read.failed"),
                  not_found
          end,
    NodeIdx = {State#state.partition, State#state.node},
    {reply, {ok, ReqID, NodeIdx, Res}, State};

handle_command({create, {ReqID, Coordinator}, {Img, Idx}, Data},
               _Sender, State) ->
    I0 = statebox:new(fun sniffle_img_state:new/0),
    I1 = statebox:modify({fun sniffle_img_state:data/2, [Data]}, I0),
    VC0 = vclock:fresh(),
    VC = vclock:increment(Coordinator, VC0),
    HObject = #sniffle_obj{val=I1, vclock=VC},
    sniffle_db:put(State#state.db, <<"img">>, <<Img/binary, Idx:32>>, HObject),
    {reply, {ok, ReqID}, State};

handle_command({delete, {ReqID, _Coordinator}, {Img, Idx}}, _Sender, State) ->
    sniffle_db:delete(State#state.db, <<"img">>, <<Img/binary, Idx:32>>),
    {reply, {ok, ReqID}, State};

handle_command(Message, _Sender, State) ->
    ?PRINT({unhandled_command, Message}),
    {noreply, State}.

handle_handoff_command(?FOLD_REQ{foldfun=Fun, acc0=Acc0}, _Sender, State) ->
    Acc = sniffle_db:fold(State#state.db, <<"img">>, Fun, Acc0),
    {reply, Acc, State}.

handoff_starting(_TargetNode, State) ->
    estatsd:increment("sniffle.imgs.handoff.start"),
    {true, State}.

handoff_cancelled(State) ->
    estatsd:increment("sniffle.imgs.handoff.cancelled"),
    {ok, State}.

handoff_finished(_TargetNode, State) ->
    estatsd:increment("sniffle.imgs.handoff.finished"),
    {ok, State}.

handle_handoff_data(Data, State) ->
    {Img, HObject} = binary_to_term(Data),
    sniffle_db:put(State#state.db, <<"img">>, Img, HObject),
    {reply, ok, State}.

encode_handoff_item(Img, Data) ->
    term_to_binary({Img, Data}).

is_empty(State) ->
    sniffle_db:fold(State#state.db,
                    <<"img">>,
                    fun (_, _, _) ->
                            {false, State}
                    end, {true, State}).

delete(State) ->
    Trans = sniffle_db:fold(State#state.db,
                            <<"img">>,
                            fun (K,_, A) ->
                                    [{delete, K} | A]
                            end, []),
    sniffle_db:transact(State#state.db, Trans),
    {ok, State}.

handle_coverage({list, ReqID}, _KeySpaces, _Sender, State) ->
    estatsd:increment("sniffle.imgs.list"),
    List = sniffle_db:fold(State#state.db,
                           <<"img">>,
                           fun (K, _, []) ->
                                   S = bit_size(K) - 32,
                                   <<K1:S/binary, _:32>> = K,
                                   [K1];
                               (K, _, [F|R]) ->
                                   S = bit_size(K) - 32,
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

handle_coverage({list, ReqID, Img}, _KeySpaces, _Sender, State) ->
    estatsd:increment("sniffle.imgs.list"),
    List = sniffle_db:fold(State#state.db,
                          <<"img", Img/binary>>,
                           fun (<<Idx:32>>, _, L) ->
                                   [Idx|L]
                           end, []),
    {reply,
     {ok, ReqID, {State#state.partition,State#state.node}, List},
     State};

handle_coverage(_Req, _KeySpaces, _Sender, State) ->
    {stop, not_implemented, State}.

handle_exit(_Pid, _Reason, State) ->
    {noreply, State}.

terminate(_Reason,  _State) ->
    ok.
