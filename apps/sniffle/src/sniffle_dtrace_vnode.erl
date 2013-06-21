-module(sniffle_dtrace_vnode).
-behaviour(riak_core_vnode).
-include("sniffle.hrl").
-include_lib("riak_core/include/riak_core_vnode.hrl").

-export([
         repair/4,
         get/3,
         list/2,
         list/3,
         create/4,
         delete/3,
         set/4
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
              set/4,
              start_vnode/1
             ]).

-define(MASTER, sniffle_dtrace_vnode_master).

%%%===================================================================
%%% API
%%%===================================================================

start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

repair(IdxNode, Dtrace, VClock, Obj) ->
    riak_core_vnode_master:command([IdxNode],
                                   {repair, Dtrace, VClock, Obj},
                                   ?MASTER).

%%%===================================================================
%%% API - reads
%%%===================================================================

get(Preflist, ReqID, Dtrace) ->
    riak_core_vnode_master:command(Preflist,
                                   {get, ReqID, Dtrace},
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

list(Preflist, ReqID, Requirements) ->
    riak_core_vnode_master:coverage(
      {list, ReqID, Requirements},
      Preflist,
      all,
      {fsm, undefined, self()},
      ?MASTER).

%%%===================================================================
%%% API - writes
%%%===================================================================

create(Preflist, ReqID, Dtrace, Data) ->
    riak_core_vnode_master:command(Preflist,
                                   {create, ReqID, Dtrace, Data},
                                   {fsm, undefined, self()},
                                   ?MASTER).

delete(Preflist, ReqID, Dtrace) ->
    riak_core_vnode_master:command(Preflist,
                                   {delete, ReqID, Dtrace},
                                   {fsm, undefined, self()},
                                   ?MASTER).
set(Preflist, ReqID, Dtrace, Data) ->
    riak_core_vnode_master:command(Preflist,
                                   {set, ReqID, Dtrace, Data},
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

handle_command({repair, Dtrace, VClock, Obj}, _Sender, State) ->
    case sniffle_db:get(State#state.db, <<"dtrace">>, Dtrace) of
        {ok, #sniffle_obj{vclock = VC1}} when VC1 =:= VClock ->
            sniffle_db:put(State#state.db, <<"dtrace">>, Dtrace, Obj);
        not_found ->
            sniffle_db:put(State#state.db, <<"dtrace">>, Dtrace, Obj);
        _ ->
            lager:error("[dtraces] Read repair failed, data was updated too recent.")
    end,
    {noreply, State};

handle_command({get, ReqID, Dtrace}, _Sender, State) ->
    Res = case sniffle_db:get(State#state.db, <<"dtrace">>, Dtrace) of
              {ok, R} ->
                  R;
              not_found ->
                  not_found
          end,
    NodeIdx = {State#state.partition, State#state.node},
    {reply, {ok, ReqID, NodeIdx, Res}, State};

handle_command({create, {ReqID, Coordinator}, ID, [Name, Script]},
               _Sender, State) ->
    I0 = statebox:new(fun sniffle_dtrace_state:new/0),
    I1 = statebox:modify({fun sniffle_dtrace_state:set/3, [<<"uuid">>, ID]}, I0),
    I2 = statebox:modify({fun sniffle_dtrace_state:set/3, [<<"name">>, Name]}, I1),
    I3 = statebox:modify({fun sniffle_dtrace_state:set/3, [<<"script">>, Script]}, I2),
    VC0 = vclock:fresh(),
    VC = vclock:increment(Coordinator, VC0),
    HObject = #sniffle_obj{val=I3, vclock=VC},
    sniffle_db:put(State#state.db, <<"dtrace">>, ID, HObject),
    {reply, {ok, ReqID}, State};

handle_command({delete, {ReqID, _Coordinator}, Dtrace}, _Sender, State) ->
    sniffle_db:delete(State#state.db, <<"dtrace">>, Dtrace),
    {reply, {ok, ReqID}, State};

handle_command({set,
                {ReqID, Coordinator}, Dtrace,
                Resources}, _Sender, State) ->
    case sniffle_db:get(State#state.db, <<"dtrace">>, Dtrace) of
        {ok, #sniffle_obj{val=H0} = O} ->
            H1 = statebox:modify({fun sniffle_dtrace_state:load/1,[]}, H0),
            H2 = lists:foldr(
                   fun ({Resource, Value}, H) ->
                           statebox:modify(
                             {fun sniffle_dtrace_state:set/3,
                              [Resource, Value]}, H)
                   end, H1, Resources),
            H3 = statebox:expire(?STATEBOX_EXPIRE, H2),
            sniffle_db:put(State#state.db, <<"dtrace">>, Dtrace,
                           sniffle_obj:update(H3, Coordinator, O)),
            {reply, {ok, ReqID}, State};
        R ->
            lager:error("[dtraces] tried to write to a non existing dtrace: ~p", [R]),
            {reply, {ok, ReqID, not_found}, State}
    end;

handle_command(_Message, _Sender, State) ->
    {noreply, State}.

handle_handoff_command(?FOLD_REQ{foldfun=Fun, acc0=Acc0}, _Sender, State) ->
    Acc = sniffle_db:fold(State#state.db, <<"dtrace">>, Fun, Acc0),
    {reply, Acc, State}.

handoff_starting(_TargetNode, State) ->
    {true, State}.

handoff_cancelled(State) ->
    {ok, State}.

handoff_finished(_TargetNode, State) ->
    {ok, State}.

handle_handoff_data(Data, State) ->
    {Dtrace, HObject} = binary_to_term(Data),
    sniffle_db:put(State#state.db, <<"dtrace">>, Dtrace, HObject),
    {reply, ok, State}.

encode_handoff_item(Dtrace, Data) ->
    term_to_binary({Dtrace, Data}).

is_empty(State) ->
    sniffle_db:fold(State#state.db,
                    <<"dtrace">>,
                    fun (_, _, _) ->
                            {false, State}
                    end, {true, State}).

delete(State) ->
    Trans = sniffle_db:fold(State#state.db,
                            <<"dtrace">>,
                            fun (K,_, A) ->
                                    [{delete, K} | A]
                            end, []),
    sniffle_db:transact(State#state.db, Trans),
    {ok, State}.

handle_coverage({list, ReqID}, _KeySpaces, _Sender, State) ->
    List = sniffle_db:fold(State#state.db,
                          <<"dtrace">>,
                           fun (K, _, L) ->
                                   [K|L]
                           end, []),
    {reply,
     {ok, ReqID, {State#state.partition,State#state.node}, List},
     State};

handle_coverage({list, ReqID, Requirements}, _KeySpaces, _Sender, State) ->
    Getter = fun(#sniffle_obj{val=S0}, Resource) ->
                     jsxd:get(Resource, 0, statebox:value(S0))
             end,
    List = sniffle_db:fold(State#state.db,
                          <<"dtrace">>,
                           fun (Key, E, C) ->
                                   case sniffle_matcher:match(E, Getter, Requirements) of
                                       false ->
                                           C;
                                       Pts ->
                                           [{Key, Pts} | C]
                                   end
                           end, []),
    {reply,
     {ok, ReqID, {State#state.partition, State#state.node}, List},
     State};

handle_coverage(_Req, _KeySpaces, _Sender, State) ->
    {stop, not_implemented, State}.

handle_exit(_Pid, _Reason, State) ->
    {noreply, State}.

terminate(_Reason,  _State) ->
    ok.
