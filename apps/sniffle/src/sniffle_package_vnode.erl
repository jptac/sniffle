-module(sniffle_package_vnode).
-behaviour(riak_core_vnode).
-include("sniffle.hrl").
-include_lib("riak_core/include/riak_core_vnode.hrl").

-export([
         repair/4,
         get/3,
         list/2,
         list/3,
         create/4,
         lookup/3,
         delete/3,
         set/4
        ]).

-export([start_vnode/1,
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
         handle_exit/3]).

-record(state, {
          partition,
          node
         }).

-ignore_xref([
              create/4,
              delete/3,
              get/3,
              lookup/3,
              list/2,
              list/3,
              repair/4,
              set/4,
              start_vnode/1
             ]).

-define(MASTER, sniffle_package_vnode_master).

%%%===================================================================
%%% API
%%%===================================================================

start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

repair(IdxNode, Package, VClock, Obj) ->
    riak_core_vnode_master:command(IdxNode,
                                   {repair, Package, VClock, Obj},
                                   ignore,
                                   ?MASTER).

%%%===================================================================
%%% API - reads
%%%===================================================================

get(Preflist, ReqID, Package) ->
    ?PRINT({get, Preflist, ReqID, Package}),
    riak_core_vnode_master:command(Preflist,
                                   {get, ReqID, Package},
                                   {fsm, undefined, self()},
                                   ?MASTER).

lookup(Preflist, ReqID, Name) ->
    riak_core_vnode_master:coverage(
      {lookup, ReqID, Name},
      Preflist,
      all,
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

create(Preflist, ReqID, UUID, Data) ->
    riak_core_vnode_master:command(Preflist,
                                   {create, ReqID, UUID, Data},
                                   {fsm, undefined, self()},
                                   ?MASTER).

delete(Preflist, ReqID, Package) ->
    riak_core_vnode_master:command(Preflist,
                                   {delete, ReqID, Package},
                                   {fsm, undefined, self()},
                                   ?MASTER).

set(Preflist, ReqID, Vm, Data) ->
    riak_core_vnode_master:command(Preflist,
                                   {set, ReqID, Vm, Data},
                                   {fsm, undefined, self()},
                                   ?MASTER).

%%%===================================================================
%%% VNode
%%%===================================================================

init([Partition]) ->
    sniffle_db:start(Partition),
    {ok, #state{
       partition = Partition,
       node = node()
      }}.

handle_command(ping, _Sender, State) ->
    {reply, {pong, State#state.partition}, State};

handle_command({repair, Package, VClock, Obj}, _Sender, State) ->
    case sniffle_db:get(State#state.partition, <<"package">>, Package) of
        {ok, #sniffle_obj{vclock = VC1}} when VC1 =:= VClock ->
            estatsd:increment("sniffle.packages.readrepair.success"),
            sniffle_db:put(State#state.partition, <<"package">>, Package, Obj);
        not_found ->
            estatsd:increment("sniffle.packages.readrepair.success"),
            sniffle_db:put(State#state.partition, <<"package">>, Package, Obj);
        _ ->
            estatsd:increment("sniffle.packages.readrepair.failed"),
            lager:error("[packages] Read repair failed, data was updated too recent.")
    end,
    {noreply, State};

handle_command({get, ReqID, Package}, _Sender, State) ->
    Res = case sniffle_db:get(State#state.partition, <<"package">>, Package) of
              {ok, R} ->
                  estatsd:increment("sniffle.packages.readrepair.success"),
                  R;
              not_found ->
                  estatsd:increment("sniffle.packages.readrepair.failed"),
                  not_found
          end,
    NodeIdx = {State#state.partition, State#state.node},
    {reply, {ok, ReqID, NodeIdx, Res}, State};

handle_command({create, {ReqID, Coordinator}, UUID, [Package]},
               _Sender, State) ->
    estatsd:increment("sniffle.packages.create"),
    I0 = statebox:new(fun sniffle_package_state:new/0),
    I1 = statebox:modify({fun sniffle_package_state:uuid/2, [UUID]}, I0),
    I2 = statebox:modify({fun sniffle_package_state:name/2, [Package]}, I1),
    VC0 = vclock:fresh(),
    VC = vclock:increment(Coordinator, VC0),
    HObject = #sniffle_obj{val=I2, vclock=VC},
    sniffle_db:put(State#state.partition, <<"package">>, UUID, HObject),
    {reply, {ok, ReqID}, State};

handle_command({delete, {ReqID, _Coordinator}, Package}, _Sender, State) ->
    estatsd:increment("sniffle.packages.delete"),
    sniffle_db:delete(State#state.partition, <<"package">>, Package),
    {reply, {ok, ReqID}, State};

handle_command({set,
                {ReqID, Coordinator}, Package,
                Resources}, _Sender, State) ->
    case sniffle_db:get(State#state.partition, <<"package">>, Package) of
        {ok, #sniffle_obj{val=H0} = O} ->
            estatsd:increment("sniffle.packages.write.success"),
            H1 = statebox:modify({fun sniffle_package_state:load/1,[]}, H0),
            H2 = lists:foldr(
                   fun ({Resource, Value}, H) ->
                           statebox:modify(
                             {fun sniffle_package_state:set/3,
                              [Resource, Value]}, H)
                   end, H1, Resources),
            H3 = statebox:expire(?STATEBOX_EXPIRE, H2),
            sniffle_db:put(State#state.partition, <<"package">>, Package,
                           sniffle_obj:update(H3, Coordinator, O));
        _ ->
            estatsd:increment("sniffle.packages.write.failed"),
            lager:error("[packages] tried to write to a non existing package.")
    end,
    {reply, {ok, ReqID}, State};

handle_command(Message, _Sender, State) ->
    estatsd:increment("sniffle.packages.unknown_command"),
    lager:error("[packages] Unknown command: ~p", [Message]),
    {noreply, State}.

handle_handoff_command(?FOLD_REQ{foldfun=Fun, acc0=Acc0}, _Sender, State) ->
    Acc = sniffle_db:fold(State#state.partition,
                          <<"package">>, Fun, Acc0),
    {reply, Acc, State}.

handoff_starting(_TargetNode, State) ->
    estatsd:increment("sniffle.packages.handoff.started"),
    {true, State}.

handoff_cancelled(State) ->
    estatsd:increment("sniffle.packages.handoff.cancelled"),
    {ok, State}.

handoff_finished(_TargetNode, State) ->
    estatsd:increment("sniffle.packages.handoff.finished"),
    {ok, State}.

handle_handoff_data(Data, State) ->
    {Package, HObject} = binary_to_term(Data),
    sniffle_db:put(State#state.partition, <<"package">>, Package, HObject),
    {reply, ok, State}.

encode_handoff_item(Package, Data) ->
    term_to_binary({Package, Data}).

is_empty(State) ->
    sniffle_db:fold(State#state.partition,
                    <<"package">>,
                    fun (_,_, _) ->
                            {true, State}
                    end, {false, State}).

delete(State) ->
    sniffle_db:fold(State#state.partition,
                    <<"package">>,
                    fun (K,_, _) ->
                            sniffle_db:delete(State#state.partition, <<"dataset">>, K)
                    end, ok),
    {ok, State}.

handle_coverage({lookup, ReqID, Name}, _KeySpaces, _Sender, State) ->
    estatsd:increment("sniffle.vms.lookup"),
    Res = sniffle_db:fold(State#state.partition,
                          <<"package">>,
                          fun (_U, #sniffle_obj{val=SB}, Res) ->
                                  V = statebox:value(SB),
                                  case jsxd:get(<<"name">>, V) of
                                      {ok, Name} ->
                                          V;
                                      _ ->
                                          Res
                                  end
                          end, not_found),
    {reply,
     {ok, ReqID, {State#state.partition,State#state.node}, [Res]},
     State};

handle_coverage({list, ReqID}, _KeySpaces, _Sender, State) ->
    estatsd:increment("sniffle.vms.list"),
    List = sniffle_db:fold(State#state.partition,
                          <<"package">>,
                           fun (K, _, L) ->
                                   [K|L]
                           end, []),

    {reply,
     {ok, ReqID, {State#state.partition,State#state.node}, List},
     State};

handle_coverage({list, ReqID, Requirements}, _KeySpaces, _Sender, State) ->
    estatsd:increment("sniffle.vms.select"),
    Getter = fun(#sniffle_obj{val=S0}, Resource) ->
                     jsxd:get(Resource, 0, statebox:value(S0))
             end,
    List = sniffle_db:fold(State#state.partition,
                          <<"package">>,
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

terminate(_Reason, _State) ->
    ok.
