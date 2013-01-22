-module(sniffle_vm_vnode).
-behaviour(riak_core_vnode).
-include("sniffle.hrl").
-include_lib("riak_core/include/riak_core_vnode.hrl").

-export([repair/4,
         get/3,
         list/2,
         list/3,
         log/4,
         register/4,
         unregister/3,
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

%% those functions do not get called directly.
-ignore_xref([
              get/3,
              list/2,
              list/3,
              log/4,
              register/4,
              repair/4,
              set/4,
              start_vnode/1,
              unregister/3
             ]).


-define(MASTER, sniffle_vm_vnode_master).

%%%===================================================================
%%% API
%%%===================================================================

start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

repair(IdxNode, Vm, VClock, Obj) ->
    riak_core_vnode_master:command(IdxNode,
                                   {repair, Vm, VClock, Obj},
                                   ignore,
                                   ?MASTER).

%%%===================================================================
%%% API - reads
%%%===================================================================

-spec get(any(), any(), Vm::fifo:uuid()) -> ok.

get(Preflist, ReqID, Vm) ->
    ?PRINT({get, Preflist, ReqID, Vm}),
    riak_core_vnode_master:command(Preflist,
                                   {get, ReqID, Vm},
                                   {fsm, undefined, self()},
                                   ?MASTER).

%%%===================================================================
%%% API - coverage
%%%===================================================================

-spec list(any(), any()) -> ok.

list(Preflist, ReqID) ->
    riak_core_vnode_master:coverage(
      {list, ReqID},
      Preflist,
      all,
      {fsm, undefined, self()},
      ?MASTER).

-spec list(any(), any(), [fifo:matcher()]) -> ok.

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

-spec register(any(), any(), fifo:uuid(), binary()) -> ok.

register(Preflist, ReqID, Vm, Hypervisor) ->
    riak_core_vnode_master:command(Preflist,
                                   {register, ReqID, Vm, Hypervisor},
                                   {fsm, undefined, self()},
                                   ?MASTER).

log(Preflist, ReqID, Vm, Log) ->
    riak_core_vnode_master:command(Preflist,
                                   {log, ReqID, Vm, Log},
                                   {fsm, undefined, self()},
                                   ?MASTER).

-spec unregister(any(), any(), fifo:uuid()) -> ok.

unregister(Preflist, ReqID, Vm) ->
    riak_core_vnode_master:command(Preflist,
                                   {unregister, ReqID, Vm},
                                   {fsm, undefined, self()},
                                   ?MASTER).


-spec set(any(), any(), fifo:uuid(), [{Key::binary(), V::fifo:value()}]) -> ok.

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

-type vm_command() ::
        ping |
        {repair, Vm::fifo:uuid(), Obj::any()} |
        {get, ReqID::any(), Vm::fifo:uuid()} |
        {get, ReqID::any(), Vm::fifo:uuid()} |
        {log, ReqID::any(), Vm::fifo:uuid(), Log::fifo:log()} |
        {register, {ReqID::any(), Coordinator::any()}, Vm::fifo:uuid(), Hypervisor::binary()} |
        {unregister, {ReqID::any(), _Coordinator::any()}, Vm::fifo:uuid()} |
        {set,
         {ReqID::any(), Coordinator::any()}, Vm::fifo:uuid(),
         Resources::[{Key::binary(), Value::fifo:value()}]}.

-spec handle_command(vm_command(), any(), any()) ->
                            {reply, any(), any()} |
                            {noreply, any()}.

handle_command(ping, _Sender, State) ->
    {reply, {pong, State#state.partition}, State};

handle_command({repair, Vm, VClock, Obj}, _Sender, State) ->
    case sniffle_db:get(State#state.partition, <<"vm">>, Vm) of
        {ok, #sniffle_obj{vclock = VC1}} when VC1 =:= VClock ->
            estatsd:increment("sniffle.vms.readrepair.success"),
            sniffle_db:put(State#state.partition, <<"vm">>, Vm, Obj);
        not_found ->
            estatsd:increment("sniffle.vms.readrepair.success"),
            sniffle_db:put(State#state.partition, <<"vm">>, Vm, Obj);
        _ ->
            estatsd:increment("sniffle.vms.readrepair.failed"),
            lager:error("[vms] Read repair failed, data was updated too recent.")
    end,
    {noreply, State};

handle_command({get, ReqID, Vm}, _Sender, State) ->
    Res = case sniffle_db:get(State#state.partition, <<"vm">>, Vm) of
              {ok, R} ->
                  estatsd:increment("sniffle.vms.read.success"),
                  R;
              not_found ->
                  estatsd:increment("sniffle.vms.read.failed"),
                  not_found
          end,
    NodeIdx = {State#state.partition, State#state.node},
    {reply, {ok, ReqID, NodeIdx, Res}, State};

handle_command({register, {ReqID, Coordinator}, Vm, Hypervisor}, _Sender, State) ->
    HObject = case sniffle_db:get(State#state.partition, <<"vm">>, Vm) of
                  not_found ->
                      estatsd:increment("sniffle.vms.register"),
                      H0 = statebox:new(fun sniffle_vm_state:new/0),
                      H1 = statebox:modify({fun sniffle_vm_state:uuid/2, [Vm]}, H0),
                      H2 = statebox:modify({fun sniffle_vm_state:hypervisor/2, [Hypervisor]}, H1),
                      VC0 = vclock:fresh(),
                      VC = vclock:increment(Coordinator, VC0),
                      #sniffle_obj{val=H2, vclock=VC};
                  {ok, #sniffle_obj{val=H0} = O} ->
                      estatsd:increment("sniffle.vms.write.success"),
                      H1 = statebox:modify({fun sniffle_vm_state:load/1,[]}, H0),
                      H2 = statebox:modify({fun sniffle_vm_state:hypervisor/3, [Coordinator, Log]}, H1),
                      H3 = statebox:expire(?STATEBOX_EXPIRE, H2),
                      sniffle_obj:update(H3, Coordinator, O)
              end,
    sniffle_db:put(State#state.partition, <<"vm">>, Vm, HObject),
    {reply, {ok, ReqID}, State};

handle_command({unregister, {ReqID, _Coordinator}, Vm}, _Sender, State) ->
    estatsd:increment("sniffle.vms.unregister"),
    sniffle_db:delete(State#state.partition, <<"vm">>, Vm),
    {reply, {ok, ReqID}, State};

handle_command({set,
                {ReqID, Coordinator}, Vm,
                Resources}, _Sender, State) ->
    case sniffle_db:get(State#state.partition, <<"vm">>, Vm) of
        {ok, #sniffle_obj{val=H0} = O} ->
            estatsd:increment("sniffle.vms.write.success"),
            H1 = statebox:modify({fun sniffle_vm_state:load/1,[]}, H0),
            H2 = lists:foldr(
                   fun ({Resource, Value}, H) ->
                           statebox:modify(
                             {fun sniffle_vm_state:set/3,
                              [Resource, Value]}, H)
                   end, H1, Resources),
            H3 = statebox:expire(?STATEBOX_EXPIRE, H2),
            sniffle_db:put(State#state.partition, <<"vm">>, Vm,
                           sniffle_obj:update(H3, Coordinator, O)),
            {reply, {ok, ReqID}, State};
        R ->
            estatsd:increment("sniffle.vms.write.failed"),
            lager:error("[vms] tried to write to a non existing vm: ~p", [R]),
            {reply, {ok, ReqID, not_found}, State}
    end;

handle_command({log,
                {ReqID, Coordinator}, Vm,
                {Time, Log}}, _Sender, State) ->
    case sniffle_db:get(State#state.partition, <<"vm">>, Vm) of
        {ok, #sniffle_obj{val=H0} = O} ->
            estatsd:increment("sniffle.vms.write.success"),
            H1 = statebox:modify({fun sniffle_vm_state:load/1,[]}, H0),
            H2 = statebox:modify({fun sniffle_vm_state:log/3, [Time, Log]}, H1),
            H3 = statebox:expire(?STATEBOX_EXPIRE, H2),
            sniffle_db:put(State#state.partition, <<"vm">>, Vm,
                           sniffle_obj:update(H3, Coordinator, O)),
            {reply, {ok, ReqID}, State};
        R ->
            estatsd:increment("sniffle.vms.write.failed"),
            lager:error("[vms] tried to write to a non existing vm: ~p", [R]),
            {reply, {ok, ReqID, not_found}, State}
    end;

handle_command(Message, _Sender, State) ->
    estatsd:increment("sniffle.vms.unknown_command"),
    lager:error("[vms] Unknown command: ~p", [Message]),
    {noreply, State}.

handle_handoff_command(?FOLD_REQ{foldfun=Fun, acc0=Acc0}, _Sender, State) ->
    Acc = sniffle_db:fold(State#state.partition, <<"vm">>, Fun, Acc0),
    {reply, Acc, State}.

handoff_starting(_TargetNode, State) ->
    estatsd:increment("sniffle.vms.handoff.started"),
    {true, State}.

handoff_cancelled(State) ->
    estatsd:increment("sniffle.vms.handoff.cancled"),
    {ok, State}.

handoff_finished(_TargetNode, State) ->
    estatsd:increment("sniffle.vms.handoff.finished"),
    {ok, State}.

handle_handoff_data(Data, State) ->
    {Vm, HObject} = binary_to_term(Data),
    sniffle_db:put(State#state.partition, <<"vm">>, Vm, HObject),
    {reply, ok, State}.

encode_handoff_item(Vm, Data) ->
    term_to_binary({Vm, Data}).

is_empty(State) ->
    sniffle_db:fold(State#state.partition,
                    <<"vm">>,
                    fun (_,_, _) ->
                            {true, State}
                    end, {false, State}).

delete(State) ->
    sniffle_db:fold(State#state.partition,
                    <<"dataset">>,
                    fun (K,_, _) ->
                            sniffle_db:delete(State#state.partition, <<"dataset">>, K)
                    end, ok),
    {ok, State}.

handle_coverage({list, ReqID}, _KeySpaces, _Sender, State) ->
    estatsd:increment("sniffle.vms.list"),
    List = sniffle_db:fold(State#state.partition,
                          <<"vm">>,
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
                          <<"vm">>,
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
