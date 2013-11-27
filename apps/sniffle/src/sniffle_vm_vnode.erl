-module(sniffle_vm_vnode).
-behaviour(riak_core_vnode).
-behaviour(riak_core_aae_vnode).
-include("sniffle.hrl").
-include_lib("riak_core/include/riak_core_vnode.hrl").

-export([repair/4,
         get/3,
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
         handle_exit/3,
         handle_info/2]).

-export([
         master/0,
         aae_repair/2,
         hashtree_pid/1,
         rehash/3,
         hash_object/2,
         request_hashtree_pid/1
        ]).

%% those functions do not get called directly.
-ignore_xref([
              get/3,
              log/4,
              register/4,
              repair/4,
              set/4,
              start_vnode/1,
              unregister/3,
              handle_info/2
             ]).

-define(SERVICE, sniffle_vm).

-define(MASTER, sniffle_vm_vnode_master).

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
    sniffle_vm:get(Key).

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
    riak_core_vnode_master:command(Preflist,
                                   {get, ReqID, Vm},
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
    sniffle_vnode:init(Partition, <<"vm">>, ?SERVICE).

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

%%%===================================================================
%%% Node Specific
%%%===================================================================

handle_command({register, {ReqID, Coordinator}, Vm, Hypervisor}, _Sender, State) ->
    HObject = case fifo_db:get(State#vstate.db, <<"vm">>, Vm) of
                  not_found ->
                      H0 = statebox:new(fun sniffle_vm_state:new/0),
                      H1 = statebox:modify({fun sniffle_vm_state:uuid/2, [Vm]}, H0),
                      H2 = statebox:modify({fun sniffle_vm_state:hypervisor/2, [Hypervisor]}, H1),
                      VC0 = vclock:fresh(),
                      VC = vclock:increment(Coordinator, VC0),
                      #sniffle_obj{val=H2, vclock=VC};
                  {ok, #sniffle_obj{val=H0} = O} ->
                      H1 = statebox:modify({fun sniffle_vm_state:load/1,[]}, H0),
                      H2 = statebox:modify({fun sniffle_vm_state:hypervisor/2, [Hypervisor]}, H1),
                      H3 = statebox:expire(?STATEBOX_EXPIRE, H2),
                      sniffle_obj:update(H3, Coordinator, O)
              end,
    sniffle_vnode:put(Vm, HObject, State),
    {reply, {ok, ReqID}, State};

handle_command({unregister, {ReqID, _Coordinator}, Vm}, _Sender, State) ->
    fifo_db:delete(State#vstate.db, <<"vm">>, Vm),
    riak_core_index_hashtree:delete({<<"vm">>, Vm}, State#vstate.hashtrees),
    {reply, {ok, ReqID}, State};

handle_command({set,
                {ReqID, Coordinator}, Vm,
                Resources}, _Sender, State) ->
    case fifo_db:get(State#vstate.db, <<"vm">>, Vm) of
        {ok, #sniffle_obj{val=H0} = O} ->
            H1 = statebox:modify({fun sniffle_vm_state:load/1,[]}, H0),
            H2 = lists:foldr(
                   fun ({Resource, Value}, H) ->
                           statebox:modify(
                             {fun sniffle_vm_state:set/3,
                              [Resource, Value]}, H)
                   end, H1, Resources),
            H3 = statebox:expire(?STATEBOX_EXPIRE, H2),
            Obj = sniffle_obj:update(H3, Coordinator, O),
            sniffle_vnode:put(Vm, Obj, State),
            {reply, {ok, ReqID}, State};
        R ->
            lager:error("[vms] tried to write to a non existing vm: ~p", [R]),
            {reply, {ok, ReqID, not_found}, State}
    end;

handle_command({log,
                {ReqID, Coordinator}, Vm,
                {Time, Log}}, _Sender, State) ->
    case fifo_db:get(State#vstate.db, <<"vm">>, Vm) of
        {ok, #sniffle_obj{val=H0} = O} ->
            H1 = statebox:modify({fun sniffle_vm_state:load/1,[]}, H0),
            H2 = statebox:modify({fun sniffle_vm_state:log/3, [Time, Log]}, H1),
            H3 = statebox:expire(?STATEBOX_EXPIRE, H2),
            Obj = sniffle_obj:update(H3, Coordinator, O),
            sniffle_vnode:put(Vm, Obj, State),
            {reply, {ok, ReqID}, State};
        R ->
            lager:error("[vms] tried to write to a non existing vm: ~p", [R]),
            {reply, {ok, ReqID, not_found}, State}
    end;

%%%===================================================================
%%% Generic
%%%===================================================================

handle_command(Message, Sender, State) ->
    sniffle_vnode:handle_command(Message, Sender, State).

handle_handoff_command(?FOLD_REQ{foldfun=Fun, acc0=Acc0}, _Sender, State) ->
    Acc = fifo_db:fold(State#vstate.db, <<"vm">>, Fun, Acc0),
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
    {Vm, HObject} = binary_to_term(Data),
    sniffle_vnode:put(Vm, HObject, State),
    {reply, ok, State}.

encode_handoff_item(Vm, Data) ->
    term_to_binary({Vm, Data}).

is_empty(State) ->
    sniffle_vnode:is_empty(State).

delete(State) ->
    sniffle_vnode:delete(State).

handle_coverage(list, _KeySpaces, Sender, State) ->
    sniffle_vnode:list_keys(Sender, State);

handle_coverage({list, Requirements}, _KeySpaces, Sender, State) ->
    Getter = fun(#sniffle_obj{val=S0}, Resource) ->
                     jsxd:get(Resource, 0, statebox:value(S0))
             end,
    sniffle_vnode:list_keys(Getter, Requirements, Sender, State);

handle_coverage(_Req, _KeySpaces, _Sender, State) ->
    {stop, not_implemented, State}.

handle_exit(_Pid, _Reason, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

%%%===================================================================
%%% AAE
%%%===================================================================

handle_info(Msg, State) ->
    sniffle_vnode:handle_info(Msg, State).
