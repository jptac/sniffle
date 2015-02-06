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
         handle_info/2,
         sync_repair/4,
         set_network_map/4,
         set_backup/4,
         set_snapshot/4,
         set_service/4,
         set_config/4,
         set_info/4,
         set_metadata/4,
         add_grouping/4,
         remove_grouping/4,
         add_fw_rule/4,
         delete_fw_rule/4,
         state/4,
         alias/4,
         owner/4,
         dataset/4,
         package/4,
         hypervisor/4
        ]).

-export([
         master/0,
         aae_repair/2,
         hash_object/2
        ]).

%% those functions do not get called directly.
-ignore_xref([
              set_network_map/4,
              set_backup/4,
              set_config/4,
              set_snapshot/4,
              set_service/4,
              set_metadata/4,
              add_grouping/4,
              set_info/4,
              remove_grouping/4,
              add_fw_rule/4,
              delete_fw_rule/4,
              state/4,
              alias/4,
              owner/4,
              dataset/4,
              package/4,
              hypervisor/4,

              get/3,
              log/4,
              register/4,
              repair/4,
              set/4,
              start_vnode/1,
              unregister/3,
              handle_info/2,
              sync_repair/4
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

sync_repair(Preflist, ReqID, UUID, Obj) ->
    riak_core_vnode_master:command(Preflist,
                                   {sync_repair, ReqID, UUID, Obj},
                                   {fsm, undefined, self()},
                                   ?MASTER).

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
                                   {delete, ReqID, Vm},
                                   {fsm, undefined, self()},
                                   ?MASTER).

set_network_map(Preflist, ReqID, Vm, [IP, Net]) ->
    riak_core_vnode_master:command(Preflist,
                                   {set_network_map, ReqID, Vm, IP, Net},
                                   {fsm, undefined, self()},
                                   ?MASTER).

-define(S(Field),
        Field(Preflist, ReqID, Vm, Val) ->
               riak_core_vnode_master:command(Preflist,
                                              {Field, ReqID, Vm, Val},
                                              {fsm, undefined, self()},
                                              ?MASTER)).

?S(set_service).
?S(set_backup).
?S(set_snapshot).
?S(set_config).
?S(set_info).
?S(set_metadata).
?S(add_fw_rule).
?S(delete_fw_rule).

add_grouping(Preflist, ReqID, Vm, Grouping) ->
    riak_core_vnode_master:command(Preflist,
                                   {add_grouping, ReqID, Vm, Grouping},
                                   {fsm, undefined, self()},
                                   ?MASTER).

remove_grouping(Preflist, ReqID, Vm, Grouping) ->
    riak_core_vnode_master:command(Preflist,
                                   {remove_grouping, ReqID, Vm, Grouping},
                                   {fsm, undefined, self()},
                                   ?MASTER).
?S(state).
?S(alias).
?S(owner).
?S(dataset).
?S(package).
?S(hypervisor).


-spec set(any(), any(), fifo:uuid(), [{Key::binary(), V::fifo:value()}]) -> ok.

set(Preflist, ReqID, Vm, Data) ->
    riak_core_vnode_master:command(Preflist,
                                   {set, ReqID, Vm, Data},
                                   {fsm, undefined, self()},
                                   ?MASTER).

%%%===================================================================
%%% VNode
%%%===================================================================

init([Part]) ->
    sniffle_vnode:init(Part, <<"vm">>, ?SERVICE, ?MODULE, ft_vm).

-type vm_command() ::
        ping |
        {repair, Vm::fifo:uuid(), Obj::any()} |
        {get, ReqID::any(), Vm::fifo:uuid()} |
        {get, ReqID::any(), Vm::fifo:uuid()} |
        {log, ReqID::any(), Vm::fifo:uuid(), Log::fifo:log()} |
        {register, {ReqID::any(), Coordinator::any()}, Vm::fifo:uuid(), Hypervisor::binary()} |
        {delete, {ReqID::any(), _Coordinator::any()}, Vm::fifo:uuid()} |
        {set,
         {ReqID::any(), Coordinator::any()}, Vm::fifo:uuid(),
         Resources::[{Key::binary(), Value::fifo:value()}]}.

-spec handle_command(vm_command(), any(), any()) ->
                            {reply, any(), any()} |
                            {noreply, any()}.

%%%===================================================================
%%% Node Specific
%%%===================================================================

handle_command({register, {ReqID, Coordinator}=ID, Vm, Hypervisor}, _Sender, State) ->
    HObject = case fifo_db:get(State#vstate.db, <<"vm">>, Vm) of
                  not_found ->
                      H0 = ft_vm:new(ID),
                      H1 = ft_vm:uuid(ID, Vm, H0),
                      H2 = ft_vm:hypervisor(ID, Hypervisor, H1),
                      ft_obj:new(H2, Coordinator);
                  {ok, O} ->
                      H0 = ft_obj:val(O),
                      H1 = ft_vm:load(ID, H0),
                      H2 = ft_vm:hypervisor(ID, Hypervisor, H1),
                      ft_obj:update(H2, Coordinator, O)
              end,
    sniffle_vnode:put(Vm, HObject, State),
    {reply, {ok, ReqID}, State};

handle_command({log,
                {ReqID, Coordinator}=ID, Vm,
                {Time, Log}}, _Sender, State) ->
    case fifo_db:get(State#vstate.db, <<"vm">>, Vm) of
        {ok, O} ->
            H0 = ft_obj:val(O),
            H1 = ft_vm:load(ID, H0),
            H2 = ft_vm:log(ID, Time, Log, H1),
            Obj = ft_obj:update(H2, Coordinator, O),
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
    sniffle_vnode:repair(Data, State).

encode_handoff_item(Vm, Data) ->
    term_to_binary({Vm, Data}).

is_empty(State) ->
    sniffle_vnode:is_empty(State).

delete(State) ->
    sniffle_vnode:delete(State).

handle_coverage(Req, KeySpaces, Sender, State) ->
    sniffle_vnode:handle_coverage(Req, KeySpaces, Sender, State).

handle_exit(_Pid, _Reason, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

%%%===================================================================
%%% AAE
%%%===================================================================

handle_info(Msg, State) ->
    sniffle_vnode:handle_info(Msg, State).
