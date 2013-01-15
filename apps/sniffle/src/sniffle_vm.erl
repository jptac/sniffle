-module(sniffle_vm).

-include("sniffle.hrl").

-export(
   [
    register/2,
    unregister/1,
    create/3,
    get/1,
    list/0,
    list/1,
    start/1,
    log/2,
    logs/1,
    stop/1,
    reboot/1,
    delete/1,
    snapshot/2,
    delete_snapshot/2,
    set/2,
    set/3
   ]
  ).


-ignore_xref([logs/1]).

-spec register(VM::fifo:uuid(), Hypervisor::binary()) -> ok.


register(Vm, Hypervisor) ->
    case sniffle_vm:get(Vm) of
        {ok, not_found} ->
            do_write(Vm, register, Hypervisor);
        {ok, _UserObj} ->
            set(Vm, <<"hypervisor">>, Hypervisor)
    end.

-spec unregister(VM::fifo:uuid()) -> ok.

unregister(Vm) ->
    case sniffle_vm:get(Vm) of
        {ok, V} ->
            lists:map(fun(N) ->
                              {ok, Net} = jsxd:get(<<"network">>, N),
                              {ok, Ip} = jsxd:get(<<"ip">>, N),
                              sniffle_iprange:release_ip(Net, Ip)
                      end,jsxd:get(<<"network_mappings">>, [], V));
        _ ->
            ok
    end,
    do_update(Vm, unregister).

-spec create(Package::binary(), Dataset::binary(), Config::fifo:config()) ->
                    {ok, fifo:uuid()}.

create(Package, Dataset, Config) ->
    UUID = list_to_binary(uuid:to_string(uuid:uuid4())),
    do_write(UUID, register, <<"pending">>), %we've to put pending here since undefined will cause a wrong call!
    sniffle_create_fsm:create(UUID, Package, Dataset, Config),
    {ok, UUID}.

-spec get(Vm::fifo:uuid()) ->
                 not_found |
                 {error, timeout} |
                 fifo:vm_config().

get(Vm) ->
    sniffle_entity_read_fsm:start(
      {sniffle_vm_vnode, sniffle_vm},
      get, Vm
     ).

-spec list() -> {error, timeout} | [fifo:uuid()].

list() ->
    sniffle_entity_coverage_fsm:start(
      {sniffle_vm_vnode, sniffle_vm},
      list
     ).

-spec list([fifo:matcher()]) -> {error, timeout} |[fifo:uuid()].

list(Requirements) ->
    sniffle_entity_coverage_fsm:start(
      {sniffle_vm_vnode, sniffle_vm},
      list, Requirements
     ).

-spec delete(Vm::fifo:uuid()) ->
                    {error, timeout} | not_found | ok.

delete(Vm) ->
    case sniffle_vm:get(Vm) of
        {error, timeout} ->
            {error, timeout};
        {ok, not_found} ->
            not_found;
        {ok, V} ->
            case jsxd:get(<<"hypervisor">>, V) of
                undefined ->
                    sniffle_vm:unregister(Vm),
                    libhowl:send(Vm, [{<<"event">>, <<"delete">>}]);
                {ok, <<"pending">>} ->
                    sniffle_vm:unregister(Vm),
                    libhowl:send(Vm, [{<<"event">>, <<"delete">>}]);
                {ok, H} ->
                    case jsxd:get(<<"state">>, V) of
                        undefined ->
                            not_found;
                        {ok, <<"deleting">>} ->
                            sniffle_vm:unregister(Vm),
                            libhowl:send(Vm, [{<<"event">>, <<"delete">>}]);
                        _ ->
                            set(Vm, <<"state">>, <<"deleting">>),
                            {Host, Port} = get_hypervisor(H),
                            libchunter:delete_machine(Host, Port, Vm)
                    end
            end,
            ok
    end.



-spec start(Vm::fifo:uuid()) ->
                   {error, timeout} | not_found | ok.

start(Vm) ->
    case sniffle_vm:get(Vm) of
        {error, timeout} ->
            {error, timeout};
        {ok, not_found} ->
            not_found;
        {ok, V} ->
            {ok, H} = jsxd:get(<<"hypervisor">>, V),
            {Server, Port} = get_hypervisor(H),
            libchunter:start_machine(Server, Port, Vm),
            ok
    end.

-spec stop(Vm::fifo:uuid()) ->
                  {error, timeout} | not_found | ok.

stop(Vm) ->
    case sniffle_vm:get(Vm) of
        {error, timeout} ->
            {error, timeout};
        {ok, not_found} ->
            not_found;
        {ok, V} ->
            {ok, H} = jsxd:get(<<"hypervisor">>, V),
            {Server, Port} = get_hypervisor(H),
            libchunter:stop_machine(Server, Port, Vm),
            ok
    end.

-spec reboot(Vm::fifo:uuid()) ->
                    {error, timeout} | not_found | ok.

reboot(Vm) ->
    case sniffle_vm:get(Vm) of
        {error, timeout} ->
            {error, timeout};
        {ok, not_found} ->
            not_found;
        {ok, V} ->
            {ok, H} = jsxd:get(<<"hypervisor">>, V),
            {Server, Port} = get_hypervisor(H),
            libchunter:reboot_machine(Server, Port, Vm),
            ok
    end.

-spec logs(Vm::fifo:uuid()) ->
                  not_found | [fifo:log()].

logs(Vm) ->
    case sniffle_vm:get(Vm) of
        {error, timeout} ->
            {error, timeout};
        {ok, not_found} ->
            not_found;
        {ok, V} ->
            {ok, jsxd:get(<<"log">>, [], V)}
    end.

-spec log(Vm::fifo:uuid(), Log::term()) ->
                 {error, timeout} | not_found | ok.

log(Vm, Log) ->
    {Mega,Sec,Micro} = erlang:now(),
    do_update(Vm, log, {(Mega*1000000+Sec)*1000000+Micro, Log}).


snapshot(Vm, Comment) ->
    case sniffle_vm:get(Vm) of
        {error, timeout} ->
            {error, timeout};
        {ok, not_found} ->
            not_found;
        {ok, V} ->
            {ok, H} = jsxd:get(<<"hypervisor">>, V),
            {Server, Port} = get_hypervisor(H),
            UUID = list_to_binary(uuid:to_string(uuid:uuid4())),
            {Mega,Sec,Micro} = erlang:now(),
            TimeStamp = (Mega*1000000+Sec)*1000000+Micro,
            case libchunter:snapshot(Server, Port, Vm, UUID) of
                {reply,ok} ->
                    do_write(Vm, set, [{[<<"snapshots">>, UUID, <<"timestamp">>], TimeStamp},
                                       {[<<"snapshots">>, UUID, <<"comment">>], Comment}]),
                    log(Vm, <<"Created snapshot ", UUID/binary, ": ", Comment/binary>>),
                    {ok, UUID};
                E ->
                    {error, E}
            end
    end.

delete_snapshot(Vm, UUID) ->
    case sniffle_vm:get(Vm) of
        {error, timeout} ->
            {error, timeout};
        {ok, not_found} ->
            not_found;
        {ok, V} ->
            case jsxd:get([<<"snapshots">>, UUID, <<"timestamp">>], V) of
                {ok, _} ->
                    {ok, Snapshots} = jsxd:get(<<"snapshots">>, V),
                    {ok, H} = jsxd:get(<<"hypervisor">>, V),
                    {Server, Port} = get_hypervisor(H),
                    case libchunter:delete_snapshot(Server, Port, Vm, UUID) of
                        {reply,ok} ->
                            Snapshots1 = jsxd:delete([<<"snapshots">>, UUID], Snapshots),
                            do_write(Vm, set, [{[<<"snapshots">>], Snapshots1}]),
                            log(Vm, <<"Deleted snapshot ", UUID/binary, ".">>),
                            ok;
                        E ->
                            {error, E}
                    end
            end
    end.


-spec set(Vm::fifo:uuid(), Attribute::binary(), Value::fifo:value()) ->
                 {error, timeout} | not_found | ok.

set(Vm, Attribute, Value) ->
    do_update(Vm, set, [{Attribute, Value}]).


-spec set(Vm::fifo:uuid(), Attributes::fifo:config_list()) ->
                 {error, timeout} | not_found | ok.

set(Vm, Attributes) ->
    do_update(Vm, set, Attributes).

%%%===================================================================
%%% Internal Functions
%%%===================================================================

-spec do_update(VM::fifo:uuid(), Op::atom()) -> not_found | ok.

do_update(VM, Op) ->
    case sniffle_vm:get(VM) of
        {error, timeout} ->
            {error, timeout};
        {ok, not_found} ->
            not_found;
        {ok, _Obj} ->
            do_write(VM, Op)
    end.

-spec do_update(VM::fifo:uuid(), Op::atom(), Val::term()) -> not_found | ok.

do_update(VM, Op, Val) ->
    case sniffle_vm:get(VM) of
        {error, timeout} ->
            {error, timeout};
        {ok, not_found} ->
            not_found;
        {ok, _Obj} ->
            do_write(VM, Op, Val)
    end.

-spec do_write(VM::fifo:uuid(), Op::atom()) -> not_found | ok.

do_write(VM, Op) ->
    sniffle_entity_write_fsm:write({sniffle_vm_vnode, sniffle_vm}, VM, Op).

-spec do_write(VM::fifo:uuid(), Op::atom(), Val::term()) -> not_found | ok.

do_write(VM, Op, Val) ->
    sniffle_entity_write_fsm:write({sniffle_vm_vnode, sniffle_vm}, VM, Op, Val).

get_hypervisor(Hypervisor) ->
    {ok, HypervisorObj} = sniffle_hypervisor:get(Hypervisor),
    {ok, Port} = jsxd:get(<<"port">>, HypervisorObj),
    {ok, Host} = jsxd:get(<<"host">>, HypervisorObj),
    {binary_to_list(Host), Port}.
