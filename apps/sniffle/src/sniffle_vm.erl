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
    get_attribute/2,
    get_attribute/1,
    set_attribute/2,
    set_attribute/3
   ]
  ).

-spec register(VM::fifo:uuid(), Hypervisor::binary()) -> ok.

register(Vm, Hypervisor) ->
    do_write(Vm, register, Hypervisor).

-spec unregister(VM::fifo:uuid()) -> ok.

unregister(Vm) ->
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
            case V#vm.hypervisor of
                <<"pending">> ->
                    sniffle_vm:unregister(Vm);
                undefined ->
                    sniffle_vm:unregister(Vm);
                H ->
                    case dict:find(<<"state">>, V#vm.attributes) of
                        error ->
                            not_found;
                        {ok, <<"deleting">>} ->
                            sniffle_vm:unregister(Vm);
                        _ ->
                            {ok, #hypervisor{host = Server, port = Port}} = sniffle_hypervisor:get(H),
                            set_attribute(Vm, <<"state">>, <<"deleting">>),
                            libchunter:delete_machine(Server, Port, Vm)
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
            {ok, #hypervisor{host = Server, port = Port}} = sniffle_hypervisor:get(V#vm.hypervisor),
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
            {ok, #hypervisor{host = Server, port = Port}} = sniffle_hypervisor:get(V#vm.hypervisor),
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
            {ok, #hypervisor{host = Server, port = Port}} = sniffle_hypervisor:get(V#vm.hypervisor),
            libchunter:reboot_machine(Server, Port, Vm),
            ok
    end.

-spec get_attribute(Vm::fifo:uuid()) ->
                           not_found | fifo:vm_config().

get_attribute(Vm) ->
    case sniffle_vm:get(Vm) of
        {error, timeout} ->
            {error, timeout};
        {ok, not_found} ->
            not_found;
        {ok, V} ->
            {ok, dict:to_list(V#vm.attributes)}
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
            {ok, V#vm.log}
    end.

-spec log(Vm::fifo:uuid(), Log::term()) ->
                 {error, timeout} | not_found | ok.

log(Vm, Log) ->
    do_update(Vm, log, {now(), Log}).

-spec get_attribute(Vm::fifo:uuid(), Attribute::binary()) ->
                           {error, timeout} | not_found | fifo:value().

get_attribute(Vm, Attribute) ->
    case sniffle_vm:get(Vm) of
        {error, timeout} ->
            {error, timeout};
        {ok, not_found} ->
            not_found;
        {ok, V} ->
            case dict:find(Attribute, V#vm.attributes) of
                error ->
                    not_found;
                Result ->
                    Result
            end
    end.

-spec set_attribute(Vm::fifo:uuid(), Attribute::binary(), Value::fifo:value()) ->
                           {error, timeout} | not_found | ok.

set_attribute(Vm, Attribute, Value) ->
    do_update(Vm, set_attribute, [{Attribute, Value}]).


-spec set_attribute(Vm::fifo:uuid(), Attributes::fifo:config_list()) ->
                           {error, timeout} | not_found | ok.

set_attribute(Vm, Attributes) ->
    do_update(Vm, set_attribute, Attributes).

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
