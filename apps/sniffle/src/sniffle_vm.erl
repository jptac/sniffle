-module(sniffle_vm).

-include("sniffle.hrl").

-export(
   [
    register/2,
    unregister/1,
    create/3,
    update/3,
    get/1,
    list/0,
    list/1,
    start/1,
    log/2,
    logs/1,
    stop/1,
    reboot/1,
    stop/2,
    reboot/2,
    delete/1,
    snapshot/2,
    delete_snapshot/2,
    rollback_snapshot/2,
    set/2,
    set/3
   ]
  ).

-ignore_xref([logs/1]).

update(Vm, Package, Config) ->
    case sniffle_vm:get(Vm) of
        {ok, V} ->
            {ok, Hypervisor} = jsxd:get(<<"hypervisor">>, V),
            {ok, HypervisorObj} = sniffle_hypervisor:get(Hypervisor),
            {ok, Port} = jsxd:get(<<"port">>, HypervisorObj),
            {ok, HostB} = jsxd:get(<<"host">>, HypervisorObj),
            Host = binary_to_list(HostB),
            {ok, OrigRam} = jsxd:get([<<"config">>, <<"ram">>], V),
            OrigPkg = jsxd:get(<<"package">>, <<"custom">>, V),
            case Package of
                undefined ->
                    libchunter:update_machine(Host, Port, Vm, [], Config);
                _ ->
                    case sniffle_package:get(Package) of
                        {ok, P} ->
                            {ok, NewRam} = jsxd:get(<<"ram">>, P),
                            case jsxd:get([<<"resources">>, <<"free-memory">>], HypervisorObj) of
                                {ok, Ram} when
                                      Ram > (NewRam - OrigRam) ->
                                    set(Vm, <<"package">>, Package),
                                    log(Vm, <<"Updating VM from package '",
                                              OrigPkg/binary, "' to '",
                                              Package/binary, "'.">>),
                                    libchunter:update_machine(Host, Port, Vm, P, Config);
                                _ ->
                                    {error, not_enough_resources}
                            end;
                        E2 ->
                            E2
                    end
            end;
        E ->
            E
    end.

-spec register(VM::fifo:uuid(), Hypervisor::binary()) -> ok.

register(Vm, Hypervisor) ->
    do_write(Vm, register, Hypervisor).

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
    do_write(Vm, unregister).

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
                            sniffle_vm:unregister(Vm),
                            libhowl:send(Vm, [{<<"event">>, <<"delete">>}]);
                        {ok, <<"deleting">>} ->
                            sniffle_vm:unregister(Vm),
                            libhowl:send(Vm, [{<<"event">>, <<"delete">>}]);
                        %% When the vm was in failed state it got never handed off to the hypervisor
                        {ok, <<"failed-", _/binary>>} ->
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
    stop(Vm, []).

-spec stop(Vm::fifo:uuid(), Options::[atom()|{atom(), term()}]) ->
                  {error, timeout} | not_found | ok.

stop(Vm, Options) ->
    case sniffle_vm:get(Vm) of
        {error, timeout} ->
            {error, timeout};
        {ok, not_found} ->
            not_found;
        {ok, V} ->
            {ok, H} = jsxd:get(<<"hypervisor">>, V),
            {Server, Port} = get_hypervisor(H),
            libchunter:stop_machine(Server, Port, Vm, Options),
            ok
    end.

-spec reboot(Vm::fifo:uuid()) ->
                    {error, timeout} | not_found | ok.
reboot(Vm) ->
    reboot(Vm, []).

-spec reboot(Vm::fifo:uuid(), Options::[atom()|{atom(), term()}]) ->
                    {error, timeout} | not_found | ok.

reboot(Vm, Options) ->
    case sniffle_vm:get(Vm) of
        {error, timeout} ->
            {error, timeout};
        {ok, not_found} ->
            not_found;
        {ok, V} ->
            {ok, H} = jsxd:get(<<"hypervisor">>, V),
            {Server, Port} = get_hypervisor(H),
            libchunter:reboot_machine(Server, Port, Vm, Options),
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
    Timestamp = (Mega*1000000+Sec)*1000000+Micro,
    case do_write(Vm, log, {Timestamp, Log}) of
        ok ->
            libhowl:send(Vm, [{<<"event">>, <<"log">>},
                              {<<"data">>,
                               [{<<"log">>, Log},
                                {<<"date">>, Timestamp}]}]),
            ok;
        R ->
            R
    end.

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
                            Snapshots1 = jsxd:delete(UUID, Snapshots),
                            do_write(Vm, set, [{[<<"snapshots">>], Snapshots1}]),
                            log(Vm, <<"Deleted snapshot ", UUID/binary, ".">>),
                            ok;
                        E ->
                            {error, E}
                    end;
                undefined ->
                    {error, not_found}
            end
    end.

rollback_snapshot(Vm, UUID) ->
    case sniffle_vm:get(Vm) of
        {error, timeout} ->
            {error, timeout};
        {ok, not_found} ->
            not_found;
        {ok, V} ->
            case jsxd:get(<<"state">>, V) of
                {ok, <<"stopped">>} ->
                    case jsxd:get([<<"snapshots">>, UUID, <<"timestamp">>], V) of
                        {ok, T} ->
                            {ok, H} = jsxd:get(<<"hypervisor">>, V),
                            {Server, Port} = get_hypervisor(H),
                            case libchunter:rollback_snapshot(Server, Port, Vm, UUID) of
                                {reply,ok} ->
                                    Snapshots1 = jsxd:fold(fun (SUUID, Sn, A) ->
                                                                   case jsxd:get(<<"timestamp">>, 0, Sn) of
                                                                       X when X > T ->
                                                                           A;
                                                                       _ ->
                                                                           jsxd:set(SUUID, Sn, A)
                                                      end
                                                           end, [], jsxd:get(<<"snapshots">>, [], V)),
                                    do_write(Vm, set, [{[<<"snapshots">>], Snapshots1}]),
                                    ok;
                                E ->
                                    {error, E}
                            end;
                        undefined ->
                            {error, not_found}
                    end;
                {ok, State} ->
                    log(Vm, <<"Not rolled back since state is ", State/binary, ".">>),
                    {error, not_stopped}
            end
    end.

-spec set(Vm::fifo:uuid(), Attribute::binary(), Value::fifo:value()) ->
                 {error, timeout} | not_found | ok.

set(Vm, Attribute, Value) ->
    do_write(Vm, set, [{Attribute, Value}]).


-spec set(Vm::fifo:uuid(), Attributes::fifo:config_list()) ->
                 {error, timeout} | not_found | ok.

set(Vm, Attributes) ->
    do_write(Vm, set, Attributes).

%%%===================================================================
%%% Internal Functions
%%%===================================================================

-spec do_write(VM::fifo:uuid(), Op::atom()) -> not_found | ok.

do_write(VM, Op) ->
    case sniffle_entity_write_fsm:write({sniffle_vm_vnode, sniffle_vm}, VM, Op) of
        {ok, not_found} ->
            not_found;
        R ->
            R
    end.

-spec do_write(VM::fifo:uuid(), Op::atom(), Val::term()) -> not_found | ok.

do_write(VM, Op, Val) ->
    case sniffle_entity_write_fsm:write({sniffle_vm_vnode, sniffle_vm}, VM, Op, Val) of
        {ok, not_found} ->
            not_found;
        R ->
            R
    end.

get_hypervisor(Hypervisor) ->
    {ok, HypervisorObj} = sniffle_hypervisor:get(Hypervisor),
    {ok, Port} = jsxd:get(<<"port">>, HypervisorObj),
    {ok, Host} = jsxd:get(<<"host">>, HypervisorObj),
    {binary_to_list(Host), Port}.
