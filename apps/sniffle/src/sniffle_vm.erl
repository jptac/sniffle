-module(sniffle_vm).

-include("sniffle.hrl").

-export([
         register/2,
         unregister/1,
         create/3,
         update/3,
         list/0,
         list/1,
         get/1,
         log/2,
         logs/1,
         set/2,
         set/3,
         start/1,
         stop/1,
         stop/2,
         reboot/1,
         reboot/2,
         delete/1,
         snapshot/2,
         delete_snapshot/2,
         rollback_snapshot/2,
         commit_snapshot_rollback/2,
         promote_to_image/3,
         remove_nic/2,
         add_nic/2,
         primary_nic/2,
         set_owner/2,
         create_backup/4,
         restore_backup/2
        ]).

-ignore_xref([logs/1]).

-type backup_opts() ::
        delete |
        {delete, parent} |
        xml.

-spec restore_backup(Vm::fifo:uuid(), Snap::fifo:uuid()) ->
                            not_found |
                            {error, not_supported} |
                            {error, nopath}.
restore_backup(Vm, Snap) ->
    case sniffle_vm:get(Vm) of
        {ok, V} ->
            {ok, H} = jsxd:get(<<"hypervisor">>, V),
            {Server, Port} = get_hypervisor(H),

            case jsxd:get(V, [<<"backups">>, Snap]) of
                {ok, _} ->
                    case sniffle_s3:config(snapshot) of
                        error ->
                            {error, not_supported};
                        {ok, {S3Host, S3Port, AKey, SKey, Bucket}} ->
                            libchunter:restore_backup(Server, Port, Vm, Snap,
                                                      S3Host, S3Port, Bucket,
                                                      AKey, SKey)
                    end;
                _ ->
                    not_found
            end;
        _ ->
            not_found
    end.

-spec create_backup(Vm::fifo:uuid(), Type::full | incremental,
                    Comment::binary(), Opts::[backup_opts()]) ->
                           not_found |
                           {error, no_parent} |
                           {error, timeout} |
                           {error, not_supported} |
                           {ok, fifo:uuid()}.
create_backup(Vm, full, Comment, Opts) ->
    case sniffle_vm:get(Vm) of
        {ok, V} ->
            do_snap(Vm, V, Comment, Opts);
        _ ->
            not_found
    end;

create_backup(Vm, incremental, Comment, Opts) ->
    case sniffle_vm:get(Vm) of
        {ok, V} ->
            Parent = proplists:get_value(parent, Opts),
            case jsxd:get(V, [<<"backups">>, Parent, <<"local">>]) of
                {ok, true} ->
                    do_snap(Vm, V, Comment, Opts);
                _ ->
                    {error, parent}
            end;
        _ ->
            not_found
    end.

do_snap(Vm, V, Comment, Opts) ->
    UUID = uuid:uuid4s(),
    Opts1 = [create | Opts],
    {ok, H} = jsxd:get(<<"hypervisor">>, V),
    {Server, Port} = get_hypervisor(H),
    case sniffle_s3:config(snapshot) of
        error ->
            {error, not_supported};
        {ok, {S3Host, S3Port, AKey, SKey, Bucket}} ->
            case libchunter:backup(Server, Port, Vm, UUID,
                                   S3Host, S3Port, Bucket, AKey,
                                   SKey, Bucket, Opts1) of
                ok ->
                    C = [{[<<"backups">>, UUID, <<"comment">>], Comment},
                         {[<<"backups">>, UUID, <<"timestamp">>], timestamp()}],
                    sniffle_vm:set(Vm, C),
                    {ok, UUID};
                E ->
                    E
            end
    end.

promote_to_image(Vm, SnapID, Config) ->
    case sniffle_vm:get(Vm) of
        {ok, V} ->
            case jsxd:get([<<"snapshots">>, SnapID, <<"timestamp">>], V) of
                {ok, _} ->
                    {ok, H} = jsxd:get(<<"hypervisor">>, V),
                    {Server, Port} = get_hypervisor(H),
                    Img = uuid:uuid4s(),
                    {ok, C} = jsxd:get(<<"config">>, V),
                    Config1 = jsxd:select([<<"name">>, <<"version">>, <<"os">>, <<"description">>],
                                          jsxd:from_list(Config)),
                    {ok, Nets} = jsxd:get([<<"networks">>], C),
                    Nets1 =
                        jsxd:map(fun (Idx, E) ->
                                         Name = io_lib:format("net~p", [Idx]),
                                         [{<<"description">>, jsxd:get(<<"tag">>, <<"undefined">>, E)},
                                          {<<"name">>, list_to_binary(Name)}]
                                 end, Nets),
                    Config2 =
                        jsxd:thread([{set, <<"type">>, jsxd:get([<<"type">>], <<"zone">>, C)},
                                     {set, <<"dataset">>, Img},
                                     {set, <<"networks">>, Nets1}], Config1),
                    Config3 =
                        case jsxd:get(<<"type">>, Config2) of
                            {ok, <<"zone">>} ->
                                Config2;
                            _ ->
                                ND = case jsxd:get(<<"nic_driver">>, Config) of
                                         {ok, ND0} ->
                                             ND0;
                                         _ ->
                                             jsxd:get([<<"networks">>, 0, model], <<"virtio">>, C)
                                     end,
                                DD = case jsxd:get(<<"disk_driver">>, Config)  of
                                         {ok, DD0} ->
                                             DD0;
                                         _ ->
                                             jsxd:get([<<"disks">>, 0, model], <<"virtio">>, C)
                                     end,
                                jsxd:thread([{set, <<"disk_driver">>, DD},
                                             {set, <<"nic_driver">>, ND}],
                                            Config2)
                        end,
                    ok = sniffle_dataset:create(Img),
                    sniffle_dataset:set(Img, Config3),
                    ok = libchunter:store_snapshot(Server, Port, Vm, SnapID, Img),
                    {ok, Img};
                undefined ->
                    not_found
            end;
        E ->
            E
    end.

add_nic(Vm, Network) ->
    lager:info("[NIC ADD] Adding a new nic in ~s to ~s", [Network, Vm]),
    case sniffle_vm:get(Vm) of
        {ok, V} ->
            lager:info("[NIC ADD] VM found.", []),
            {ok, H} = jsxd:get(<<"hypervisor">>, V),
            {ok, HypervisorObj} = sniffle_hypervisor:get(H),
            {ok, Port} = jsxd:get(<<"port">>, HypervisorObj),
            {ok, HostB} = jsxd:get(<<"host">>, HypervisorObj),
            HypervisorsNetwork = jsxd:get([<<"networks">>], [], HypervisorObj),
            Server = binary_to_list(HostB),
            libchunter:ping(Server, Port),
            case jsxd:get(<<"state">>, V) of
                {ok, <<"stopped">>} ->
                    Requirements = [{must, oneof, <<"tag">>, HypervisorsNetwork}],
                    lager:info("[NIC ADD] Checking requirements: ~p.", [Requirements]),
                    case sniffle_network:claim_ip(Network, Requirements) of
                        {ok, IPRange, {Tag, IP, Net, Gw}} ->
                            {ok, Range} = sniffle_iprange:get(IPRange),
                            IPb = sniffle_iprange_state:to_bin(IP),
                            Netb = sniffle_iprange_state:to_bin(Net),
                            GWb = sniffle_iprange_state:to_bin(Gw),

                            NicSpec0 =
                                jsxd:from_list([{<<"ip">>, IPb},
                                                {<<"gateway">>, GWb},
                                                {<<"netmask">>, Netb},
                                                {<<"nic_tag">>, Tag }]),
                            NicSpec1 =
                                case jsxd:get([<<"config">>, <<"networks">>], V) of
                                    {ok, [_|_]} ->
                                        NicSpec0;
                                    _ ->
                                        jsxd:set([<<"primary">>], true, NicSpec0)
                                end,
                            NicSpec2 =
                                case jsxd:get(<<"vlan">>, 0, Range) of
                                    0 ->
                                        eplugin:apply(
                                          'vm:ip_assigned',
                                          [Vm, update, <<"unknown">>, Tag, IPb, Netb, GWb, none]),
                                        NicSpec1;
                                    VLAN ->
                                        eplugin:apply(
                                          'vm:ip_assigned',
                                          [Vm, update, <<"unknown">>, Tag, IPb, Netb, GWb, VLAN]),
                                        jsxd:set(<<"vlan_id">>, VLAN, NicSpec1)
                                end,
                            UR = [{<<"add_nics">>, [NicSpec2]}],
                            ok = libchunter:update_machine(Server, Port, Vm, [], UR),
                            M = [{<<"network">>, IPRange},
                                 {<<"ip">>, IP}],
                            Ms1= case jsxd:get([<<"network_mappings">>], V) of
                                     {ok, Ms} ->
                                         [M | Ms];
                                     _ ->
                                         [M]
                                 end,
                            sniffle_vm:set(Vm, [<<"network_mappings">>], Ms1);
                        E ->
                            lager:error("Could not get claim new IP: ~p for ~p ~p",
                                        [E, Network, Requirements]),
                            {error, claim_failed}
                    end;
                E ->
                    lager:error("VM needs to be stoppped: ~p", [E]),
                    {error, not_stopped}
            end;
        E ->
            lager:error("Could not get new IP - could not get VM: ~p", [E]),
            E
    end.

remove_nic(Vm, Mac) ->
    case sniffle_vm:get(Vm) of
        {ok, V} ->
            NicMap = make_nic_map(V),
            case jsxd:get(Mac, NicMap) of
                {ok, Idx}  ->
                    {ok, H} = jsxd:get(<<"hypervisor">>, V),
                    {Server, Port} = get_hypervisor(H),
                    libchunter:ping(Server, Port),
                    case jsxd:get(<<"state">>, V) of
                        {ok, <<"stopped">>} ->
                            UR = [{<<"remove_nics">>, [Mac]}],
                            {ok, IpStr} = jsxd:get([<<"config">>, <<"networks">>, Idx, <<"ip">>], V),
                            IP = sniffle_iprange_state:parse_bin(IpStr),
                            {ok, Ms} = jsxd:get([<<"network_mappings">>], V),
                            ok = libchunter:update_machine(Server, Port, Vm, [], UR),
                            case [ Network || [{<<"network">>, Network},
                                               {<<"ip">>, IP1}] <- Ms, IP1 =:= IP] of
                                [Network] ->
                                    sniffle_iprange:release_ip(Network, IP),
                                    Ms1 = [ [{<<"network">>, N},
                                             {<<"ip">>, IP1}] ||
                                              [{<<"network">>, N},
                                               {<<"ip">>, IP1}] <- Ms,
                                              IP1 =/= IP],
                                    sniffle_vm:set(Vm, [<<"network_mappings">>], Ms1);
                                _ ->
                                    ok
                            end;
                        _ ->
                            {error, not_stopped}
                    end;
                _ ->
                    {error, not_found}
            end;
        E ->
            E
    end.

primary_nic(Vm, Mac) ->
    case sniffle_vm:get(Vm) of
        {ok, V} ->
            NicMap = make_nic_map(V),
            case jsxd:get(Mac, NicMap) of
                {ok, _Idx}  ->
                    {ok, H} = jsxd:get(<<"hypervisor">>, V),
                    {Server, Port} = get_hypervisor(H),
                    libchunter:ping(Server, Port),
                    case jsxd:get(<<"state">>, V) of
                        {ok, <<"stopped">>} ->
                            UR = [{<<"update_nics">>, [[{<<"mac">>, Mac}, {<<"primary">>, true}]]}],
                            libchunter:update_machine(Server, Port, Vm, [], UR);
                        _ ->
                            {error, not_stopped}
                    end;
                _ ->
                    {error, not_found}
            end;
        E ->
            E
    end.

%%--------------------------------------------------------------------
%% @doc Updates a virtual machine form a package uuid and a config
%%   object.
%% @end
%%--------------------------------------------------------------------
-spec update(Vm::fifo:uuid(), Package::fifo:uuid(), Config::fifo:config()) ->
                    not_found | {error, timeout} | ok.
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

%%--------------------------------------------------------------------
%% @doc Registers am existing VM, no checks made here.
%% @end
%%--------------------------------------------------------------------
-spec register(VM::fifo:uuid(), Hypervisor::binary()) ->
                      {error, timeout} | ok.
register(Vm, Hypervisor) ->
    do_write(Vm, register, Hypervisor).

%%--------------------------------------------------------------------
%% @doc Unregisteres an existing VM, this includs freeling the IP
%%   addresses it had.
%% @end
%%--------------------------------------------------------------------
-spec unregister(VM::fifo:uuid()) ->
                        {error, timeout} |
                        ok.
unregister(Vm) ->
    case sniffle_vm:get(Vm) of
        {ok, V} ->
            lists:map(fun(N) ->
                              {ok, Net} = jsxd:get(<<"network">>, N),
                              {ok, Ip} = jsxd:get(<<"ip">>, N),
                              sniffle_iprange:release_ip(Net, Ip)
                      end,jsxd:get(<<"network_mappings">>, [], V)),
            VmPrefix = [<<"vms">>, Vm],
            ChannelPrefix = [<<"channels">>, Vm],
            case libsnarl:user_list() of
                {ok, Users} ->
                    spawn(fun () ->
                                  [libsnarl:user_revoke_prefix(U, VmPrefix) || U <- Users],
                                  [libsnarl:user_revoke_prefix(U, ChannelPrefix) || U <- Users]
                          end);
                _ ->
                    ok
            end,
            case libsnarl:group_list() of
                {ok, Groups} ->
                    spawn(fun () ->
                                  [libsnarl:group_revoke_prefix(G, VmPrefix) || G <- Groups],
                                  [libsnarl:group_revoke_prefix(G, ChannelPrefix) || G <- Groups]
                          end);
                _ ->
                    ok
            end;
        _ ->
            ok
    end,
    do_write(Vm, unregister).

%%--------------------------------------------------------------------
%% @doc Tries to creat a VM from a Package and dataset uuid. This
%%   function just creates the UUID and returns it after handing the
%%   data off to the create fsm.
%% @end
%%--------------------------------------------------------------------
-spec create(Package::binary(), Dataset::binary(), Config::fifo:config()) ->
                    {error, timeout} | {ok, fifo:uuid()}.
create(Package, Dataset, Config) ->
    UUID = uuid:uuid4s(),
    do_write(UUID, register, <<"pending">>), %we've to put pending here since undefined will cause a wrong call!
    sniffle_create_fsm:create(UUID, Package, Dataset, Config),
    {ok, UUID}.


%%--------------------------------------------------------------------
%% @doc Reads a VM object form the DB.
%% @end
%%--------------------------------------------------------------------
-spec get(Vm::fifo:uuid()) ->
                 not_found | {error, timeout} | fifo:vm_config().
get(Vm) ->
    sniffle_entity_read_fsm:start(
      {sniffle_vm_vnode, sniffle_vm},
      get, Vm
     ).

%%--------------------------------------------------------------------
%% @doc Lists all vm's.
%% @end
%%--------------------------------------------------------------------
-spec list() ->
                  {error, timeout} | [fifo:uuid()].
list() ->
    sniffle_coverage:start(
      sniffle_vm_vnode_master, sniffle_vm,
      list).

%%--------------------------------------------------------------------
%% @doc Lists all vm's and fiters by a given matcher set.
%% @end
%%--------------------------------------------------------------------
-spec list([fifo:matcher()]) -> {error, timeout} | {ok, [fifo:uuid()]}.

list(Requirements) ->
    {ok, Res} = sniffle_coverage:start(
                  sniffle_vm_vnode_master, sniffle_vm,
                  {list, Requirements}),
    Res1 = rankmatcher:apply_scales(Res),
    {ok,  lists:sort(Res1)}.

%%--------------------------------------------------------------------
%% @doc Tries to delete a VM, either unregistering it if no
%%   Hypervisor was assigned or triggering the delete on hypervisor
%%   site.
%% @end
%%--------------------------------------------------------------------
-spec delete(Vm::fifo:uuid()) ->
                    {error, timeout} | not_found | ok.
delete(Vm) ->
    case sniffle_vm:get(Vm) of
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
            ok;
        E ->
            E
    end.

%%--------------------------------------------------------------------
%% @doc Triggers the start of a VM on the hypervisor.
%% @end
%%--------------------------------------------------------------------
-spec start(Vm::fifo:uuid()) ->
                   {error, timeout} | not_found | ok.
start(Vm) ->
    case fetch_hypervisor(Vm) of
        {ok, Server, Port} ->
            libchunter:start_machine(Server, Port, Vm);
        E ->
            E
    end.

%%--------------------------------------------------------------------
%% @doc Triggers the stop of a VM on the hypervisor.
%% @end
%%--------------------------------------------------------------------
-spec stop(Vm::fifo:uuid()) ->
                  {error, timeout} | not_found | ok.
stop(Vm) ->
    stop(Vm, []).

%%--------------------------------------------------------------------
%% @doc Triggers the start of a VM on the hypervisor allowing options.
%% @end
%%--------------------------------------------------------------------
-spec stop(Vm::fifo:uuid(), Options::[atom()|{atom(), term()}]) ->
                  {error, timeout} | not_found | ok.
stop(Vm, Options) ->
    case fetch_hypervisor(Vm) of
        {ok, Server, Port} ->
            libchunter:stop_machine(Server, Port, Vm, Options);
        E ->
            E
    end.

%%--------------------------------------------------------------------
%% @doc Triggers the reboot of a VM on the hypervisor.
%% @end
%%--------------------------------------------------------------------
-spec reboot(Vm::fifo:uuid()) ->
                    {error, timeout} | not_found | ok.
reboot(Vm) ->
    reboot(Vm, []).

%%--------------------------------------------------------------------
%% @doc Triggers the reboot of a VM on the hypervisor allowing
%%   options.
%% @end
%%--------------------------------------------------------------------
-spec reboot(Vm::fifo:uuid(), Options::[atom()|{atom(), term()}]) ->
                    {error, timeout} | not_found | ok.
reboot(Vm, Options) ->
    case fetch_hypervisor(Vm) of
        {ok, Server, Port} ->
            libchunter:reboot_machine(Server, Port, Vm, Options);
        E ->
            E
    end.

%%--------------------------------------------------------------------
%% @doc Reads the logs of a vm.
%% @end
%%--------------------------------------------------------------------
-spec logs(Vm::fifo:uuid()) ->
                  not_found | {error, timeout} | [fifo:log()].
logs(Vm) ->
    case sniffle_vm:get(Vm) of
        {ok, V} ->
            {ok, jsxd:get(<<"log">>, [], V)};
        E ->
            E
    end.

%%--------------------------------------------------------------------
%% @doc Sets the owner of a VM.
%% @end
%%--------------------------------------------------------------------
-spec set_owner(Vm::fifo:uuid(), Owner::fifo:uuid()) ->
                       not_found | {error, timeout} | [fifo:log()].
set_owner(Vm, Owner) ->
    set(Vm, <<"owner">>, Owner).

%%--------------------------------------------------------------------
%% @doc Adds a new log to the VM and timestamps it.
%% @end
%%--------------------------------------------------------------------
-spec log(Vm::fifo:uuid(), Log::term()) ->
                 {error, timeout} | not_found | ok.
log(Vm, Log) ->
    Timestamp = timestamp(),
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

%%--------------------------------------------------------------------
%% @doc Creates a new ZFS snapshot of the Vm's disks on the
%%   hypervisor.
%% @end
%%--------------------------------------------------------------------
-spec snapshot(VM::fifo:uuid(), Comment::binary()) ->
                      {error, timeout} | not_found | {ok, UUID::fifo:uuid()}.
snapshot(Vm, Comment) ->
    case sniffle_vm:get(Vm) of
        {ok, V} ->
            {ok, H} = jsxd:get(<<"hypervisor">>, V),
            {Server, Port} = get_hypervisor(H),
            UUID = uuid:uuid4s(),
            TimeStamp = timestamp(),
            case libchunter:snapshot(Server, Port, Vm, UUID) of
                ok ->
                    Prefix = [<<"snapshots">>, UUID],
                    do_write(Vm, set,
                             [{Prefix ++ [<<"timestamp">>], TimeStamp},
                              {Prefix ++ [<<"comment">>], Comment},
                              {Prefix ++ [<<"state">>], <<"pending">>}]),
                    log(Vm, <<"Created snapshot ", UUID/binary, ": ", Comment/binary>>),
                    {ok, UUID};
                E ->
                    E
            end;
        E ->
            E
    end.

%%--------------------------------------------------------------------
%% @doc Deletes a ZFS snapshot of the Vm's disks on the ahypervisor.
%% @end
%%--------------------------------------------------------------------
-spec delete_snapshot(VM::fifo:uuid(), UUID::binary()) ->
                             {error, timeout} | not_found | ok.
delete_snapshot(Vm, UUID) ->
    case sniffle_vm:get(Vm) of
        {ok, V} ->
            case jsxd:get([<<"snapshots">>, UUID, <<"timestamp">>], V) of
                {ok, _} ->
                    {ok, H} = jsxd:get(<<"hypervisor">>, V),
                    {Server, Port} = get_hypervisor(H),
                    case libchunter:delete_snapshot(Server, Port, Vm, UUID) of
                        ok ->
                            Prefix = [<<"snapshots">>, UUID],
                            do_write(Vm, set,
                                     [{Prefix ++ [<<"state">>], <<"deleting">>}]),
                            log(Vm, <<"Deleting snapshot ", UUID/binary, ".">>),
                            ok;
                        E ->
                            {error, E}
                    end;
                undefined ->
                    {error, not_found}
            end;
        E ->
            E
    end.

%%--------------------------------------------------------------------
%% @doc Rolls back a ZFS snapshot of the Vm's disks on the
%%   ahypervisor.
%% @end
%%--------------------------------------------------------------------
-spec rollback_snapshot(VM::fifo:uuid(), UUID::binary()) ->
                               {error, timeout} | not_found | ok.
rollback_snapshot(Vm, UUID) ->
    case sniffle_vm:get(Vm) of
        {ok, V} ->
            case jsxd:get(<<"state">>, V) of
                {ok, <<"stopped">>} ->
                    {ok, H} = jsxd:get(<<"hypervisor">>, V),
                    {Server, Port} = get_hypervisor(H),
                    libchunter:rollback_snapshot(Server, Port, Vm, UUID);
                {ok, State} ->
                    log(Vm, <<"Not rolled back since state is ",
                              State/binary, ".">>),
                    {error, not_stopped}
            end;
        E ->
            E
    end.

-spec commit_snapshot_rollback(VM::fifo:uuid(), UUID::binary()) ->
                                      {error, timeout} | not_found | ok.

commit_snapshot_rollback(Vm, UUID) ->
    case sniffle_vm:get(Vm) of
        {ok, V} ->
            case jsxd:get([<<"snapshots">>, UUID, <<"timestamp">>], V) of
                {ok, T} when is_number(T) ->
                    Snapshots1 =
                        jsxd:fold(
                          fun (SUUID, Sn, A) ->
                                  case jsxd:get(<<"timestamp">>, 0, Sn) of
                                      X when is_number(X),
                                             X > T ->
                                          A;
                                      _ ->
                                          jsxd:set(SUUID, Sn, A)
                                  end
                          end, [], jsxd:get(<<"snapshots">>, [], V)),
                    do_write(Vm, set, [{[<<"snapshots">>], Snapshots1}]);
                undefined ->
                    {error, not_found}
            end;
        E ->
            E
    end.

%%--------------------------------------------------------------------
%% @doc Sets a attribute on the VM object.
%% @end
%%--------------------------------------------------------------------
-spec set(Vm::fifo:uuid(), Attribute::fifo:keys(), Value::fifo:value()) ->
                 {error, timeout} | not_found | ok.
set(Vm, Attribute, Value) ->
    do_write(Vm, set, [{Attribute, Value}]).


%%--------------------------------------------------------------------
%% @doc Sets multiple attributes on the VM object.
%% @end
%%--------------------------------------------------------------------
-spec set(Vm::fifo:uuid(), Attributes::fifo:attr_list()) ->
                 {error, timeout} | not_found | ok.
set(Vm, Attributes) ->
    do_write(Vm, set, Attributes).

%%%===================================================================
%%% Internal Functions
%%%===================================================================

-spec do_write(VM::fifo:uuid(), Op::atom()) -> fifo:write_fsm_reply().

do_write(VM, Op) ->
    sniffle_entity_write_fsm:write({sniffle_vm_vnode, sniffle_vm}, VM, Op).

-spec do_write(VM::fifo:uuid(), Op::atom(), Val::term()) -> fifo:write_fsm_reply().

do_write(VM, Op, Val) ->
    sniffle_entity_write_fsm:write({sniffle_vm_vnode, sniffle_vm}, VM, Op, Val).

get_hypervisor(Hypervisor) ->
    {ok, HypervisorObj} = sniffle_hypervisor:get(Hypervisor),
    {ok, Port} = jsxd:get(<<"port">>, HypervisorObj),
    {ok, Host} = jsxd:get(<<"host">>, HypervisorObj),
    {binary_to_list(Host), Port}.

fetch_hypervisor(Vm) ->
    case sniffle_vm:get(Vm) of
        {ok, V} ->
            case jsxd:get(<<"hypervisor">>, V) of
                {ok, H} ->
                    {Server, Port} = get_hypervisor(H),
                    {ok, Server, Port};
                _ ->
                    not_found
            end;
        _ ->
            not_found
    end.

make_nic_map(V) ->
    jsxd:map(fun(Idx, Nic) ->
                     {ok, NicMac} = jsxd:get([<<"mac">>], Nic),
                     {NicMac, Idx}
             end, jsxd:get([<<"config">>, <<"networks">>], [], V)).

timestamp() ->
    {Mega,Sec,Micro} = erlang:now(),
    (Mega*1000000+Sec)*1000000+Micro.
