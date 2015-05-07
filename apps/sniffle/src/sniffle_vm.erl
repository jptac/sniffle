-module(sniffle_vm).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-include("sniffle.hrl").

-define(MASTER, sniffle_vm_vnode_master).
-define(VNODE, sniffle_vm_vnode).
-define(SERVICE, sniffle_vm).
-define(S, ft_vm).
-define(FM(Met, Mod, Fun, Args),
        folsom_metrics:histogram_timed_update(
          {sniffle, vm, Met},
          Mod, Fun, Args)).

-export([
         create/3,
         delete/1, delete/2,
         restore/4,
         store/2,
         update/4
        ]).

-export([
         timestamp/0,
         add_nic/2,
         children/2,
         commit_snapshot_rollback/2,
         create_backup/4,
         delete_backup/2,
         delete_snapshot/2,
         get/1,
         list/0,
         list/2,
         log/2,
         logs/1,
         primary_nic/2,
         promote_to_image/3,
         reboot/1,
         reboot/2,
         register/2,
         remove_backup/2,
         remove_nic/2,
         restore_backup/2,
         rollback_snapshot/2,
         service_clear/2,
         service_disable/2,
         service_enable/2,
         service_refresh/2,
         service_restart/2,
         set_owner/3,
         snapshot/2,
         start/1,
         stop/1,
         stop/2,
         unregister/1,
         wipe/1,
         sync_repair/2,
         list_/0,
         dry_run/3,
         set_backup/2,
         set_config/2,
         set_info/2,
         set_service/2,
         set_snapshot/2,
         set_metadata/2,
         add_fw_rule/2,
         remove_fw_rule/2
        ]).


-export([
         add_network_map/3,
         remove_network_map/2,
         add_grouping/2,
         remove_grouping/2,
         state/2,
         %%deleting/2,
         creating/2,
         alias/2,
         owner/2,
         dataset/2,
         package/2,
         hypervisor/2
        ]).

-ignore_xref([
              alias/2,
              hypervisor/2,

              add_grouping/2,
              remove_grouping/2,
              dataset/2,
              logs/1,
              sync_repair/2,
              list_/0,
              wipe/1,
              children/2
             ]).

-type backup_opts() ::
        delete |
        {delete, parent} |
        xml.

-spec wipe(fifo:vm_id()) -> ok.

wipe(UUID) ->
    ?FM(wipe, sniffle_coverage, start, [?MASTER, ?SERVICE, {wipe, UUID}]).

-spec sync_repair(fifo:vm_id(), ft_obj:obj()) -> ok.
sync_repair(UUID, Obj) ->
    do_write(UUID, sync_repair, Obj).

list_() ->
    {ok, Res} = ?FM(list_all, sniffle_full_coverage, raw, [?MASTER, ?SERVICE, []]),
    Res1 = [R || {_, R} <- Res],
    {ok,  Res1}.

store(User, Vm) ->
    case sniffle_vm:get(Vm) of
        {ok, V} ->
            Bs = ?S:backups(V),
            case has_xml(Bs) of
                true ->
                    state(Vm, <<"storing">>),
                    [set_backup(Vm, [{[B, <<"local">>], false}])
                     || {B, _} <- Bs],
                    [set_backup(Vm, [{[B, <<"local_size">>], 0}])
                     || {B, _} <- Bs],
                    S1 = [{UUID, delete} || {UUID, _ } <- ?S:snapshots(V)],
                    set_snapshot(Vm, S1),
                    hypervisor(Vm, <<>>),
                    {Host, Port} = get_hypervisor(V),
                    resource_action(V, store, User, []),
                    libchunter:delete_machine(Host, Port, Vm);
                false ->
                    {error, no_backup}
            end;
        _ ->
            not_found
    end.

has_xml([]) ->
    false;
has_xml([{_, B} | Bs]) ->
    case jsxd:get(<<"xml">>, false, B) of
        true ->
            true;
        false ->
            has_xml(Bs)
    end.

restore(User, Vm, BID, Hypervisor) ->
    case sniffle_vm:get(Vm) of
        {ok, V} ->
            case ?S:hypervisor(V) of
                <<>> ->
                    {Server, Port} = get_hypervisor(Hypervisor),
                    case jsxd:get([BID, <<"xml">>], true, ?S:backups(V)) of
                        true ->
                            case sniffle_s3:config(snapshot) of
                                error ->
                                    {error, not_supported};
                                {ok, {S3Host, S3Port, AKey, SKey, Bucket}} ->
                                    resource_action(V, restore, User, []),
                                    libchunter:restore_backup(Server, Port, Vm,
                                                              BID, S3Host,
                                                              S3Port, Bucket,
                                                              AKey, SKey)
                            end;
                        false ->
                            no_xml
                    end;
                _ ->
                    already_deployed
            end;
        _ ->
            not_found
    end.

%% Removes a backup from the hypervisor
remove_backup(Vm, BID) ->
    case sniffle_vm:get(Vm) of
        {ok, V} ->
            {Server, Port} = get_hypervisor(V),
            case jsxd:get([BID], ?S:backups(V)) of
                {ok, _} ->
                    libchunter:delete_backup(Server, Port, Vm, BID);
                _ ->
                    not_found
            end;
        _ ->
            not_found
    end.

delete_backup(VM, BID) ->
    case sniffle_vm:get(VM) of
        {ok, V} ->
            Backups = ?S:backups(V),
            case jsxd:get([BID], Backups) of
                {ok, _} ->
                    Children = children(Backups, BID, true),
                    [do_delete_backup(VM, V, C) || C <- Children],
                    do_delete_backup(VM, V, BID);
                _ ->
                    not_found
            end;
        _ ->
            not_found
    end.

-spec restore_backup(Vm::fifo:uuid(), Snap::fifo:uuid()) ->
                            not_found |
                            {error, not_supported} |
                            {error, nopath}.
restore_backup(Vm, Snap) ->
    case sniffle_vm:get(Vm) of
        {ok, V} ->
            {Server, Port} = get_hypervisor(V),
            case jsxd:get([Snap], ?S:backups(V)) of
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
            case jsxd:get([Parent, <<"local">>], ?S:backups(V)) of
                {ok, true} ->
                    do_snap(Vm, V, Comment, Opts);
                _ ->
                    {error, parent}
            end;
        _ ->
            not_found
    end.

service_enable(Vm, Service) ->
    case sniffle_vm:get(Vm) of
        {ok, V} ->
            {Server, Port} = get_hypervisor(V),
            libchunter:service_enable(Server, Port, Vm, Service);
        _ ->
            not_found
    end.

service_disable(Vm, Service) ->
    case sniffle_vm:get(Vm) of
        {ok, V} ->
            {Server, Port} = get_hypervisor(V),
            libchunter:service_disable(Server, Port, Vm, Service);
        _ ->
            not_found
    end.

service_clear(Vm, Service) ->
    case sniffle_vm:get(Vm) of
        {ok, V} ->
            {Server, Port} = get_hypervisor(V),
            libchunter:service_clear(Server, Port, Vm, Service);
        _ ->
            not_found
    end.

service_refresh(Vm, Service) ->
    case sniffle_vm:get(Vm) of
        {ok, V} ->
            {Server, Port} = get_hypervisor(V),
            libchunter:service_refresh(Server, Port, Vm, Service);
        _ ->
            not_found
    end.

service_restart(Vm, Service) ->
    case sniffle_vm:get(Vm) of
        {ok, V} ->
            {Server, Port} = get_hypervisor(V),
            libchunter:service_restart(Server, Port, Vm, Service);
        _ ->
            not_found
    end.

do_snap(Vm, V, Comment, Opts) ->
    UUID = uuid:uuid4s(),
    Opts1 = [create | Opts],
    {Server, Port} = get_hypervisor(V),
    case sniffle_s3:config(snapshot) of
        error ->
            {error, not_supported};
        {ok, {S3Host, S3Port, AKey, SKey, Bucket}} ->
            libchunter:backup(Server, Port, Vm, UUID,
                              S3Host, S3Port, Bucket, AKey,
                              SKey, Bucket, Opts1),
            set_backup(Vm, [{[UUID, <<"comment">>], Comment},
                            {[UUID, <<"timestamp">>], timestamp()},
                            {[UUID, <<"pending">>], true}]),
            {ok, UUID}
    end.

ds_set([], _UUID, _O) ->
    ok;

ds_set([{K, F} | R], UUID, O) ->
    case jsxd:get([K], O) of
        {ok, V}  ->
            F(UUID, V);
        _ ->
            ok
    end,
    ds_set(R, UUID, O).


promote_to_image(Vm, SnapID, Config) ->
    case sniffle_vm:get(Vm) of
        {ok, V} ->
            case jsxd:get([SnapID, <<"timestamp">>], ?S:snapshots(V)) of
                {ok, _} ->
                    {Server, Port} = get_hypervisor(V),
                    UUID = uuid:uuid4s(),
                    ok = sniffle_dataset:create(UUID),
                    C = ?S:config(V),
                    Config1 = jsxd:from_list(Config),
                    ds_set(
                      [
                       {<<"name">>, fun sniffle_dataset:name/2},
                       {<<"version">>, fun sniffle_dataset:version/2},
                       {<<"os">>, fun sniffle_dataset:os/2},
                       {<<"description">>, fun sniffle_dataset:description/2}
                      ], UUID, jsxd:from_list(Config)),

                    {ok, Nets} = jsxd:get([<<"networks">>], C),
                    Nets1 =
                        jsxd:map(fun (Idx, E) ->
                                         Name = io_lib:format("net~p", [Idx]),
                                         {list_to_binary(Name),
                                          jsxd:get(<<"tag">>, <<"undefined">>, E)}
                                 end, Nets),
                    [sniffle_dataset:add_network(UUID, Net) || Net <- Nets1],
                    Type = jsxd:get([<<"type">>], <<"zone">>, C),
                    sniffle_dataset:type(UUID, Type),
                    case Type of
                        <<"zone">> ->
                            ok;
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
                            sniffle_dataset:nic_driver(UUID, ND),
                            sniffle_dataset:disk_driver(UUID, DD)
                    end,
                    {ok, {S3Host, S3Port, AKey, SKey, Bucket}} = sniffle_s3:config(image),
                    ok = libchunter:store_snapshot(
                           Server, Port, Vm, SnapID, UUID, S3Host,
                           S3Port, Bucket, AKey, SKey, []),
                    {ok, UUID};
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
            H = ?S:hypervisor(V),
            {ok, HypervisorObj} = sniffle_hypervisor:get(H),
            HypervisorsNetwork = ft_hypervisor:networks(HypervisorObj),
            {Server, Port} = ft_hypervisor:endpoint(HypervisorObj),
            libchunter:ping(Server, Port),
            case ?S:state(V) of
                <<"stopped">> ->
                    Requirements = [{must, oneof, <<"tag">>, HypervisorsNetwork}],
                    lager:info("[NIC ADD] Checking requirements: ~p.", [Requirements]),
                    case sniffle_network:claim_ip(Network, Requirements) of
                        {ok, IPRange, {Tag, IP, Net, Gw, VLan}} ->
                            IPb = ft_iprange:to_bin(IP),
                            Netb = ft_iprange:to_bin(Net),
                            GWb = ft_iprange:to_bin(Gw),

                            NicSpec0 =
                                jsxd:from_list([{<<"ip">>, IPb},
                                                {<<"gateway">>, GWb},
                                                {<<"network_uuid">>, IPRange},
                                                {<<"netmask">>, Netb},
                                                {<<"nic_tag">>, Tag }]),
                            NicSpec1 =
                                case jsxd:get([<<"networks">>], ?S:config(V)) of
                                    {ok, [_|_]} ->
                                        NicSpec0;
                                    _ ->
                                        jsxd:set([<<"primary">>], true, NicSpec0)
                                end,
                            NicSpec2 =
                                case VLan of
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
                            ok = libchunter:update_machine(Server, Port, Vm,
                                                           undefined, UR),
                            add_network_map(Vm, IP, IPRange);
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
                    {Server, Port} = get_hypervisor(V),
                    case ?S:state(V) of
                        <<"stopped">> ->
                            UR = [{<<"remove_nics">>, [Mac]}],
                            {ok, IpStr} = jsxd:get([<<"networks">>, Idx, <<"ip">>], ?S:config(V)),
                            IP = ft_iprange:parse_bin(IpStr),
                            Ms = ?S:network_map(V),
                            ok = libchunter:update_machine(Server, Port, Vm,
                                                           undefined, UR),
                            remove_network_map(Vm, IP),
                            [{IP, Network}] = [ {IP1, Network} || {IP1, Network} <- Ms, IP1 =:= IP],
                            sniffle_iprange:release_ip(Network, IP);
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
            case {?S:state(V), jsxd:get(Mac, NicMap)} of
                {<<"stopped">>, {ok, _Idx}}  ->
                    {Server, Port} = get_hypervisor(V),
                    libchunter:ping(Server, Port),
                    UR = [{<<"update_nics">>, [[{<<"mac">>, Mac},
                                                {<<"primary">>, true}]]}],
                    libchunter:update_machine(Server, Port, Vm,
                                              undefined, UR);
                {_, {ok, _}} ->
                    {error, not_stopped};
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
-spec update(User::fifo:user_id() | undefined, Vm::fifo:vm_id(),
             Package::fifo:package_id(), Config::fifo:config()) ->
                    not_found | {error, timeout} | ok.

update(User, Vm, Package, Config) ->
    case sniffle_vm:get(Vm) of
        {ok, V} ->
            Hypervisor = ?S:hypervisor(V),
            {ok, H} = sniffle_hypervisor:get(Hypervisor),
            {Host, Port} = get_hypervisor(H),
            OrigPkg = ?S:package(V),
            {ok, OrigRam} = jsxd:get([<<"ram">>], ?S:config(V)),
            case test_pkg(Package, OrigRam, H) of
                no_pkg_change ->
                    libchunter:update_machine(Host, Port, Vm,
                                              undefined, Config);
                {ok, P} ->
                    package(Vm, Package),
                    log(Vm, <<"Updating VM from package '",
                              OrigPkg/binary, "' to '",
                              Package/binary, "'.">>),
                    resource_action(V, update, User, [{package, Package}]),
                    libchunter:update_machine(Host, Port, Vm, P, Config);
                E2 ->
                    E2
            end;
        E ->
            E
    end.

test_pkg(undefined, _, _) ->
    no_pkg_change;

test_pkg(Package, OrigRam, H) ->
    case sniffle_package:get(Package) of
        {ok, P} ->
            NewRam = ft_package:ram(P),
            case jsxd:get([<<"free-memory">>],
                          ft_hypervisor:resources(H)) of
                {ok, Ram} when
                      Ram > (NewRam - OrigRam) ->
                    {ok, P};
                _ ->
                    {error, not_enough_resources}
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
%%   addresses it had, removing it from groupings and cleaning
%%   permissions.
%% @end
%%--------------------------------------------------------------------
-spec unregister(VM::fifo:uuid()) ->
                        {error, timeout} |
                        ok.
unregister(Vm) ->
    case sniffle_vm:get(Vm) of
        {ok, V} ->
            do_write(Vm, unregister),
            lists:map(fun({Ip, Net}) ->
                              sniffle_iprange:release_ip(Net, Ip)
                      end, ?S:network_map(V)),
            VmPrefix = [<<"vms">>, Vm],
            ChannelPrefix = [<<"channels">>, Vm],
            case ls_user:list() of
                {ok, Users} ->
                    spawn(fun () ->
                                  [ls_user:revoke_prefix(U, VmPrefix) || U <- Users],
                                  [ls_user:revoke_prefix(U, ChannelPrefix) || U <- Users]
                          end);
                _ ->
                    ok
            end,
            case ls_role:list() of
                {ok, Roles} ->
                    spawn(fun () ->
                                  [ls_role:revoke_prefix(G, VmPrefix) || G <- Roles],
                                  [ls_role:revoke_prefix(G, ChannelPrefix) || G <- Roles]
                          end);
                _ ->
                    ok
            end,
            case ?S:groupings(V) of
                [] ->
                    ok;
                Gs ->
                    [sniffle_grouping:remove_element(G, Vm) || G <- Gs]
            end;
        _ ->
            do_write(Vm, unregister)
    end.

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
    do_write(UUID, register, <<"pooled">>), %we've to put pending here since undefined will cause a wrong call!
    creating(UUID, {started, now()}),
    Config1 = jsxd:from_list(Config),
    Config2 = jsxd:update(<<"networks">>,
                          fun (N) ->
                                  jsxd:from_list(
                                    lists:map(fun ({Iface, Net}) ->
                                                      [{<<"interface">>, Iface},
                                                       {<<"network">>, Net}]
                                              end, N))
                          end, [], Config1),
    set_config(UUID, Config2),
    state(UUID, <<"pooled">>),
    package(UUID, Package),
    dataset(UUID, Dataset),
    libhowl:send(UUID, [{<<"event">>, <<"update">>},
                        {<<"data">>,
                         [{<<"config">>, Config2},
                          {<<"package">>, Package}]}]),
    %% TODO: is this the right place?
    {ok, Creator} = jsxd:get([<<"owner">>], Config),
    ls_user:grant(Creator, [<<"vms">>, UUID, <<"...">>]),
    ls_user:grant(Creator, [<<"channels">>, UUID, <<"join">>]),
    sniffle_create_pool:add(UUID, Package, Dataset, Config),
    {ok, UUID}.

dry_run(Package, Dataset, Config) ->
    UUID = uuid:uuid4s(),
    Ref = make_ref(),
    sniffle_create_fsm:create(UUID, Package, Dataset, Config, {self(), Ref}),
    receive
        {Ref, Res} ->
            {ok, Res}
    after
        5000 ->
            {error, timeout}
    end.


%%--------------------------------------------------------------------
%% @doc Reads a VM object form the DB.
%% @end
%%--------------------------------------------------------------------
-spec get(Vm::fifo:uuid()) ->
                 not_found | {error, timeout} | {ok, fifo:vm()}.
get(Vm) ->
    ?FM(get, sniffle_entity_read_fsm, start, [{?VNODE, ?SERVICE}, get, Vm]).


%%--------------------------------------------------------------------
%% @doc Lists all vm's.
%% @end
%%--------------------------------------------------------------------
-spec list() ->
                  {error, timeout} | {ok, [fifo:uuid()]}.
list() ->
    ?FM(list, sniffle_coverage, start, [?MASTER, ?SERVICE, list]).

%%--------------------------------------------------------------------
%% @doc Lists all vm's and fiters by a given matcher set.
%% @end
%%--------------------------------------------------------------------
-spec list([fifo:matcher()], boolean()) -> {error, timeout} | {ok, [fifo:uuid()]}.

list(Requirements, true) ->
    {ok, Res} = ?FM(list_all, sniffle_full_coverage, list,
                    [?MASTER, ?SERVICE, Requirements]),
    Res1 = lists:sort(rankmatcher:apply_scales(Res)),
    {ok,  Res1};

list(Requirements, false) ->
    {ok, Res} = ?FM(list_all, sniffle_coverage, start,
                    [?MASTER, ?SERVICE, {list, Requirements}]),
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
    delete(undefined, Vm).

delete(User, Vm) ->
    case sniffle_vm:get(Vm) of
        {ok, V} ->
            case {?S:hypervisor(V), ?S:deleting(V), ?S:state(V)} of
                {_, true, _} ->
                    finish_delete(Vm);
                {_, _, <<"storing">>} ->
                    libhowl:send(<<"command">>,
                                 [{<<"event">>, <<"vm-stored">>},
                                  {<<"uuid">>, uuid:uuid4s()},
                                  {<<"data">>,
                                   [{<<"uuid">>, Vm}]}]),
                    state(Vm, <<"stored">>),
                    hypervisor(Vm, <<>>);
                {undefined, _, _} ->
                    finish_delete(Vm);
                {<<>>, _, _} ->
                    finish_delete(Vm);
                {<<"pooled">>, _, _} ->
                    finish_delete(Vm);
                {<<"pending">>, _, _} ->
                    finish_delete(Vm);
                {_, _, undefined} ->
                    finish_delete(Vm);
                {_, _, <<"failed-", _/binary>>} ->
                    finish_delete(Vm);
                {H, _, _} ->
                    state(Vm, <<"deleting">>),
                    deleting(Vm, true),
                    {Host, Port} = get_hypervisor(H),
                    resource_action(V, destroy, User, []),
                    libchunter:delete_machine(Host, Port, Vm)
            end;
        E ->
            E
    end.

finish_delete(Vm) ->
    {ok, V} = ?MODULE:get(Vm),
    [do_delete_backup(Vm, V, BID) || {BID, _} <- ?S:backups(V)],
    sniffle_vm:unregister(Vm),
    resource_action(V, confirm_destroy, []),
    libhowl:send(Vm, [{<<"event">>, <<"delete">>}]),
    libhowl:send(<<"command">>,
                 [{<<"event">>, <<"vm-delete">>},
                  {<<"uuid">>, uuid:uuid4s()},
                  {<<"data">>,
                   [{<<"uuid">>, Vm}]}]).

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
            {ok, ?S:logs(V)};
        E ->
            E
    end.

%%--------------------------------------------------------------------
%% @doc Sets the owner of a VM.
%% @end
%%--------------------------------------------------------------------
-spec set_owner(User::fifo:user_id() | undefined, Vm::fifo:uuid(),
                Owner::fifo:uuid()) ->
                       not_found | {error, timeout} | [fifo:log()].
set_owner(User, Vm, Owner) ->
    case fetch_hypervisor(Vm) of
        {ok, Server, Port} ->
            UR = [{<<"owner">>, Owner}],
            ok = libchunter:update_machine(
                   Server, Port, Vm, undefined, UR);
        _ ->
            ok
    end,
    ls_org:execute_trigger(Owner, vm_create, Vm),
    libhowl:send(Vm, [{<<"event">>, <<"update">>},
                      {<<"data">>,
                       [{<<"owner">>, Owner}]}]),
    case sniffle_vm:get(Vm) of
        {ok, V} ->
            %% We can use the Vm object we got to end the accouting period
            %% for the old org
            resource_action(V, destroy, User, []),
            resource_action(V, confirm_destroy, User, []),
            case owner(Vm, Owner) of
                ok ->
                    %% After a successful changed the owner we refetch the vm
                    %% to add accounting to the new org.
                    Opts = [{package, ft_vm:package(V)},
                            {dataset, ft_vm:dataset(V)}],
                    {ok, V1} = sniffle_vm:get(Vm),
                    resource_action(V1, create, User, Opts),
                    resource_action(V1, confirm_create, User, []),
                    ok;
                E1 ->
                    E1
            end;
        E ->
            E
    end.

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
            {Server, Port} = get_hypervisor(V),
            UUID = uuid:uuid4s(),
            TimeStamp = timestamp(),
            libchunter:snapshot(Server, Port, Vm, UUID),
            set_snapshot(Vm,
                         [{[UUID, <<"timestamp">>], TimeStamp},
                          {[UUID, <<"comment">>], Comment},
                          {[UUID, <<"state">>], <<"pending">>}]),
            log(Vm, <<"Created snapshot ", UUID/binary, ": ", Comment/binary>>),
            {ok, UUID};
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
            case jsxd:get([UUID, <<"timestamp">>], ?S:snapshots(V)) of
                {ok, _} ->
                    {Server, Port} = get_hypervisor(V),
                    libchunter:delete_snapshot(Server, Port, Vm, UUID),
                    set_snapshot(Vm,
                                 [{[UUID, <<"state">>], <<"deleting">>}]),
                    log(Vm, <<"Deleting snapshot ", UUID/binary, ".">>),
                    ok;
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
            case ?S:state(V) of
                <<"stopped">> ->
                    {Server, Port} = get_hypervisor(V),
                    libchunter:rollback_snapshot(Server, Port, Vm, UUID);
                State ->
                    log(Vm, <<"Not rolled back since state is ",
                              State/binary, ".">>),
                    {error, not_stopped}
            end;
        E ->
            E
    end.


%% TODO
-spec commit_snapshot_rollback(VM::fifo:uuid(), UUID::binary()) ->
                                      {error, timeout} | not_found | ok.

commit_snapshot_rollback(Vm, UUID) ->
    case sniffle_vm:get(Vm) of
        {ok, V} ->
            Snapshots = ?S:snapshots(V),
            case jsxd:get([UUID, <<"timestamp">>], Snapshots) of
                {ok, T} when is_number(T) ->
                    Snapshots1 = [{SUUID, jsxd:get([<<"timestamp">>], 0, Sn)}
                                  || {SUUID, Sn} <- Snapshots],
                    Snapshots2 = [{SUUID, delete}
                                  || {SUUID, X} <- Snapshots1,
                                     is_number(X), X > T],
                    set_snapshot(Vm, Snapshots2),
                    ok;
                undefined ->
                    {error, not_found}
            end;
        E ->
            E
    end.

add_network_map(UUID, IP, Net) ->
    do_write(UUID, set_network_map, [IP, Net]).

remove_network_map(UUID, IP) ->
    do_write(UUID, set_network_map, [IP, delete]).



trigger_fw_change(UUID) ->
    {ok, VM} = sniffle_vm:get(UUID),
    {Host, Port} = get_hypervisor(VM),
    libchunter:update_fw(Host, Port, UUID).


add_fw_rule(UUID, V) ->
    R = do_write(UUID, add_fw_rule, V),
    trigger_fw_change(UUID),
    R.

remove_fw_rule(UUID, V) ->
    R = do_write(UUID, remove_fw_rule, V),
    trigger_fw_change(UUID),
    R.

-define(S(T),
        T(UUID, V) ->
               do_write(UUID, T, V)).
?S(remove_grouping).
?S(add_grouping).
?S(set_metadata).
?S(set_info).
?S(set_config).
?S(set_backup).
?S(set_snapshot).
?S(set_service).

?S(state).
?S(creating).
deleting(UUID, V) 
  when V =:= true;
       V =:= false ->
    do_write(UUID, deleting, V).
?S(alias).
?S(owner).
?S(dataset).
?S(package).
?S(hypervisor).

%%%===================================================================
%%% Internal Functions
%%%===================================================================

-spec do_write(VM::fifo:uuid(), Op::atom()) -> fifo:write_fsm_reply().

do_write(VM, Op) ->
    ?FM(Op, sniffle_entity_write_fsm, write, [{?VNODE, ?SERVICE}, VM, Op]).

-spec do_write(VM::fifo:uuid(), Op::atom(), Val::term()) ->
                      fifo:write_fsm_reply().

do_write(VM, Op, Val) ->
    ?FM(Op, sniffle_entity_write_fsm, write, [{?VNODE, ?SERVICE}, VM, Op, Val]).

get_hypervisor(V) ->
    get_hypervisor(fifo_dt:type(V), V).

get_hypervisor(ft_vm, V) ->
    get_hypervisor(undefined, ?S:hypervisor(V));

get_hypervisor(ft_hypervisor, H) ->
    ft_hypervisor:endpoint(H);

get_hypervisor(undefined, not_found) ->
    not_found;

get_hypervisor(undefined, Hypervisor) ->
    case sniffle_hypervisor:get(Hypervisor) of
        {ok, HypervisorObj} ->
            get_hypervisor(HypervisorObj);
        E ->
            E
    end.

fetch_hypervisor(Vm) ->
    case sniffle_vm:get(Vm) of
        {ok, V} ->
            {Server, Port} = get_hypervisor(V),
            {ok, Server, Port};
        _ ->
            not_found
    end.

make_nic_map(V) ->
    jsxd:map(fun(Idx, Nic) ->
                     {ok, NicMac} = jsxd:get([<<"mac">>], Nic),
                     {NicMac, Idx}
             end, jsxd:get([<<"networks">>], [], ?S:config(V))).

timestamp() ->
    {Mega,Sec,Micro} = erlang:now(),
    (Mega*1000000+Sec)*1000000+Micro.

children(Backups, Parent) ->
    children(Backups, Parent, false).

children(Backups, Parent, Recursive) ->
    R = [U ||
            {U, B} <- Backups,
            jsxd:get(<<"parent">>, B) =:= {ok, Parent}],
    case Recursive of
        true ->
            lists:flatten([children(Backups, C, true) || C <- R]) ++ R;
        false ->
            R
    end.

do_delete_backup(UUID, VM, BID) ->
    Backups = ?S:backups(VM),
    case jsxd:get([BID, <<"files">>], Backups) of
        {ok, Files} ->
            Fs = case jsxd:get([BID, <<"xml">>], false, Backups) of
                     true ->
                         [<<UUID/binary, "/", BID/binary, ".xml">> | Files];
                     _ ->
                         Files
                 end,
            [sniffle_s3:delete(snapshot, F) || F <- Fs];
        _ ->
            ok
    end,
    case ?S:hypervisor(VM) of
        <<>> ->
            ok;
        H ->
            {Server, Port} = get_hypervisor(H),
            libchunter:delete_snapshot(Server, Port, UUID, BID),
            set_backup(UUID, [{[BID], delete}]),
            libhowl:send(UUID, [{<<"event">>, <<"backup">>},
                                {<<"data">>, [{<<"action">>, <<"deleted">>},
                                              {<<"uuid">>, BID}]}])
    end.

resource_action(UUID, Action, Opts) ->
    resource_action(UUID, Action, undefined, Opts).

resource_action(VM, Action, User, Opts) ->
    case ft_vm:owner(VM) of
        <<>> ->
            ok;
        Org ->
            Opts1 = case User of
                        undefined ->
                            Opts;
                        _ ->
                            [{user, User} | Opts]
                    end,
            UUID = ft_vm:uuid(VM),
            T = timestamp(),
            ls_org:resource_action(Org, UUID, T, Action, Opts1)
    end.
