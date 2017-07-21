-module(sniffle_vm).
-define(CMD, sniffle_vm_cmd).
-define(BUCKET, <<"vm">>).
-define(S, ft_vm).
-define(EMPTY, <<"00000000-0000-0000-0000-000000000000">>).
-include("sniffle.hrl").

-define(FM(Met, Mod, Fun, Args),
        folsom_metrics:histogram_timed_update(
          {sniffle, vm, Met},
          Mod, Fun, Args)).

-export([
         create/3,
         delete/1, delete/2,
         restore/4,
         restore/5,
         store/2,
         update/4,
         set_hostname/3
        ]).

-export([
         timestamp/0,
         add_nic/2,
         children/2,
         commit_snapshot_rollback/2,
         create_backup/4,
         delete_backup/2,
         delete_snapshot/2,
         get_docker/1,
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
         dry_run/3,
         set_backup/2,
         set_config/2,
         set_info/2,
         set_service/2,
         set_snapshot/2,
         set_metadata/2,
         set_docker/2,
         vm_type/2,
         created_at/2,
         created_by/2,
         add_fw_rule/2,
         remove_fw_rule/2
        ]).


-export([
         add_iprange_map/3,
         remove_iprange_map/2,
         add_network_map/3,
         remove_network_map/2,
         add_hostname_map/3,
         remove_hostname_map/2,
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

-type backup_opts() ::
        delete |
        {delete, parent} |
        xml.


%%%===================================================================
%%% General section
%%%===================================================================

-spec do_write(VM::fifo:uuid(), Op::atom()) -> fifo:write_fsm_reply().

-spec do_write(VM::fifo:uuid(), Op::atom(), Val::term()) ->
                      fifo:write_fsm_reply().
-spec wipe(fifo:vm_id()) -> ok.

-spec sync_repair(fifo:vm_id(), ft_obj:obj()) -> ok.
-spec get(Vm::fifo:uuid()) ->
                 not_found | {error, timeout} | {ok, ft_vm:vm()}.
-spec list() ->
                  {error, timeout} | {ok, [fifo:uuid()]}.

-spec list([fifo:matcher()], boolean()) ->
                  {error, timeout} | {ok, [fifo:uuid()]}.

-include("sniffle_api.hrl").
%%%===================================================================
%%% Custom section
%%%===================================================================

store(User, Vm) ->
    case sniffle_vm:get(Vm) of
        {ok, V} ->
            Bs = ?S:backups(V),
            case {?S:dataset(V), has_xml(Bs)} of
                {<<>>, _} ->
                    {error, no_dataset};
                {_, true} ->
                    state(Vm, <<"storing">>),
                    BIDs = maps:keys(Bs),
                    [set_backup(Vm, [{[B, <<"local">>], false}])
                     || B <- BIDs],
                    [set_backup(Vm, [{[B, <<"local_size">>], 0}])
                     || B <- BIDs],
                    S1 = [{UUID, delete} || UUID <- maps:keys(?S:snapshots(V))],
                    set_snapshot(Vm, S1),
                    hypervisor(Vm, <<>>),
                    {Host, Port} = get_hypervisor(V),
                    resource_action(V, store, User, []),
                    free_res(V),
                    libchunter:store_machine(Host, Port, Vm);
                {_, false} ->
                    {error, no_backup}
            end;
        _ ->
            not_found
    end.

-spec has_xml(maps:map()) ->
                     boolean().
has_xml(M) ->
    has_xml_(maps:values(M)).

-spec has_xml_([maps:map()]) ->
                      boolean().
has_xml_([]) ->
    false;
has_xml_([B | Bs]) ->
    case jsxd:get(<<"xml">>, false, B) of
        true ->
            true;
        false ->
            has_xml_(Bs)
    end.

restore(User, Vm, BID, Requeirements) ->
    restore(User, Vm, BID, Requeirements, undefined).
restore(User, Vm, BID, Requeirements, Package) ->
    case sniffle_vm:get(Vm) of
        {ok, V} ->
            %% ensure we can't restore a VM while it's still being stored
            case {?S:state(V), ?S:hypervisor(V)} of
                {<<"storing">>, _} ->
                    {error, storing};
                {_, <<>>} ->
                    restore_(User, Vm, V, BID, Requeirements, Package);
                _ ->
                    {error, already_deployed}
            end;
        _ ->
            not_found
    end.

restore_(User, UUID, V, BID, Requeirements, Package) ->
    case jsxd:get([BID, <<"xml">>], true, ?S:backups(V)) of
        true ->
            Meta = case Package of
                       undefined -> [];
                       _ -> [{package, Package}]
                   end,
            resource_action(V, restore, User, Meta),
            sniffle_create_pool:restore(
              UUID, BID, Requeirements, Package, User);
        false ->
            {error, no_xml}
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
                    Children = children(maps:to_list(Backups), BID, true),
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
                    {ok, {S3Host, S3Port, AKey, SKey, Bucket}} =
                        sniffle_s3:config(snapshot),
                    libchunter:restore_backup(Server, Port, Vm, Snap,
                                              S3Host, S3Port, Bucket,
                                              AKey, SKey);
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
            do_backup(Vm, V, Comment, Opts);
        _ ->
            not_found
    end;

create_backup(Vm, incremental, Comment, Opts) ->
    case sniffle_vm:get(Vm) of
        {ok, V} ->
            Parent = proplists:get_value(parent, Opts),
            case jsxd:get([Parent, <<"local">>], ?S:backups(V)) of
                {ok, true} ->
                    do_backup(Vm, V, Comment, Opts);
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

do_backup(Vm, V, Comment, Opts) ->
    Conf = ft_vm:config(V),
    case jsxd:get(<<"datasets">>, Conf) of
        {ok, Es} when is_list(Es), length(Es) > 0 ->
            lager:warning("Can't create backups from zones w/ delegates"),
            {error, not_supported};
        _ ->
            do_backup_(Vm, V, Comment, Opts)
    end.

do_backup_(Vm, V, Comment, Opts) ->
    UUID = fifo_utils:uuid(),
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
                    promote_to_image_(Vm, SnapID, Config, V);
                undefined ->
                    not_found
            end;
        E ->
            E
    end.

promote_to_image_(UUID, SnapID, Config, V) ->
    {Server, Port} = get_hypervisor(V),
    DatasetUUID = fifo_utils:uuid(dataset),
    ok = sniffle_dataset:create(DatasetUUID),
    C = ?S:config(V),
    lager:info("config: ~p", [C]),
    case jsxd:get([<<"disks">>], C) of
        undefined ->
            ok;
        {ok, Disks} -> % for kvm dataset, image_size is required
            [BootDisk] = lists:filter(fun(E) ->
                                              proplists:get_value(<<"boot">>, E)
                                      end, Disks),
            {ok, ImageSize} = jsxd:get([<<"size">>], BootDisk),
            sniffle_dataset:image_size(DatasetUUID, ImageSize)
    end,
    Config1 = jsxd:from_list(Config),
    ds_set(
      [
       {<<"name">>, fun sniffle_dataset:name/2},
       {<<"version">>, fun sniffle_dataset:version/2},
       {<<"os">>, fun sniffle_dataset:os/2},
       {<<"description">>, fun sniffle_dataset:description/2}
      ], DatasetUUID, jsxd:from_list(Config)),

    {ok, Nets} = jsxd:get([<<"networks">>], C),
    Nets1 =
        jsxd:map(fun (Idx, E) ->
                         Name = io_lib:format("net~p", [Idx]),
                         {list_to_binary(Name),
                          jsxd:get(<<"tag">>, <<"undefined">>, E)}
                 end, Nets),
    [sniffle_dataset:add_network(DatasetUUID, Net) || Net <- Nets1],
    {ok, D} = sniffle_dataset:get(?S:dataset(V)),
    case ft_dataset:zone_type(D) of
        <<>> ->
            ok;
        ZoneType ->
            sniffle_dataset:zone_type(DatasetUUID, ZoneType)
    end,
    [sniffle_dataset:add_requirement(DatasetUUID, R) ||
        R <- ft_dataset:requirements(D)],
    ds_set(
      [
       {<<"kernel_version">>, fun sniffle_dataset:kernel_version/2}
      ], DatasetUUID, C),
    Type = jsxd:get([<<"type">>], <<"zone">>, C),
    sniffle_dataset:type(DatasetUUID, Type),
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
            sniffle_dataset:nic_driver(DatasetUUID, ND),
            sniffle_dataset:disk_driver(DatasetUUID, DD)
    end,
    {ok, {S3Host, S3Port, AKey, SKey, Bucket}} = sniffle_s3:config(image),
    ok = libchunter:store_snapshot(
           Server, Port, UUID, SnapID, DatasetUUID, S3Host,
           S3Port, Bucket, AKey, SKey, []),
    {ok, DatasetUUID}.

add_nic(Vm, Network) ->
    lager:info("[NIC ADD] Adding a new nic in ~s to ~s", [Network, Vm]),
    case sniffle_vm:get(Vm) of
        {ok, V} ->
            lager:info("[NIC ADD] VM found.", []),
            case ?S:state(V) of
                <<"stopped">> ->
                    add_nic_(Vm, V, Network);
                E ->
                    lager:error("VM needs to be stoppped: ~p", [E]),
                    {error, not_stopped}
            end;
        E ->
            lager:error("Could not get new IP - could not get VM: ~p", [E]),
            E
    end.

add_nic_(Vm, V, Network) ->
    H = ?S:hypervisor(V),
    {ok, HypervisorObj} = sniffle_hypervisor:get(H),
    HypervisorsNetwork = ft_hypervisor:networks(HypervisorObj),
    {Server, Port} = ft_hypervisor:endpoint(HypervisorObj),
    libchunter:ping(Server, Port),

    Requirements = [{must, oneof, <<"tag">>, HypervisorsNetwork}],
    lager:info("[NIC ADD] Checking requirements: ~p.", [Requirements]),
    case sniffle_network:claim_ip(Network, Requirements) of
        {ok, IPRange, {Tag, IP, Net, Gw, VLan}} ->
            IPb = ft_iprange:to_bin(IP),
            Netb = ft_iprange:to_bin(Net),
            GWb = ft_iprange:to_bin(Gw),
            NicSpec0 = #{
              <<"ip">> => IPb,
              <<"gateway">> => GWb,
              <<"network_uuid">> => IPRange,
              <<"netmask">> => Netb,
              <<"nic_tag">> => Tag
             },
            NicSpec1 = set_primary(jsxd:get([<<"networks">>], ?S:config(V)),
                                   NicSpec0),
            NicSpec2 = add_vlan(VLan, NicSpec1),
            UR = #{<<"add_nics">> => [NicSpec2]},
            ok = libchunter:update_machine(Server, Port, Vm, undefined, UR),
            add_network_map(Vm, IP, Network),
            add_iprange_map(Vm, IP, IPRange);
        E ->
            lager:error("Could not get claim new IP: ~p for ~p ~p",
                        [E, Network, Requirements]),
            {error, claim_failed}
    end.

add_vlan(0, Spec) ->
    Spec;
add_vlan(VLAN, Spec) ->
    jsxd:set(<<"vlan_id">>, VLAN, Spec).

%% TODO: check if the networks actually include a primary
set_primary({ok, [_|_]}, Spec) ->
    Spec;
set_primary(_, Spec) ->
    jsxd:set([<<"primary">>], true, Spec).

remove_nic(Vm, Mac) ->
    case sniffle_vm:get(Vm) of
        {ok, V} ->
            NicMap = make_nic_map(V),
            case {?S:state(V), jsxd:get(Mac, NicMap)} of
                {<<"stopped">>, {ok, Idx}}  ->
                    {Server, Port} = get_hypervisor(V),
                    UR = #{<<"remove_nics">> => [Mac]},
                    {ok, IpStr} = jsxd:get([<<"networks">>, Idx, <<"ip">>],
                                           ?S:config(V)),
                    IP = ft_iprange:parse_bin(IpStr),
                    Ms = ?S:iprange_map(V),
                    ok = libchunter:update_machine(Server, Port, Vm,
                                                   undefined, UR),
                    remove_network_map(Vm, IP),
                    remove_hostname_map(Vm, IP, V),
                    remove_iprange_map(Vm, IP),
                    IPRange = maps:get(IP, Ms),
                    sniffle_iprange:release_ip(IPRange, IP);
                {<<"stopped">>, _} ->
                    {error, not_stopped};
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
                    UR = #{
                      <<"update_nics">> => [#{<<"mac">> => Mac,
                                              <<"primary">> => true}]},
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
            H = ?S:hypervisor(V),
            {ok, Hv} = sniffle_hypervisor:get(H),
            {Host, Port} = ft_hypervisor:endpoint(Hv),
            OrigPkg = ?S:package(V),
            {ok, OrigRam} = jsxd:get([<<"ram">>], ?S:config(V)),
            case test_pkg(Package, OrigRam, Hv, V) of
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

test_pkg(undefined, _, _, _) ->
    no_pkg_change;

test_pkg(Package, OrigRam, H, Vm) ->
    case sniffle_package:get(Package) of
        {ok, P} ->
            NewRam = ft_package:ram(P),
            case jsxd:get([<<"free-memory">>],
                          ft_hypervisor:resources(H)) of
                {ok, Ram} when
                      Ram > (NewRam - OrigRam) ->
                    check_org_res(P, ft_vm:package(Vm), ft_vm:owner(Vm));
                _ ->
                    {error, not_enough_resources}
            end;
        E ->
            E
    end.


%% If there was no package we got no resources.
old_res(<<>>) ->
    #{};
old_res(Package) ->
    case sniffle_package:get(Package) of
        {ok, POld} ->
            ft_package:org_resources(POld);
        _ ->
            #{}
    end.

-spec res_fold_fn(binary(), number(), maps:map()) ->
                         maps:map().
res_fold_fn(K, VOld, AccNew) ->
    VNew = case maps:find(K, AccNew) of
               error ->
                   0;
               {ok, VNewX} ->
                   VNewX
           end,
    maps:put(K, VNew - VOld, AccNew).

%% If we have no org we always allow upgrading
check_org_res(P, _, <<>>) ->
    {ok, P};

check_org_res(P, OldID, OrgID) ->
    ResOld = old_res(OldID),
    ResNew = ft_package:org_resources(P),
    Res = maps:fold(fun res_fold_fn/3 , ResNew, ResOld),
    {ok, Org} = ls_org:get(OrgID),
    case check_resources(Org, Res) of
        ok ->
            [ls_org:resource_dec(OrgID, R, V)  || {R, V} <- maps:to_list(Res)],
            {ok, P};
        {R, V} ->
            lager:warning("Could not resize VM since the Org missed the "
                          "resource ~s: ~p", [R, V]),
            {error, org_resource_missing}
    end.

-spec check_resources(maps:map(), #{binary() => number()}) ->
                             ok | {binary(), number()}.
check_resources(Org, Res) when is_map(Res) ->
    check_resources_(Org, maps:to_list(Res)).

-spec check_resources_(maps:map(), [{binary(), number()}]) ->
                              ok | {binary(), number()}.
check_resources_(_Org, []) ->
    ok;
check_resources_(Org, [{_R, V} | Rest]) when V =< 0 ->
    check_resources_(Org, Rest);
check_resources_(Org, [{R, V} | Rest]) ->
    case ft_org:resource(Org, R) of
        {ok, V1} when V1 >= V ->
            check_resources_(Org, Rest);
        _ ->
            {R, V}
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
            maps:fold(fun(Ip, Net, _) ->
                              sniffle_iprange:release_ip(Net, Ip)
                      end, ok, ?S:iprange_map(V)),
            VmPrefix = [<<"vms">>, Vm],
            ChannelPrefix = [<<"channels">>, Vm],
            Users = r_to_list(ls_user:list()),
            spawn(fun () ->
                          [begin
                               ls_user:revoke_prefix(U, VmPrefix),
                               ls_user:revoke_prefix(U, ChannelPrefix)
                           end || U <- Users]
                  end),
            Roles = r_to_list(ls_role:list()),
            spawn(fun () ->
                          [begin
                               ls_role:revoke_prefix(G, VmPrefix),
                               ls_role:revoke_prefix(G, ChannelPrefix)
                           end || G <- Roles]
                  end),
            [sniffle_grouping:remove_element(G, Vm) ||
                G <- ?S:groupings(V)],
            ok;
        _ ->
            do_write(Vm, unregister)
    end.

r_to_list({ok, R}) ->
    R;
r_to_list(_) ->
    [].

%%--------------------------------------------------------------------
%% @doc Tries to creat a VM from a Package and dataset uuid. This
%%   function just creates the UUID and returns it after handing the
%%   data off to the create fsm.
%% @end
%%--------------------------------------------------------------------
-spec create(Package::binary(), Dataset::binary(), Config::fifo:config()) ->
                    {error, timeout} | {ok, fifo:uuid()}.
create(Package, Dataset, Config) ->
    UUID = fifo_utils:uuid(vm),
    %%we've to put pending here since undefined will cause a wrong call!
    do_write(UUID, register, <<"pooled">>),
    Secs = erlang:system_time(seconds),
    creating(UUID, {started, Secs}),
    created_at(UUID, Secs),
    Config0 = jsxd:from_list(Config),
    Config1 = jsxd:set(<<"uuid">>, UUID, Config0),
    %% We need to set this for the config to be correct later on
    %% but we do NOT use config2 as part of the create, we do
    %% however use Config1
    Config2 = jsxd:update(<<"networks">>,
                          fun (N) ->
                                  lists:sort(
                                    [#{<<"interface">> => Iface,
                                       <<"network">> => Net} ||
                                        {Iface, Net} <- maps:to_list(N)])
                          end, [], Config1),
    set_config(UUID, jsxd:delete([<<"owner">>], Config2)),
    state(UUID, <<"pooled">>),
    package(UUID, Package),
    case Dataset of
        {docker, DockerImage} ->
            {ok, Docker} = jsxd:get([<<"docker">>], Config1),
            set_docker(UUID, Docker),
            {ok, DockerID} = jsxd:get([<<"id">>], Docker),
            sniffle_2i:add(docker, DockerID, UUID),
            vm_type(UUID, docker),
            dataset(UUID, DockerImage);
        _ ->
            vm_type(UUID, zone),
            dataset(UUID, Dataset)
    end,
    libhowl:send(UUID, #{
                   <<"event">> => <<"update">>,
                   <<"data">> => #{
                       <<"config">> => Config1,
                       <<"package">> => Package}}),
    %% TODO: is this the right place?
    {ok, Creator} = jsxd:get([<<"owner">>], Config1),
    created_by(UUID, Creator),
    ls_user:grant(Creator, [<<"vms">>, UUID, <<"...">>]),
    ls_user:grant(Creator, [<<"channels">>, UUID, <<"join">>]),
    sniffle_create_pool:add(UUID, Package, Dataset, Config1),
    {ok, UUID}.

dry_run(Package, Dataset, Config) ->
    UUID = fifo_utils:uuid(vm),
    Ref = make_ref(),
    sniffle_create_fsm:create(UUID, Package, Dataset, Config, {self(), Ref}),
    receive
        {Ref, Res} ->
            {ok, Res}
    after
        5000 ->
            {error, timeout}
    end.


get_docker(DockerID) ->
    case sniffle_2i:get(docker, DockerID) of
        {ok, UUID} ->
            ?MODULE:get(UUID);
        E ->
            E
    end.


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
            case {is_creating(V), ?S:hypervisor(V),
                  ?S:deleting(V), ?S:state(V)} of
                {true, _, _, _} ->
                    {error, creating};
                {_, _, true, _} ->
                    finish_delete(User, Vm, V);
                {_, _, _, <<"storing">>} ->
                    state(Vm, <<"stored">>),
                    hypervisor(Vm, <<>>);
                {_, undefined, _, <<"stored">>} ->
                    finish_delete(User, Vm, V);
                {_, <<>>, _, _} ->
                    finish_delete(User, Vm, V);
                {_, <<"pooled">>, _, _} ->
                    finish_delete(User, Vm, V);
                {_, <<"pending">>, _, _} ->
                    finish_delete(User, Vm, V);
                {_, _, _, undefined} ->
                    finish_delete(User, Vm, V);
                {_, _, _, <<"failed-", _/binary>>} ->
                    finish_delete(User, Vm, V);
                {_, H, _, _} ->
                    state(Vm, <<"deleting">>),
                    deleting(Vm, true),
                    {Host, Port} = get_hypervisor(H),
                    resource_action(V, update, User,
                                    [{<<"request">>, <<"destroy">>}]),
                    libchunter:delete_machine(Host, Port, Vm)
            end;
        E ->
            E
    end.

finish_delete(User, Vm, V) ->
    [do_delete_backup(Vm, V, BID) || BID <- maps:keys(?S:backups(V))],
    Docker = ft_vm:docker(V),
    case jsxd:get([<<"id">>], Docker) of
        {ok, DockerID} ->
            sniffle_2i:delete(docker, DockerID);
        _ ->
            ok
    end,
    sniffle_vm:unregister(Vm),
    resource_action(V, destroy, User, []),
    libhowl:send(Vm, #{<<"event">> => <<"delete">>}),
    free_res(V).

free_res(V) ->
    free_package_res(ft_vm:package(V), ft_vm:owner(V)).


-spec free_package_res(binary(), binary()) ->
                              ok.
free_package_res(<<>>, _) ->
    ok;
free_package_res(_, <<>>) ->
    ok;
free_package_res(PkgID, OrgID) ->
    {ok, P} = sniffle_package:get(PkgID),
    RVs = maps:to_list(ft_package:org_resources(P)),
    [ls_org:resource_inc(OrgID, R, V) || {R, V} <- RVs],
    ok.

%%--------------------------------------------------------------------
%% @doc Triggers the start of a VM on the hypervisor.
%% @end
%%--------------------------------------------------------------------
-spec start(Vm::fifo:uuid()) ->
                   {error, timeout|creating|not_deployed} | not_found | ok.
start(Vm) ->
    case sniffle_vm:get(Vm) of
        {ok, V} ->
            case is_creating(V) of
                true ->
                    {error, creating};
                false ->
                    start_(Vm, V)
            end;
        E ->
            E
    end.

start_(UUID, V) ->
    case get_hypervisor(V) of
        not_found ->
            {error, not_deployed};
        {Server, Port} when is_list(Server),
                            is_integer(Port) ->
            libchunter:start_machine(Server, Port, UUID)
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
                  {error, timeout|creating} | not_found | ok.
stop(Vm, Options) ->
    case sniffle_vm:get(Vm) of
        {ok, V} ->
            case is_creating(V) of
                true ->
                    {error, creating};
                false ->
                    {Server, Port} = get_hypervisor(V),
                    libchunter:stop_machine(Server, Port, Vm, Options)
            end;
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
                    {error, timeout|creating} | not_found | ok.
reboot(Vm, Options) ->
    case sniffle_vm:get(Vm) of
        {ok, V} ->
            case is_creating(V) of
                true ->
                    {error, creating};
                false ->
                    {Server, Port} = get_hypervisor(V),
                    libchunter:reboot_machine(Server, Port, Vm, Options)
            end;
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
    case sniffle_vm:get(Vm) of
        {ok, V} ->
            case fetch_hypervisor(Vm) of
                {ok, Server, Port} ->
                    UR = #{<<"owner">> => Owner},
                    ok = libchunter:update_machine(
                           Server, Port, Vm, undefined, UR);
                _ ->
                    ok
            end,
            ls_org:execute_trigger(Owner, vm_create, Vm),
            case ft_vm:owner(V) of
                <<>> ->
                    ok;
                OldOwner ->
                    ls_org:reverse_trigger(OldOwner, vm_create, Vm)
            end,
            libhowl:send(Vm, #{
                           <<"event">> => <<"update">>,
                           <<"data">> =>
                               #{<<"owner">> => Owner}}),
            %% We can use the Vm object we got to end the accouting period
            %% for the old org
            resource_action(V, destroy, User, []),
            delete_network_2i(Vm),
            case owner(Vm, Owner) of
                ok ->
                    update_network_2i(Vm),
                    %% After a successful changed the owner we refetch the vm
                    %% to add accounting to the new org.
                    Opts = [{package, ft_vm:package(V)},
                            {dataset, encode_dataset(ft_vm:dataset(V))}],
                    {ok, V1} = sniffle_vm:get(Vm),
                    resource_action(V1, create, User, Opts),
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
-spec log(Vm :: fifo:uuid(), Log :: binary()) ->
                 {error, timeout} | not_found | ok.
log(Vm, Log) ->
    Timestamp = timestamp(),
    case do_write(Vm, log, [Timestamp, Log]) of
        ok ->
            libhowl:send(Vm, #{
                           <<"event">> => <<"log">>,
                           <<"data">> => #{
                               <<"log">> => Log,
                               <<"date">> => Timestamp}}),
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
            UUID = fifo_utils:uuid(),
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
                                  || {SUUID, Sn} <- maps:to_list(Snapshots)],
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

add_iprange_map(UUID, IP, Net) ->
    do_write(UUID, set_iprange_map, [IP, Net]).

remove_iprange_map(UUID, IP) ->
    do_write(UUID, set_iprange_map, [IP, delete]).

add_network_map(UUID, IP, Net) ->
    do_write(UUID, set_network_map, [IP, Net]).

remove_network_map(UUID, IP) ->
    do_write(UUID, set_network_map, [IP, delete]).

add_hostname_map(UUID, IP, Hostname) ->
    Res = do_write(UUID, set_hostname_map, [IP, Hostname]),
    update_network_2i(UUID),
    Res.

set_hostname(UUID, IFace, Hostname) ->
    {ok, V} = sniffle_vm:get(UUID),
    Config = ft_vm:config(V),
    Nics = jsxd:get([<<"networks">>], [], Config),
    case find_ip(Nics, IFace) of
        not_found ->
            not_found;
        {ok, IPs} ->
            IP = ft_iprange:parse_bin(IPs),
            remove_hostname_map(UUID, IP, V),
            case Hostname of
                <<>> ->
                    ok;
                _ ->
                    add_hostname_map(UUID, IP, Hostname)
            end
    end.

find_ip([], _IFace) ->
    not_found;
find_ip([I | R], IFace) ->
    case jsxd:get([<<"interface">>], I) of
        {ok, IFace} ->
            jsxd:get([<<"ip">>], I);
        _ ->
            find_ip(R, IFace)
    end.


remove_hostname_map(UUID, IP) ->
    {ok, V} = sniffle_vm:get(UUID),
    remove_hostname_map(UUID, IP, V).

remove_hostname_map(UUID, IP, V) ->
    case ft_vm:owner(V) of
        <<>> ->
            ok;
        Owner ->
            Map = ft_vm:hostname_map(V),
            case maps:find(IP, Map) of
                {ok, Hostname} ->
                    sniffle_hostname:remove(Hostname, Owner, {UUID, IP});
                _ ->
                    ok
            end
    end,
    do_write(UUID, set_hostname_map, [IP, delete]).

trigger_fw_change(UUID) ->
    {ok, VM} = sniffle_vm:get(UUID),
    {Host, Port} = get_hypervisor(VM),
    libchunter:update_fw(Host, Port, UUID).


add_fw_rule(UUID, V) ->
    R = do_write(UUID, add_fw_rule, [V]),
    trigger_fw_change(UUID),
    R.

remove_fw_rule(UUID, V) ->
    R = do_write(UUID, remove_fw_rule, [V]),
    trigger_fw_change(UUID),
    R.

?SET(remove_grouping).
?SET(add_grouping).
?SET(set_metadata).
?SET(set_info).
set_config(UUID, Config) ->
    case jsxd:get([<<"dataset">>], Config) of
        {ok, D} when D =/= ?EMPTY ->
            dataset(UUID, D);
        _ ->
            ok
    end,
    case jsxd:get([<<"alias">>], Config) of
        {ok, A} when A =/= ?EMPTY ->
            alias(UUID, A);
        _ ->
            ok
    end,
    case jsxd:get([<<"owner">>], Config) of
        {ok, O} when O =/= ?EMPTY ->
            owner(UUID, O);
        _ ->
            ok
    end,
    case jsxd:get([<<"creator">>], Config) of
        {ok, C} when C =/= ?EMPTY ->
            created_by(UUID, C);
        _ ->
            ok
    end,
    case jsxd:get([<<"package">>], Config) of
        {ok, P} when P =/= ?EMPTY ->
            package(UUID, P);
        _ ->
            ok
    end,
    do_write(UUID, set_config, [Config]).
?SET(set_backup).
?SET(set_snapshot).
?SET(set_service).
?SET(set_docker).

?SET(state).
?SET(creating).
deleting(UUID, V)
  when V =:= true;
       V =:= false ->
    do_write(UUID, deleting, [V]).
?SET(alias).
?SET(owner).
?SET(dataset).
?SET(package).
?SET(hypervisor).
?SET(vm_type).
?SET(created_at).
?SET(created_by).

%%%===================================================================
%%% Internal Functions
%%%===================================================================

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
    erlang:system_time(micro_seconds).

-spec children(maps:map(), binary()) ->
                      [binary()].
children(Backups, Parent) ->
    children(maps:to_list(Backups), Parent, false).

-spec children([{binary(), maps:map()}], binary(), boolean()) ->
                      [binary()].
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
                         [<<UUID/binary, "/", BID/binary, ".xml">>
                              | maps:keys(Files)];
                     _ ->
                         maps:keys(Files)
                 end,
            [sniffle_s3:delete(snapshot, F) || F <- Fs, is_binary(F)];
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
            libhowl:send(UUID, #{<<"event">> => <<"backup">>,
                                 <<"data">> => #{
                                     <<"action">> => <<"deleted">>,
                                     <<"uuid">> => BID}})
    end.

%% resource_action(UUID, Action, Opts) ->
%%     resource_action(UUID, Action, undefined, Opts).

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
            case Action of
                create ->
                    ls_acc:create(Org, UUID, T, Opts1);
                destroy ->
                    ls_acc:destroy(Org, UUID, T, Opts1);
                Event ->
                    Event1 = atom_to_binary(Event, utf8),
                    ls_acc:update(Org, UUID, T, [{<<"event">>, Event1} | Opts1])
            end
    end.

is_creating(V) ->
    case ?S:creating(V) of
        false ->
            false;
        {_State, {MegaSecs, Secs, _MicroSecs}} ->
            T0 = MegaSecs*1000000 + Secs,
            case erlang:system_time(seconds) - T0 of
                T when T > 120 -> %% 2 minutes
                    false;
                _ ->
                    true
            end;
        {_State, T0} ->
            case erlang:system_time(seconds) -  T0 of
                T when T > 120 -> %% 2 minutes
                    false;
                _ ->
                    true
            end;
        _ ->
            true
    end.

encode_dataset({docker, D}) ->
    <<"docker:", D/binary>>;
encode_dataset(D) when is_binary(D) ->
    D.


update_network_2i(UUID) ->
    {ok, V} = sniffle_vm:get(UUID),
    case ft_vm:owner(V) of
        <<>> ->
            ok;
        Owner ->
            [sniffle_hostname:add(Hostname, Owner, {UUID, IP})
             || {IP, Hostname} <- maps:to_list(ft_vm:hostname_map(V))]
    end.

delete_network_2i(UUID) ->
    {ok, V} = sniffle_vm:get(UUID),
    case ft_vm:owner(V) of
        <<>> ->
            ok;
        Owner ->
            [sniffle_hostname:remove(Hostname, Owner, {UUID, IP})
             || {IP, Hostname} <- maps:to_list(ft_vm:hostname_map(V))]
    end.
