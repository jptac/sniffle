-module(sniffle_app).

-behaviour(application).

-include("sniffle_version.hrl").

%% Application callbacks
-export([start/2, stop/1]).

-define(SRV(VNode, Srv),
        ok = riak_core:register([{vnode_module, VNode}]),
        ok = riak_core_node_watcher:service_up(Srv, self())).

-define(SRV_WITH_AAE(VNode, Srv),
        ?SRV(VNode, Srv),
        case application:get_env(sniffle, list_to_atom(atom_to_list(Srv) ++ "_aae"), true) of
            true ->
                ok = riak_core_capability:register({Srv, anti_entropy},
                                                   [enabled_v1, disabled],
                                                   enabled_v1);
            _ ->
                ok
        end).

%% ===================================================================
%% Application callbacks
%% ===================================================================

start(_StartType, _StartArgs) ->
    riak_core_entropy_info:create_table(),
    case application:get_env(fifo_db, db_path) of
        {ok, _} ->
            ok;
        undefined ->
            case application:get_env(sniffle, db_path) of
                {ok, P} ->
                    application:set_env(fifo_db, db_path, P);
                _ ->
                    application:set_env(fifo_db, db_path, "/var/db/sniffle")
            end
    end,
    case application:get_env(fifo_db, backend) of
        {ok, _} ->
            ok;
        undefined ->
            application:set_env(fifo_db, backend, fifo_db_hanoidb)
    end,
    init_folsom(),
    case sniffle_sup:start_link() of
        {ok, Pid} ->
            ok = riak_core_ring_events:add_guarded_handler(sniffle_ring_event_handler, []),
            ok = riak_core_node_watcher_events:add_guarded_handler(sniffle_node_event_handler, []),

            ?SRV_WITH_AAE(sniffle_hypervisor_vnode, sniffle_hypervisor),
            ?SRV_WITH_AAE(sniffle_vm_vnode, sniffle_vm),
            ?SRV_WITH_AAE(sniffle_iprange_vnode, sniffle_iprange),
            ?SRV_WITH_AAE(sniffle_package_vnode, sniffle_package),
            ?SRV_WITH_AAE(sniffle_dataset_vnode, sniffle_dataset),
            ?SRV_WITH_AAE(sniffle_grouping_vnode, sniffle_grouping),
            case sniffle_opt:get(storage, general, backend, large_data_backend, internal) of
                internal ->
                    ?SRV_WITH_AAE(sniffle_img_vnode, sniffle_img);
                O ->
                    lager:info("[img] VNode disabled since images are handed by ~p", [O])
            end,

            ?SRV_WITH_AAE(sniffle_network_vnode, sniffle_network),
            ?SRV_WITH_AAE(sniffle_dtrace_vnode, sniffle_dtrace),
            timer:apply_after(2000, sniffle_opt, update, []),
            sniffle_snmp_handler:start(),
            {ok, Pid};
        {error, Reason} ->
            {error, Reason}
    end.

stop(_State) ->
    ok.


init_folsom() ->
    DBMs = [fold_keys, fold, get, put, delete, transact],
    Basic = [wipe, get, list, list_all, sync_repair, set_metadata],
    Datasets = Basic ++
        [description, disk_driver, homepage, image_size, name,
         networks, nic_driver, os, type, zone_type, users, version,
         kernel_version, sha1, status, imported, remove_requirement,
         add_requirement, create, delete],
    Dtraces = Basic ++
        [create, delete, set, name, uuid, script, set_config],
    Groupings = Basic ++
        [create, delete, add_element, add_grouping, remove_element,
         remove_grouping, set_config],
    HVs = Basic ++
        [set_resource, set_characteristic, set_pool, set_service,
         alias, etherstubs, host, networks, path, port, sysinfo, uuid, version,
         virtualisation, register, unregister],
    IPRs = Basic ++
        [create, delete, lookup, name, uuid, network, netmask, gateway,
         tag, vlan, release_ip, claim_ip],
    Nets = Basic ++
        [create, delete, name, uuid, add_iprange, remove_iprange,
         set],
    Pkgs = Basic ++
        [create, delete, blocksize, compression, cpu_cap,
         cpu_shares, max_swap, name, quota, ram, uuid, zfs_io_priority,
         remove_requirement, add_requirement],
    VMs = Basic ++
        [register, unregister, log, set_network_map, remove_grouping,
         add_grouping, set_info, set_config, set_backup,
         set_snapshot, set_service, state, alias, owner, dataset, package,
         hypervisor],
    [folsom_metrics:new_histogram(Name, slide, 60) ||
        Name <-
            [{fifo_db, M} || M <- DBMs] ++
            [{sniffle, dataset, M} || M <- Datasets] ++
            [{sniffle, dtrace, M} || M <- Dtraces] ++
            [{sniffle, grouping, M} || M <- Groupings] ++
            [{sniffle, hypervisor, M} || M <- HVs] ++
            [{sniffle, iprange, M} || M <- IPRs] ++
            [{sniffle, network, M} || M <- Nets] ++
            [{sniffle, package, M} || M <- Pkgs] ++
            [{sniffle, vm, M} || M <- VMs] ++
            []
    ].
