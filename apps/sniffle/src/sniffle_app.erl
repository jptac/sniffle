-module(sniffle_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

-define(SRV(VNode, Srv),
        ok = riak_core:register([{vnode_module, VNode}]),
        ok = riak_core_node_watcher:service_up(Srv, self())).

-define(SRV_WITH_AAE(VNode, Srv),
        ?SRV(VNode, Srv),
        ok = riak_core_capability:register({Srv, anti_entropy},
                                           [enabled_v1, disabled],
                                           enabled_v1)).

%% ===================================================================
%% Application callbacks
%% ===================================================================

start(_StartType, _StartArgs) ->
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
    case sniffle_sup:start_link() of
        {ok, Pid} ->
            ok = riak_core_ring_events:add_guarded_handler(sniffle_ring_event_handler, []),
            ok = riak_core_node_watcher_events:add_guarded_handler(sniffle_node_event_handler, []),

            ?SRV_WITH_AAE(sniffle_hypervisor_vnode, sniffle_hypervisor),
            ?SRV_WITH_AAE(sniffle_vm_vnode, sniffle_vm),
            ?SRV_WITH_AAE(sniffle_iprange_vnode, sniffle_iprange),
            ?SRV_WITH_AAE(sniffle_package_vnode, sniffle_package),
            ?SRV_WITH_AAE(sniffle_dataset_vnode, sniffle_dataset),
            ?SRV_WITH_AAE(sniffle_img_vnode, sniffle_img),
            ?SRV_WITH_AAE(sniffle_network_vnode, sniffle_network),
            ?SRV_WITH_AAE(sniffle_dtrace_vnode, sniffle_dtrace),

            statman_server:add_subscriber(statman_aggregator),
            sniffle_snmp_handler:start(),
            case application:get_env(newrelic,license_key) of
                undefined ->
                    ok;
                _ ->
                    newrelic_poller:start_link(fun newrelic_statman:poll/0)
            end,

            {ok, Pid};
        {error, Reason} ->
            {error, Reason}
    end.

stop(_State) ->
    ok.
