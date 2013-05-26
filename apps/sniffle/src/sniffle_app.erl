-module(sniffle_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

%% ===================================================================
%% Application callbacks
%% ===================================================================

start(_StartType, _StartArgs) ->
    case sniffle_sup:start_link() of
        {ok, Pid} ->
            ok = riak_core:register([{vnode_module, sniffle_hypervisor_vnode}]),
            ok = riak_core:register([{vnode_module, sniffle_vm_vnode}]),
            ok = riak_core:register([{vnode_module, sniffle_iprange_vnode}]),
            ok = riak_core:register([{vnode_module, sniffle_package_vnode}]),
            ok = riak_core:register([{vnode_module, sniffle_dataset_vnode}]),
            ok = riak_core:register([{vnode_module, sniffle_img_vnode}]),
            ok = riak_core:register([{vnode_module, sniffle_dtrace_vnode}]),

            ok = riak_core_ring_events:add_guarded_handler(sniffle_ring_event_handler, []),
            ok = riak_core_node_watcher_events:add_guarded_handler(sniffle_node_event_handler, []),

            ok = riak_core_node_watcher:service_up(sniffle_hypervisor, self()),
            ok = riak_core_node_watcher:service_up(sniffle_vm, self()),
            ok = riak_core_node_watcher:service_up(sniffle_iprange, self()),
            ok = riak_core_node_watcher:service_up(sniffle_package, self()),
            ok = riak_core_node_watcher:service_up(sniffle_dataset, self()),
            ok = riak_core_node_watcher:service_up(sniffle_img, self()),
            ok = riak_core_node_watcher:service_up(sniffle_dtrace, self()),

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
