-module(sniffle_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init(_Args) ->
    %% ===================================================================
    %% VNodes
    %% ===================================================================

    VHypervisor = {
      sniffle_hypervisor_vnode_master,
      { riak_core_vnode_master, start_link, [sniffle_hypervisor_vnode]},
      permanent, 5000, worker, [sniffle_hypervisor_vnode_master]},
    VVM = {
      sniffle_vm_vnode_master,
      { riak_core_vnode_master, start_link, [sniffle_vm_vnode]},
      permanent, 5000, worker, [sniffle_vm_vnode_master]},
    VIprange = {
      sniffle_iprange_vnode_master,
      { riak_core_vnode_master, start_link, [sniffle_iprange_vnode]},
      permanent, 5000, worker, [sniffle_iprange_vnode_master]},
    VNetwork = {
      sniffle_network_vnode_master,
      { riak_core_vnode_master, start_link, [sniffle_network_vnode]},
      permanent, 5000, worker, [sniffle_network_vnode_master]},
    VDataset = {
      sniffle_dataset_vnode_master,
      { riak_core_vnode_master, start_link, [sniffle_dataset_vnode]},
      permanent, 5000, worker, [sniffle_dataset_vnode_master]},
    VGrouping = {
      sniffle_grouping_vnode_master,
      { riak_core_vnode_master, start_link, [sniffle_grouping_vnode]},
      permanent, 5000, worker, [sniffle_grouping_vnode_master]},
    Img = case sniffle_opt:get(storage, general, backend, large_data_backend, internal) of
              internal ->
                  [{sniffle_img_vnode_master,
                    {riak_core_vnode_master, start_link, [sniffle_img_vnode]},
                    permanent, 5000, worker, [sniffle_img_vnode_master]},
                   {sniffle_img_entropy_manager,
                    {riak_core_entropy_manager, start_link,
                     [sniffle_img, sniffle_img_vnode]},
                    permanent, 30000, worker, [riak_core_entropy_manager]}];
              O ->
                  lager:info("[img] VNode disabled since images are handed by ~p", [O]),
                  []
          end,

    VDTrace = {
      sniffle_dtrace_vnode_master,
      { riak_core_vnode_master, start_link, [sniffle_dtrace_vnode]},
      permanent, 5000, worker, [sniffle_dtrace_vnode_master]},
    VPackage = {
      sniffle_package_vnode_master,
      { riak_core_vnode_master, start_link, [sniffle_package_vnode]},
      permanent, 5000, worker, [sniffle_package_vnode_master]},

    %% ===================================================================
    %% FSMs
    %% ===================================================================
    WriteFSMs = {
      sniffle_entity_write_fsm_sup,
      { sniffle_entity_write_fsm_sup, start_link, []},
      permanent, infinity, supervisor, [sniffle_entity_write_fsm_sup]},

    CoverageFSMs = {
      sniffle_coverage_sup,
      { sniffle_coverage_sup, start_link, []},
      permanent, infinity, supervisor, [sniffle_coverage_sup]},

    ReadFSMs = {
      sniffle_entity_read_fsm_sup,
      {sniffle_entity_read_fsm_sup, start_link, []},
      permanent, infinity, supervisor, [sniffle_entity_read_fsm_sup]},

    %% ===================================================================
    %% General
    %% ===================================================================
    CreateFSMs = {
      sniffle_create_fsm_sup,
      {sniffle_create_fsm_sup, start_link, []},
      permanent, infinity, supervisor, [sniffle_create_fsm_sup]},

    DTrace = {
      sniffle_dtrace_sup,
      {sniffle_dtrace_sup, start_link, []},
      permanent, infinity, supervisor, [sniffle_dtrace_sup]},

    Watchdog = {
      sniffle_watchdog_sup,
      {sniffle_watchdog_sup, start_link, []},
      permanent, infinity, supervisor, [sniffle_watchdog_sup]},

    %% ===================================================================
    %% AAE
    %% ===================================================================
    EntropyManagerHypervisor =
        {sniffle_hypervisor_entropy_manager,
         {riak_core_entropy_manager, start_link,
          [sniffle_hypervisor, sniffle_hypervisor_vnode]},
         permanent, 30000, worker, [riak_core_entropy_manager]},

    EntropyManagerVm =
        {sniffle_vm_entropy_manager,
         {riak_core_entropy_manager, start_link,
          [sniffle_vm, sniffle_vm_vnode]},
         permanent, 30000, worker, [riak_core_entropy_manager]},

    EntropyManagerIPRange =
        {sniffle_iprange_entropy_manager,
         {riak_core_entropy_manager, start_link,
          [sniffle_iprange, sniffle_iprange_vnode]},
         permanent, 30000, worker, [riak_core_entropy_manager]},

    EntropyManagerNetwork =
        {sniffle_network_entropy_manager,
         {riak_core_entropy_manager, start_link,
          [sniffle_network, sniffle_network_vnode]},
         permanent, 30000, worker, [riak_core_entropy_manager]},

    EntropyManagerDataset =
        {sniffle_dataset_entropy_manager,
         {riak_core_entropy_manager, start_link,
          [sniffle_dataset, sniffle_dataset_vnode]},
         permanent, 30000, worker, [riak_core_entropy_manager]},

    EntropyManagerDtrace =
        {sniffle_dtrace_entropy_manager,
         {riak_core_entropy_manager, start_link,
          [sniffle_dtrace, sniffle_dtrace_vnode]},
         permanent, 30000, worker, [riak_core_entropy_manager]},

    EntropyManagerPackage =
        {sniffle_package_entropy_manager,
         {riak_core_entropy_manager, start_link,
          [sniffle_package, sniffle_package_vnode]},
         permanent, 30000, worker, [riak_core_entropy_manager]},

    EntropyManagerGrouping =
        {sniffle_grouping_entropy_manager,
         {riak_core_entropy_manager, start_link,
          [sniffle_grouping, sniffle_grouping_vnode]},
         permanent, 30000, worker, [riak_core_entropy_manager]},

    {ok,
     {{one_for_one, 5, 10},
      [{sniffle_create_pool, {sniffle_create_pool, start_link, []},
        permanent, 5000, worker, [sniffle_create_pool]},
       %% General FSM's
       CoverageFSMs, WriteFSMs, ReadFSMs,
       %% Logic
       CreateFSMs, DTrace, Watchdog,
       %% VNodes
       VHypervisor, VVM, VIprange, VDataset, VPackage, VDTrace, VNetwork,
       VGrouping,
       %% AAE
       EntropyManagerVm, EntropyManagerHypervisor, EntropyManagerIPRange,
       EntropyManagerNetwork, EntropyManagerDataset, EntropyManagerDtrace,
       EntropyManagerPackage, EntropyManagerGrouping] ++ Img}}.
