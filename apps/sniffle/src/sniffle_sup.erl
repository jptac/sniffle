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
    WriteFSMs = { 
      sniffle_entity_write_fsm_sup,
      { sniffle_entity_write_fsm_sup, start_link, []},
      permanent, infinity, supervisor, [sniffle_entity_write_fsm_sup]},
    CoverageFSMs = { 
      sniffle_entity_coverage_fsm_sup,
      { sniffle_entity_coverage_fsm_sup, start_link, []},
      permanent, infinity, supervisor, [sniffle_entity_coverage_fsm_sup]},
    ReadFSMs = {
      sniffle_entity_read_fsm_sup,
      {sniffle_entity_read_fsm_sup, start_link, []},
      permanent, infinity, supervisor, [sniffle_entity_read_fsm_sup]},
    CreateFSMs = {
      sniffle_create_fsm_sup,
      {sniffle_create_fsm_sup, start_link, []},
      permanent, infinity, supervisor, [sniffle_create_fsm_sup]},
    
    { ok,
      { {one_for_one, 5, 10},
	[VHypervisor, VVM, VIprange,
	 WriteFSMs, ReadFSMs, CoverageFSMs,
	 CreateFSMs]}}.
