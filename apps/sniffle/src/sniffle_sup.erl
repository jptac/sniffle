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
    VNode = {
      sniffle_vnode_master,
      { riak_core_vnode_master, start_link, [sniffle_vnode]},
      permanent, 5000, worker, [riak_core_vnode_master]},

    %% ===================================================================
    %% AAE
    %% ===================================================================
    EntropyManager =
        {sniffle_entropy_manager,
         {riak_core_entropy_manager, start_link,
          [sniffle, sniffle_vnode]},
         permanent, 30000, worker, [riak_core_entropy_manager]},

    {ok,
     {{one_for_one, 5, 10},
      [VNode, EntropyManager]}}.
