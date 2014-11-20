-module(sniffle_create_sup).

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

init([]) ->
    CreateFSMs = {
      sniffle_create_fsm_sup,
      {sniffle_create_fsm_sup, start_link, []},
      permanent, infinity, supervisor, [sniffle_create_fsm_sup]},
    Pool = {sniffle_create_pool, {sniffle_create_pool, start_link, []},
            permanent, 5000, worker, [sniffle_create_pool]},
    {ok, { {one_for_one, 5, 10}, [CreateFSMs, Pool]} }.

