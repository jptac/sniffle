-module(sniffle_dtrace_sup).

-behaviour(supervisor).

%% API
-export([start_link/0, start_child/1]).

%% Supervisor callbacks
-export([init/1]).


-ignore_xref([start_link/0]).

%% ===================================================================
%% API functions
%% ===================================================================

start_child(UUID) ->
    supervisor:start_child(?MODULE, [UUID]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init([]) ->
    {ok, {{simple_one_for_one, 5, 10},
          [{sniffle_dtrace_server,
            {sniffle_dtrace_server, start_link, []},
            temporary, 5000, worker, [sniffle_dtrace_server]}]}}.
