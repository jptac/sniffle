
-module(sniffle_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

%% Helper macro for declaring children of supervisor
-define(CHILD(I, Type), {I, {I, start_link, []}, permanent, 5000, Type, [I]}).

%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init([]) ->
    Url = get_env_default(url, "redis://localhost:6379/"),
    Opts = redo_uri:parse(Url),
    {ok, { {one_for_one, 5, 10}, [{redo, {redo, start_link, [Opts]}, permanent, 5000, worker, [redo]},
				  ?CHILD(sniffle_host_sup, supervisor),
				  ?CHILD(sniffle_server, worker)]}}.

get_env_default(Key, Default) ->
    case  application:get_env(redgrid, Key) of
	{ok, Res} ->
	    Res;
	_ ->
	    Default
    end.
