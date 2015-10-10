-module(sniffle_watchdog_app).

-behaviour(application).


%% Application callbacks
-export([start/2, stop/1]).

%% ===================================================================
%% Application callbacks
%% ===================================================================

start(_StartType, _StartArgs) ->
    R = sniffle_watchdog_sup:start_link(),
    lager_watchdog_srv:set_version(sniffle_version:v()),
    R.

stop(_State) ->
    ok.
