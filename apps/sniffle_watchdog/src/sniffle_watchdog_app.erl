-module(sniffle_watchdog_app).

-behaviour(application).

-include_lib("sniffle/include/sniffle_version.hrl").


%% Application callbacks
-export([start/2, stop/1]).

%% ===================================================================
%% Application callbacks
%% ===================================================================

start(_StartType, _StartArgs) ->
    R = sniffle_watchdog_sup:start_link(),
    lager_watchdog_srv:set_version(?VERSION),
    R.

stop(_State) ->
    ok.
