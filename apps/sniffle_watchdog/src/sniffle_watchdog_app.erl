-module(sniffle_watchdog_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

%% ===================================================================
%% Application callbacks
%% ===================================================================

start(_StartType, _StartArgs) ->
    case riak_ensemble_manager:cluster() of
        [] ->
            ok = riak_ensemble_manager:enable();
        _ ->
            ok
    end,
    sniffle_watchdog_sup:start_link().

stop(_State) ->
    ok.
