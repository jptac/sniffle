-module(sniffle_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1, load/0]).

load() ->
    application:start(sasl),
    application:start(alog),
    application:start(lager),
    application:start(crypto),
    application:start(nodefinder),
    application:start(gproc),
    application:start(public_key),
    application:start(ssl),
    application:start(lhttpc),
    application:start(erllibcloudapi),
    application:start(redo),
    application:start(uuid),
    application:start(libsnarl),
    application:start(libchunter),
    application:start(sniffle),
    ok.    

%% ===================================================================
%% Application callbacks
%% ===================================================================

start(_StartType, _StartArgs) ->
    sniffle_sup:start_link().

stop(_State) ->
    ok.
