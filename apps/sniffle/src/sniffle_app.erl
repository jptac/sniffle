-module(sniffle_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1, load/0]).


check_grid() ->
    timer:sleep(100),
    case length(redgrid:nodes()) of
	X when X =< 1 ->
	    check_grid();
	_ ->
	    application:stop(gproc),
	    application:start(gproc),
	    ok
    end.

load() ->
    application:start(sasl),
    application:start(alog),
    application:start(redgrid),
    application:start(gproc),
    application:start(crypto),
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
    check_grid(),
    sniffle_sup:start_link().

stop(_State) ->
    ok.
