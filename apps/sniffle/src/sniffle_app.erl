-module(sniffle_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1, load/0]).


check_grid() ->
    case length(redgrid:nodes()) of
	1 ->
	    timer:sleep(100),
	    check_grid();
	_ ->
	    application:stop(gproc),
	    application:start(gproc),
	    sniffle_server:reregister()
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
    application:start(sniffle),
    ok.    

%% ===================================================================
%% Application callbacks
%% ===================================================================

start(_StartType, _StartArgs) ->
    spawn(fun check_grid/0),
    sniffle_sup:start_link().

stop(_State) ->
    ok.
