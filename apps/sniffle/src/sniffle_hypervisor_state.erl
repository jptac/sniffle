%%% @author Heinz Nikolaus Gies <heinz@licenser.net>
%%% @copyright (C) 2012, Heinz Nikolaus Gies
%%% @doc
%%%
%%% @end
%%% Created : 23 Aug 2012 by Heinz Nikolaus Gies <heinz@licenser.net>

-module(sniffle_hypervisor_state).

-include("sniffle.hrl").

-export([
         new/0,
         name/2,
         host/2,
         port/2,
         resource/3
        ]).

new() ->
    jsxd:set(<<"version">>, <<"0.1.0">>, jsxd:new()).

name(Name, Hypervisor) ->
    jsxd:set(<<"name">>, Name, Hypervisor).

host(Host, Hypervisor) ->
    jsxd:set(<<"host">>, Host, Hypervisor).

port(Port, Hypervisor) ->
    jsxd:set(<<"port">>, Port, Hypervisor).

resource(Resource, Value, Hypervisor) ->
    jsxd:set(re:split(Resource, <<"\\.">>), Value, Hypervisor).
