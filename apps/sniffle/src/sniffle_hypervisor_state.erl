%%% @author Heinz Nikolaus Gies <heinz@licenser.net>
%%% @copyright (C) 2012, Heinz Nikolaus Gies
%%% @doc
%%%
%%% @end
%%% Created : 23 Aug 2012 by Heinz Nikolaus Gies <heinz@licenser.net>

-module(sniffle_hypervisor_state).

-include("sniffle.hrl").

-export([
         load/1,
         new/0,
         uuid/2,
         host/2,
         port/2,
         set/3,
         getter/2
        ]).

-ignore_xref([load/1, set/3, getter/2]).

getter(#sniffle_obj{val=S0}, Resource) ->
    jsxd:get(Resource, 0, statebox:value(S0)).

load(H) ->
    H.

new() ->
    jsxd:set(<<"version">>, <<"0.1.0">>, jsxd:new()).

uuid(Name, Hypervisor) ->
    jsxd:set(<<"uuid">>, Name, Hypervisor).

host(Host, Hypervisor) ->
    jsxd:set(<<"host">>, Host, Hypervisor).

port(Port, Hypervisor) ->
    jsxd:set(<<"port">>, Port, Hypervisor).

set(Resource, delete, Hypervisor) ->
    jsxd:delete(Resource, Hypervisor);

set(Resource, Value, Hypervisor) ->
    jsxd:set(Resource, Value, Hypervisor).
