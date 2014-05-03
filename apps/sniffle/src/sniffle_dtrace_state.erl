%%% @author Heinz Nikolaus Gies <heinz@licenser.net>
%%% @copyright (C) 2012, Heinz Nikolaus Gies
%%% @doc
%%%
%%% @end
%%% Created : 23 Aug 2012 by Heinz Nikolaus Gies <heinz@licenser.net>

-module(sniffle_dtrace_state).

-include("sniffle.hrl").

-export([
         new/0,
         load/2,
         uuid/1,
         set/4,
         set/3,
         getter/2
        ]).

-ignore_xref([load/2, set/4, set/3, getter/2, uuid/1]).

getter(#sniffle_obj{val=S0}, Resource) ->
    jsxd:get(Resource, 0, statebox:value(S0)).

uuid(Vm) ->
    {ok, UUID} = jsxd:get(<<"uuid">>, statebox:value(Vm)),
    UUID.

load(_, D) ->
    load(D).

load(Dtrace) ->
    Dtrace.

new() ->
    jsxd:set(<<"version">>, <<"0.1.0">>,
             jsxd:new()).

set(_ID, Attribute, Value, D) ->
    statebox:modify({fun set/3, [Attribute, Value]}, D).

set(Attribute, delete, Dtrace) ->
    jsxd:delete(Attribute, Dtrace);

set(Attribute, Value, Dtrace) ->
    jsxd:set(Attribute, Value, Dtrace).
