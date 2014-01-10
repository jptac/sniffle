%%% @author Heinz Nikolaus Gies <heinz@licenser.net>
%%% @copyright (C) 2012, Heinz Nikolaus Gies
%%% @doc
%%%
%%% @end
%%% Created : 23 Aug 2012 by Heinz Nikolaus Gies <heinz@licenser.net>

-module(sniffle_package_state).

-include("sniffle.hrl").

-export([
         load/1,
         new/0,
         uuid/2,
         name/2,
         set/3,
         getter/2
        ]).

-ignore_xref([load/1, set/3, getter/2]).

getter(#sniffle_obj{val=S0}, Resource) ->
    jsxd:get(Resource, 0, statebox:value(S0)).

load(#package{name = Name,
              attributes = Attributes}) ->
    jsxd:thread([{set, <<"name">>, Name},
                 {merge, jsxd:from_list(dict:to_list(Attributes))}],
                new());

load(Package) ->
    Package.

new() ->
    jsxd:set(<<"version">>, <<"0.1.0">>,
             jsxd:new()).

name(Name, Package) ->
    jsxd:set(<<"name">>, Name, Package).

uuid(UUID, Package) ->
    jsxd:set(<<"uuid">>, UUID, Package).

set(Attribute, delete, Package) ->
    jsxd:delete(Attribute, Package);

set(Attribute, Value, Package) ->
    jsxd:set(Attribute, Value, Package).
