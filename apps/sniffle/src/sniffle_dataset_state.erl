%%% @author Heinz Nikolaus Gies <heinz@licenser.net>
%%% @copyright (C) 2012, Heinz Nikolaus Gies
%%% @doc
%%%
%%% @end
%%% Created : 23 Aug 2012 by Heinz Nikolaus Gies <heinz@licenser.net>

-module(sniffle_dataset_state).

-include("sniffle.hrl").

-export([
         new/0,
         load/1,
         load/2,
         uuid/1,
         name/2,
         set/4,
         set/3,
         getter/2
        ]).

-ignore_xref([load/2, load/1, set/4, set/3, getter/2, uuid/1]).

getter(#sniffle_obj{val=S0}, Resource) ->
    jsxd:get(Resource, 0, statebox:value(S0)).

uuid(Vm) ->
    {ok, UUID} = jsxd:get(<<"dataset">>, statebox:value(Vm)),
    UUID.

load(_, D) ->
    load(D).

load(#dataset{name = Name,
              attributes = Attributes}) ->
    jsxd:thread([{set, <<"name">>, Name},
                 {merge, jsxd:from_list(dict:to_list(Attributes))}],
                new());

load(Dataset) ->
    Dataset.

new() ->
    jsxd:set(<<"version">>, <<"0.1.0">>, jsxd:new()).

name(Name, Dataset) ->
    jsxd:set(<<"name">>, Name, Dataset).

set(_ID, Attribute, Value, D) ->
    statebox:modify({fun set/3, [Attribute, Value]}, D).

set(Attribute, delete, Dataset) ->
    jsxd:delete(Attribute, Dataset);

set(Attribute, Value, Dataset) ->
    jsxd:set(Attribute, Value, Dataset).
