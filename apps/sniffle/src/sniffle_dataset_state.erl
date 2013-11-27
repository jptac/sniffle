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
         name/2,
         set/3
        ]).

-ignore_xref([load/1, set/3]).

load(#dataset{name = Name,
              attributes = Attributes}) ->
    jsxd:thread([{set, <<"name">>, Name},
                 {merge, jsxd:from_list(dict:to_list(Attributes))}],
                new());

load(Dataset) ->
    Dataset.

new() ->
    jsxd:set(<<"version">>, <<"0.1.0">>,
             jsxd:new()).

name(Name, Dataset) ->
    jsxd:set(<<"name">>, Name, Dataset).

set(Attribute, delete, Dataset) ->
    jsxd:delete(Attribute, Dataset);

set(Attribute, Value, Dataset) ->
    jsxd:set(Attribute, Value, Dataset).
