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
	 name/2,
	 attribute/3
	]).

new() ->
    #dataset{}.


name(Name, Dataset) ->
    Dataset#dataset{name = Name}.

attribute(Attribute, Value, Dataset) ->
    Dataset#dataset{
      attributes = dict:store(Attribute, Value, Dataset#dataset.attributes)
     }.
