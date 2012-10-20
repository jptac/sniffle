%%% @author Heinz Nikolaus Gies <heinz@licenser.net>
%%% @copyright (C) 2012, Heinz Nikolaus Gies
%%% @doc
%%%
%%% @end
%%% Created : 23 Aug 2012 by Heinz Nikolaus Gies <heinz@licenser.net>

-module(sniffle_package_state).

-include("sniffle.hrl").

-export([
	 new/0,
	 name/2,
	 attribute/3
	]).

new() ->
    #package{attributes = dict:new()}.


name(Name, Package) ->
    Package#package{name = Name}.

attribute(Attribute, Value, Package) ->
    Package#package{
      attributes = dict:store(Attribute, Value, Package#package.attributes)
     }.
