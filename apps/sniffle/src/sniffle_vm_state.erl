%%% @author Heinz Nikolaus Gies <heinz@licenser.net>
%%% @copyright (C) 2012, Heinz Nikolaus Gies
%%% @doc
%%%
%%% @end
%%% Created : 23 Aug 2012 by Heinz Nikolaus Gies <heinz@licenser.net>

-module(sniffle_vm_state).

-include("sniffle.hrl").

-export([
	 new/0,
	 uuid/2,
	 alias/2,
	 hypervisor/2,
	 attribute/3
	]).

new() ->
    #vm{attributes=dict:new()}.


uuid(UUID, Vm) ->
    Vm#vm{uuid = UUID}.

alias(Alias, Vm) ->
    Vm#vm{alias = Alias}.

hypervisor(Hypervisor, Vm) ->
    Vm#vm{hypervisor = Hypervisor}.

attribute(Attribute, Value, Vm) ->
    Vm#vm{
      attributes = dict:store(Attribute, Value, Vm#vm.attributes)
     }.
