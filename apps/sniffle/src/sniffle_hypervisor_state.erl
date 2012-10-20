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
    #hypervisor{resources = dict:new()}.


name(Name, Hypervisor) ->
    Hypervisor#hypervisor{name = Name}.

host(Host, Hypervisor) ->
    Hypervisor#hypervisor{host = Host}.

port(Port, Hypervisor) ->
    Hypervisor#hypervisor{port = Port}.

resource(Resource, Value, Hypervisor) ->
    Hypervisor#hypervisor{
      resources = dict:store(Resource, Value, Hypervisor#hypervisor.resources)
     }.
