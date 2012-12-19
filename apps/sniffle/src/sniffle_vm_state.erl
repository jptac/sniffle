%%% @author Heinz Nikolaus Gies <heinz@licenser.net>
%%% @copyright (C) 2012, Heinz Nikolaus Gies
%%% @doc
%%%
%%% @end
%%% Created : 23 Aug 2012 by Heinz Nikolaus Gies <heinz@licenser.net>

-module(sniffle_vm_state).

-include("sniffle.hrl").

-define(LOGLEN, 100).

                                                %    alias/2,
-export([
         new/0,
         uuid/2,
         log/3,
         hypervisor/2,
         attribute/3
        ]).

new() ->
    #vm{attributes=dict:new()}.


uuid(UUID, Vm) ->
    Vm#vm{uuid = UUID}.

                                                %alias(Alias, Vm) ->
                                                %    Vm#vm{alias = Alias}.

log(Time, Log, Vm = #vm{log = Log0}) ->
    Log1 = case length(Log0) of
               ?LOGLEN ->
                   [_ | L] = Log0,
                   L;
               _ ->
                   Log0
           end,
    Vm#vm{log = ordsets:add_element({Time, Log}, Log1)}.

hypervisor(Hypervisor, Vm) ->
    Vm#vm{hypervisor = Hypervisor}.

attribute(Attribute, Value, Vm) ->
    Vm#vm{
      attributes = dict:store(Attribute, Value, Vm#vm.attributes)
     }.
