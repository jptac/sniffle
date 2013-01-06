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
    jsxd:new().


uuid(UUID, Vm) ->
    jsxd:set(<<"uuid">>, UUID, Vm).

%%alias(Alias, Vm) ->
%%    Vm#vm{alias = Alias}.

log(Time, Log, Vm) ->
    jsxd:update(<<"log">>,
                fun(Log0) ->
                        Log1 = case length(Log0) of
                                   ?LOGLEN ->
                                       [_ | L] = Log0,
                                       L;
                                   _ ->
                                       Log0
                               end,
                        ordsets:add_element([{<<"date">>, Time},
                                             {<<"log">>, Log}], Log1)
                end, Vm).

hypervisor(Hypervisor, Vm) ->
    jsxd:set(<<"hypervisor">>, Hypervisor, Vm).

attribute(Attribute, Value, Vm) ->
    jsxd:set(Attribute, Value, Vm).
