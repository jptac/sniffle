%%% @author Heinz Nikolaus Gies <heinz@licenser.net>
%%% @copyright (C) 2012, Heinz Nikolaus Gies
%%% @doc
%%%
%%% @end
%%% Created : 23 Aug 2012 by Heinz Nikolaus Gies <heinz@licenser.net>

-module(sniffle_vm_state).

-include("sniffle.hrl").

-define(LOGLEN, 100).

%%    alias/2,
-export([
         load/1,
         new/0,
         uuid/2,
         log/3,
         hypervisor/2,
         set/3
        ]).

load(H) ->
    H.

new() ->
    jsxd:new().


uuid(UUID, Vm) ->
    jsxd:set(<<"uuid">>, UUID, Vm).

%%alias(Alias, Vm) ->
%%    Vm#vm{alias = Alias}.

log(Time, Log, Vm) ->
    LogEntry = [{<<"date">>, Time},
                {<<"log">>, Log}],
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
                end, [LogEntry], Vm).

hypervisor(Hypervisor, Vm) ->
    jsxd:set(<<"hypervisor">>, Hypervisor, Vm).

set(Attribute, Value, Vm) ->
    jsxd:set(Attribute, Value, Vm).
