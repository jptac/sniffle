%%% @author Heinz Nikolaus Gies <heinz@licenser.net>
%%% @copyright (C) 2012, Heinz Nikolaus Gies
%%% @doc
%%%
%%% @end
%%% Created : 23 Aug 2012 by Heinz Nikolaus Gies <heinz@licenser.net>

-module(sniffle_vm_state).


-include("sniffle.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(LOGLEN, 100).

%%    alias/2,
-export([
         load/1,
         new/0,
         uuid/1,
         uuid/2,
         log/3,
         hypervisor/2,
         set/3,
         getter/2
        ]).

-ignore_xref([load/1, set/3, getter/2]).

getter(#sniffle_obj{val=S0}, Resource) ->
    jsxd:get(Resource, 0, statebox:value(S0)).

uuid(Vm) ->
    {ok, UUID} = jsxd:get(<<"uuid">>, Vm),
    UUID.

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

set(Attribute, delete, Vm) ->
    jsxd:delete(Attribute, Vm);

set(Attribute, Value, Vm) ->
    jsxd:set(Attribute, Value, Vm).

-ifdef(TEST).
fold_test() ->
    H0 = statebox:new(fun sniffle_vm_state:new/0),
    H1 = statebox:modify({fun sniffle_vm_state:uuid/2, [<<"u">>]}, H0),
    H2 = statebox:modify({fun sniffle_vm_state:hypervisor/2, [<<"h">>]}, H1),

    H2a = statebox:modify({fun sniffle_vm_state:set/3, [[<<"nested">>, <<"key">>], 2]}, H2),
    H2b = statebox:modify({fun sniffle_vm_state:set/3, [<<"key">>, 2]}, H2a),

    Resources =
        [
         {[<<"nested">>, <<"1">>], 1},
         {[<<"nested">>, <<"2">>], 2}
        ],
    H3 = statebox:modify({fun load/1, []}, H2b),
    H4 = lists:foldr(
           fun ({Resource, Value}, H) ->
                   statebox:modify(
                     {fun set/3,
                      [Resource, Value]}, H)
           end, H3, Resources),
    H5 = statebox:expire(?STATEBOX_EXPIRE, H4),
    V1 = statebox:value(load(H5)),
    Expected = [{<<"hypervisor">>, <<"h">>},
                {<<"key">>, 2},
                {<<"nested">>,
                 [{<<"1">>, 1},
                  {<<"2">>, 2},
                  {<<"key">>, 2}]},
                {<<"uuid">>,<<"u">>}],
    ?assertEqual(Expected, V1).
-endif.
