%%%-------------------------------------------------------------------
%%% @author Heinz Nikolaus Gies <heinz@licenser.net>
%%% @copyright (C) 2012, Heinz Nikolaus Gies
%%% @doc
%%%
%%% @end
%%% Created : 19 Oct 2012 by Heinz Nikolaus Gies <heinz@licenser.net>
%%%-------------------------------------------------------------------
-module(sniffle_host_matcher).

-include("sniffle.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([match/2]).

match(Hypervisor, [M]) ->
    match(Hypervisor, M);

match(Hypervisor, [M | R]) ->
    match(Hypervisor, M) andalso match(Hypervisor, R);

match(Hypervisor, {'>=', Resource, Value}) ->
    res(Hypervisor, Resource) >= Value;

match(Hypervisor, {'=<', Resource, Value}) ->
    res(Hypervisor, Resource) =< Value;

match(Hypervisor, {'>', Resource, Value}) ->
    res(Hypervisor, Resource) > Value;

match(Hypervisor, {'<', Resource, Value}) ->
    res(Hypervisor, Resource) < Value;

match(Hypervisor, {'=:=', Resource, Value}) ->
    res(Hypervisor, Resource) == Value;

match(Hypervisor, {'=/=', Resource, Value}) ->
    res(Hypervisor, Resource) =/= Value;

match(Hypervisor, {'subset', Resource, Value}) ->
    ordsets:is_subset(
      ordsets:from_list(Value), 
      ordsets:from_list(res(Hypervisor, Resource)));

match(Hypervisor, {'superset', Resource, Value}) ->
    ordsets:is_subset(
      ordsets:from_list(res(Hypervisor, Resource)),
      ordsets:from_list(Value));

match(Hypervisor, {'disjoint', Resource, Value}) ->
    ordsets:is_disjoint(
      ordsets:from_list(Value), 
      ordsets:from_list(res(Hypervisor, Resource)));

match(Hypervisor, {'element', Resource, Value}) ->
    ordsets:is_element(
      Value,
      ordsets:from_list(res(Hypervisor, Resource)));

match(Hypervisor, {'allowed', Perission, User}) ->
    libsnarl:allowed(User,create_permission(Hypervisor, Perission, [])).

create_permission(_, [], Out) ->
    lists:reverse(Out);

create_permission(Hypervisor, [{<<"res">>, <<"name">>} | In], Out) ->
    create_permission(Hypervisor, In, [Hypervisor#hypervisor.name | Out]);

create_permission(Hypervisor, [{<<"res">>, R} | In], Out) ->
    create_permission(Hypervisor,  In, [ res(Hypervisor, R) | Out]);

create_permission(Hypervisor, [P | In], Out) ->
    create_permission(Hypervisor,  In, [ P | Out]).


res(Hypervisor, Resource) ->
    dict:fetch(Resource, Hypervisor#hypervisor.resources).

-ifdef(TEST).

test_hypervisort() ->
    #hypervisor{
	   name = <<"test-hypervisor">>,
	   resources = 
	       dict:from_list(
		 [{<<"num-res">>, 1024},
		  {<<"set-res">>, [1,2,3]},
		  {<<"str-res">>, <<"str">>}])}.

number_gt_test() ->
    ?assert(match(test_hypervisort(), {'=<', <<"num-res">>, 1024})),
    ?assert(match(test_hypervisort(), {'=<', <<"num-res">>, 2000})),
    ?assert(match(test_hypervisort(), {'<', <<"num-res">>, 2000})),
    ?assertNot(match(test_hypervisort(), {'<', <<"num-res">>, 1000})),
    ?assertNot(match(test_hypervisort(), {'=<', <<"num-res">>, 1000})).

number_lt_test() ->
    ?assert(match(test_hypervisort(), {'>=', <<"num-res">>, 1024})),
    ?assert(match(test_hypervisort(), {'>=', <<"num-res">>, 1000})),
    ?assert(match(test_hypervisort(), {'>', <<"num-res">>, 1000})),
    ?assertNot(match(test_hypervisort(), {'>', <<"num-res">>, 2000})),
    ?assertNot(match(test_hypervisort(), {'>=', <<"num-res">>, 2000})).

number_eq_test() ->
    ?assert(match(test_hypervisort(), {'=:=', <<"num-res">>, 1024})),
    ?assert(match(test_hypervisort(), {'=:=', <<"str-res">>, <<"str">>})),
    ?assertNot(match(test_hypervisort(), {'=:=', <<"num-res">>, 1000})).

number_meq_test() ->
    ?assert(match(test_hypervisort(), {'=/=', <<"num-res">>, 1000})),
    ?assertNot(match(test_hypervisort(), {'=/=', <<"num-res">>, 1024})).

number_element_test() ->
    ?assert(match(test_hypervisort(), {'element', <<"set-res">>, 1})),
    ?assertNot(match(test_hypervisort(), {'element', <<"set-res">>, 4})).

number_subset_test() ->
    ?assert(match(test_hypervisort(), {'subset', <<"set-res">>, [1,2,3]})),
    ?assert(match(test_hypervisort(), {'subset', <<"set-res">>, [1,2]})),
    ?assert(match(test_hypervisort(), {'subset', <<"set-res">>, [1,3]})),
    ?assertNot(match(test_hypervisort(), {'subset', <<"set-res">>, [1,2,3,4]})).

number_superset_test() ->
    ?assert(match(test_hypervisort(), {'superset', <<"set-res">>, [1,2,3]})),
    ?assert(match(test_hypervisort(), {'superset', <<"set-res">>, [1,2,3,4]})),
    ?assertNot(match(test_hypervisort(), {'superset', <<"set-res">>, [1,2]})),
    ?assertNot(match(test_hypervisort(), {'superset', <<"set-res">>, [1,4]})).

number_disjoint_test() ->
    ?assert(match(test_hypervisort(), {'disjoint', <<"set-res">>, [4,5,6]})),
    ?assertNot(match(test_hypervisort(), {'disjoint', <<"set-res">>, [1]})),
    ?assertNot(match(test_hypervisort(), {'disjoint', <<"set-res">>, [1,4]})).

multi_test() ->
    ?assert(match(test_hypervisort(), [{'=<', <<"num-res">>, 1024}])),
    ?assert(match(test_hypervisort(), [{'=<', <<"num-res">>, 1024}, {'=:=', <<"num-res">>, 1024}])),
    ?assertNot(match(test_hypervisort(), [{'<', <<"num-res">>, 1024}, {'=:=', <<"num-res">>, 1024}])),
    ?assertNot(match(test_hypervisort(), [{'=<', <<"num-res">>, 1024}, {'=/=', <<"num-res">>, 1024}])).

create_permission_test() ->
    ?assertEqual(create_permission(test_hypervisort(), [some, permission], []), [some, permission]),
    ?assertEqual(create_permission(test_hypervisort(), [some, <<"test">>, permission], []), [some, <<"test">>, permission]),
    ?assertEqual(create_permission(test_hypervisort(), [some, {<<"res">>, <<"str-res">>}, permission], []), [some, <<"str">>, permission]),
    ?assertEqual(create_permission(test_hypervisort(), [some, {<<"res">>, <<"name">>}, permission], []), [some, <<"test-hypervisor">>, permission]),
    ?assertEqual(create_permission(test_hypervisort(), [some, permission], []), [some, permission]).
-endif.
