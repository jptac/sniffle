%%%-------------------------------------------------------------------
%%% @author Heinz Nikolaus Gies <heinz@licenser.net>
%%% @copyright (C) 2012, Heinz Nikolaus Gies
%%% @doc
%%%
%%% @end
%%% Created : 19 Oct 2012 by Heinz Nikolaus Gies <heinz@licenser.net>
%%%-------------------------------------------------------------------
-module(sniffle_matcher).

-include("sniffle.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([match/3,
         apply_scales/1]).

apply_scales([]) ->
    [];

apply_scales([{{_, []}, _} | _] = Res) ->
    [{N, Key} || {{N, []}, Key} <- Res];

apply_scales([{{_, [{scale, _, V, _, _} | _]}, _} | _] = Res) ->
    {_, _, R} = apply_scale(Res, V, V),
    apply_scales(R).

apply_scale([{{N, [{scale, _, V, Low, High} | RScales] }, Key} | R],
            Min, Max) when V < Min->
    {MinR, MaxR, Res} = apply_scale(R, V, Max),
    {MinR, MaxR, [{{N + scale(Low, High, MinR, MaxR, V), RScales}, Key} | Res]};

apply_scale([{{N, [{scale, _, V, Low, High} | RScales] }, Key} | R],
            Min, Max) when V > Max->
    {MinR, MaxR, Res} = apply_scale(R, Min, V),
    {MinR, MaxR, [{{N + scale(Low, High, MinR, MaxR, V), RScales}, Key} | Res]};

apply_scale([{{N, [{scale, _, V, Low, High} | RScales] }, Key} | R],
            Min, Max) ->
    {MinR, MaxR, Res} = apply_scale(R, Min, Max),
    {MinR, MaxR, [{{N + scale(Low, High, MinR, MaxR, V), RScales}, Key} | Res]};

apply_scale([], Min, Max) ->
    {Min, Max, []}.

scale(Low, High, Min, Max, V) ->
    DOut = High - Low,
    DIn = case Max - Min of
              0 ->
                  1;
              R ->
                  R
          end,
    io:format("~p + ((~p - ~p) / ~p) * ~p",
              [Low, V, Min, DIn, DOut]),
    round(Low + ((V - Min) / DIn) * DOut).

match(Hypervisor, Getter, [{must, Op, Res, V}]) ->
    match(Hypervisor, Getter, {Op, Res, V}) andalso {0, []};

match(Hypervisor, Getter, [{cant, Op, Res, V}]) ->
    (not match(Hypervisor, Getter, {Op, Res, V})) andalso {0, []};

match(Hypervisor, Getter, [{scale, Res, Min, Max}]) ->
    {0, [{scale, Res, Getter(Hypervisor, Res), Min, Max}]};

match(Hypervisor, Getter, [{N, Op, Res, V}]) when is_integer(N) ->
    case match(Hypervisor, Getter, {Op, Res, V}) of
        true ->
            {N, []};
        false ->
            {0, []}
    end;

match(Hypervisor, Getter, [{must, Op, Res, V} | R]) ->
    match(Hypervisor, Getter, {Op, Res, V}) andalso match(Hypervisor, Getter, R);

match(Hypervisor, Getter, [{cant, Op, Res, V} | R]) ->
    (not match(Hypervisor, Getter, {Op, Res, V})) andalso match(Hypervisor, Getter, R);

match(Hypervisor, Getter, [{scale, Res, Min, Max} | R]) ->
    case match(Hypervisor, Getter, R) of
        false ->
            false;
        {N, Scales} when is_integer(N) ->
            {N, [{scale, Res, Getter(Hypervisor, Res), Min, Max} | Scales]}
    end;

match(Hypervisor, Getter, [{N, Op, Res, V} | R]) when is_integer(N) ->
    case match(Hypervisor, Getter, {Op, Res, V}) of
        false ->
            match(Hypervisor, Getter, R);
        true ->
            case match(Hypervisor, Getter, R) of
                false ->
                    false;
                {M, Scales} when is_integer(M) ->
                    {N + M, Scales}
            end
    end;

match(Hypervisor, Getter, {'>=', Resource, Value}) ->
    Getter(Hypervisor, Resource) >= Value;

match(Hypervisor, Getter, {'=<', Resource, Value}) ->
    Getter(Hypervisor, Resource) =< Value;

match(Hypervisor, Getter, {'>', Resource, Value}) ->
    Getter(Hypervisor, Resource) > Value;

match(Hypervisor, Getter, {'<', Resource, Value}) ->
    Getter(Hypervisor, Resource) < Value;

match(Hypervisor, Getter, {'=:=', Resource, Value}) ->
    Getter(Hypervisor, Resource) =:= Value;

match(Hypervisor, Getter, {'=/=', Resource, Value}) ->
    Getter(Hypervisor, Resource) =/= Value;

match(Hypervisor, Getter, {'subset', Resource, Value}) ->
    ordsets:is_subset(
      ordsets:from_list(Value),
      ordsets:from_list(Getter(Hypervisor, Resource)));

match(Hypervisor, Getter, {'superset', Resource, Value}) ->
    ordsets:is_subset(
      ordsets:from_list(Getter(Hypervisor, Resource)),
      ordsets:from_list(Value));

match(Hypervisor, Getter, {'disjoint', Resource, Value}) ->
    ordsets:is_disjoint(
      ordsets:from_list(Value),
      ordsets:from_list(Getter(Hypervisor, Resource)));

match(Hypervisor, Getter, {'element', Resource, Value}) ->
    ordsets:is_element(
      Value,
      ordsets:from_list(Getter(Hypervisor, Resource)));

match(Hypervisor, Getter, {'allowed', Perission, Permissions}) ->
    libsnarl:test(create_permission(Hypervisor, Getter, Perission, []), Permissions).

create_permission(_, _, [], Out) ->
    lists:reverse(Out);

create_permission(Hypervisor, Getter, [{<<"res">>, R} | In], Out) ->
    create_permission(Hypervisor, Getter,  In, [Getter(Hypervisor, R) | Out]);

create_permission(Hypervisor, Getter, [P | In], Out) ->
    create_permission(Hypervisor, Getter, In, [ P | Out]).

-ifdef(TEST).

test_hypervisort() ->
    #hypervisor{
                 name = <<"test-hypervisor">>,
                 resources =
                     dict:from_list(
                       [{<<"num-res">>, 1024},
                        {<<"set-res">>, [1,2,3]},
                        {<<"str-res">>, <<"str">>}])}.

test_getter(Hypervisor, <<"name">>) ->
    Hypervisor#hypervisor.name;

test_getter(Hypervisor, Resource) ->
    dict:fetch(Resource, Hypervisor#hypervisor.resources).

number_gt_test() ->
    ?assert(match(test_hypervisort(),
                  fun test_getter/2,
                  {'=<', <<"num-res">>, 1024})),
    ?assert(match(test_hypervisort(),
                  fun test_getter/2,
                  {'=<', <<"num-res">>, 2000})),
    ?assert(match(test_hypervisort(),
                  fun test_getter/2,
                  {'<', <<"num-res">>, 2000})),
    ?assertNot(match(test_hypervisort(),
                     fun test_getter/2,
                     {'<', <<"num-res">>, 1000})),
    ?assertNot(match(test_hypervisort(),
                     fun test_getter/2,
                     {'=<', <<"num-res">>, 1000})).

number_lt_test() ->
    ?assert(match(test_hypervisort(),
                  fun test_getter/2,
                  {'>=', <<"num-res">>, 1024})),
    ?assert(match(test_hypervisort(),
                  fun test_getter/2,
                  {'>=', <<"num-res">>, 1000})),
    ?assert(match(test_hypervisort(),
                  fun test_getter/2,
                  {'>', <<"num-res">>, 1000})),
    ?assertNot(match(test_hypervisort(),
                     fun test_getter/2,
                     {'>', <<"num-res">>, 2000})),
    ?assertNot(match(test_hypervisort(),
                     fun test_getter/2,
                     {'>=', <<"num-res">>, 2000})).

number_eq_test() ->
    ?assert(match(test_hypervisort(),
                  fun test_getter/2,
                  {'=:=', <<"num-res">>, 1024})),
    ?assert(match(test_hypervisort(),
                  fun test_getter/2,
                  {'=:=', <<"str-res">>, <<"str">>})),
    ?assertNot(match(test_hypervisort(),
                     fun test_getter/2,
                     {'=:=', <<"num-res">>, 1000})).

number_meq_test() ->
    ?assert(match(test_hypervisort(),
                  fun test_getter/2,
                  {'=/=', <<"num-res">>, 1000})),
    ?assertNot(match(test_hypervisort(),
                     fun test_getter/2,
                     {'=/=', <<"num-res">>, 1024})).

number_element_test() ->
    ?assert(match(test_hypervisort(),
                  fun test_getter/2,
                  {'element', <<"set-res">>, 1})),
    ?assertNot(match(test_hypervisort(),
                     fun test_getter/2,
                     {'element', <<"set-res">>, 4})).

number_subset_test() ->
    ?assert(match(test_hypervisort(),
                  fun test_getter/2,
                  {'subset', <<"set-res">>, [1,2,3]})),
    ?assert(match(test_hypervisort(),
                  fun test_getter/2,
                  {'subset', <<"set-res">>, [1,2]})),
    ?assert(match(test_hypervisort(),
                  fun test_getter/2,
                  {'subset', <<"set-res">>, [1,3]})),
    ?assertNot(match(test_hypervisort(),
                     fun test_getter/2,
                     {'subset', <<"set-res">>, [1,2,3,4]})).

number_superset_test() ->
    ?assert(match(test_hypervisort(),
                  fun test_getter/2,
                  {'superset', <<"set-res">>, [1,2,3]})),
    ?assert(match(test_hypervisort(),
                  fun test_getter/2,
                  {'superset', <<"set-res">>, [1,2,3,4]})),
    ?assertNot(match(test_hypervisort(),
                     fun test_getter/2,
                     {'superset', <<"set-res">>, [1,2]})),
    ?assertNot(match(test_hypervisort(),
                     fun test_getter/2,
                     {'superset', <<"set-res">>, [1,4]})).

number_disjoint_test() ->
    ?assert(match(test_hypervisort(),
                  fun test_getter/2,
                  {'disjoint', <<"set-res">>, [4,5,6]})),
    ?assertNot(match(test_hypervisort(),
                     fun test_getter/2,
                     {'disjoint', <<"set-res">>, [1]})),
    ?assertNot(match(test_hypervisort(),
                     fun test_getter/2,
                     {'disjoint', <<"set-res">>, [1,4]})).

multi_must_one_ok_test() ->
    ?assertEqual({0, []},
                 match(test_hypervisort(),
                       fun test_getter/2,
                       [{must, '=<', <<"num-res">>, 1024}])),
    ?assertEqual({0, []},
                 match(test_hypervisort(),
                       fun test_getter/2,
                       [{must, '=:=', <<"num-res">>, 1024}])).

multi_must_two_ok_test() ->
    ?assertEqual({0, []},
                 match(test_hypervisort(),
                       fun test_getter/2,
                       [{must, '=<', <<"num-res">>, 1024},
                        {must, '=:=', <<"num-res">>, 1024}])).
multi_must_one_not_ok_test() ->
    ?assertEqual(false,
                 match(test_hypervisort(),
                       fun test_getter/2,
                       [{must, '<', <<"num-res">>, 1024}])),
    ?assertEqual(false,
                 match(test_hypervisort(),
                       fun test_getter/2,
                       [{must, '=/=', <<"num-res">>, 1024}])).

multi_must_two_not_ok_test() ->
    ?assertEqual(false,
                 match(test_hypervisort(),
                       fun test_getter/2,
                       [{must, '<', <<"num-res">>, 1024},
                        {must, '=:=', <<"num-res">>, 1024}])),
    ?assertEqual(false,
                 match(test_hypervisort(),
                       fun test_getter/2,
                       [{must, '=<', <<"num-res">>, 1024},
                        {must, '=/=', <<"num-res">>, 1024}])).

multi_cant_one_ok_test() ->
    ?assertEqual(false,
                 match(test_hypervisort(),
                       fun test_getter/2,
                       [{cant, '=<', <<"num-res">>, 1024}])),
    ?assertEqual(false,
                 match(test_hypervisort(),
                       fun test_getter/2,
                       [{cant, '=:=', <<"num-res">>, 1024}])).

multi_cant_two_ok_test() ->
    ?assertEqual(false,
                 match(test_hypervisort(),
                       fun test_getter/2,
                       [{cant, '=<', <<"num-res">>, 1024},
                        {cant, '=:=', <<"num-res">>, 1024}])).
multi_cant_one_not_ok_test() ->
    ?assertEqual({0, []},
                 match(test_hypervisort(),
                       fun test_getter/2,
                       [{cant, '<', <<"num-res">>, 1024}])),
    ?assertEqual({0, []},
                 match(test_hypervisort(),
                       fun test_getter/2,
                       [{cant, '=/=', <<"num-res">>, 1024}])).

multi_cant_two_not_ok_test() ->
    ?assertEqual(false,
                 match(test_hypervisort(),
                       fun test_getter/2,
                       [{cant, '<', <<"num-res">>, 1024},
                        {cant, '=:=', <<"num-res">>, 1024}])),
    ?assertEqual(false,
                 match(test_hypervisort(),
                       fun test_getter/2,
                       [{cant, '=<', <<"num-res">>, 1024},
                        {cant, '=/=', <<"num-res">>, 1024}])).
count_test() ->
    ?assertEqual({2, []},
                 match(test_hypervisort(),
                       fun test_getter/2,
                       [{1, '<', <<"num-res">>, 1024},
                        {2, '=:=', <<"num-res">>, 1024}])),
    ?assertEqual({1, []},
                 match(test_hypervisort(),
                       fun test_getter/2,
                       [{1, '=<', <<"num-res">>, 1024},
                        {2, '=/=', <<"num-res">>, 1024}])).

mix_test() ->
    ?assertEqual(false,
                 match(test_hypervisort(),
                       fun test_getter/2,
                       [{must, '<', <<"num-res">>, 1024},
                        {2, '=:=', <<"num-res">>, 1024}])),
    ?assertEqual({0, []},
                 match(test_hypervisort(),
                       fun test_getter/2,
                       [{1, '<', <<"num-res">>, 1024},
                        {must, '=:=', <<"num-res">>, 1024}])).

scale_gen_test() ->
    ?assertEqual({0, [{scale, <<"num-res">>, 1024, 0, 10}]},
                 match(test_hypervisort(),
                       fun test_getter/2,
                       [{scale, <<"num-res">>, 0, 10}])).

scale_calc_test() ->
    ?assertEqual(0, scale(0, 10, 0, 20, 0)),
    ?assertEqual(10, scale(0, 10, 0, 20, 20)),
    ?assertEqual(5, scale(0, 10, 0, 20, 10)).

scale_full_test() ->
    ?assertEqual(
       [{0, a}, {5, b}, {10, c}],
       apply_scales(
         [{{0, [{scale, <<"num-res">>, 0, 0, 10}]}, a},
          {{0, [{scale, <<"num-res">>, 1024, 0, 10}]}, b},
          {{0, [{scale, <<"num-res">>, 2048, 0, 10}]}, c}])).

create_permission_test() ->
    ?assertEqual(create_permission(test_hypervisort(), fun test_getter/2, [some, permission], []), [some, permission]),
    ?assertEqual(create_permission(test_hypervisort(), fun test_getter/2, [some, <<"test">>, permission], []), [some, <<"test">>, permission]).

create_permission_res_test() ->
    ?assertEqual(create_permission(test_hypervisort(), fun test_getter/2, [some, {<<"res">>, <<"str-res">>}, permission], []), [some, <<"str">>, permission]).

create_permission_name_test() ->
    ?assertEqual(create_permission(test_hypervisort(), fun test_getter/2, [some, {<<"res">>, <<"name">>}, permission], []), [some, <<"test-hypervisor">>, permission]).

-endif.
