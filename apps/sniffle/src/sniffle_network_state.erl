%%% @author Heinz Nikolaus Gies <heinz@licenser.net>
%%% @copyright (C) 2012, Heinz Nikolaus Gies
%%% @doc
%%%
%%% @end
%%% Created : 23 Aug 2012 by Heinz Nikolaus Gies <heinz@licenser.net>

-module(sniffle_network_state).

-include("sniffle.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([
         new/0,
         load/1,
         name/2,
         uuid/2,
         add_iprange/2,
         remove_iprange/2,
         set/3
        ]).

-ignore_xref([load/1, set/3]).

-ignore_xref([
              add_iprange/2,
              remove_iprange/2
             ]).

load(Iprange) ->
    Iprange.

new() ->
    jsxd:set(<<"version">>, <<"0.1.0">>, jsxd:new()).

name(Name, Iprange) when
      is_binary(Name),
      is_list(Iprange) ->
    jsxd:set(<<"name">>, Name, Iprange).

uuid(UUID, Iprange) when
      is_binary(UUID),
      is_list(Iprange) ->
    jsxd:set(<<"uuid">>, UUID, Iprange).

add_iprange(Iprange, Network) when
      is_binary(Iprange),
      is_list(Network) ->
    jsxd:update(<<"ipranges">>,
                fun (R) ->
                        ordsets:add_element(Iprange, R)
                end, [Iprange], Network).

remove_iprange(Iprange, Network) when
      is_binary(Iprange),
      is_list(Network) ->
    jsxd:update(<<"ipranges">>,
                fun (R) ->
                        ordsets:del_element(Iprange, R)
                end, [Iprange], Network).

set(Ks, delete, Network) ->
    jsxd:delete([<<"metadata">> | Ks], Network);

set(Ks, V, Network) ->
    jsxd:set([<<"metadata">> | Ks], V, Network).

-ifdef(TEST).

example_setup_test() ->
    R = new(),
    R1 = add_iprange(<<"r1">>, R),
    R2 = add_iprange(<<"r2">>, R1),
    R3 = remove_iprange(<<"r1">>, R2),
    R4 = remove_iprange(<<"r2">>, R3),
    ?assertEqual({ok, [<<"r1">>]}, jsxd:get(<<"ipranges">>, R1)),
    ?assertEqual({ok, [<<"r1">>, <<"r2">>]}, jsxd:get(<<"ipranges">>, R2)),
    ?assertEqual({ok, [<<"r2">>]}, jsxd:get(<<"ipranges">>, R3)),
    ?assertEqual({ok, []}, jsxd:get(<<"ipranges">>, R4)).

-endif.
