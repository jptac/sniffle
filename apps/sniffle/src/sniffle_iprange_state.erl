%%% @author Heinz Nikolaus Gies <heinz@licenser.net>
%%% @copyright (C) 2012, Heinz Nikolaus Gies
%%% @doc
%%%
%%% @end
%%% Created : 23 Aug 2012 by Heinz Nikolaus Gies <heinz@licenser.net>

-module(sniffle_iprange_state).

-include("sniffle.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([
         new/0,
         load/2,
         name/2,
         uuid/1,
         uuid/2,
         network/2,
         netmask/2,
         first/2,
         is_free/2,
         last/2,
         current/2,
         release_ip/2,
         gateway/2,
         claim_ip/2,
         tag/2,
         vlan/2,
         set/4,
         set/3,
         to_bin/1,
         parse_bin/1,
         getter/2
        ]).

-ignore_xref([load/2, set/4, set/3, getter/2, uuid/1]).

uuid(Vm) ->
    {ok, UUID} = jsxd:get(<<"uuid">>, statebox:value(Vm)),
    UUID.

getter(#sniffle_obj{val=S0}, Resource) ->
    jsxd:get(Resource, 0, statebox:value(S0)).

load(_, D) ->
    load(D).

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

gateway(Gateway, Iprange) when
      is_integer(Gateway),
      is_list(Iprange) ->
    jsxd:set(<<"gateway">>, Gateway, Iprange).

network(Network, Iprange) when
      is_integer(Network),
      is_list(Iprange) ->
    jsxd:set(<<"network">>, Network, Iprange).

netmask(Netmask, Iprange) when
      is_integer(Netmask),
      is_list(Iprange) ->
    jsxd:set(<<"netmask">>, Netmask, Iprange).

first(First, Iprange) when
      is_integer(First),
      is_list(Iprange) ->
    jsxd:set(<<"first">>, First, Iprange).

last(Last, Iprange) when
      is_integer(Last),
      is_list(Iprange) ->
    jsxd:set(<<"last">>, Last, Iprange).

current(Current, Iprange) when
      is_integer(Current),
      is_list(Iprange) ->
    jsxd:set(<<"current">>, Current, Iprange).

tag(Tag, Iprange) when
      is_binary(Tag),
      is_list(Iprange) ->
    jsxd:set(<<"tag">>, Tag, Iprange).

vlan(Vlan, Iprange) when
      is_integer(Vlan),
      is_list(Iprange) ->
    jsxd:set(<<"vlan">>, Vlan, Iprange).

is_free(IP, Iprange) when
      is_integer(IP),
      is_list(Iprange) ->
    case jsxd:get(<<"current">>, Iprange) of
        {ok, Current} when
              is_integer(Current),
              Current =< IP ->
            true;
        _ ->
            IPs = jsxd:get(<<"free">>, [], Iprange),
            lists:member(IP, IPs)
    end.

claim_ip(IP, Iprange) when
      is_integer(IP),
      is_list(Iprange) ->
    case jsxd:get(<<"current">>, Iprange) of
        {ok, IP} ->
            jsxd:set(<<"current">>, IP + 1 , Iprange);
        {ok, Current} when is_integer(Current),
                           Current< IP ->
            jsxd:thread([{set, <<"current">>, IP + 1},
                         {update, <<"free">>, fun (Free) when is_list(Free) ->
                                                      lists:seq(Current, IP-1) ++ Free
                                              end}],
                        Iprange);
        _ ->
            jsxd:update(<<"free">>, fun (IPs) when is_list(IPs) ->
                                            %%TODO: This does not work well with the way statebox works.
                                            %%true = lists:member(IP, IPs),
                                            lists:filter(fun(X) -> X =/= IP end, IPs)
                                    end, Iprange)
    end.


release_ip(IP, Iprange) when
      is_integer(IP),
      is_list(Iprange) ->
    release_ip(IP,
               jsxd:get(<<"first">>, 0, Iprange),
               jsxd:get(<<"current">>, 0, Iprange),
               jsxd:get(<<"last">>, 0, Iprange),
               Iprange).

release_ip(IP, First, Current, Last, Iprange) when
      is_integer(IP),
      is_integer(First),
      is_integer(Current),
      is_integer(Last),
      is_list(Iprange),
      First =< IP,
      Last >= IP,
      Current =:= IP + 1 ->
    jsxd:set(<<"current">>, IP, Iprange);

release_ip(IP, First, Current, Last, Iprange) when
      is_integer(IP),
      is_integer(First),
      is_integer(Current),
      is_integer(Last),
      is_list(Iprange),
      First =< IP,
      Last >= IP,
      Current > IP ->
    jsxd:prepend(<<"free">>, IP, Iprange);

release_ip(_IP, _First, _Current, _Last, Iprange) when
      is_integer(_IP),
      is_integer(_First),
      is_integer(_Current),
      is_integer(_Last),
      is_list(Iprange) ->
    Iprange.

set(_ID, Attribute, Value, D) ->
    statebox:modify({fun set/3, [Attribute, Value]}, D).

set(Resource, delete, Iprange) ->
    jsxd:delete(Resource, Iprange);

set(Resource, Value, Iprange) ->
    jsxd:set(Resource, Value, Iprange).

to_bin(IP) ->
    <<A, B, C, D>> = <<IP:32>>,
    list_to_binary(io_lib:format("~p.~p.~p.~p", [A, B, C, D])).

parse_bin(Bin) ->
    [A,B,C,D] = [ list_to_integer(binary_to_list(P)) || P <- re:split(Bin, "[.]")],
    <<IP:32>> = <<A:8, B:8, C:8, D:8>>,
    IP.

-ifdef(TEST).

example_setup() ->
    R = new(),
    R1 = network(100, R),
    R2 = gateway(200, R1),
    R3 = netmask(0, R2),
    R4 = first(110, R3),
    R5 = current(110, R4),
    last(120, R5).

example_setup_test() ->
    R = example_setup(),
    ?assertEqual({ok, 100}, jsxd:get(<<"network">>, R)),
    ?assertEqual({ok, 200}, jsxd:get(<<"gateway">>, R)),
    ?assertEqual({ok, 0}, jsxd:get(<<"netmask">>, R)),
    ?assertEqual({ok, 110}, jsxd:get(<<"first">>, R)),
    ?assertEqual({ok, 110}, jsxd:get(<<"current">>, R)),
    ?assertEqual({ok, 120}, jsxd:get(<<"last">>, R)).

claim1_test() ->
    R = example_setup(),
    R1 = claim_ip(110, R),
    ?assertEqual({ok, 110}, jsxd:get(<<"first">>, R1)),
    ?assertEqual({ok, 111}, jsxd:get(<<"current">>, R1)),
    R2 = claim_ip(111, R1),
    ?assertEqual({ok, 112}, jsxd:get(<<"current">>, R2)).

claim2_test() ->
    R = example_setup(),
    R1 = jsxd:set(<<"free">>, [123, 124], R),
    R2 = jsxd:set(<<"current">>, 125, R1),
    R3 = claim_ip(123, R2),
    ?assertEqual({ok, 125}, jsxd:get(<<"current">>, R3)),
    ?assertEqual({ok, [124]}, jsxd:get(<<"free">>, R3)),
    R4 = claim_ip(125, R2),
    ?assertEqual({ok, 126}, jsxd:get(<<"current">>, R4)),
    ?assertEqual({ok, [123, 124]}, jsxd:get(<<"free">>, R4)),
    R5 = claim_ip(124, R2),
    ?assertEqual({ok, 125}, jsxd:get(<<"current">>, R5)),
    ?assertEqual({ok, [123]}, jsxd:get(<<"free">>, R5)).

return_test() ->
    R = example_setup(),
    R1 = jsxd:thread([{set, <<"free">>, []},
                      {set, <<"current">>, 115}],
                     R),
    R2 = release_ip(114, R1),
    ?assertEqual({ok, 114}, jsxd:get(<<"current">>, R2)),
    ?assertEqual({ok, []}, jsxd:get(<<"free">>, R2)),
    R3 = release_ip(113, R1),
    ?assertEqual({ok, 115}, jsxd:get(<<"current">>, R3)),
    ?assertEqual({ok, [113]}, jsxd:get(<<"free">>, R3)),
    R4 = release_ip(115, R1),
    ?assertEqual({ok, 115}, jsxd:get(<<"current">>, R4)),
    ?assertEqual({ok, []}, jsxd:get(<<"free">>, R4)).

-endif.
