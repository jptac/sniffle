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
         load/1,
         name/2,
         network/2,
         netmask/2,
         first/2,
         last/2,
         current/2,
         release_ip/2,
         gateway/2,
         claim_ip/2,
         tag/2,
         to_bin/1
        ]).

load(#iprange{name = Name,
              tag = Tag,
              network = Net,
              gateway = GW,
              netmask = Mask,
              first = First,
              last = Last,
              current = Current,
              free = Free}) ->
    jsxd:thread([{set, <<"name">>, Name},
                 {set, <<"tag">>, Tag},
                 {set, <<"network">>, Net},
                 {set, <<"gateway">>, GW},
                 {set, <<"netmask">>, Mask},
                 {set, <<"first">>, First},
                 {set, <<"last">>, Last},
                 {set, <<"current">>, Current},
                 {set, <<"free">>, Free}],
                new());

load(Iprange) ->
    Iprange.

new() ->
    jsxd:set(<<"version">>, <<"0.1.0">>, jsxd:new()).


name(Name, Iprange) ->
    jsxd:set(<<"name">>, Name, Iprange).

gateway(Gateway, Iprange) ->
    jsxd:set(<<"gateway">>, Gateway, Iprange).

network(Network, Iprange) ->
    jsxd:set(<<"network">>, Network, Iprange).

netmask(Netmask, Iprange) ->
    jsxd:set(<<"netmask">>, Netmask, Iprange).

first(First, Iprange) ->
    jsxd:set(<<"first">>, First, Iprange).

last(Last, Iprange) ->
    jsxd:set(<<"last">>, Last, Iprange).

current(Current, Iprange) ->
    jsxd:set(<<"current">>, Current, Iprange).


tag(Tag, Iprange) ->
    jsxd:set(<<"tag">>, Tag, Iprange).


claim_ip(IP, Iprange) ->
    case jsxd:get(<<"current">>, Iprange) of
        {ok, IP} ->
            jsxd:set(<<"current">>, IP + 1 , Iprange);
        _ ->
            jsxd:update(<<"free">>, fun (IPs) ->
                                            lists:filter(fun(X) -> X =/= IP end, IPs)
                                    end, Iprange)
    end.


release_ip(IP, Iprange) ->
    release_ip(IP,
               jsxd:get(<<"first">>, 0, Iprange),
               jsxd:get(<<"current">>, 0, Iprange),
               jsxd:get(<<"last">>, 0, Iprange),
               Iprange).

release_ip(IP, First, Current, Last, Iprange) when
      First =< IP,
      Last >= IP,
      Current == IP + 1 ->
    jsxd:set(<<"current">>, IP, Iprange);

release_ip(IP, First, Current, Last, Iprange) when
      First < IP,
      Last > IP,
      Current > IP ->
    jsxd:prepend(<<"free">>, IP, Iprange);

release_ip(_IP, _First, _Current, _Last, Iprange) ->
    Iprange.

to_bin(IP) ->
    <<A, B, C, D>> = <<IP:32>>,
    list_to_binary(io_lib:format("~p.~p.~p.~p", [A, B, C, D])).

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
    R2 = claim_ip(123, R1),
    ?assertEqual({ok, 110}, jsxd:get(<<"current">>, R2)),
    ?assertEqual({ok, [124]}, jsxd:get(<<"free">>, R2)),
    R3 = claim_ip(110, R1),
    ?assertEqual({ok, 111}, jsxd:get(<<"current">>, R3)),
    ?assertEqual({ok, [123, 124]}, jsxd:get(<<"free">>, R3)),
    R4 = claim_ip(124, R1),
    ?assertEqual({ok, 110}, jsxd:get(<<"current">>, R4)),
    ?assertEqual({ok, [123]}, jsxd:get(<<"free">>, R4)).

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
