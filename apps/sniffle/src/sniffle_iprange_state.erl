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

new() ->
    #iprange{free=[]}.

name(Name, Iprange) ->
    Iprange#iprange{name = Name}.

gateway(Gateway, Iprange) ->
    Iprange#iprange{gateway = Gateway}.

network(Network, Iprange) ->
    Iprange#iprange{network = Network}.

netmask(Netmask, Iprange) ->
    Iprange#iprange{netmask = Netmask}.

first(First, Iprange) ->
    Iprange#iprange{first = First}.

last(Last, Iprange) ->
    Iprange#iprange{last = Last}.

current(Current, Iprange) ->
    Iprange#iprange{current = Current}.

tag(Tag, Iprange) ->
    Iprange#iprange{tag = Tag}.

claim_ip(IP, #iprange{current = IP} = Iprange) ->
    Iprange#iprange{current = IP + 1};

claim_ip(IP, Iprange) ->
    Iprange#iprange{free = ordsets:del_element(IP, Iprange#iprange.free)}.

release_ip(IP, #iprange{current = LIP} = Iprange) when
      Iprange#iprange.first =< IP,
      Iprange#iprange.last >= IP,
      LIP == IP + 1 ->
    Iprange#iprange{current = IP};

release_ip(IP, Iprange) when
      Iprange#iprange.first < IP,
      Iprange#iprange.last > IP,
      Iprange#iprange.current > IP ->
    Iprange#iprange{free = ordsets:add_element(IP, Iprange#iprange.free)};

release_ip(_IP, Iprange) ->
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
    ?assertEqual(R#iprange.network,  100),
    ?assertEqual(R#iprange.gateway,  200),
    ?assertEqual(R#iprange.netmask, 0),
    ?assertEqual(R#iprange.first, 110),
    ?assertEqual(R#iprange.current, 110),
    ?assertEqual(R#iprange.last, 120).

claim1_test() ->
    R = example_setup(),
    R1 = claim_ip(110, R),
    ?assertEqual(R1#iprange.first, 110),
    ?assertEqual(R1#iprange.current, 111),
    R2 = claim_ip(111, R1),
    ?assertEqual(R2#iprange.current, 112).

claim2_test() ->
    R = example_setup(),
    R1 = R#iprange{free = [123, 124]},
    R2 = claim_ip(123, R1),
    ?assertEqual(R2#iprange.current, 110),
    ?assertEqual(R2#iprange.free, [124]),
    R3 = claim_ip(110, R1),
    ?assertEqual(R3#iprange.current, 111),
    ?assertEqual(R3#iprange.free, [123, 124]),
    R4 = claim_ip(124, R1),
    ?assertEqual(R4#iprange.current, 110),
    ?assertEqual(R4#iprange.free, [123]).

return_test() ->
    R = example_setup(),
    R1 = R#iprange{free = [],
		   current = 115},
    R2 = release_ip(114, R1),
    ?assertEqual(R2#iprange.current, 114),
    ?assertEqual(R2#iprange.free, []),
    R3 = release_ip(113, R1),
    ?assertEqual(R3#iprange.current, 115),
    ?assertEqual(R3#iprange.free, [113]),
    R4 = release_ip(115, R1),
    ?assertEqual(R4#iprange.current, 115),
    ?assertEqual(R4#iprange.free, []).

-endif.
