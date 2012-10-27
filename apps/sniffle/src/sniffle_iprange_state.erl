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
      Iprange#iprange.first =< IP andalso
      Iprange#iprange.last >= IP andalso
      LIP == IP + 1 ->
    Iprange#iprange{current = IP};

release_ip(IP, Iprange) when 
      Iprange#iprange.first =< IP andalso
      Iprange#iprange.last >= IP ->
    Iprange#iprange{free = ordsets:add_element(IP, Iprange#iprange.free)}.

to_bin(IP) ->
    <<A, B, C, D>> = <<IP:32>>,
    list_to_binary(io_lib:format("~p.~p.~p.~p", [A, B, C, D])).

-ifdef(TEST).

create_test() ->
    ?assert(#iprange{free=[]} == new()).

-endif.
