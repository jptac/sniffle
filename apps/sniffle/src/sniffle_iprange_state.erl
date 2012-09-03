%%% @author Heinz Nikolaus Gies <heinz@licenser.net>
%%% @copyright (C) 2012, Heinz Nikolaus Gies
%%% @doc
%%%
%%% @end
%%% Created : 23 Aug 2012 by Heinz Nikolaus Gies <heinz@licenser.net>

-module(sniffle_iprange_state).

-include("sniffle.hrl").

-export([
	 new/0,
	 name/2,
	 network/2,
	 netmask/2,
	 first/2,
	 last/2,
	 current/2,
	 return_ip/2,
	 gateway/2,
	 claim_ip/2,
	 tag/2
	]).

new() ->
    #iprange{}.

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

return_ip(IP, #iprange{current = LIP} = Iprange) when LIP == IP + 1 ->
    Iprange#iprange{current = IP};

return_ip(IP, Iprange) ->
    Iprange#iprange{free = ordsets:add_element(IP, Iprange#iprange.free)}.
