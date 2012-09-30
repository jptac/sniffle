-module(sniffle_iprange).
-include("sniffle.hrl").
%-include_lib("riak_core/include/riak_core_vnode.hrl").

-export(
   [
    create/7,
    delete/1,
    get/1,
    list/0,
    list/1,
    claim_ip/1,
    release_ip/2
   ]
  ).

create(Iprange, Network, Gateway, Netmask, First, Last, Tag) ->
    case sniffle_iprange:get(Iprange) of
	{ok, not_found} ->
	    do_write(Iprange, create, [Network, Gateway, Netmask, First, Last, Tag]);
	{ok, _RangeObj} ->
	    duplicate
    end.

delete(Iprange) ->    
    do_update(Iprange, delete).

get(Iprange) ->
    sniffle_entity_read_fsm:start(
      {sniffle_iprange_vnode, sniffle_iprange},
      get, Iprange
     ).

list() ->
    sniffle_entity_coverage_fsm:start(
      {sniffle_iprange_vnode, sniffle_iprange},
      list
     ).

list(User) ->
    sniffle_entity_coverage_fsm:start(
      {sniffle_iprange_vnode, sniffle_iprange},
      list, undefined, User
     ).

release_ip(Iprange, IP) ->
    do_update(Iprange, release_ip, IP).
    
claim_ip(Iprange) ->
    case sniffle_iprange:get(Iprange) of
	{ok, not_found} ->
	    not_found;
	{ok,
	 #iprange{free=[],
		  last=Last,
		  current=FoundIP}} when FoundIP >= Last ->
	    no_ips_left;	    	    
	{ok,
	 #iprange{free=[],
		  current=FoundIP}} ->
	    do_write(Iprange, claim_ip, FoundIP);
	{ok,
	 #iprange{free=[FoundIP|_]}} ->
	    
	    do_write(Iprange, claim_ip, FoundIP)
    end.

%%%===================================================================
%%% Internal Functions
%%%===================================================================


do_update(Iprange, Op) ->
    case sniffle_iprange:get(Iprange) of
	{ok, not_found} ->
	    not_found;
	{ok, _RangeObj} ->
	    do_write(Iprange, Op)
    end.

do_update(Iprange, Op, Val) ->
    case sniffle_user:get(Iprange) of
	{ok, not_found} ->
	    not_found;
	{ok, _RangeObj} ->
	    do_write(Iprange, Op, Val)
    end.

do_write(Iprange, Op) ->
    sniffle_entity_write_fsm:write({sniffle_iprange_vnode, sniffle_iprange}, Iprange, Op).

do_write(Iprange, Op, Val) ->
    sniffle_entity_write_fsm:write({sniffle_iprange_vnode, sniffle_iprange}, Iprange, Op, Val).
