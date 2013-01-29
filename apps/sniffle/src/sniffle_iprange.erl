-module(sniffle_iprange).
-include("sniffle.hrl").
                                                %-include_lib("riak_core/include/riak_core_vnode.hrl").

-export([
         create/8,
         delete/1,
         get/1,
         lookup/1,
         list/0,
         list/1,
         claim_ip/1,
         release_ip/2
        ]).

lookup(User) ->
    {ok, Res} = sniffle_entity_coverage_fsm:start(
                  {sniffle_iprange_vnode, sniffle_iprange},
                  lookup, User),
    Res1 = lists:foldl(fun (not_found, Acc) ->
                               Acc;
                           (R, _) ->
                               R
                       end, not_found, Res),
    {ok, Res1}.

create(Iprange, Network, Gateway, Netmask, First, Last, Tag, Vlan) ->
    UUID = list_to_binary(uuid:to_string(uuid:uuid4())),
    case sniffle_iprange:lookup(Iprange) of
        {ok, not_found} ->
            ok = do_write(UUID, create, [Iprange, Network, Gateway, Netmask, First, Last, Tag, Vlan]),
            {ok, UUID};
        {ok, _RangeObj} ->
            duplicate
    end.

delete(Iprange) ->
    do_update(Iprange, delete).

get(Iprange) ->
    sniffle_entity_read_fsm:start(
      {sniffle_iprange_vnode, sniffle_iprange},
      get, Iprange).

list() ->
    sniffle_entity_coverage_fsm:start(
      {sniffle_iprange_vnode, sniffle_iprange},
      list).

list(Requirements) ->
    sniffle_entity_coverage_fsm:start(
      {sniffle_iprange_vnode, sniffle_iprange},
      list, Requirements).

release_ip(Iprange, IP) ->
    do_update(Iprange, release_ip, IP).

claim_ip(Iprange) ->
    case sniffle_iprange:get(Iprange) of
        {ok, not_found} ->
            not_found;
        {ok, Obj} ->
            case {jsxd:get(<<"free">>, [], Obj), jsxd:get(<<"current">>, 0, Obj), jsxd:get(<<"last">>, 0, Obj)} of
                {[], FoundIP, Last} when FoundIP > Last ->
                    no_ips_left;
                {[], FoundIP, _} ->
                    do_write(Iprange, claim_ip, FoundIP);
                {[FoundIP|_], _, _} ->
                    do_write(Iprange, claim_ip, FoundIP)
            end
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
    case sniffle_iprange:get(Iprange) of
        {ok, not_found} ->
            not_found;
        {ok, _RangeObj} ->
            do_write(Iprange, Op, Val)
    end.

do_write(Iprange, Op) ->
    sniffle_entity_write_fsm:write({sniffle_iprange_vnode, sniffle_iprange}, Iprange, Op).

do_write(Iprange, Op, Val) ->
    sniffle_entity_write_fsm:write({sniffle_iprange_vnode, sniffle_iprange}, Iprange, Op, Val).
