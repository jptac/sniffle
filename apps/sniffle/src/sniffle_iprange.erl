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
         release_ip/2,
         set/3,
         set/2
        ]).

-define(MAX_TRIES, 3).

-spec lookup(IPRange::binary()) ->
                    not_found | {ok, IPR::fifo:object()} | {error, timeout}.
lookup(Name) when
      is_binary(Name) ->
    {ok, Res} = sniffle_entity_coverage_fsm:start(
                  {sniffle_iprange_vnode, sniffle_iprange},
                  lookup, Name),
    lists:foldl(fun (not_found, Acc) ->
                        Acc;
                    (R, _) ->
                        {ok, R}
                end, not_found, Res).

-spec create(Iprange::binary(),
             Network::integer(),
             Gateway::integer(),
             Netmask::integer(),
             First::integer(),
             Last::integer(),
             Tag::binary(),
             Vlan::integer()) ->
                    duplicate | {error, timeout} | {ok, UUID::fifo:uuid()}.
create(Iprange, Network, Gateway, Netmask, First, Last, Tag, Vlan) when
      is_binary(Iprange) ->
    UUID = list_to_binary(uuid:to_string(uuid:uuid4())),
    case sniffle_iprange:lookup(Iprange) of
        not_found ->
            ok = do_write(UUID, create, [Iprange, Network, Gateway, Netmask, First, Last, Tag, Vlan]),
            {ok, UUID};
        {ok, _RangeObj} ->
            duplicate
    end.

-spec delete(Iprange::fifo:iprange_id()) ->
                    not_found | {error, timeout} | ok.
delete(Iprange) ->
    do_write(Iprange, delete).

-spec get(Iprange::fifo:iprange_id()) ->
                 not_found | {ok, IPR::fifo:object()} | {error, timeout}.
get(Iprange) ->
    sniffle_entity_read_fsm:start(
      {sniffle_iprange_vnode, sniffle_iprange},
      get, Iprange).

-spec list() ->
                  {ok, [IPR::fifo:iprange_id()]} | {error, timeout}.
list() ->
    sniffle_entity_coverage_fsm:start(
      {sniffle_iprange_vnode, sniffle_iprange},
      list).

-spec list(Reqs::[fifo:matcher()]) ->
                  {ok, [IPR::fifo:iprange_id()]} | {error, timeout}.
list(Requirements) ->
    sniffle_entity_coverage_fsm:start(
      {sniffle_iprange_vnode, sniffle_iprange},
      list, Requirements).

-spec release_ip(Iprange::fifo:iprange_id(),
                 IP::integer()) ->
                        ok | {error, timeout}.
release_ip(Iprange, IP) ->
    do_write(Iprange, release_ip, IP).

-spec claim_ip(Iprange::fifo:iprange_id()) ->
                      not_found |
                      {ok, {Tag::binary(),
                            IP::pos_integer(),
                            Netmask::pos_integer(),
                            Gateway::pos_integer()}} |
                      {error, failed} |
                      {'error','no_servers'}.
claim_ip(Iprange) ->
    claim_ip(Iprange, 0).

-spec set(Iprange::fifo:iprange_id(),
          Attribute::fifo:keys(),
          Value::fifo:value()) ->
                 ok | {error, timeout}.
set(Iprange, Attribute, Value) ->
    set(Iprange, [{Attribute, Value}]).

-spec set(Iprange::fifo:iprange_id(),
          Attributes::fifo:attr_list()) ->
                 ok | {error, timeout}.
set(Iprange, Attributes) ->
    do_write(Iprange, set, Attributes).

%%%===================================================================
%%% Internal Functions
%%%===================================================================

do_write(Iprange, Op) ->
    sniffle_entity_write_fsm:write({sniffle_iprange_vnode, sniffle_iprange}, Iprange, Op).

do_write(Iprange, Op, Val) ->
    sniffle_entity_write_fsm:write({sniffle_iprange_vnode, sniffle_iprange}, Iprange, Op, Val).

claim_ip(_Iprange, ?MAX_TRIES) ->
    {error, failed};

claim_ip(Iprange, N) ->
    case sniffle_iprange:get(Iprange) of
        {error, timeout} ->
            timer:sleep(N*50),
            claim_ip(Iprange, N + 1);
        not_found ->
            not_found;
        {ok, Obj} ->
            case {jsxd:get(<<"free">>, [], Obj), jsxd:get(<<"current">>, 0, Obj), jsxd:get(<<"last">>, 0, Obj)} of
                {[], FoundIP, Last} when FoundIP > Last ->
                    {error, full};
                {[], FoundIP, _} ->
                    case do_write(Iprange, claim_ip, FoundIP) of
                        {error, _} ->
                            timer:sleep(N*50),
                            claim_ip(Iprange, N + 1);
                        R ->
                            R
                    end;
                {[FoundIP|_], _, _} ->
                    case do_write(Iprange, claim_ip, FoundIP) of
                        {error, _} ->
                            timer:sleep(N*50),
                            claim_ip(Iprange, N + 1);
                        R ->
                            R
                    end
            end
    end.
