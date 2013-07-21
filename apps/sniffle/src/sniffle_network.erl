-module(sniffle_network).
-include("sniffle.hrl").
%%-include_lib("riak_core/include/riak_core_vnode.hrl").

-export([
         create/1,
         delete/1,
         get/1,
         lookup/1,
         list/0,
         list/1,
         set/3,
         set/2,
         add_iprange/2,
         remove_iprange/2
        ]).

-ignore_xref([
              create/1,
              delete/1,
              get/1,
              lookup/1,
              list/0,
              list/1,
              set/3,
              set/2,
              add_iprange/2,
              remove_iprange/2
             ]).

-define(MAX_TRIES, 3).

-spec lookup(Network::binary()) ->
                    not_found | {ok, IPR::fifo:object()} | {error, timeout}.
lookup(Name) when
      is_binary(Name) ->
    {ok, Res} = sniffle_entity_coverage_fsm:start(
                  {sniffle_network_vnode, sniffle_network},
                  lookup, Name),
    lists:foldl(fun (not_found, Acc) ->
                        Acc;
                    (R, _) ->
                        {ok, R}
                end, not_found, Res).

-spec create(Name::binary()) ->
                    duplicate | {error, timeout} | {ok, UUID::fifo:uuid()}.
create(Network) when
      is_binary(Network) ->
    UUID = list_to_binary(uuid:to_string(uuid:uuid4())),
    case sniffle_network:lookup(Network) of
        not_found ->
            ok = do_write(UUID, create, [Network]),
            {ok, UUID};
        {ok, _RangeObj} ->
            duplicate
    end.

-spec delete(Network::fifo:network_id()) ->
                    not_found | {error, timeout} | ok.
delete(Network) ->
    do_write(Network, delete).

-spec get(Network::fifo:network_id()) ->
                 not_found | {ok, IPR::fifo:object()} | {error, timeout}.
get(Network) ->
    sniffle_entity_read_fsm:start(
      {sniffle_network_vnode, sniffle_network},
      get, Network).


add_iprange(Network, IPRange) ->
    case sniffle_iprange:get(IPRange) of
        not_found ->
            not_found;
        _ ->
            do_write(Network, add_iprange, IPRange)
    end.

remove_iprange(Network, IPRange) ->
    do_write(Network, remove_iprange, IPRange).

-spec list() ->
                  {ok, [IPR::fifo:network_id()]} | {error, timeout}.
list() ->
    sniffle_entity_coverage_fsm:start(
      {sniffle_network_vnode, sniffle_network},
      list).

-spec list(Reqs::[fifo:matcher()]) ->
                  {ok, [IPR::fifo:network_id()]} | {error, timeout}.
list(Requirements) ->
    {ok, Res} = sniffle_entity_coverage_fsm:start(
                  {sniffle_network_vnode, sniffle_network},
                  list, Requirements),
    Res1 = rankmatcher:apply_scales(Res),
    {ok,  lists:sort(Res1)}.

-spec set(Network::fifo:network_id(),
          Attribute::fifo:keys(),
          Value::fifo:value()) ->
                 ok | {error, timeout}.
set(Network, Attribute, Value) ->
    set(Network, [{Attribute, Value}]).

-spec set(Network::fifo:network_id(),
          Attributes::fifo:attr_list()) ->
                 ok | {error, timeout}.
set(Network, Attributes) ->
    do_write(Network, set, Attributes).

%%%===================================================================
%%% Internal Functions
%%%===================================================================

do_write(Network, Op) ->
    sniffle_entity_write_fsm:write({sniffle_network_vnode, sniffle_network}, Network, Op).

do_write(Network, Op, Val) ->
    sniffle_entity_write_fsm:write({sniffle_network_vnode, sniffle_network}, Network, Op, Val).
