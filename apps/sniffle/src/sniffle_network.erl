-module(sniffle_network).
-include("sniffle.hrl").

-define(MASTER, sniffle_network_vnode_master).
-define(VNODE, sniffle_network_vnode).
-define(SERVICE, sniffle_network).

-define(FM(Met, Mod, Fun, Args),
        folsom_metrics:histogram_timed_update(
          {sniffle, network, Met},
          Mod, Fun, Args)).

-export([
         create/1,
         delete/1,
         get/1,
         lookup/1,
         list/0,
         list/2,
         set/3,
         set/2,
         add_iprange/2,
         remove_iprange/2,
         claim_ip/1,
         claim_ip/2,
         wipe/1,
         sync_repair/2,
         list_/0
        ]).

-ignore_xref([
              create/1,
              delete/1,
              get/1,
              lookup/1,
              list/0,
              list/2,
              set/3,
              set/2,
              add_iprange/2,
              remove_iprange/2,
              claim_ip/1,
              claim_ip/2,
              sync_repair/2,
              list_/0,
              wipe/1
             ]).

-export([
         name/2,
         set_metadata/2,
         uuid/2
        ]).

-define(MAX_TRIES, 3).

-spec wipe(fifo:network_id()) ->
                  ok.
wipe(UUID) ->
    ?FM(wipe, sniffle_coverage, start, [?MASTER, ?SERVICE, {wipe, UUID}]).

-spec sync_repair(fifo:network_id(), ft_obj:obj()) ->
                         ok.
sync_repair(UUID, Obj) ->
    do_write(UUID, sync_repair, Obj).

-spec list_() -> {ok, [fifo:obj()]}.
list_() ->
    {ok, Res} = ?FM(list_all, sniffle_coverage, raw,
                    [?MASTER, ?SERVICE, []]),
    Res1 = [R || {_, R} <- Res],
    {ok,  Res1}.

-spec lookup(Network::binary()) ->
                    not_found | {ok, IPR::fifo:network()} | {error, timeout}.
lookup(Name) when
      is_binary(Name) ->
    {ok, Res} = ?FM(list, sniffle_coverage, start,
                    [?MASTER, ?SERVICE, {lookup, Name}]),
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
                 not_found | {ok, IPR::fifo:network()} | {error, timeout}.
get(Network) ->
    ?FM(get, sniffle_entity_read_fsm, start,
        [{?VNODE, ?SERVICE}, get, Network]).

-spec add_iprange(fifo:network_id(), fifo:iprange_id()) ->
                         ok | not_found | {error, timeout}.
add_iprange(Network, IPRange) ->
    case sniffle_iprange:get(IPRange) of
        not_found ->
            not_found;
        _ ->
            do_write(Network, add_iprange, IPRange)
    end.

-spec remove_iprange(fifo:network_id(), fifo:iprange_id()) ->
                         ok | not_found | {error, timeout}.
remove_iprange(Network, IPRange) ->
    do_write(Network, remove_iprange, IPRange).

-spec list() ->
                  {ok, [IPR::fifo:network_id()]} | {error, timeout}.
list() ->
    ?FM(list, sniffle_coverage, start, [?MASTER, ?SERVICE, list]).
%%--------------------------------------------------------------------
%% @doc Lists all vm's and fiters by a given matcher set.
%% @end
%%--------------------------------------------------------------------
-spec list([fifo:matcher()], boolean()) ->
                  {error, timeout} |
                  {ok, [{integer(), fifo:network_id()}]
                   | [{integer(), fifo:network()}]}.

list(Requirements, Full) ->
    {ok, Res} = ?FM(list_all, sniffle_coverage, list,
                    [?MASTER, ?SERVICE, Requirements]),
    Res1 = lists:sort(rankmatcher:apply_scales(Res)),
    Res2 = case Full of
               true ->
                   Res1;
               false ->
                   [{P, ft_network:uuid(O)} || {P, O} <- Res1]
           end,
    {ok, Res2}.

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

-spec claim_ip(Iprange::fifo:network_id()) ->
                      not_found |
                      {ok, {Tag::binary(),
                            IP::non_neg_integer(),
                            Netmask::non_neg_integer(),
                            Gateway::non_neg_integer(),
                            VLAN::non_neg_integer()}} |
                      {error, failed} |
                      {'error', 'no_servers'}.

claim_ip(UUID) ->
    claim_ip(UUID, []).

claim_ip(UUID, Rules) ->
    case sniffle_network:get(UUID) of
        {ok, Network} ->
            Rs = ft_network:ipranges(Network),
            get_ip(Rs, Rules);
        R ->
            R
    end.

?SET(name).
?SET(set_metadata).
?SET(uuid).


%%%===================================================================
%%% Internal Functions
%%%===================================================================

do_write(Network, Op) ->
    ?FM(Op, sniffle_entity_write_fsm, write, [{?VNODE, ?SERVICE}, Network, Op]).

do_write(Network, Op, Val) ->
    ?FM(Op, sniffle_entity_write_fsm, write,
        [{?VNODE, ?SERVICE}, Network, Op, Val]).

get_ip([N | R], []) ->
    case sniffle_iprange:claim_ip(N) of
        Res = {ok, _} ->
            Res;
        _ ->
            get_ip(R, [])
    end;

get_ip([N | R], Rules) ->
    case sniffle_iprange:get(N) of
        {ok, E} ->
            G = fun ft_iprange:getter/2,
            case rankmatcher:match(E, G, Rules) of
                false ->
                    get_ip(R, Rules);
                _ ->
                    try_claim(N, R, Rules)
            end;
        _ ->
            get_ip(R, Rules)
    end;

get_ip([], _) ->
    not_found.


try_claim(N, R, Rules) ->
    case sniffle_iprange:claim_ip(N) of
        {ok, Res} ->
            {ok, N, Res};
        _ ->
            get_ip(R, Rules)
    end.
