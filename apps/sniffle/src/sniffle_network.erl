-module(sniffle_network).
-define(CMD, sniffle_network_cmd).
-define(BUCKET, <<"network">>).
-define(S, ft_network).
-include("sniffle.hrl").

-define(FM(Met, Mod, Fun, Args),
        folsom_metrics:histogram_timed_update(
          {sniffle, network, Met},
          Mod, Fun, Args)).

-export([
         create/1,
         delete/1,
         lookup/1,
         add_iprange/2,
         remove_iprange/2,
         claim_ip/1,
         claim_ip/2,
         name/2,
         set_metadata/2,
         uuid/2
        ]).

%%%===================================================================
%%% General section
%%%===================================================================
-spec wipe(fifo:network_id()) ->
                  ok.

-spec sync_repair(fifo:network_id(), ft_obj:obj()) ->
                         ok.
-spec list_() -> {ok, [fifo:obj()]} | {error, term()}.
-spec get(Network::fifo:network_id()) ->
                 not_found | {ok, IPR::fifo:network()} | {error, timeout}.
-spec list() ->
                  {ok, [IPR::fifo:network_id()]} | {error, timeout}.
-spec list([fifo:matcher()], boolean()) ->
                  {error, timeout} |
                  {ok, [{integer(), fifo:network_id()}]
                   | [{integer(), fifo:network()}]}.
-include("sniffle_api.hrl").
%%%===================================================================
%%% Custom section
%%%===================================================================
-define(MAX_TRIES, 3).

-spec lookup(Network::binary()) ->
                    not_found | {ok, IPR::fifo:network()} | {error, timeout}.
lookup(Name) when
      is_binary(Name) ->
    {ok, Res} = ?FM(list, sniffle_coverage, start,
                    [?MASTER, ?MODULE, {lookup, Name}]),
    lists:foldl(fun (not_found, Acc) ->
                        Acc;
                    (R, _) ->
                        {ok, R}
                end, not_found, Res).

-spec create(Name::binary()) ->
                    duplicate | {error, timeout} | {ok, UUID::fifo:uuid()}.
create(Network) when
      is_binary(Network) ->
    UUID = fifo_utils:uuid(network),
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

-spec add_iprange(fifo:network_id(), fifo:iprange_id()) ->
                         ok | not_found | {error, timeout}.
add_iprange(Network, IPRange) ->
    case sniffle_iprange:get(IPRange) of
        not_found ->
            not_found;
        _ ->
            do_write(Network, add_iprange, [IPRange])
    end.

-spec remove_iprange(fifo:network_id(), fifo:iprange_id()) ->
                         ok | not_found | {error, timeout}.
remove_iprange(Network, IPRange) ->
    do_write(Network, remove_iprange, [IPRange]).


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
%%% Inernal functions
%%%===================================================================

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
