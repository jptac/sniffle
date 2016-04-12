-module(sniffle_iprange).
-define(CMD, sniffle_iprange_cmd).
-define(BUCKET, <<"iprange">>).
-define(S, ft_iprange).
-include("sniffle.hrl").

-define(FM(Met, Mod, Fun, Args),
        folsom_metrics:histogram_timed_update(
          {sniffle, iprange, Met},
          Mod, Fun, Args)).

-export([
         create/8,
         delete/1,
         lookup/1,
         claim_ip/1,
         claim_specific_ip/2,
         full/1,
         release_ip/2,
         name/2,
         uuid/2,
         network/2,
         netmask/2,
         gateway/2,
         set_metadata/2,
         tag/2,
         vlan/2
        ]).


-define(MAX_TRIES, 3).
%%%===================================================================
%%% General section
%%%===================================================================
-spec wipe(fifo:iprange_id()) -> ok.

-spec sync_repair(fifo:iprange_id(), ft_obj:obj()) -> ok.

-spec list_() -> {ok, [ft_obj:obj()]}.

-spec get(Iprange::fifo:iprange_id()) ->
                 not_found | {ok, IPR::fifo:iprange()} | {error, timeout}.

-spec list() ->
                  {ok, [IPR::fifo:iprange_id()]} | {error, timeout}.

-spec list([fifo:matcher()], boolean()) ->
                  {error, timeout} |
                  {ok, [{Rating::integer(), Value::fifo:iprange()}] |
                   [{Rating::integer(), Value::fifo:iprange_id()}]}.


-include("sniffle_api.hrl").
%%%===================================================================
%%% Custom section
%%%===================================================================

-spec lookup(IPRange::binary()) ->
                    not_found | {ok, IPR::fifo:iprange()} | {error, timeout}.
lookup(Name) when
      is_binary(Name) ->
    {ok, Res} = ?FM(lookup, sniffle_coverage, start,
                    [?REQ({lookup, Name})]),
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
                    duplicate |
                    {error, timeout} |
                    {ok, UUID::fifo:iprange_id()}.
create(Iprange, Network, Gateway, Netmask, First, Last, Tag, Vlan) when
      is_binary(Iprange) ->
    UUID = fifo_utils:uuid(iprange),
    case sniffle_iprange:lookup(Iprange) of
        not_found ->
            ok = do_write(UUID, create, [Iprange, Network, Gateway, Netmask,
                                         First, Last, Tag, Vlan]),
            {ok, UUID};
        {ok, _RangeObj} ->
            duplicate
    end.

-spec delete(Iprange::fifo:iprange_id()) ->
                    not_found | {error, timeout} | ok.
delete(Iprange) ->
    do_write(Iprange, delete).

-spec release_ip(Iprange::fifo:iprange_id(),
                 IP::integer()) ->
                        ok | {error, timeout}.
release_ip(Iprange, IP) ->
    do_write(Iprange, release_ip, IP).

-spec claim_ip(Iprange::fifo:iprange_id()) ->
                      not_found |
                      {ok, {Tag::binary(),
                            IP::non_neg_integer(),
                            Netmask::non_neg_integer(),
                            Gateway::non_neg_integer(),
                            VLAN::non_neg_integer()}} |
                      {error, failed} |
                      {'error', 'no_servers'}.
claim_ip(Iprange) ->
    claim_ip(Iprange, 0).

claim_specific_ip(Iprange, IP) ->
    do_write(Iprange, claim_ip, IP).

?SET(name).
?SET(uuid).
?SET(network).
?SET(netmask).
?SET(gateway).
?SET(set_metadata).
?SET(tag).
?SET(vlan).

%%%===================================================================
%%% Internal Functions
%%%===================================================================

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
            sniffle_ip:claim(Obj)
    end.

full(Iprange) ->
    case sniffle_iprange:get(Iprange) of
        {ok, Obj} ->
            ft_iprange:free(Obj) == [];
        E ->
            E
    end.
