-module(sniffle_iprange).
-include("sniffle.hrl").

-define(MASTER, sniffle_iprange_vnode_master).
-define(VNODE, sniffle_iprange_vnode).
-define(SERVICE, sniffle_iprange).

-export([
         create/8,
         delete/1,
         get/1,
         lookup/1,
         list/0,
         list/2,
         claim_ip/1,
         full/1,
         release_ip/2,
         set/3,
         set/2,
         wipe/1,
         sync_repair/2,
         list_/0
        ]).

-ignore_xref([
              sync_repair/2,
              list_/0,
              wipe/1
              ]).

-export([
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

wipe(UUID) ->
    sniffle_coverage:start(?MASTER, ?SERVICE, {wipe, UUID}).

sync_repair(UUID, Obj) ->
    do_write(UUID, sync_repair, Obj).

list_() ->
    {ok, Res} = sniffle_full_coverage:start(
                  ?MASTER, ?SERVICE, {list, [], true, true}),
    Res1 = [R || {_, R} <- Res],
    {ok,  Res1}.

-spec lookup(IPRange::binary()) ->
                    not_found | {ok, IPR::fifo:object()} | {error, timeout}.
lookup(Name) when
      is_binary(Name) ->
    {ok, Res} = sniffle_coverage:start(
                  ?MASTER, ?SERVICE, {lookup, Name}),
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
    sniffle_entity_read_fsm:start({?VNODE, ?SERVICE}, get, Iprange).

-spec list() ->
                  {ok, [IPR::fifo:iprange_id()]} | {error, timeout}.
list() ->
    sniffle_coverage:start(?MASTER, ?SERVICE, list).

%%--------------------------------------------------------------------
%% @doc Lists all vm's and fiters by a given matcher set.
%% @end
%%--------------------------------------------------------------------
-spec list([fifo:matcher()], boolean()) -> {error, timeout} | {ok, [fifo:uuid()]}.

list(Requirements, true) ->
    {ok, Res} = sniffle_full_coverage:start(
                  ?MASTER, ?SERVICE, {list, Requirements, true}),
    Res1 = lists:sort(rankmatcher:apply_scales(Res)),
    {ok,  Res1};

list(Requirements, false) ->
    {ok, Res} = sniffle_coverage:start(
                  ?MASTER, ?SERVICE, {list, Requirements}),
    Res1 = rankmatcher:apply_scales(Res),
    {ok,  lists:sort(Res1)}.

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

do_write(Iprange, Op) ->
    sniffle_entity_write_fsm:write({?VNODE, ?SERVICE}, Iprange, Op).

do_write(Iprange, Op, Val) ->
    sniffle_entity_write_fsm:write({?VNODE, ?SERVICE}, Iprange, Op, Val).

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
            case ft_iprange:free(Obj) of
                [] ->
                    {error, full};
                [FoundIP | _] ->
                    case do_write(Iprange, claim_ip, FoundIP) of
                        {error, _} ->
                            timer:sleep(N*50),
                            claim_ip(Iprange, N + 1);
                        R ->
                            R
                    end
            end
    end.

full(Iprange) ->
    case sniffle_iprange:get(Iprange) of
        {ok, Obj} ->
            ft_iprange:free(Obj) == [];
        E ->
            E
    end.
