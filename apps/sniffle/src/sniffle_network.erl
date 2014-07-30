-module(sniffle_network).
-include("sniffle.hrl").

-define(MASTER, sniffle_network_vnode_master).
-define(VNODE, sniffle_network_vnode).
-define(SERVICE, sniffle_network).

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

wipe(UUID) ->
    sniffle_coverage:start(?MASTER, ?SERVICE, {wipe, UUID}).

sync_repair(UUID, Obj) ->
    do_write(UUID, sync_repair, Obj).

list_() ->
    {ok, Res} = sniffle_full_coverage:start(
                  ?MASTER, ?SERVICE, {list, [], true, true}),
    Res1 = [R || {_, R} <- Res],
    {ok,  Res1}.

-spec lookup(Network::binary()) ->
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
    sniffle_entity_read_fsm:start({?VNODE, ?SERVICE}, get, Network).

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
    sniffle_entity_write_fsm:write({?VNODE, ?SERVICE}, Network, Op).

do_write(Network, Op, Val) ->
    sniffle_entity_write_fsm:write({?VNODE, ?SERVICE}, Network, Op, Val).

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
            case rankmatcher:match(E, fun ft_iprange:getter/2, Rules) of
                false ->
                    get_ip(R, Rules);
                _ ->
                    case sniffle_iprange:claim_ip(N) of
                        {ok, Res} ->
                            {ok, N, Res};
                        _ ->
                            get_ip(R, Rules)
                    end
            end;
        _ ->
            get_ip(R, Rules)
    end;

get_ip([], _) ->
    not_found.
