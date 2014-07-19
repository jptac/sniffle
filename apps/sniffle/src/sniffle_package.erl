-module(sniffle_package).
-include("sniffle.hrl").

-define(MASTER, sniffle_package_vnode_master).
-define(VNODE, sniffle_package_vnode).
-define(SERVICE, sniffle_package).

-export([
         create/1,
         delete/1,
         get_/1, get/1,
         lookup/1,
         list/0, list/2,
         set/2, set/3,
         wipe/1,
         sync_repair/2,
         list_/0
        ]).

-ignore_xref([
              sync_repair/2,
              list_/0, get_/1,
              wipe/1
              ]).

wipe(UUID) ->
    sniffle_coverage:start(?MASTER, ?SERVICE, {wipe, UUID}).

sync_repair(UUID, Obj) ->
    do_write(UUID, sync_repair, Obj).

list_() ->
    {ok, Res} = sniffle_full_coverage:start(
                  ?MASTER, ?SERVICE, {list, [], true, true}),
    Res1 = [R || {_, R} <- Res],
    {ok,  Res1}.

-spec lookup(Package::binary()) ->
                    not_found | {ok, Pkg::fifo:object()} | {error, timeout}.
lookup(Package) ->
    {ok, Res} = sniffle_coverage:start(
                  ?MASTER, ?SERVICE, {lookup, Package}),
    lists:foldl(fun (not_found, Acc) ->
                        Acc;
                    (R, _) ->
                        {ok, R}
                end, not_found, Res).

-spec create(Package::binary()) ->
                    duplicate | {error, timeout} | {ok, UUID::fifo:uuid()}.
create(Package) ->
    UUID = uuid:uuid4s(),
    case sniffle_package:lookup(Package) of
        not_found ->
            ok = do_write(UUID, create, [Package]),
            {ok, UUID};
        {ok, _RangeObj} ->
            duplicate;
        E ->
            E
    end.

-spec delete(Package::fifo:package_id()) ->
                    not_found | {error, timeout} | ok.
delete(Package) ->
    do_write(Package, delete).

-spec get_(Package::fifo:package_id()) ->
                 not_found | {ok, Pkg::fifo:object()} | {error, timeout}.
get_(Package) ->
    sniffle_entity_read_fsm:start({?VNODE, ?SERVICE}, get, Package).

-spec get(Package::fifo:package_id()) ->
                 not_found | {ok, Pkg::fifo:object()} | {error, timeout}.
get(Package) ->
    case get_(Package) of
        {ok, V} ->
            {ok, ft_package:to_json(V)};
        E ->
            E
    end.

-spec list() ->
                  {ok, [Pkg::fifo:package_id()]} | {error, timeout}.
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
    Res2 = [{M, ft_package:to_json(V)} || {M, V} <- Res1],
    {ok,  Res2};

list(Requirements, false) ->
    {ok, Res} = sniffle_coverage:start(
                  ?MASTER, ?SERVICE, {list, Requirements}),
    Res1 = rankmatcher:apply_scales(Res),
    {ok,  lists:sort(Res1)}.

-spec set(Package::fifo:package_id(),
          Attribute::fifo:keys(),
          Value::fifo:value()) ->
                 ok | {error, timeout}.
set(Package, Attribute, Value) ->
    set(Package, [{Attribute, Value}]).

-spec set(Package::fifo:package_id(),
          Attributes::fifo:attr_list()) ->
                 ok | {error, timeout}.
set(Package, Attributes) ->
    do_write(Package, set, Attributes).

%%%===================================================================
%%% Internal Functions
%%%===================================================================

do_write(Package, Op) ->
    sniffle_entity_write_fsm:write({?VNODE, ?SERVICE}, Package, Op).

do_write(Package, Op, Val) ->
    sniffle_entity_write_fsm:write({?VNODE, ?SERVICE}, Package, Op, Val).
