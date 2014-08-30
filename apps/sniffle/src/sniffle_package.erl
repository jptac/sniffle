-module(sniffle_package).
-include("sniffle.hrl").

-define(MASTER, sniffle_package_vnode_master).
-define(VNODE, sniffle_package_vnode).
-define(SERVICE, sniffle_package).

-export([
         create/1,
         delete/1,
         get/1,
         lookup/1,
         list/0, list/2,
         wipe/1,
         sync_repair/2,
         list_/0
        ]).

-ignore_xref([
              sync_repair/2,
              list_/0, get_/1,
              wipe/1
              ]).

-export([
         set_metadata/2,
         blocksize/2,
         compression/2,
         cpu_cap/2,
         cpu_shares/2,
         max_swap/2,
         name/2,
         quota/2,
         ram/2,
         uuid/2,
         zfs_io_priority/2,
         remove_requirement/2,
         add_requirement/2
        ]).

-spec wipe(fifo:package_id()) -> ok.
wipe(UUID) ->
    sniffle_coverage:start(?MASTER, ?SERVICE, {wipe, UUID}).

-spec sync_repair(fifo:package_id(), ft_obj:obj()) -> ok.
sync_repair(UUID, Obj) ->
    do_write(UUID, sync_repair, Obj).

-spec list_() -> {ok, [ft_obj:obj()]}.
list_() ->
    {ok, Res} = sniffle_full_coverage:raw(?MASTER, ?SERVICE, []),
    Res1 = [R || {_, R} <- Res],
    {ok,  Res1}.

-spec lookup(Package::binary()) ->
                    not_found | {ok, Pkg::fifo:package()} | {error, timeout}.
lookup(Package) ->
    {ok, Res} = sniffle_coverage:start(
                  ?MASTER, ?SERVICE, {lookup, Package}),
    lists:foldl(fun (not_found, Acc) ->
                        Acc;
                    (R, _) ->
                        {ok, R}
                end, not_found, Res).

-spec create(Package::binary()) ->
                    duplicate | {error, timeout} | {ok, UUID::fifo:package_id()}.
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

-spec get(Package::fifo:package_id()) ->
                 not_found | {ok, Pkg::fifo:package()} | {error, timeout}.
get(Package) ->
    sniffle_entity_read_fsm:start({?VNODE, ?SERVICE}, get, Package).

-spec list() ->
                  {ok, [Pkg::fifo:package_id()]} | {error, timeout}.
list() ->
    sniffle_coverage:start(?MASTER, ?SERVICE, list).

%%--------------------------------------------------------------------
%% @doc Lists all vm's and fiters by a given matcher set.
%% @end
%%--------------------------------------------------------------------
-spec list([fifo:matcher()], boolean()) ->
                  {error, timeout} |
                  {ok, [{integer(), fifo:package_id()}] |
                   [{integer(), fifo:package()}]}.

list(Requirements, true) ->
    {ok, Res} = sniffle_full_coverage:list(?MASTER, ?SERVICE, Requirements),
    Res1 = lists:sort(rankmatcher:apply_scales(Res)),
    {ok,  Res1};

list(Requirements, false) ->
    {ok, Res} = sniffle_coverage:start(
                  ?MASTER, ?SERVICE, {list, Requirements}),
    Res1 = rankmatcher:apply_scales(Res),
    {ok,  lists:sort(Res1)}.

?SET(set_metadata).
?SET(blocksize).
?SET(compression).
?SET(cpu_cap).
?SET(cpu_shares).
?SET(max_swap).
?SET(name).
?SET(quota).
?SET(ram).
?SET(uuid).
?SET(zfs_io_priority).
?SET(remove_requirement).
?SET(add_requirement).

%%%===================================================================
%%% Internal Functions
%%%===================================================================

do_write(Package, Op) ->
    sniffle_entity_write_fsm:write({?VNODE, ?SERVICE}, Package, Op).

do_write(Package, Op, Val) ->
    sniffle_entity_write_fsm:write({?VNODE, ?SERVICE}, Package, Op, Val).
