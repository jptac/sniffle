-module(sniffle_package).
-include("sniffle.hrl").
%%-include_lib("riak_core/include/riak_core_vnode.hrl").

-export([
         create/1,
         delete/1,
         get/1,
         lookup/1,
         list/0,
         list/2,
         set/2,
         set/3
        ]).

-spec lookup(Package::binary()) ->
                    not_found | {ok, Pkg::fifo:object()} | {error, timeout}.
lookup(Package) ->
    {ok, Res} = sniffle_coverage:start(
                  sniffle_package_vnode_master, sniffle_package,
                  {lookup, Package}),
    lists:foldl(fun (not_found, Acc) ->
                        Acc;
                    (R, _) ->
                        {ok, R}
                end, not_found, Res).

-spec create(Package::binary()) ->
                    duplicate | {error, timeout} | {ok, UUID::fifo:uuid()}.
create(Package) ->
    UUID = list_to_binary(uuid:to_string(uuid:uuid4())),
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
                 not_found | {ok, Pkg::fifo:object()} | {error, timeout}.
get(Package) ->
    sniffle_entity_read_fsm:start(
      {sniffle_package_vnode, sniffle_package},
      get, Package).

-spec list() ->
                  {ok, [Pkg::fifo:package_id()]} | {error, timeout}.
list() ->
    sniffle_coverage:start(
      sniffle_package_vnode_master, sniffle_package,
      list).

-spec list(Reqs::[fifo:matcher()]) ->
                  {ok, [Pkg::fifo:package_id()]} | {error, timeout}.
list(Requirements) ->
    {ok, Res} = sniffle_coverage:start(
                  sniffle_package_vnode_master, sniffle_package,
                  {list, Requirements}),
    Res1 = rankmatcher:apply_scales(Res),
    {ok,  lists:sort(Res1)}.

%%--------------------------------------------------------------------
%% @doc Lists all vm's and fiters by a given matcher set.
%% @end
%%--------------------------------------------------------------------
-spec list([fifo:matcher()], boolean()) -> {error, timeout} | {ok, [fifo:uuid()]}.

list(Requirements, true) ->
    {ok, Ls} = list(Requirements),
    Ls1 = [{V, {UUID, ?MODULE:get(UUID)}} || {V, UUID} <- Ls],
    Ls2 = [{V, {UUID, D}} || {V, {UUID, {ok, D}}} <- Ls1],
    {ok,  Ls2};
list(Requirements, false) ->
    list(Requirements).

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
    sniffle_entity_write_fsm:write({sniffle_package_vnode, sniffle_package}, Package, Op).

do_write(Package, Op, Val) ->
    sniffle_entity_write_fsm:write({sniffle_package_vnode, sniffle_package}, Package, Op, Val).
