-module(sniffle_package).
-include("sniffle.hrl").
                                                %-include_lib("riak_core/include/riak_core_vnode.hrl").

-export([
         create/1,
         delete/1,
         get/1,
         lookup/1,
         list/0,
         list/1,
         set/2,
         set/3
        ]).

-spec lookup(Package::binary()) ->
                    not_found | {ok, Pkg::fifo:object()} | {error, timeout}.
lookup(Package) ->
    {ok, Res} = sniffle_entity_coverage_fsm:start(
                  {sniffle_package_vnode, sniffle_package},
                  lookup, Package),
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
    sniffle_entity_coverage_fsm:start(
      {sniffle_package_vnode, sniffle_package},
      list).

-spec list(Reqs::[fifo:matcher()]) ->
                  {ok, [Pkg::fifo:package_id()]} | {error, timeout}.
list(Requirements) ->
    sniffle_entity_coverage_fsm:start(
      {sniffle_package_vnode, sniffle_package},
      list, Requirements).

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
