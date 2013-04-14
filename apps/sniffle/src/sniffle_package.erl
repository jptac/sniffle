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

lookup(Package) ->
    {ok, Res} = sniffle_entity_coverage_fsm:start(
                  {sniffle_package_vnode, sniffle_package},
                  lookup, Package),
    Res1 = lists:foldl(fun (not_found, Acc) ->
                               Acc;
                           (R, _) ->
                               R
                       end, not_found, Res),
    {ok, Res1}.

create(Package) ->
    UUID = list_to_binary(uuid:to_string(uuid:uuid4())),
    case sniffle_package:lookup(Package) of
        {ok, not_found} ->
            ok = do_write(UUID, create, [Package]),
            {ok, UUID};
        {ok, _RangeObj} ->
            duplicate
    end.

delete(Package) ->
    do_write(Package, delete).

get(Package) ->
    sniffle_entity_read_fsm:start(
      {sniffle_package_vnode, sniffle_package},
      get, Package).

list() ->
    sniffle_entity_coverage_fsm:start(
      {sniffle_package_vnode, sniffle_package},
      list).

list(Requirements) ->
    sniffle_entity_coverage_fsm:start(
      {sniffle_package_vnode, sniffle_package},
      list, Requirements).

set(Package, Attribute, Value) ->
    set(Package, [{Attribute, Value}]).


set(Package, Attributes) ->
    do_write(Package, set, Attributes).


%%%===================================================================
%%% Internal Functions
%%%===================================================================

do_write(Package, Op) ->
    sniffle_entity_write_fsm:write({sniffle_package_vnode, sniffle_package}, Package, Op).

do_write(Package, Op, Val) ->
    sniffle_entity_write_fsm:write({sniffle_package_vnode, sniffle_package}, Package, Op, Val).
