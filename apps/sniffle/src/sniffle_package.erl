-module(sniffle_package).
-include("sniffle.hrl").
%-include_lib("riak_core/include/riak_core_vnode.hrl").

-export(
   [
    create/1,
    delete/1,
    get/1,
    list/0,
    list/1,
    get_attribute/2,
    get_attribute/1,
    set_attribute/2,
    set_attribute/3
   ]
  ).

create(Package) ->
    case sniffle_package:get(Package) of
	{ok, not_found} ->
	    do_write(Package, create, []);
	{ok, _RangeObj} ->
	    duplicate
    end.

delete(Package) ->    
    do_update(Package, delete).

get(Package) ->
    sniffle_entity_read_fsm:start(
      {sniffle_package_vnode, sniffle_package},
      get, Package
     ).

list() ->
    sniffle_entity_coverage_fsm:start(
      {sniffle_package_vnode, sniffle_package},
      list
     ).

list(User) ->
    sniffle_entity_coverage_fsm:start(
      {sniffle_package_vnode, sniffle_package},
      list, undefined, User
     ).

get_attribute(Package) ->
    case sniffle_package:get(Package) of
	{ok, not_found} ->
	    not_found;
	{ok, V} ->
	    dict:to_list(V#package.attributes)
    end.

get_attribute(Package, Attribute) ->
    case sniffle_package:get(Package) of
	{ok, not_found} ->
	    not_found;
	{ok, V} ->
	    case dict:find(Attribute, V#package.attributes) of
		error ->
		    not_found;
		Result ->
		    Result
	    end
    end.

set_attribute(Package, Attribute, Value) ->
    do_update(Package, set_attribute, [Attribute, Value]).


set_attribute(Package, Attributes) ->
    do_update(Package, mset_attribute, Attributes).


%%%===================================================================
%%% Internal Functions
%%%===================================================================


do_update(Package, Op) ->
    case sniffle_package:get(Package) of
	{ok, not_found} ->
	    not_found;
	{ok, _RangeObj} ->
	    do_write(Package, Op)
    end.

do_update(Package, Op, Val) ->
    case sniffle_package:get(Package) of
	{ok, not_found} ->
	    not_found;
	{ok, _RangeObj} ->
	    do_write(Package, Op, Val)
    end.

do_write(Package, Op) ->
    sniffle_entity_write_fsm:write({sniffle_package_vnode, sniffle_package}, Package, Op).

do_write(Package, Op, Val) ->
    sniffle_entity_write_fsm:write({sniffle_package_vnode, sniffle_package}, Package, Op, Val).
