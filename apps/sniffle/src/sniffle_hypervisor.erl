-module(sniffle_hypervisor).
-include("sniffle.hrl").
%-include_lib("riak_core/include/riak_core_vnode.hrl").

-export(
   [
    register/3,
    unregister/1,
    get/1,
    list/0,
    list/1,
    get_resource/2,
    set_resource/3
   ]
  ).

register(Hypervisor, IP, Port) ->
    case sniffle_hypervisor:get(Hypervisor) of
	{ok, not_found} ->
	    do_write(Hypervisor, register, [IP, Port]);
	{ok, _UserObj} ->
	    duplicate
    end.

unregister(Hypervisor) ->    
    do_update(Hypervisor, delete).

get(Hypervisor) ->
    sniffle_entity_read_fsm:start(
      {sniffle_hypervisor_vnode, sniffle_hypervisor},
      get, Hypervisor
     ).

list() ->
    sniffle_entity_coverage_fsm:start(
      {sniffle_hypervisor_vnode, sniffle_hypervisor},
      list
     ).

list(Requirements) ->
    sniffle_entity_coverage_fsm:start(
      {sniffle_hypervisor_vnode, sniffle_hypervisor},
      list, undefined, Requirements
     ).

get_resource(Hypervisor, Resource) ->
    case sniffle_hypervisor:get(Hypervisor) of
	{ok, not_found} ->
	    not_found;
	{ok, V} ->
	    case dict:find(Resource, V#hypervisor.resources) of
		error ->
		    not_found;
		Result ->
		    Result
	    end
    end.

set_resource(Hypervisor, Resource, Value) ->
    do_update(Hypervisor, resource, [Resource, Value]).

%%%===================================================================
%%% Internal Functions
%%%===================================================================


do_update(User, Op) ->
    case sniffle_hypervisor:get(User) of
	{ok, not_found} ->
	    not_found;
	{ok, _UserObj} ->
	    do_write(User, Op)
    end.

do_update(User, Op, Val) ->
    case sniffle_hypervisor:get(User) of
	{ok, not_found} ->
	    not_found;
	{ok, _UserObj} ->
	    do_write(User, Op, Val)
    end.

do_write(User, Op) ->
    sniffle_entity_write_fsm:write({sniffle_hypervisor_vnode, sniffle_hypervisor}, User, Op).

do_write(User, Op, Val) ->
    sniffle_entity_write_fsm:write({sniffle_hypervisor_vnode, sniffle_hypervisor}, User, Op, Val).
