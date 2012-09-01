-module(sniffle_vm).
-include("sniffle.hrl").
%-include_lib("riak_core/include/riak_core_vnode.hrl").


-export(
   [
    register/2,
    unregister/1,
    get/1,
    list/0,
    list/1,
    get_attribute/2,
    set_attribute/3
   ]
  ).



register(Vm, Hypervisor) ->
    case sniffle_vm:get(Vm) of
	{ok, not_found} ->
	    do_write(Vm, register, Hypervisor);
	{ok, _UserObj} ->
	    duplicate
    end.

unregister(Vm) ->    
    do_update(Vm, delete).

get(Vm) ->
    sniffle_entity_read_fsm:start(
      {sniffle_vm_vnode, sniffle_vm},
      get, Vm
     ).

list() ->
    sniffle_entity_coverage_fsm:start(
      {sniffle_vm_vnode, sniffle_vm},
      list
     ).

list(User) ->
    sniffle_entity_coverage_fsm:start(
      {sniffle_vm_vnode, sniffle_vm},
      list, undefined, User
     ).

get_attribute(Vm, Attribute) ->
    case sniffle_vm:get(Vm) of
	{ok, not_found} ->
	    not_found;
	{ok, V} ->
	    case dict:find(Attribute, V#vm.attributes) of
		error ->
		    not_found;
		Result ->
		    Result
	    end
    end.

set_attribute(Vm, Attribute, Value) ->
    do_update(Vm, resource, [Attribute, Value]).


%%%===================================================================
%%% Internal Functions
%%%===================================================================


do_update(User, Op) ->
    case sniffle_user:get(User) of
	{ok, not_found} ->
	    not_found;
	{ok, _UserObj} ->
	    do_write(User, Op)
    end.

do_update(User, Op, Val) ->
    case sniffle_user:get(User) of
	{ok, not_found} ->
	    not_found;
	{ok, _UserObj} ->
	    do_write(User, Op, Val)
    end.

do_write(User, Op) ->
    sniffle_entity_write_fsm:write({sniffle_vm_vnode, sniffle_vm}, User, Op).

do_write(User, Op, Val) ->
    sniffle_entity_write_fsm:write({sniffle_vm_vnode, sniffle_vm}, User, Op, Val).
