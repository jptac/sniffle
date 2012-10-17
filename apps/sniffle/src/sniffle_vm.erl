-module(sniffle_vm).

-include("sniffle.hrl").

-export(
   [
    register/2,
    unregister/1,
    create/3,
    get/1,
    list/0,
    list/1,
    get_attribute/2,
    get_attribute/1,
    set_attribute/2,
    set_attribute/3
   ]
  ).

register(Vm, Hypervisor) ->
    case sniffle_vm:get(Vm) of
	{ok, not_found} ->
	    do_write(Vm, register, Hypervisor);
	{ok, _Obj} ->
	    duplicate
    end.

unregister(Vm) ->    
    do_update(Vm, delete).

create(Package, Dataset, Owner) ->
    UUID = uuid:to_string(uuid:uuid1()),
    sniffle_create_fsm:create(UUID, Package, Dataset, Owner),
    {ok, UUID}.    

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

get_attribute(Vm) ->
    case sniffle_vm:get(Vm) of
	{ok, not_found} ->
	    not_found;
	{ok, V} ->
	    dict:to_list(V#vm.attributes)
    end.

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
    do_update(Vm, set_attribute, [Attribute, Value]).


set_attribute(Vm, Attributes) ->
    do_update(Vm, mset_attribute, Attributes).

%%%===================================================================
%%% Internal Functions
%%%===================================================================

do_update(VM, Op) ->
    case sniffle_vm:get(VM) of
	{ok, not_found} ->
	    not_found;
	{ok, _Obj} ->
	    do_write(VM, Op)
    end.

do_update(VM, Op, Val) ->
    case sniffle_vm:get(VM) of
	{ok, not_found} ->
	    not_found;
	{ok, _Obj} ->
	    do_write(VM, Op, Val)
    end.

do_write(VM, Op) ->
    sniffle_entity_write_fsm:write({sniffle_vm_vnode, sniffle_vm}, VM, Op).

do_write(VM, Op, Val) ->
    sniffle_entity_write_fsm:write({sniffle_vm_vnode, sniffle_vm}, VM, Op, Val).
