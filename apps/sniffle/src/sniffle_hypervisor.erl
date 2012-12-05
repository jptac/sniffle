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
    get_resource/1,
    get_resource/2,
    set_resource/3,
    set_resource/2,
    status/0
   ]).

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

status() ->
    {ok, Stat} = sniffle_entity_coverage_fsm:start(
		   {sniffle_hypervisor_vnode, sniffle_hypervisor},
		   status
		  ),
    Stat1  = lists:foldl(fun ({R, W}, {R0, W0}) ->
				 R1 = lists:foldl(fun ({K, V}, [{K, V0} | Rest]) when
							    is_list(V0),
							    is_list(V) ->
							  [{K, V ++ V0} | Rest];
						      ({K, V}, [{K, V0} | Rest]) when
							    is_number(V0),
							    is_number(V) ->
							  [{K, V + V0} | Rest];
						      ({K, V}, Rest) ->
							  [{K, V} | Rest]
						  end, [],
						  lists:keymerge(1, lists:keysort(1, R), lists:keysort(1, R0))),
				 {R1, W ++ W0}
			 end, {[],[]}, Stat),
    {ok, Stat1}.


list() ->
    sniffle_entity_coverage_fsm:start(
      {sniffle_hypervisor_vnode, sniffle_hypervisor},
      list
     ).

list(Requirements) ->
    {ok, Res} = sniffle_entity_coverage_fsm:start(
		  {sniffle_hypervisor_vnode, sniffle_hypervisor},
		  list, Requirements
		 ),
   {ok,  lists:keysort(2, Res)}.

get_resource(Hypervisor) ->
    case sniffle_hypervisor:get(Hypervisor) of
	{ok, not_found} ->
	    not_found;
	{ok, V} ->
	    {ok, dict:to_list(V#hypervisor.resources)}
    end.

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
    do_update(Hypervisor, set_resource, [Resource, Value]).

set_resource(Hypervisor, Resources) ->
    do_update(Hypervisor, mset_resource, Resources).

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
