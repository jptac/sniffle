-module(sniffle_dataset).
-include("sniffle.hrl").
%-include_lib("riak_core/include/riak_core_vnode.hrl").

-export([
	 create/1,
	 delete/1,
	 get/1,
	 list/0,
	 list/1,
	 get_attribute/2,
	 get_attribute/1,
	 set_attribute/2,
	 set_attribute/3
	]).

create(Dataset) ->
    case sniffle_dataset:get(Dataset) of
	{ok, not_found} ->
	    do_write(Dataset, create, []);
	{ok, _RangeObj} ->
	    duplicate
    end.

delete(Dataset) ->    
    do_update(Dataset, delete).

get(Dataset) ->
    sniffle_entity_read_fsm:start(
      {sniffle_dataset_vnode, sniffle_dataset},
      get, Dataset
     ).

list() ->
    sniffle_entity_coverage_fsm:start(
      {sniffle_dataset_vnode, sniffle_dataset},
      list
     ).

list(User) ->
    sniffle_entity_coverage_fsm:start(
      {sniffle_dataset_vnode, sniffle_dataset},
      list, undefined, User
     ).

get_attribute(Dataset) ->
    case sniffle_dataset:get(Dataset) of
	{ok, not_found} ->
	    not_found;
	{ok, V} ->
	    dict:to_list(V#dataset.attributes)
    end.

get_attribute(Dataset, Attribute) ->
    case sniffle_dataset:get(Dataset) of
	{ok, not_found} ->
	    not_found;
	{ok, V} ->
	    case dict:find(Attribute, V#dataset.attributes) of
		error ->
		    not_found;
		Result ->
		    Result
	    end
    end.

set_attribute(Dataset, Attribute, Value) ->
    do_update(Dataset, set_attribute, [Attribute, Value]).


set_attribute(Dataset, Attributes) ->
    do_update(Dataset, mset_attribute, Attributes).


%%%===================================================================
%%% Internal Functions
%%%===================================================================


do_update(Dataset, Op) ->
    case sniffle_dataset:get(Dataset) of
	{ok, not_found} ->
	    not_found;
	{ok, _RangeObj} ->
	    do_write(Dataset, Op)
    end.

do_update(Dataset, Op, Val) ->
    case sniffle_dataset:get(Dataset) of
	{ok, not_found} ->
	    not_found;
	{ok, _RangeObj} ->
	    do_write(Dataset, Op, Val)
    end.

do_write(Dataset, Op) ->
    sniffle_entity_write_fsm:write({sniffle_dataset_vnode, sniffle_dataset}, Dataset, Op).

do_write(Dataset, Op, Val) ->
    sniffle_entity_write_fsm:write({sniffle_dataset_vnode, sniffle_dataset}, Dataset, Op, Val).
