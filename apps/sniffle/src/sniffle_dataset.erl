-module(sniffle_dataset).
-include("sniffle.hrl").
                                                %-include_lib("riak_core/include/riak_core_vnode.hrl").

-export([
         create/1,
         delete/1,
         get/1,
         list/0,
         list/1,
         set/2,
         set/3
        ]).

create(Dataset) ->
    case sniffle_dataset:get(Dataset) of
        {ok, not_found} ->
            do_write(Dataset, create, []);
        {ok, _RangeObj} ->
            duplicate
    end.

delete(Dataset) ->
    do_write(Dataset, delete).

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

list(Requirements) ->
    sniffle_entity_coverage_fsm:start(
      {sniffle_dataset_vnode, sniffle_dataset},
      list, Requirements
     ).

set(Dataset, Attribute, Value) ->
    do_write(Dataset, set, [{Attribute, Value}]).


set(Dataset, Attributes) ->
    do_write(Dataset, set, Attributes).


%%%===================================================================
%%% Internal Functions
%%%===================================================================

do_write(Dataset, Op) ->
    case sniffle_entity_write_fsm:write({sniffle_dataset_vnode, sniffle_dataset}, Dataset, Op) of
        {ok, not_found} ->
            not_found;
        R ->
            R
    end.

do_write(Dataset, Op, Val) ->
    case sniffle_entity_write_fsm:write({sniffle_dataset_vnode, sniffle_dataset}, Dataset, Op, Val) of
        {ok, not_found} ->
            not_found;
        R ->
            R
    end.
