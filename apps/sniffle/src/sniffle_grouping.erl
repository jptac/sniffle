-module(sniffle_grouping).
-include("sniffle.hrl").

-define(MASTER, sniffle_grouping_vnode_master).
-define(VNODE, sniffle_grouping_vnode).
-define(SERVICE, sniffle_grouping).

-export([
         create/2,
         delete/1,
         get/1,
         list/0,
         list/2,
         wipe/1,
         sync_repair/2,
         add_element/2,
         remove_element/2,
         add_grouping/2,
         remove_grouping/2,
         list_/0
        ]).

-ignore_xref([
              create/2,
              delete/1,
              list/0,
              list/2,
              add_element/2,
              remove_element/2,
              add_grouping/2,
              remove_grouping/2,
              sync_repair/2,
              list_/0,
              wipe/1
              ]).

wipe(UUID) ->
    sniffle_coverage:start(?MASTER, ?SERVICE, {wipe, UUID}).

sync_repair(UUID, Obj) ->
    do_write(UUID, sync_repair, Obj).

-spec create(Name::binary(), Type::cluster|stack|none) ->
                    duplicate | ok | {error, timeout}.
create(Name, Type) ->
    UUID = uuid:uuid4s(),
    do_write(UUID, create, [Name, Type]).

-spec get(UUID::fifo:uuid()) ->
                 not_found | {ok, Grouping::fifo:grouping()} | {error, timeout}.
get(UUID) ->
    case get_(UUID) of
        {ok, G} ->
            {ok, sniffle_grouping_state:to_json(G)};
        R ->
            R
    end.
get_(UUID) ->
    sniffle_entity_read_fsm:start({?VNODE, ?SERVICE}, get, UUID).

add_element(UUID, Element) ->
    do_write(UUID, add_element, Element).

remove_element(UUID, Element) ->
    do_write(UUID, remove_element, Element).

add_grouping(UUID, Element) ->
    do_write(UUID, add_grouping, Element).

remove_grouping(UUID, Element) ->
    do_write(UUID, remove_grouping, Element).

-spec delete(UUID::fifo:uuid()) ->
                    not_found | {error, timeout} | ok.
delete(UUID) ->
    do_write(UUID, delete).

-spec list() ->
                  {ok, [UUID::fifo:uuid()]} | {error, timeout}.
list() ->
    sniffle_coverage:start(?MASTER, ?SERVICE, list).

list_() ->
    {ok, Res} = sniffle_full_coverage:start(
                  sniffle_grouping_vnode_master, sniffle_grouping,
                  {list, [], true, true}),
    Res1 = [R || {_, R} <- Res],
    {ok,  Res1}.

%%--------------------------------------------------------------------
%% @doc Lists all vm's and fiters by a given matcher set.
%% @end
%%--------------------------------------------------------------------

list(Requirements, true) ->
    {ok, Res} = sniffle_full_coverage:start(
                  ?MASTER, ?SERVICE, {list, Requirements, true}),
    Res1 = rankmatcher:apply_scales(Res),
    Res2 = [{P, sniffle_grouping_state:to_json(G)} || {P, G} <- Res1],
    {ok,  lists:sort(Res2)};

list(Requirements, false) ->
    {ok, Res} = sniffle_coverage:start(
                  ?MASTER, ?SERVICE, {list, Requirements}),
    Res1 = rankmatcher:apply_scales(Res),
    {ok,  lists:sort(Res1)}.

do_write(Grouping, Op) ->
    sniffle_entity_write_fsm:write({?VNODE, ?SERVICE}, Grouping, Op).

do_write(Grouping, Op, Val) ->
    sniffle_entity_write_fsm:write({?VNODE, ?SERVICE}, Grouping, Op, Val).
