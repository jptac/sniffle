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
         list_/0,
         metadata_set/2,
         metadata_set/3
        ]).

-ignore_xref([
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
    case do_write(UUID, create, [Name, Type]) of
        ok ->
            {ok, UUID};
        E ->
            E
    end.

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

%% Add a alement and make sure only vm's can be added to clusters
%% and only cluster groupings can be added to stacks
add_element(UUID, Element) ->
    case get_(UUID) of
        {ok, T} ->
            case sniffle_grouping_state:type(T) of
                cluster ->
                    case sniffle_vm:get(UUID) of
                        {ok, _} ->
                            do_write(UUID, add_element, Element);
                        E ->
                            E
                    end;
                stack ->
                    case get_(Element) of
                        {ok, E} ->
                            case sniffle_grouping_state:type(E) of
                                cluster ->
                                    do_write(UUID, add_element, Element);
                                _ ->
                                    {error, not_supported}
                            end;
                        E ->
                            E
                    end;
                _ ->
                    {error, not_supported}
            end;
        E ->
            E
    end.


remove_element(UUID, Element) ->
    do_write(UUID, remove_element, Element).

add_grouping(UUID, Element) ->
    case {get_(UUID), get_(Element)} of
        {{ok, T}, {ok, E}} ->
            case {sniffle_grouping_state:type(T),
                  sniffle_grouping_state:type(E)} of
                {stack, cluster} ->
                    do_write(UUID, add_grouping, Element);
                _ ->
                    {error, not_supported}
            end;
        _ ->
            not_found
    end.

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

-spec metadata_set(Grouping::fifo:uuid(),
                   Attirbute::fifo:key(), Value::fifo:value()) ->
                          not_found |
                          {error, timeout} |
                          ok.
metadata_set(Grouping, Attribute, Value) ->
    metadata_set(Grouping, [{Attribute, Value}]).

-spec metadata_set(Grouping::fifo:uuid(), Attirbutes::fifo:attr_list()) ->
                 not_found |
                 {error, timeout} |
                 ok.
metadata_set(Grouping, Attributes) ->
    do_write(Grouping, metadata_set, Attributes).

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
