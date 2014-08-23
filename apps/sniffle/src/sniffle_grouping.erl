-module(sniffle_grouping).
-include("sniffle.hrl").

-define(MASTER, sniffle_grouping_vnode_master).
-define(VNODE, sniffle_grouping_vnode).
-define(SERVICE, sniffle_grouping).

-export([
         create_rules/1,
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
              create_rules/1,
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

-spec sniffle_grouping:get(UUID::fifo:uuid()) ->
                 not_found | {ok, Grouping::fifo:grouping()} | {error, timeout}.
get(UUID) ->
    sniffle_entity_read_fsm:start({?VNODE, ?SERVICE}, get, UUID).

%% Add a alement and make sure only vm's can be added to clusters
%% and only cluster groupings can be added to stacks
add_element(UUID, Element) ->
    case sniffle_grouping:get(UUID) of
        {ok, T} ->
            case ft_grouping:type(T) of
                cluster ->
                    case sniffle_vm:get(Element) of
                        {ok, _} ->
                            sniffle_vm:add_grouping(Element, UUID),
                            do_write(UUID, add_element, Element);
                        E ->
                            E
                    end;
                none ->
                    case sniffle_vm:get(Element) of
                        {ok, _} ->
                            sniffle_vm:add_grouping(Element, UUID),
                            do_write(UUID, add_element, Element);
                        E ->
                            E
                    end;
                stack ->
                    case sniffle_grouping:get(Element) of
                        {ok, E} ->
                            case ft_grouping:type(E) of
                                cluster ->
                                    do_write(Element, add_grouping, UUID),
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
    case sniffle_grouping:get(UUID) of
        {ok, T} ->
            case ft_grouping:type(T) of
                stack ->
                    do_write(Element, remove_grouping, UUID),
                    do_write(UUID, remove_element, Element);
                _ ->
                    sniffle_vm:remove_grouping(Element, UUID),
                    do_write(UUID, remove_element, Element)
            end;
        E ->
            E
    end.

create_rules(UUID) ->
    case sniffle_grouping:get(UUID) of
        {ok, T} ->
            case ft_grouping:type(T) of
                %% Clusters impose the rule that the minimal distance from
                %% each hypervisor must be 1 (aka two machines can't be on the
                %% same hypervisor)
                cluster ->
                    VMs = [sniffle_vm:get(VM) ||
                              VM <- ft_grouping:elements(T)],
                    Hs = [ft_vm:hypervisor(VM) ||
                             {ok, VM} <- VMs],
                    Hs1 = [sniffle_hypervisor:get(H) || H <- Hs],
                    Paths = [ft_hypervisor:path(H) || {ok, H} <- Hs1],
                    Paths1 = [{must, {'min-distance', P}, <<"path">>, 1} ||
                                 P <- Paths, P =/= []],
                    %% If the cluster is part of a stack we need to get the
                    %% rules for the stacks it's part of and join them.
                    case ft_grouping:groupings(T) of
                        [] ->
                            Paths1;
                        Gs ->
                            Paths2 = [create_rules(G) || G <- Gs],
                            Paths1 ++ lists:flatten(Paths2)
                    end;
                %% Stacks try to stay as close together as possible,
                %% meaning we give points the lower the distance is.
                stack ->
                    ScaleMin = 10,
                    ScaleMax = 0,
                    Cls = [sniffle_grouping:get(Cl) ||
                              Cl <- ft_grouping:elements(T)],
                    VMs = [ft_grouping:elements(Cl) || {ok, Cl} <- Cls],
                    %% We can remove doublicate VM's before we read them.
                    VMs1 = ordsets:from_list(lists:flatten(VMs)),
                    VMs2 = [sniffle_vm:get(VM) || VM <- VMs1],
                    Hs = [ft_vm:hypervisor(VM) ||
                             {ok, VM} <- VMs2],
                    %% we can remove doublicate hypervisors.
                    Hs1 = ordsets:from_list(Hs),
                    Hs2 = [sniffle_hypervisor:get(H) || H <- Hs1],
                    Paths = [ft_hypervisor:path(H) || {ok, H} <- Hs2],
                    Paths1 = [{'scale-distance', P, <<"path">>, ScaleMin, ScaleMax} ||
                                 P <- Paths, P =/= []],
                    Paths1
            end;
        E ->
            E
    end.

add_grouping(UUID, Element) ->
    case {sniffle_grouping:get(UUID),
          sniffle_grouping:get(Element)} of
        {{ok, T}, {ok, E}} ->
            case {ft_grouping:type(T),
                  ft_grouping:type(E)} of
                {cluster, stack} ->
                    do_write(Element, add_element, UUID),
                    do_write(UUID, add_grouping, Element);
                _ ->
                    {error, not_supported}
            end;
        _ ->
            not_found
    end.

remove_grouping(UUID, Element) ->
    do_write(Element, remove_element, UUID),
    do_write(UUID, remove_grouping, Element).

-spec delete(UUID::fifo:uuid()) ->
                    not_found | {error, timeout} | ok.
delete(UUID) ->
    case sniffle_grouping:get(UUID) of
        {ok, T} ->
            Elements = ft_grouping:elements(T),
            case ft_grouping:type(T) of
                stack ->
                    [do_write(Element, remove_grouping, UUID) ||
                        Element <- Elements];
                _ ->
                    [sniffle_vm:remove_grouping(Element,UUID) ||
                        Element <- Elements],
                    [do_write(Stack, remove_element, UUID) ||
                        Stack <- ft_grouping:groupings(T)]
            end;
        _ ->
            ok
    end,
    do_write(UUID, delete).

-spec list() ->
                  {ok, [UUID::fifo:uuid()]} | {error, timeout}.
list() ->
    sniffle_coverage:start(?MASTER, ?SERVICE, list).

list_() ->
    {ok, Res} = sniffle_full_coverage:raw(?MASTER, ?SERVICE, []),
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
    {ok, Res} = sniffle_full_coverage:list(?MASTER, ?SERVICE, Requirements),
    Res1 = lists:sort(rankmatcher:apply_scales(Res)),
    {ok,  Res1};

list(Requirements, false) ->
    {ok, Res} = sniffle_coverage:start(
                  ?MASTER, ?SERVICE, {list, Requirements}),
    Res1 = rankmatcher:apply_scales(Res),
    {ok,  lists:sort(Res1)}.

do_write(Grouping, Op) ->
    sniffle_entity_write_fsm:write({?VNODE, ?SERVICE}, Grouping, Op).

do_write(Grouping, Op, Val) ->
    sniffle_entity_write_fsm:write({?VNODE, ?SERVICE}, Grouping, Op, Val).
