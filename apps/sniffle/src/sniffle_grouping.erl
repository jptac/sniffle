-module(sniffle_grouping).
-define(CMD, sniffle_grouping_cmd).
-define(BUCKET, <<"grouping">>).
-define(S, ft_grouping).
-include("sniffle.hrl").

-define(FM(Met, Mod, Fun, Args),
        folsom_metrics:histogram_timed_update(
          {sniffle, grouping, Met},
          Mod, Fun, Args)).

-export([
         create_rules/1,
         create/2,
         delete/1,
         add_element/2,
         remove_element/2,
         add_grouping/2,
         remove_grouping/2,
         set_metadata/2,
         set_config/2
        ]).

%%%===================================================================
%%% General section
%%%===================================================================
-spec sniffle_grouping:get(UUID::fifo:uuid()) ->
                                  not_found |
                                  {ok, Grouping::fifo:grouping()} |
                                  {error, timeout}.
-spec list() ->
                  {ok, [UUID::fifo:uuid()]} | {error, timeout}.


-include("sniffle_api.hrl").
%%%===================================================================
%%% Custom section
%%%===================================================================

-spec create(Name::binary(), Type::cluster|stack|none) ->
                    duplicate | {ok, ClusterID :: binary()} | {error, timeout}.
create(Name, Type) ->
    UUID = fifo_utils:uuid(grouping),
    case do_write(UUID, create, [Name, Type]) of
        ok ->
            {ok, UUID};
        E ->
            E
    end.

%% Add a alement and make sure only vm's can be added to clusters
%% and only cluster groupings can be added to stacks
add_element(UUID, Element) ->
    case sniffle_grouping:get(UUID) of
        {ok, T} ->
            case ft_grouping:type(T) of
                cluster ->
                    add_to_cluster(UUID, Element);
                none ->
                    add_to_cluster(UUID, Element);
                stack ->
                    add_to_stack(UUID, Element);
                _ ->
                    {error, not_supported}
            end;
        E ->
            E
    end.

add_to_cluster(UUID, Element) ->
    case sniffle_vm:get(Element) of
        {ok, _} ->
            sniffle_vm:add_grouping(Element, UUID),
            do_write(UUID, add_element, [Element]);
        E ->
            E
    end.

add_to_stack(UUID, Element) ->
    case sniffle_grouping:get(Element) of
        {ok, E} ->
            case ft_grouping:type(E) of
                cluster ->
                    do_write(Element, add_grouping, [UUID]),
                    do_write(UUID, add_element, [Element]);
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
                    do_write(Element, remove_grouping, [UUID]),
                    do_write(UUID, remove_element, [Element]);
                _ ->
                    sniffle_vm:remove_grouping(Element, [UUID]),
                    do_write(UUID, remove_element, [Element])
            end;
        E ->
            E
    end.

create_rules(UUID) ->
    case sniffle_grouping:get(UUID) of
        {ok, T} ->
            case ft_grouping:type(T) of
                cluster ->
                    create_cluster_rules(T);
                stack ->
                    create_stack_rules(T)
            end;
        E ->
            E
    end.

%% Clusters impose the rule that the minimal distance from
%% each hypervisor must be 1 (aka two machines can't be on the
%% same hypervisor)
create_cluster_rules(T) ->
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
    end.

%% Stacks try to stay as close together as possible,
%% meaning we give points the lower the distance is.
create_stack_rules(T) ->
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
    Paths1.

add_grouping(UUID, Element) ->
    case {sniffle_grouping:get(UUID),
          sniffle_grouping:get(Element)} of
        {{ok, T}, {ok, E}} ->
            case {ft_grouping:type(T),
                  ft_grouping:type(E)} of
                {cluster, stack} ->
                    do_write(Element, add_element, [UUID]),
                    do_write(UUID, add_grouping, [Element]);
                _ ->
                    {error, not_supported}
            end;
        _ ->
            not_found
    end.

remove_grouping(UUID, Element) ->
    do_write(Element, remove_element, [UUID]),
    do_write(UUID, remove_grouping, [Element]).

-spec delete(UUID::fifo:uuid()) ->
                    not_found | {error, timeout} | ok.
delete(UUID) ->
    case sniffle_grouping:get(UUID) of
        {ok, T} ->
            Elements = ft_grouping:elements(T),
            case ft_grouping:type(T) of
                stack ->
                    [do_write(Element, remove_grouping, [UUID]) ||
                        Element <- Elements];
                _ ->
                    [sniffle_vm:remove_grouping(Element, [UUID]) ||
                        Element <- Elements],
                    [do_write(Stack, remove_element, [UUID]) ||
                        Stack <- ft_grouping:groupings(T)]
            end;
        _ ->
            ok
    end,
    do_write(UUID, delete).

?SET(set_metadata).
?SET(set_config).

