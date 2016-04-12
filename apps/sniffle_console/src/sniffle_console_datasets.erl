%% @doc Interface for sniffle-admin commands.
-module(sniffle_console_datasets).
-export([commands/0]).

commands() ->
    [
     {["list"], [], [], fun cmd_list/3, help_list()},
     {["get", '*'], [], [], fun cmd_get/3, help_get()},
     {["delete", '*'], [], [], fun cmd_delete/3, help_delete()}
    ].

help_list() ->
    "Prints a list of all datasets".

cmd_list(_, _, _) ->
    {ok, Hs} = sniffle_dataset:list([], true),
    Tbl = lists:map(fun to_tbl/1, Hs),
    [clique_status:table(Tbl)].

help_get() ->
    "Reads a single dataset from the database.".

cmd_get(["sniffle-admin", "datasets", "get", UUIDs], _, _) ->
    UUID = list_to_binary(UUIDs),
    case sniffle_dataset:get(UUID) of
        {ok, H} ->
            Tbl = [to_tbl(H)],
            [clique_status:table(Tbl)];
        not_found ->
            [clique_status:alert([clique_status:text("Dataset not found.")])]
    end.

help_delete() ->
    "Deletes an object from the database.".

cmd_delete(["sniffle-admin", "datasets", "delete", UUIDs], _, _) ->
    UUID = list_to_binary(UUIDs),
    case sniffle_dataset:delete(UUID) of
        ok ->
            [clique_status:text("Dataset deleted.")];
        not_found ->
            [clique_status:alert([clique_status:text("Dataset not found.")])];
        _ ->
            [clique_status:alert([clique_status:text("Deletion failed.")])]
    end.

to_tbl({_, D}) ->
    to_tbl(D);
to_tbl(D) ->
    [{"UUID",        ft_dataset:uuid(D)},
     {"OS",          ft_dataset:os(D)},
     {"Name",        ft_dataset:name(D)},
     {"Version",     ft_dataset:version(D)},
     {"Imported",    ft_dataset:imported(D) * 100},
     {"Description", ft_dataset:description(D)}].
