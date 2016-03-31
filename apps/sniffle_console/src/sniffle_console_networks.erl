%% @doc Interface for sniffle-admin commands.
-module(sniffle_console_networks).
-export([commands/0]).


commands() ->
    [
     {["list"], [], [], fun cmd_list/3, help_list()},
     {["get", '*'], [], [], fun cmd_get/3, help_get()},
     {["delete", '*'], [], [], fun cmd_delete/3, help_delete()}
    ].

help_list() ->
    "Prints a list of all networks".

cmd_list(_, _, _) ->
    {ok, Hs} = sniffle_network:list([], true),
    Tbl = lists:map(fun to_tbl/1, Hs),
    [clique_status:table(Tbl)].

help_get() ->
    "Reads a single network from the database.".

cmd_get(["sniffle-admin", "networks", "get", UUIDs], _, _) ->
    UUID = list_to_binary(UUIDs),
    case sniffle_network:get(UUID) of
        {ok, H} ->
            Tbl = [to_tbl(H)],
            [clique_status:table(Tbl)];
        not_found ->
            [clique_status:alert([clique_status:text("Network not found.")])]
    end.

help_delete() ->
    "Deletes an object from the database.".

cmd_delete(["sniffle-admin", "networks", "delete", UUIDs], _, _) ->
    UUID = list_to_binary(UUIDs),
    case sniffle_network:delete(UUID) of
        ok ->
            [clique_status:text("Network deleted.")];
        not_found ->
            [clique_status:alert([clique_status:text("Network not found.")])];
        _ ->
            [clique_status:alert([clique_status:text("Deletion failed.")])]
    end.

to_tbl({_, N}) ->
    to_tbl(N);
to_tbl(N) ->
    [{"UUDI", ft_network:uuid(N)},
     {"Name", ft_network:name(N)},
     {"IPRanges", length(ft_network:ipranges(N))}].
