%% @doc Interface for sniffle-admin commands.
-module(sniffle_console_datasets).
-export([commands/0]).

commands() ->
    [
     {["servers", "list"], [], [],
      fun cmd_servers_list/3, help_servers_list()},
     {["servers", "add", '*'], [], [],
      fun cmd_servers_add/3, help_servers_add()},
     {["servers", "remove", '*'], [], [],
      fun cmd_servers_remove/3, help_servers_remove()},
     {["list"], [], [], fun cmd_list/3, help_list()},
     {["get", '*'], [], [], fun cmd_get/3, help_get()},
     {["delete", '*'], [], [], fun cmd_delete/3, help_delete()}
    ].


help_servers_list() ->
    "Lists configured dataser servers".
cmd_servers_list(_, _, _) ->
    Servers = case sniffle_opt:get("endpoints", "datasets", "servers") of
                 undefined -> [];
                 L -> L
             end,
    [io:format(" * ~s~n", [S]) || S <- Servers],
    [].

help_servers_remove() ->
    "Removes a dataset server".
cmd_servers_remove(["sniffle-admin", "datasets", "servers",
                    "remove", Server], _, _) ->
    ServerB = list_to_binary(Server),
    Servers = case sniffle_opt:get("endpoints", "datasets", "servers") of
                  undefined -> [];
                  L -> L
              end,
    Servers1 = lists:delete(ServerB, Servers),
    sniffle_opt:set(["endpoints", "datasets", "servers"], Servers1),
    [clique_status:text("Server removed")].

help_servers_add() ->
    "Adds a dataset server".
cmd_servers_add(["sniffle-admin", "datasets", "servers",
                 "add", Server], _, _) ->
    ServerB = list_to_binary(Server),
    Servers = case sniffle_opt:get("endpoints", "datasets", "servers") of
                  undefined -> [];
                  L -> L
              end,
    case lists:member(ServerB, Servers) of
        true ->
            [clique_status:alert(
               [clique_status:text("Server already selected")])];
        false ->
            Servers1 = Servers ++ [ServerB],
            sniffle_opt:set(["endpoints", "datasets", "servers"], Servers1),
            [clique_status:text("Server added")]
    end.


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
