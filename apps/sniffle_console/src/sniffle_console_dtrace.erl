%% @doc Interface for sniffle-admin commands.
-module(sniffle_console_dtrace).
-export([commands/0]).

-define(T, ft_dtrace).
-define(F(Hs, Vs), fifo_console:fields(Hs, Vs)).
-define(H(Hs), fifo_console:hdr(Hs)).
-define(HDR, [{"UUID", 36}, {"Name", 15}]).


commands() ->
    [
     {["list"], [], [], fun cmd_list/3, help_list()},
     {["get", '*'], [], [], fun cmd_get/3, help_get()},
     {["import", '*'], [], [], fun cmd_import/3, help_import()},
     {["delete", '*'], [], [], fun cmd_delete/3, help_delete()}
    ].

help_list() ->
    "Prints a list of all dtrace scripts".

cmd_list(_, _, _) ->
    {ok, Hs} = sniffle_dtrace:list([], true),
    Tbl = lists:map(fun to_tbl/1, Hs),
    [clique_status:table(Tbl)].

help_get() ->
    "Reads a single dtrace script from the database.".

cmd_get(["sniffle-admin", "dtrace", "get", UUIDs], _, _) ->
    UUID = list_to_binary(UUIDs),
    case sniffle_dtrace:get(UUID) of
        {ok, D} ->
            Tbl = [to_tbl(D)],
            [clique_status:table(Tbl) |
             print_vars(D)] ++
                [clique_status:text(
                   io:format("~.78c~n~s~n~.78c~n",
                             [$=, ?T:script(D), $=]))];
        not_found ->
            [clique_status:alert([clique_status:text("Dtrace not found.")])]
    end.

help_delete() ->
    "Deletes an object from the database.".

cmd_delete(["sniffle-admin", "dtrace", "delete", UUIDs], _, _) ->
    UUID = list_to_binary(UUIDs),
    case sniffle_dtrace:delete(UUID) of
        ok ->
            [clique_status:text("Dtrace deleted.")];
        not_found ->
            [clique_status:alert([clique_status:text("Dtrace not found.")])];
        _ ->
            [clique_status:alert([clique_status:text("Deletion failed.")])]
    end.

help_import() ->
    "Imports a script json format.".


cmd_import(["sniffle-admin", "dtrace", "import", File], _, _) ->
    case file:read_file(File) of
        {error, enoent} ->
            [clique_status:alert([clique_status:text("Could not open file.")])];
        {ok, B} ->
            JSON = jsx:decode(B),
            JSX = jsxd:from_list(JSON),
            Name = jsxd:get(<<"name">>, <<"unnamed">>, JSX),
            %% CopyFields = [<<"config">>, <<"type">>, <<"filter">>],
            {ok, Config} = jsxd:get([<<"config">>], JSX),
            %% {ok, Type} = jsxd:get([<<"config">>], JSX),
            %% {ok, Filter} = jsxd:get([<<"config">>], JSX),
            {ok, UUID} =
                sniffle_dtrace:add(
                  Name,
                  binary_to_list(jsxd:get(<<"script">>, <<"">>, JSX))),
            sniffle_dtrace:set_config(UUID, Config),
            %% sniffle_dtrace:type(UUID, Type),
            %% sniffle_dtrace:set_filter(UUID, Filter),
            Str = io_lib:format("Imported ~s with uuid ~s.~n", [Name, UUID]),
            [clique_status:text(Str)]
    end.

to_tbl({_, N}) ->
    to_tbl(N);
to_tbl(N) ->
    [{"UUDI", ft_dtrace:uuid(N)},
     {"Name", ft_dtrace:name(N)}].

print_vars(D) ->
    Tbl =[[{"Variable", N}, {"Default", Def}] || {N, Def} <- ?T:config(D)],
    clique_status:table(Tbl).
