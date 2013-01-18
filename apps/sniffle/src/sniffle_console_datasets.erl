%% @doc Interface for sniffle-admin commands.
-module(sniffle_console_datasets).
-export([command/2, help/0]).

help() ->
    io:format("Usage~n"),
    io:format("  list [-p]~n"),
    io:format("  get [-p] <uuid>~n"),
    io:format("  delete <uuid>~n").

command(text, ["delete", ID]) ->
    case sniffle_package:delete(list_to_binary(ID)) of
        ok ->
            io:format("Package ~s delete.~n", [ID]),
            ok;
        E ->
            io:format("Package ~s not deleted (~p).~n", [ID, E]),
            ok
    end;

command(json, ["get", UUID]) ->
    case sniffle_dataset:get(list_to_binary(UUID)) of
        {ok, H} ->
            sniffle_console:pp_json(H),
            ok;
        _ ->
            sniffle_console:pp_json([]),
            error
    end;

command(text, ["get", ID]) ->
    io:format("UUID                                 OS      Name            Version  Desc~n"),
    io:format("------------------------------------ ------- --------------- -------- --------------~n", []),
    case sniffle_dataset:get(list_to_binary(ID)) of
        {ok, D} ->
            io:format("~36s ~7s ~15s ~8s ~s~n",
                      [ID,
                       jsxd:get(<<"os">>, <<"-">>, P),
                       jsxd:get(<<"name">>, <<"-">>, P),
                       jsxd:get(<<"version">>, <<"-">>, P),
                       jsxd:get(<<"description">>, <<"-">>, P)]),
            ok;
        _ ->
            error
    end;

command(json, ["list"]) ->
    case sniffle_dataset:list() of
        {ok, Hs} ->
            sniffle_console:pp_json(lists:map(fun (ID) ->
                                                      {ok, H} = sniffle_dataset:get(ID),
                                                      H
                                              end, Hs)),
            ok;
        _ ->
            sniffle_console:pp_json([]),
            error
    end;

command(text, ["list"]) ->
    io:format("UUID                                 OS      Name            Version  Desc~n"),
    io:format("------------------------------------ ------- --------------- -------- --------------~n", []),
    case sniffle_dataset:list() of
        {ok, Ds} ->
            lists:map(fun (ID) ->
                              {ok, D} = sniffle_dataset:get(ID),
                              io:format("~36s ~7s ~15s ~8s ~s~n",
                                        [ID,
                                         jsxd:get(<<"os">>, <<"-">>, D),
                                         jsxd:get(<<"name">>, <<"-">>, D),
                                         jsxd:get(<<"version">>, <<"-">>, D),
                                         jsxd:get(<<"description">>, <<"-">>, D)]),
                      end, Ds);
        _ ->
            []
    end;

command(_, C) ->
    io:format("Unknown parameters: ~p", [C]),
    error.
