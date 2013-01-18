%% @doc Interface for sniffle-admin commands.
-module(sniffle_console_packages).
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
    case sniffle_package:get(list_to_binary(UUID)) of
        {ok, H} ->
            sniffle_console:pp_json(H),
            ok;
        _ ->
            sniffle_console:pp_json([]),
            error
    end;

command(text, ["get", ID]) ->
    io:format("Name            Ram      Quota  CPU Cap~n"),
    io:format("--------------- -------- ------ -------~n", []),
    case sniffle_package:get(list_to_binary(ID)) of
        {ok, P} ->
            io:format("~-15s ~-6pMB ~-7pGB ~-5s%~n",
                      [ID,
                       jsxd:get(<<"ram">>, 0, P),
                       jsxd:get(<<"quota">>, 0, P),
                       jsxd:get(<<"cpu_cap">>, 0, P)]),
            ok;
        _ ->
            error
    end;

command(json, ["list"]) ->
    case sniffle_package:list() of
        {ok, Hs} ->
            sniffle_console:pp_json(lists:map(fun (ID) ->
                                                      {ok, H} = sniffle_package:get(ID),
                                                      H
                                              end, Hs)),
            ok;
        _ ->
            sniffle_console:pp_json([]),
            error
    end;

command(text, ["list"]) ->
    io:format("Name            Ram      Quota  CPU Cap~n"),
    io:format("--------------- -------- ------ -------~n", []),
    case sniffle_package:list() of
        {ok, Ps} ->
            lists:map(fun (ID) ->
                              {ok, P} = sniffle_package:get(list_to_binary(ID)),
                              io:format("~-15s ~-6pMB ~-7pGB ~-5s%~n",
                                        [ID,
                                         jsxd:get(<<"ram">>, 0, P),
                                         jsxd:get(<<"quota">>, 0, P),
                                         jsxd:get(<<"cpu_cap">>, 0, P)])
                      end, Ps);
        _ ->
            []
    end;

command(_, C) ->
    io:format("Unknown parameters: ~p", [C]),
    error.
