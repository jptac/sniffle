%% @doc Interface for sniffle-admin commands.
-module(sniffle_console_datasets).
-export([command/2, help/0]).


-include_lib("kernel/include/file.hrl").
-include("sniffle_version.hrl").

-define(F(Hs, Vs), sniffle_console:fields(Hs,Vs)).
-define(H(Hs), sniffle_console:hdr(Hs)).
-define(Hdr, [{"UUID", 36}, {"OS", 7}, {"Name", 15}, {"Version", 8},
              {"Imported", 7}, {"Desc", n}]).

help() ->
    io:format("Usage~n"
              "  list [-j]~n"
              "  get [-j] <uuid>~n"
              "  delete <uuid>~n").

command(text, ["delete", ID]) ->
    case sniffle_dataset:delete(list_to_binary(ID)) of
        ok ->
            io:format("Dataset ~s delete.~n", [ID]),
            ok;
        E ->
            io:format("Dataset ~s not deleted (~p).~n", [ID, E]),
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
    ?H(?Hdr),
    case sniffle_dataset:get(list_to_binary(ID)) of
        {ok, D} ->
            print(D),
            ok;
        _ ->
            error
    end;

command(json, ["list"]) ->
    case sniffle_dataset:list() of
        {ok, Hs} ->
            sniffle_console:pp_json(
              lists:map(fun (ID) ->
                                {ok, H} = sniffle_dataset:get(ID),
                                H
                        end, Hs)),
            ok;
        _ ->
            sniffle_console:pp_json([]),
            error
    end;

command(text, ["list"]) ->
    ?H(?Hdr),
    case sniffle_dataset:list() of
        {ok, Ds} ->
            lists:map(fun (ID) ->
                              {ok, D} = sniffle_dataset:get(ID),
                              print(D)
                      end, Ds);
        _ ->
            []
    end;

command(_, C) ->
    io:format("Unknown parameters: ~p", [C]),
    error.

print(D) ->
    ?F(?Hdr, [jsxd:get(<<"dataset">>, <<"-">>, D),
              jsxd:get(<<"os">>, <<"-">>, D),
              jsxd:get(<<"name">>, <<"-">>, D),
              jsxd:get(<<"version">>, <<"-">>, D),
              jsxd:get(<<"imported">>, 1, D) * 100,
              jsxd:get(<<"description">>, <<"-">>, D)]).
