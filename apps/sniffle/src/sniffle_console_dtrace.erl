%% @doc Interface for sniffle-admin commands.
-module(sniffle_console_dtrace).
-export([command/2, help/0]).

help() ->
    io:format("Usage~n"),
    io:format("  list [-j]~n"),
    io:format("  get [-j] <uuid>~n"),
    io:format("  delete <uuid>~n").

command(text, ["delete", ID]) ->
    case sniffle_dtrace:delete(list_to_binary(ID)) of
        ok ->
            io:format("Dtrace ~s delete.~n", [ID]),
            ok;
        E ->
            io:format("Dtrace ~s not deleted (~p).~n", [ID, E]),
            ok
    end;

command(json, ["get", UUID]) ->
    case sniffle_dtrace:get(list_to_binary(UUID)) of
        {ok, H} ->
            sniffle_console:pp_json(jsxd:update(<<"script">>,
                                                fun(S) ->
                                                        list_to_binary(S)
                                                end, <<>>, H)),
            ok;
        _ ->
            sniffle_console:pp_json([]),
            error
    end;

command(text, ["get", ID]) ->
    header(),
    case sniffle_dtrace:get(list_to_binary(ID)) of
        {ok, D} ->
            print(D),
            print_vars(D),
            io:format("~.78c~n~s~n~.78c~n", [$=, jsxd:get(<<"script">>,<<"">>, D), $=]),
            ok;
        _ ->
            error
    end;

command(_, ["import", File]) ->
    {ok, B} = file:read_file(File),
    JSON = jsx:decode(B),
    JSX = jsxd:from_list(JSON),
    Name = jsxd:get(<<"name">>, <<"unnamed">>, JSX),
    {ok, UUID} = sniffle_dtrace:add(Name,
                                    binary_to_list(jsxd:get(<<"script">>, <<"">>, JSX))),
    sniffle_dtrace:set(UUID, [{<<"config">>, jsxd:get(<<"config">>, [], JSX)}]),
    io:format("Imported ~s with uuid ~s.~n", [Name, UUID]),
    ok;

command(json, ["list"]) ->
    case sniffle_dtrace:list() of
        {ok, Hs} ->
            sniffle_console:pp_json(lists:map(fun (ID) ->
                                                      {ok, H} = sniffle_dtrace:get(ID),
                                                      H
                                              end, Hs)),
            ok;
        _ ->
            sniffle_console:pp_json([]),
            error
    end;
command(text, ["list"]) ->
    header(),
    case sniffle_dtrace:list() of
        {ok, Ds} ->
            lists:map(fun (ID) ->
                              {ok, D} = sniffle_dtrace:get(ID),
                              print(D)
                      end, Ds);
        _ ->
            []
    end;

command(_, C) ->
    io:format("Unknown parameters: ~p", [C]),
    error.

header() ->
    io:format("UUID                                 Name~n"),
    io:format("------------------------------------ ---------------~n", []).

print(D) ->
    io:format("~36s ~15s~n",
              [jsxd:get(<<"uuid">>, <<"-">>, D),
               jsxd:get(<<"name">>, <<"-">>, D)]).


print_vars(D) ->
    io:format("Variable        Default~n"),
    io:format("--------------- ---------------~n", []),
    [io:format("~15s ~p~n", [N, Def]) || {N, Def} <- jsxd:get(<<"config">>, [], D)].
