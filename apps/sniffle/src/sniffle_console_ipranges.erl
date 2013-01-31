%% @doc Interface for sniffle-admin commands.
-module(sniffle_console_ipranges).
-export([command/2, help/0]).

help() ->
    io:format("Usage~n"),
    io:format("  list~n"),
    io:format("  get <uuid>~n"),
    io:format("  delete <uuid>~n").

command(text, ["delete", UUID]) ->
    case sniffle_iprange:delete(list_to_binary(UUID)) of
        ok ->
            io:format("Network ~s removed.~n", [UUID]),
            ok;
        E ->
            io:format("Network ~s not removed (~p).~n", [UUID, E]),
            ok
    end;

command(text, ["get", ID]) ->
    io:format("UUID                                 Name       Tag      " ++
                  "First           Next            Last            " ++
                  "Netmask         Gateway         Vlan~n"),
    io:format("------------------------------------ ---------- -------- " ++
                  "--------------- --------------- --------------- " ++
                  "--------------- --------------- ----~n"),
    case sniffle_iprange:get(list_to_binary(ID)) of
        {ok, N} ->
            io:format("~-36s ~10s ~8s " ++
                          "~15s ~15s ~15s " ++
                          "~15s ~15s ~-4s ~n",
                      [jsxd:get(<<"uuid">>, <<"-">>, N),
                       jsxd:get(<<"name">>, <<"-">>, N),
                       jsxd:get(<<"tag">>, <<"-">>, N),
                       sniffle_iprange_state:to_bin(jsxd:get(<<"first">>, 0, N)),
                       sniffle_iprange_state:to_bin(jsxd:get(<<"current">>, 0, N)),
                       sniffle_iprange_state:to_bin(jsxd:get(<<"last">>, 0, N)),
                       sniffle_iprange_state:to_bin(jsxd:get(<<"netmask">>, 0, N)),
                       sniffle_iprange_state:to_bin(jsxd:get(<<"gateway">>, 0, N)),
                       jsxd:get(<<"vlan">>, 0, N)]),
            ok;
        _ ->
            error
    end;

command(text, ["list"]) ->
    io:format("UUID                                 Name       Tag      " ++
                  "First           Next            Last            " ++
                  "Netmask         Gateway         Vlan~n"),
    io:format("------------------------------------ ---------- -------- " ++
                  "--------------- --------------- --------------- " ++
                  "--------------- --------------- ----~n"),
    case sniffle_iprange:list() of
        {ok, Hs} ->
            lists:map(fun (ID) ->
                              {ok, N} = sniffle_iprange:get(ID),
                              io:format("~-36s ~10s ~8s " ++
                                            "~15s ~15s ~15s " ++
                                            "~15s ~15s ~-4s ~n",
                                        [jsxd:get(<<"uuid">>, <<"-">>, N),
                                         jsxd:get(<<"name">>, <<"-">>, N),
                                         jsxd:get(<<"tag">>, <<"-">>, N),
                                         sniffle_iprange_state:to_bin(jsxd:get(<<"first">>, 0, N)),
                                         sniffle_iprange_state:to_bin(jsxd:get(<<"current">>, 0, N)),
                                         sniffle_iprange_state:to_bin(jsxd:get(<<"last">>, 0, N)),
                                         sniffle_iprange_state:to_bin(jsxd:get(<<"netmask">>, 0, N)),
                                         sniffle_iprange_state:to_bin(jsxd:get(<<"gateway">>, 0, N)),
                                         jsxd:get(<<"vlan">>, 0, N)])
                      end, Hs);
        _ ->
            []
    end;

command(_, C) ->
    io:format("Unknown parameters: ~p", [C]),
    error.
