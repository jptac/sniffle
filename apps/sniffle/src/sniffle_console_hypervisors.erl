%% @doc Interface for sniffle-admin commands.
-module(sniffle_console_hypervisors).
-export([command/2, help/0]).

help() ->
    io:format("Usage~n"),
    io:format("  list [-j]~n"),
    io:format("  get [-j] <uuid>~n"),
    io:format("  delete <uuid>~n").

command(text, ["delete", UUID]) ->
    case sniffle_hypervisor:unregister(list_to_binary(UUID)) of
        ok ->
            io:format("Hypervisor ~s removed.~n", [UUID]),
            ok;
        E ->
            io:format("Hypervisor ~s not removed (~p).~n", [UUID, E]),
            ok
    end;

command(json, ["get", UUID]) ->
    case sniffle_hypervisor:get(list_to_binary(UUID)) of
        {ok, H} ->
            sniffle_console:pp_json(H),
            ok;
        _ ->
            sniffle_console:pp_json([]),
            error
    end;

command(text, ["get", ID]) ->
    io:format(
      "Hypervisor         "
      "UUID                                 "
      "IP               "
      "Memory             "
      "Version       "
      "State~n"),
    io:format("------------------ "
              "------------------------------------ "
              "---------------- "
              "------------------ "
              "------------- "
              "-------------~n", []),
    case sniffle_hypervisor:get(list_to_binary(ID)) of
        {ok, H} ->
            print(H),
            ok;
        _ ->
            error
    end;

command(json, ["list"]) ->
    case sniffle_hypervisor:list() of
        {ok, Hs} ->
            sniffle_console:pp_json(lists:map(fun (ID) ->
                                                      {ok, H} = sniffle_hypervisor:get(ID),
                                                      H
                                              end, Hs)),
            ok;
        _ ->
            sniffle_console:pp_json([]),
            error
    end;

command(text, ["list"]) ->
    io:format("Hypervisor         "
              "UUID                                 "
              "IP               "
              "Memory             "
              "State         "
              "Version~n"),
    io:format("------------------ "
              "------------------------------------ "
              "---------------- "
              "------------------ "
              "------------- "
              "-------------~n", []),
    case sniffle_hypervisor:list() of
        {ok, Hs} ->
            lists:map(fun (ID) ->
                              {ok, H} = sniffle_hypervisor:get(ID),
                              print(H)
                      end, Hs);
        _ ->
            []
    end;

command(_, C) ->
    io:format("Unknown parameters: ~p", [C]),
    error.


print(H) ->
    {ok, Alias} = jsxd:get(<<"alias">>, H),
    {ok, ID} = jsxd:get(<<"uuid">>, H),
    {ok, Host} = jsxd:get(<<"host">>, H),
    {ok, Port} = jsxd:get(<<"port">>, H),
    State = case libchunter:ping(binary_to_list(Host), Port) of
                pong ->
                    <<"ok">>;
                _ ->
                    <<"disconnected">>
            end,
    Mem = io_lib:format("~p/~p",
                        [jsxd:get(<<"resources.provisioned-memory">>, 0, H),
                         jsxd:get(<<"resources.total-memory">>, 0, H)]),
    io:format("~-18s ~36s ~16s ~18s ~-13s ~s~n",
              [Alias, ID, Host, Mem, State,
               jsxd:get(<<"version">>, <<"-">>, H)]).
