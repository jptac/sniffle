%% @doc Interface for sniffle-admin commands.
-module(sniffle_console_hypervisors).
-export([command/2, help/0]).

-define(F(Hs, Vs), sniffle_console:fields(Hs,Vs)).
-define(H(Hs), sniffle_console:hdr(Hs)).
-define(Hdr, [{"Hypervisor", 18}, {"UUID", 36}, {"IP", 16},
              {"Memory", 18}, {"Version", -13}, {"State", n}]).
help() ->
    io:format("Usage~n"
              "  list [-j]~n"
              "  get [-j] <uuid>~n"
              "  delete <uuid>~n").

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
    ?H(?Hdr),
    case sniffle_hypervisor:get(list_to_binary(ID)) of
        {ok, H} ->
            print(H),
            ok;
        _ ->
            error
    end;

command(text, ["update", ID]) ->
    case sniffle_hypervisor:update(list_to_binary(ID)) of
        ok ->
            print("Update started..."),
            ok;
        _ ->
            error
    end;

command(text, ["update", "all"]) ->
    case sniffle_hypervisor:update() of
        ok ->
            print("Update started..."),
            ok;
        _ ->
            error
    end;

command(json, ["list"]) ->
    case sniffle_hypervisor:list() of
        {ok, Hs} ->
            sniffle_console:pp_json(
              lists:map(fun (ID) ->
                                {ok, H} = sniffle_hypervisor:get(ID),
                                H
                        end, Hs)),
            ok;
        _ ->
            sniffle_console:pp_json([]),
            error
    end;

command(text, ["list"]) ->
    ?H(?Hdr),
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
    {ok, Host} = jsxd:get(<<"host">>, H),
    {ok, Port} = jsxd:get(<<"port">>, H),
    State = case libchunter:ping(binary_to_list(Host), Port) of
                pong ->
                    <<"ok">>;
                _ ->
                    <<"disconnected">>
            end,
    Name = case jsxd:get(<<"uuid">>, H) of
               {ok, N} ->
                   N;
               _ ->
                   jsxd:get(<<"name">>, <<"-">>, H)
           end,
    Mem = io_lib:format("~p/~p",
                        [jsxd:get(<<"resources.provisioned-memory">>, 0, H),
                         jsxd:get(<<"resources.total-memory">>, 0, H)]),
    ?F(?Hdr,
       [jsxd:get(<<"alias">>, <<"-">>, H), Name, Host, Mem, State,
        jsxd:get(<<"version">>, <<"-">>, H)]).
