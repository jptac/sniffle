%% @doc Interface for sniffle-admin commands.
-module(sniffle_console_hypervisors).
-export([command/2, help/0]).

-define(T, ft_hypervisor).
-define(F(Hs, Vs), fifo_console:fields(Hs, Vs)).
-define(H(Hs), fifo_console:hdr(Hs)).
-define(HDR, [{"Hypervisor", 18}, {"UUID", 36}, {"IP", 16},
              {"Memory", 18}, {"State", -8}, {"Version", n}]).
help() ->
    io:format("Usage~n"
              "  list [-j]~n"
              "  get [-j] <uuid>~n"
              "  delete <uuid>~n").
hdr() ->
    ?H(?HDR).

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
            sniffle_console:pp_json(?T:to_json(H)),
            ok;
        _ ->
            sniffle_console:pp_json([]),
            error
    end;

command(text, ["get", ID]) ->
    hdr(),
    case sniffle_hypervisor:get(list_to_binary(ID)) of
        {ok, H} ->
            print(H),
            ok;
        _ ->
            error
    end;

command(text, ["update"]) ->
    sniffle_hypervisor:update(),
    io:format("Update started...~n"),
    ok;

command(text, ["update", ID]) ->
    sniffle_hypervisor:update(list_to_binary(ID)),
    io:format("Update started...~n"),
    ok;

command(json, ["list"]) ->
    case sniffle_hypervisor:list() of
        {ok, Hs} ->
            sniffle_console:pp_json(
              lists:map(fun (ID) ->
                                {ok, H} = sniffle_hypervisor:get(ID),
                                ?T:to_json(H)
                        end, Hs)),
            ok;
        _ ->
            sniffle_console:pp_json([]),
            error
    end;

command(text, ["list"]) ->
    hdr(),
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
    {Host, Port} = ?T:endpoint(H),
    State = case libchunter:ping(Host, Port) of
                pong ->
                    <<"ok">>;
                _ ->
                    <<"disconnected">>
            end,
    R = ?T:resources(H),
    Mem = io_lib:format("~p/~p",
                        [jsxd:get(<<"provisioned-memory">>, 0, R),
                         jsxd:get(<<"total-memory">>, 0, R)]),
    ?F(?HDR,
       [?T:alias(H), ?T:uuid(H), Host, Mem, State, ?T:version(H)]).
