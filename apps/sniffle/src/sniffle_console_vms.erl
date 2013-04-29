%% @doc Interface for sniffle-admin commands.
-module(sniffle_console_vms).
-export([command/2, help/0]).

help() ->
    io:format("Usage~n"),
    io:format("  list [-j]~n"),
    io:format("  get [-j] <uuid>~n"),
    io:format("  logs [-j] <uuid>~n"),
    io:format("  snapshots [-j] <uuid>~n"),
    io:format("  snapshot <uuid> <comment>~n"),
    io:format("  start <uuid>~n"),
    io:format("  stop <uuid>~n"),
    io:format("  reboot <uuid>~n"),
    io:format("  delete <uuid>~n").

command(text, ["start", UUID]) ->
    case sniffle_vm:start(list_to_binary(UUID)) of
        ok ->
            io:format("VM ~s starting.~n", [UUID]),
            ok;
        E ->
            io:format("VM ~s did not start (~p).~n", [UUID, E]),
            ok
    end;

command(text, ["stop", UUID]) ->
    case sniffle_vm:stop(list_to_binary(UUID)) of
        ok ->
            io:format("VM ~s stopping.~n", [UUID]),
            ok;
        E ->
            io:format("VM ~s did not stop (~p).~n", [UUID, E]),
            ok
    end;

command(text, ["reboot", UUID]) ->
    case sniffle_vm:reboot(list_to_binary(UUID)) of
        ok ->
            io:format("VM ~s rebooting.~n", [UUID]),
            ok;
        E ->
            io:format("VM ~s did not reboot (~p).~n", [UUID, E]),
            ok
    end;

command(text, ["delete", UUID]) ->
    case sniffle_vm:delete(list_to_binary(UUID)) of
        ok ->
            io:format("VM ~s deleted.~n", [UUID]),
            ok;
        E ->
            io:format("VM ~s did not delete (~p).~n", [UUID, E]),
            ok
    end;

command(json, ["get", UUID]) ->
    case sniffle_vm:get(list_to_binary(UUID)) of
        {ok, VM} ->
            sniffle_console:pp_json(jsxd:thread([{select, [<<"hypervisor">>, <<"state">>]},
                                 {merge, jsxd:get(<<"config">>, [], VM)}],
                                VM)),
            ok;
        _ ->
            sniffle_console:pp_json([]),
            ok
    end;

command(text, ["get", UUID]) ->
    io:format("UUID                                 Hypervisor        Name            State~n"),
    io:format("------------------------------------ ----------------- --------------- ----------~n", []),
    case sniffle_vm:get(list_to_binary(UUID)) of
        {ok, VM} ->
            io:format("~36s ~17s ~10s ~-15s~n",
                      [UUID,
                       jsxd:get(<<"hypervisor">>, <<"-">>, VM),
                       jsxd:get(<<"state">>, <<"-">>, VM),
                       jsxd:get(<<"config.alias">>, <<"-">>, VM)]),
            ok;
        _ ->
            ok
    end;

command(json, ["logs", UUID]) ->
    case sniffle_vm:get(list_to_binary(UUID)) of
        {ok, VM} ->
            sniffle_console:pp_json(jsxd:get(<<"log">>, [], VM)),
            ok;
        _ ->
            sniffle_console:pp_json([]),
            ok
    end;

command(text, ["logs", UUID]) ->
    io:format("Timestamp        Log~n"),
    io:format("---------------- -------------------------------------------------------------~n", []),
    case sniffle_vm:get(list_to_binary(UUID)) of
        {ok, VM} ->
            lists:map(fun (Log) ->
                              io:format("~17p ~s~n",
                                        [jsxd:get(<<"date">>, <<"-">>, Log),
                                         jsxd:get(<<"log">>, <<"-">>, Log)])
                      end, jsxd:get(<<"log">>, [], VM)),
            ok;
        _ ->
            ok
    end;


command(text, ["snapshot", UUID | Comment]) ->
    case sniffle_vm:snapshot(list_to_binary(UUID), iolist_to_binary(join(Comment, " "))) of
        {ok, Snap} ->
            io:format("New snapshot created: ~s.~n", [Snap]),
            ok;
        E ->
            io:format("Failed to create: ~p.~n", [E]),
            error
    end;

command(json, ["snapshots", UUID]) ->
    case sniffle_vm:get(list_to_binary(UUID)) of
        {ok, VM} ->
            sniffle_console:pp_json(jsxd:get(<<"snapshots">>, [], VM)),
            ok;
        _ ->
            sniffle_console:pp_json([]),
            ok
    end;

command(text, ["snapshots", UUID]) ->
    io:format("Timestamp        UUID                                 Comment~n"),
    io:format("---------------- ------------------------------------ -----------~n", []),
    case sniffle_vm:get(list_to_binary(UUID)) of
        {ok, VM} ->
            jsxd:map(fun (SUUID, Snapshot) ->
                              io:format("~16p ~36s ~s~n",
                                        [jsxd:get(<<"timestamp">>, <<"-">>, Snapshot),
                                         SUUID,
                                         jsxd:get(<<"comment">>, <<"-">>, Snapshot)])
                      end, jsxd:get(<<"snapshots">>, [], VM)),
            ok;
        _ ->
            ok
    end;

command(json, ["list"]) ->
    case sniffle_vm:list() of
        {ok, VMs} ->
            sniffle_console:pp_json(lists:map(fun (UUID) ->
                                      {ok, VM} = sniffle_vm:get(UUID),
                                      jsxd:thread([{select, [<<"hypervisor">>, <<"state">>]},
                                                   {merge, jsxd:get(<<"config">>, [], VM)}],
                                                  VM)
                              end, VMs)),
            ok;
        _ ->
            sniffle_console:pp_json([]),
            ok
    end;

command(text, ["list"]) ->
    io:format("UUID                                 Hypervisor        Name            State~n"),
    io:format("------------------------------------ ----------------- --------------- ----------~n", []),
    case sniffle_vm:list() of
        {ok, VMs} ->
            lists:map(fun (UUID) ->
                              {ok, VM} = sniffle_vm:get(UUID),
                              io:format("~36s ~17s ~15s ~-10s~n",
                                        [UUID,
                                         jsxd:get(<<"hypervisor">>, <<"-">>, VM),
                                         jsxd:get(<<"config.alias">>, <<"-">>, VM),
                                         jsxd:get(<<"state">>, <<"-">>, VM)])
                      end, VMs);
        _ ->
            []
    end;

command(_, C) ->
    io:format("Unknown parameters: ~p", [C]),
    error.


join([ Head | [] ], _Sep) ->
   [Head];

join([ Head | Rest], Sep) ->
   [Head,Sep | join(Rest,Sep) ].
