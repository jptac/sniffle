%% @doc Interface for sniffle-admin commands.
-module(sniffle_console).
-export([join/1,
         leave/1,
         remove/1,
         vms/1,
         ringready/1]).

-ignore_xref([
              join/1,
              leave/1,
              vms/1,
              remove/1,
              ringready/1
             ]).


vms([C, "-p" | R]) ->
    vms(json, [C | R]);

vms([]) ->
    io:format("Usage~n"),
    io:format("list [-p]~n"),
    io:format("get [-p] <uuid>~n"),
    io:format("logs [-p] <uuid>~n"),
    io:format("snapshots [-p] <uuid>~n"),
    io:format("start <uuid>~n"),
    io:format("stop <uuid>~n"),
    io:format("reboot <uuid>~n"),
    ok;

vms(R) ->
    vms(text, R).

vms(text, ["start", UUID]) ->
    case sniffle_vm:start(list_to_binary(UUID)) of
        ok ->
            io:format("VM ~s starting.~n", [UUID]),
            ok;
        E ->
            io:format("VM ~s did not start (~p).~n", [UUID, E]),
            ok
    end;

vms(text, ["stop", UUID]) ->
    case sniffle_vm:stop(list_to_binary(UUID)) of
        ok ->
            io:format("VM ~s stopping.~n", [UUID]),
            ok;
        E ->
            io:format("VM ~s did not stop (~p).~n", [UUID, E]),
            ok
    end;

vms(text, ["reboot", UUID]) ->
    case sniffle_vm:reboot(list_to_binary(UUID)) of
        ok ->
            io:format("VM ~s rebooting.~n", [UUID]),
            ok;
        E ->
            io:format("VM ~s did not reboot (~p).~n", [UUID, E]),
            ok
    end;

vms(json, ["get", UUID]) ->
    case sniffle_vm:get(list_to_binary(UUID)) of
        {ok, VM} ->
            pp_json(jsxd:thread([{select, [<<"hypervisor">>, <<"state">>]},
                                 {merge, jsxd:get(<<"config">>, [], VM)}],
                                VM)),
            ok;
        _ ->
            pp_json([]),
            ok
    end;

vms(text, ["get", UUID]) ->
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

vms(json, ["logs", UUID]) ->
    case sniffle_vm:get(list_to_binary(UUID)) of
        {ok, VM} ->
            pp_json(jsxd:get(<<"log">>, [], VM)),
            ok;
        _ ->
            pp_json([]),
            ok
    end;

vms(text, ["logs", UUID]) ->
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

vms(json, ["snapshots", UUID]) ->
    case sniffle_vm:get(list_to_binary(UUID)) of
        {ok, VM} ->
            pp_json(jsxd:get(<<"snapshots">>, [], VM)),
            ok;
        _ ->
            pp_json([]),
            ok
    end;


vms(text, ["snapshots", UUID]) ->
    io:format("Timestamp        UUID                                 Comment~n"),
    io:format("---------------- ------------------------------------ -----------~n", []),
    case sniffle_vm:get(list_to_binary(UUID)) of
        {ok, VM} ->
            jsxd:map(fun (SUUID, Snapshot) ->
                              io:format("~36s ~17p ~s~n",
                                        [jsxd:get(<<"timestamp">>, <<"-">>, Snapshot),
                                         SUUID,
                                         jsxd:get(<<"comment">>, <<"-">>, Snapshot)])
                      end, jsxd:get(<<"snapshots">>, [], VM)),
            ok;
        _ ->
            ok
    end;

vms(json, ["list"]) ->
    case sniffle_vm:list() of
        {ok, VMs} ->
            pp_json(lists:map(fun (UUID) ->
                                      {ok, VM} = sniffle_vm:get(UUID),
                                      jsxd:thread([{select, [<<"hypervisor">>, <<"state">>]},
                                                   {merge, jsxd:get(<<"config">>, [], VM)}],
                                                  VM)
                              end, VMs)),
            ok;
        _ ->
            pp_json([]),
            ok
    end;

vms(text, ["list"]) ->
    io:format("UUID                                 Hypervisor        Name            State~n"),
    io:format("------------------------------------ ----------------- --------------- ----------~n", []),
    case sniffle_vm:list() of
        {ok, VMs} ->
            lists:map(fun (UUID) ->
                              {ok, VM} = sniffle_vm:get(UUID),
                              io:format("~36s ~17s ~10s ~-15s~n",
                                        [UUID,
                                         jsxd:get(<<"hypervisor">>, <<"-">>, VM),
                                         jsxd:get(<<"state">>, <<"-">>, VM),
                                         jsxd:get(<<"config.alias">>, <<"-">>, VM)])
                      end, VMs);
        _ ->
            []
    end;

vms(_, C) ->
    io:format("Unknown parameters: ~p", [C]),
    error.

join([NodeStr]) ->
    try riak_core:join(NodeStr) of
        ok ->
            io:format("Sent join request to ~s\n", [NodeStr]),
            ok;
        {error, not_reachable} ->
            io:format("Node ~s is not reachable!\n", [NodeStr]),
            error;
        {error, different_ring_sizes} ->
            io:format("Failed: ~s has a different ring_creation_size~n",
                      [NodeStr]),
            error
    catch
        Exception:Reason ->
            lager:error("Join failed ~p:~p", [Exception, Reason]),
            io:format("Join failed, see log for details~n"),
            error
    end.

leave([]) ->
    remove_node(node()).

remove([Node]) ->
    remove_node(list_to_atom(Node)).

remove_node(Node) when is_atom(Node) ->
    try catch(riak_core:remove_from_cluster(Node)) of
        {'EXIT', {badarg, [{erlang, hd, [[]]}|_]}} ->
            %% This is a workaround because
            %% riak_core_gossip:remove_from_cluster doesn't check if
            %% the result of subtracting the current node from the
            %% cluster member list results in the empty list. When
            %% that code gets refactored this can probably go away.
            io:format("Leave failed, this node is the only member.~n"),
            error;
        Res ->
            io:format(" ~p\n", [Res])
    catch
        Exception:Reason ->
            lager:error("Leave failed ~p:~p", [Exception, Reason]),
            io:format("Leave failed, see log for details~n"),
            error
    end.

-spec(ringready([]) -> ok | error).
ringready([]) ->
    try riak_core_status:ringready() of
        {ok, Nodes} ->
            io:format("TRUE All nodes agree on the ring ~p\n", [Nodes]);
        {error, {different_owners, N1, N2}} ->
            io:format("FALSE Node ~p and ~p list different partition owners\n",
                      [N1, N2]),
            error;
        {error, {nodes_down, Down}} ->
            io:format("FALSE ~p down.  All nodes need to be up to check.\n",
                      [Down]),
            error
    catch
        Exception:Reason ->
            lager:error("Ringready failed ~p:~p", [Exception, Reason]),
            io:format("Ringready failed, see log for details~n"),
            error
    end.


pp_json(Obj) ->
    io:format("~s~n", [jsx:prettify(jsx:encode(Obj))]).
