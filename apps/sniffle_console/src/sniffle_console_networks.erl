%% @doc Interface for sniffle-admin commands.
-module(sniffle_console_networks).
-export([init/0, command/2, help/0]).

-define(T, ft_network).
-define(F(Hs, Vs), fifo_console:fields(Hs, Vs)).
-define(H(Hs), fifo_console:hdr(Hs)).
-define(HDR, [{"UUID", 18}, {"Name", 10}, {"IPRanges", 10}]).

init() ->
    CmdList = ["sniffle-admin", "networks", "list"],
    clique:register_command(CmdList, [], [], fun cmd_list/3).

cmd_list(_, _, _) ->
    {ok, Hs} = sniffle_network:list([], true),
    Tbl = lists:map(fun to_tbl/1, Hs),
    clique_status:table([?HDR | Tbl]).

help() ->
    io:format("Usage~n"
              "  list~n"
              "  get <uuid>~n"
              "  delete <uuid>~n").
hdr() ->
    ?H(?HDR).

command(text, ["delete", UUID]) ->
    case sniffle_network:delete(list_to_binary(UUID)) of
        ok ->
            io:format("Network ~s removed.~n", [UUID]),
            ok;
        E ->
            io:format("Network ~s not removed (~p).~n", [UUID, E]),
            ok
    end;

command(json, ["get", ID]) ->
    case sniffle_network:get(list_to_binary(ID)) of
        {ok, N} ->
            sniffle_console:pp_json(?T:to_json(N)),
            ok;
        _ ->
            error
    end;

command(text, ["get", ID]) ->
    hdr(),
    case sniffle_network:get(list_to_binary(ID)) of
        {ok, N} ->
            print(N),
            ok;
        _ ->
            error
    end;


command(_, Cmd) ->
    clique:run(Cmd).


to_tbl(N) ->
    [{uuid, ?T:uuid(N)},
     {name, ?T:name(N)},
     {ipranges, length(?T:ipranges(N))}].
print(N) ->
    ?F(?HDR, [?T:uuid(N),
              ?T:name(N),
              length(?T:ipranges(N))]).
