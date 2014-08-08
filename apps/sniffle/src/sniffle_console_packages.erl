%% @doc Interface for sniffle-admin commands.
-module(sniffle_console_packages).
-export([command/2, help/0]).

-define(T, ft_package).
-define(F(Hs, Vs), sniffle_console:fields(Hs,Vs)).
-define(H(Hs), sniffle_console:hdr(Hs)).
-define(Hdr, [{"UUID", 36}, {"Name", 18}, {"Ram (MB)", 6},
              {"Quota (GB)", 4}, {"Cpu Cap (%)", 5}]).
help() ->
    io:format("Usage~n"
              "  list [-j]~n"
              "  get [-j] <uuid>~n"
              "  delete <uuid>~n").

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
            sniffle_console:pp_json(?T:to_json(H)),
            ok;
        _ ->
            sniffle_console:pp_json([]),
            error
    end;

command(text, ["get", ID]) ->
    ?H(?Hdr),
    case sniffle_package:get(list_to_binary(ID)) of
        {ok, P} ->
            print(P),
            ok;
        _ ->
            error
    end;

command(json, ["list"]) ->
    case sniffle_package:list() of
        {ok, Hs} ->
            sniffle_console:pp_json(
              lists:map(fun (ID) ->
                                {ok, H} = sniffle_package:get(ID),
                                ?T:to_json(H)
                        end, Hs)),
            ok;
        _ ->
            sniffle_console:pp_json([]),
            error
    end;

command(text, ["list"]) ->
    ?H(?Hdr),
    case sniffle_package:list() of
        {ok, Ps} ->
            lists:map(fun (ID) ->
                              {ok, P} = sniffle_package:get(ID),
                              print(P)
                      end, Ps);
        _ ->
            []
    end;

command(_, C) ->
    io:format("Unknown parameters: ~p", [C]),
    error.

print(P) ->
    ?F(?Hdr, [?T:uuid(P),
              ?T:name(P),
              ?T:ram(P),
              ?T:quota(P),
              ?T:cpu_cap(P)]).
