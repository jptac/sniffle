%% @doc Interface for sniffle-admin commands.
-module(sniffle_console_networks).
-export([command/2, help/0]).

-define(T, ft_network).
-define(F(Hs, Vs), sniffle_console:fields(Hs,Vs)).
-define(H(Hs), sniffle_console:hdr(Hs)).
-define(Hdr, [{"UUID", 18}, {"Name", 10}, {"IPRanges", 10}]).

help() ->
    io:format("Usage~n"
              "  list~n"
              "  get <uuid>~n"
              "  delete <uuid>~n").

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
    ?H(?Hdr),
    case sniffle_network:get(list_to_binary(ID)) of
        {ok, N} ->
            print(N),
            ok;
        _ ->
            error
    end;

command(text, ["list"]) ->
    ?H(?Hdr),
    case sniffle_network:list() of
        {ok, Hs} ->
            lists:map(fun (ID) ->
                              {ok, N} = sniffle_network:get(ID),
                              print(N)
                      end, Hs);
        _ ->
            []
    end;

command(json, ["list"]) ->
    case sniffle_network:list() of
        {ok, Hs} ->
            lists:map(fun (ID) ->
                              {ok, N} = sniffle_network:get(ID),
                              sniffle_console:pp_json(?T:to_json(N))
                      end, Hs);
        _ ->
            []
    end;


command(_, C) ->
    io:format("Unknown parameters: ~p~n", [C]),
    error.


print(N) ->
    ?F(?Hdr, [?T:uuid(N),
              ?T:name(N),
              length(?T:ipranges(N))]).
