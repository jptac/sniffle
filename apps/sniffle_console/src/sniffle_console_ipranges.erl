%% @doc Interface for sniffle-admin commands.
-module(sniffle_console_ipranges).
-export([command/2, help/0]).

-define(T, ft_iprange).
-define(F(Hs, Vs), fifo_console:fields(Hs,Vs)).
-define(H(Hs), fifo_console:hdr(Hs)).
-define(Hdr, [{"UUID", 18}, {"Name", 10}, {"Tag", 8}, {"Free", 8}, {"Used", 8},
              {"Netmask", 15}, {"Gateway", 15}, {"VLAN", 4}]).

help() ->
    io:format("Usage~n"
              "  list~n"
              "  get <uuid>~n"
              "  claim <uuid>~n"
              "  release <uuid> <ip>~n"
              "  delete <uuid>~n").

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
    ?H(?Hdr),
    case sniffle_iprange:get(list_to_binary(ID)) of
        {ok, N} ->
            print(N),
            ok;
        _ ->
            error
    end;

command(text, ["list"]) ->
    ?H(?Hdr),
    case sniffle_iprange:list() of
        {ok, Hs} ->
            lists:map(fun (ID) ->
                              {ok, N} = sniffle_iprange:get(ID),
                              print(N)
                      end, Hs);
        _ ->
            []
    end;

command(text, ["claim", ID]) ->
    case get_ip(ID) of
        {ok, {Tag, IP, Netmask, Gateway, VLan}} ->
            io:format("A IP address has been claimed from the netowkr ~s.~n"
                      "Tag:     ~s~n"
                      "IP:      ~s~n"
                      "Netmask: ~s~n"
                      "Gateway: ~s~n"
                      "VLAN:    ~s~n",
                      [ID, Tag, IP, Netmask, Gateway, VLan]),
            ok;
        _ ->
            io:format("Could not get IP address.~n"),
            error
    end;

command(json, ["claim", ID]) ->
    case get_ip(ID) of
        {ok, {Tag, IP, Netmask, Gateway, VLan}} ->
            sniffle_console:pp_json(
              [{<<"tag">>, Tag},
               {<<"ip">>, IP},
               {<<"netmask">>, Netmask},
               {<<"gateway">>, Gateway},
               {<<"vlan">>, VLan}]),
            ok;
        _ ->
            error
    end;

command(text, ["release", ID, IPS]) ->
    IP = ?T:parse_bin(list_to_binary(IPS)),
    case sniffle_iprange:release_ip(list_to_binary(ID), IP) of
        ok ->
            io:format("Released ip.~n"),
            ok;
        _ ->
            io:format("Release failed ip.~n"),
            error
    end;

command(_, C) ->
    io:format("Unknown parameters: ~p~n", [C]),
    error.

get_ip(ID) ->
    case sniffle_iprange:claim_ip(list_to_binary(ID)) of
        {ok, {Tag, IP, Netmask, Gateway, VLan}} ->
            {ok, {Tag,
                  ?T:to_bin(IP),
                  ?T:to_bin(Netmask),
                  ?T:to_bin(Gateway),
                  VLan}};
        _ ->
            error
    end.

print(N) ->
    ?F(?Hdr, [?T:uuid(N),
              ?T:name(N),
              ?T:tag(N),
              length(?T:free(N)),
              length(?T:used(N)),
              ?T:to_bin(?T:netmask(N)),
              ?T:to_bin(?T:gateway(N)),
              ?T:vlan(N)]).
