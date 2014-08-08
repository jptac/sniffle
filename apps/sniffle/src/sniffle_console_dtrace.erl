%% @doc Interface for sniffle-admin commands.
-module(sniffle_console_dtrace).
-export([command/2, help/0]).

-define(T, ft_dtrace).
-define(F(Hs, Vs), sniffle_console:fields(Hs,Vs)).
-define(H(Hs), sniffle_console:hdr(Hs)).
-define(Hdr, [{"UUID", 36}, {"Name", 15}]).

help() ->
    io:format("Usage~n"
              "  list [-j]~n"
              "  get [-j] <uuid>~n"
              "  delete <uuid>~n"
              "  import <file>~n").

command(text, ["delete", ID]) ->
    case sniffle_dtrace:delete(list_to_binary(ID)) of
        ok ->
            io:format("Dtrace ~s delete.~n", [ID]),
            ok;
        E ->
            io:format("Dtrace ~s not deleted (~p).~n", [ID, E]),
            ok
    end;

command(json, ["get", UUID]) ->
    case sniffle_dtrace:get(list_to_binary(UUID)) of
        {ok, H} ->
            sniffle_console:pp_json(jsxd:update(<<"script">>,
                                                fun(S) ->
                                                        list_to_binary(S)
                                                end, <<>>, ?T:to_json(H))),
            ok;
        _ ->
            sniffle_console:pp_json([]),
            error
    end;

command(text, ["get", ID]) ->
    ?H(?Hdr),
    case sniffle_dtrace:get(list_to_binary(ID)) of
        {ok, D} ->
            print(D),
            print_vars(D),
            io:format("~.78c~n~s~n~.78c~n",
                      [$=, ?T:script(D), $=]),
            ok;
        _ ->
            error
    end;

command(_, ["import", File]) ->
    case file:read_file(File) of
        {error,enoent} ->
            io:format("That file does not exist or is not an absolute path.~n"),
            error;
        {ok, B} ->
            JSON = jsx:decode(B),
            JSX = jsxd:from_list(JSON),
            Name = jsxd:get(<<"name">>, <<"unnamed">>, JSX),
            %% CopyFields = [<<"config">>, <<"type">>, <<"filter">>],
            {ok, Config} = jsxd:get([<<"config">>], JSX),
            %% {ok, Type} = jsxd:get([<<"config">>], JSX),
            %% {ok, Filter} = jsxd:get([<<"config">>], JSX),
            {ok, UUID} =
                sniffle_dtrace:add(
                  Name,
                  binary_to_list(jsxd:get(<<"script">>, <<"">>, JSX))),
            sniffle_dtrace:set_config(UUID, Config),
            %% sniffle_dtrace:type(UUID, Type),
            %% sniffle_dtrace:set_filter(UUID, Filter),
            io:format("Imported ~s with uuid ~s.~n", [Name, UUID]),
            ok
    end;

command(json, ["list"]) ->
    case sniffle_dtrace:list() of
        {ok, Hs} ->
            sniffle_console:pp_json(
              lists:map(fun (ID) ->
                                {ok, H} = sniffle_dtrace:get(ID),
                                ?T:to_json(H)
                        end, Hs)),
            ok;
        _ ->
            sniffle_console:pp_json([]),
            error
    end;
command(text, ["list"]) ->
    ?H(?Hdr),
    case sniffle_dtrace:list() of
        {ok, Ds} ->
            lists:map(fun (ID) ->
                              {ok, D} = sniffle_dtrace:get(ID),
                              print(D)
                      end, Ds);
        _ ->
            []
    end;

command(_, C) ->
    io:format("Unknown parameters: ~p", [C]),
    error.

print(D) ->
    ?F(?Hdr, [?T:uuid(D),
              ?T:name(D)]).


print_vars(D) ->
    H = [{"Variable", 15}, {"Default", n}],
    ?H(H),
    [?F(H, [N, Def]) || {N, Def} <- ?T:config(D)].
