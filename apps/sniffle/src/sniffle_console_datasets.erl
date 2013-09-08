%% @doc Interface for sniffle-admin commands.
-module(sniffle_console_datasets).
-export([command/2, help/0, read_image/4]).


-include_lib("kernel/include/file.hrl").
-include("sniffle_version.hrl").

help() ->
    io:format("Usage~n"),
    io:format("  list [-j]~n"),
    io:format("  get [-j] <uuid>~n"),
    io:format("  import <manifest> <data file>~n"),
    io:format("  export <uuid> <directory>~n"),
    io:format("  delete <uuid>~n").

-define(CHUNK_SIZE, 1024*1024).

read_image(UUID, File, Idx, Size) ->
    case file:read(File, 1024*1024) of
        eof ->
            libhowl:send(UUID,
                         [{<<"event">>, <<"progress">>},
                          {<<"data">>, [{<<"imported">>, 1}]}]),
            sniffle_dataset:set(UUID, <<"imported">>, 1),
            lager:debug("[IMG:~p(~p)] import done to ~p%", [UUID, Idx+1, 100]),
            ok;
        {ok, B} ->
            sniffle_img:create(UUID, Idx, binary:copy(B)),
            Idx1 = Idx + 1,
            Done = (Idx1 * 1024 * 1024) / Size,
            sniffle_dataset:set(UUID, <<"imported">>, Done),
            libhowl:send(UUID,
                         [{<<"event">>, <<"progress">>},
                          {<<"data">>, [{<<"imported">>, Done}]}]),
            lager:debug("[IMG:~p(~p)] import done to ~p%", [UUID, Idx, Done*100]),
            read_image(UUID, File, Idx1, Size);
        E ->
            lager:debug("[IMG:~p(~p)] Error ~p", [UUID, Idx, E])
    end.

write_image(File, _UUID, [Idx | _], 2) ->
    io:format("Retries exceeded could for shard ~p.~n", [Idx]),
    file:close(File),
    error;

write_image(File, _UUID, [], _Retries) ->
    file:close(File),
    ok;

write_image(File, UUID, [Idx | R], Retries) ->
    case sniffle_img:get(UUID, Idx) of
        {ok, B} ->
            case file:write(File, B) of
                {error,enospc} ->
                    io:format("No space left on device.", []),
                    error;
                ok ->
                    write_image(File, UUID, R, 0)
            end;
        _ ->
            write_image(File, UUID, [Idx | R], Retries + 1)
    end.

ensure_integer(I) when is_integer(I) ->
    I;

ensure_integer(L) when is_list(L) ->
    list_to_integer(L);
ensure_integer(B) when is_binary(B) ->
    list_to_integer(binary_to_list(B)).

command(text, ["export", UUIDS, Path]) ->
    UUID = list_to_binary(UUIDS),
    case filelib:is_dir(Path) of
        false ->
            io:format("Export directory '~s' could not be found.", [Path]),
            error;
        _ ->
            case sniffle_dataset:get(UUID) of
                {ok, Obj} ->
                    Obj0 = jsxd:set(<<"image_size">>,
                                    ensure_integer(
                                      jsxd:get(<<"image_size">>, 0, Obj)),
                                    Obj),
                    Obj1 = jsxd:set(<<"sniffle_version">>, ?VERSION, Obj0),
                    case file:write_file([Path, "/", UUIDS, ".json"],
                                         jsx:encode(Obj1)) of
                        ok ->
                            {ok, IDXs} = sniffle_img:list(UUID),
                            {ok, File} = file:open([Path, "/", UUIDS, ".gz"],
                                                   [write, raw]),
                            write_image(File, UUID, lists:sort(IDXs), 0);
                        {error,eacces} ->
                            io:format("Can't write to ~p, sniffle is running "
                                      "as sniffle user, and it must have "
                                      "permissions to write to ~p.",
                                      [Path, Path]),
                            error
                    end;
                _ ->
                    io:format("Dataset '~s' could not be found.", [UUID]),
                    error
            end
    end;

command(text, ["import", Manifest, DataFile]) ->
    case {filelib:is_file(Manifest), filelib:is_file(Manifest)} of
        {false, _} ->
            io:format("Manifest file '~s' could not be found.", [Manifest]),
            error;
        {_, false} ->
            io:format("Data file '~s' could not be found.", [DataFile]),
            error;
        _ ->
            {ok, Body} = file:read_file(Manifest),
            JSON = jsxd:from_list(jsx:decode(Body)),
            Dataset = sniffle_dataset:transform_dataset(JSON),
            {ok, UUID} = jsxd:get([<<"dataset">>], Dataset),
            sniffle_dataset:create(UUID),
            sniffle_dataset:set(UUID, Dataset),
            sniffle_dataset:set(UUID, <<"imported">>, 0),
            case file:read_file_info(DataFile) of
                {ok, Info} ->
                    {ok, F} = file:open(DataFile, [read, raw, binary]),
                    spawn(?MODULE, read_image,
                          [UUID, F, 0, Info#file_info.size]),
                    io:format("Import backgrounded for ~p.", [UUID]),

                    ok;
                _ ->
                    error
            end
    end;

command(text, ["delete", ID]) ->
    case sniffle_dataset:delete(list_to_binary(ID)) of
        ok ->
            io:format("Dataset ~s delete.~n", [ID]),
            ok;
        E ->
            io:format("Dataset ~s not deleted (~p).~n", [ID, E]),
            ok
    end;

command(json, ["get", UUID]) ->
    case sniffle_dataset:get(list_to_binary(UUID)) of
        {ok, H} ->
            sniffle_console:pp_json(H),
            ok;
        _ ->
            sniffle_console:pp_json([]),
            error
    end;

command(text, ["get", ID]) ->
    header(),
    case sniffle_dataset:get(list_to_binary(ID)) of
        {ok, D} ->
            print(D),
            ok;
        _ ->
            error
    end;

command(json, ["list"]) ->
    case sniffle_dataset:list() of
        {ok, Hs} ->
            sniffle_console:pp_json(
              lists:map(fun (ID) ->
                                {ok, H} = sniffle_dataset:get(ID),
                                H
                        end, Hs)),
            ok;
        _ ->
            sniffle_console:pp_json([]),
            error
    end;

command(text, ["list"]) ->
    header(),
    case sniffle_dataset:list() of
        {ok, Ds} ->
            lists:map(fun (ID) ->
                              {ok, D} = sniffle_dataset:get(ID),
                              print(D)
                      end, Ds);
        _ ->
            []
    end;

command(_, C) ->
    io:format("Unknown parameters: ~p", [C]),
    error.

header() ->
    io:format("UUID                                 "
              "OS      "
              "Name            "
              "Version  "
              "Imported "
              "Desc~n"
              "------------------------------------ "
              "------- "
              "--------------- "
              "-------- "
              "-------- "
              "--------------~n").

print(D) ->
    io:format("~36s ~7s ~15s ~8s ~7p% ~s~n",
              [jsxd:get(<<"dataset">>, <<"-">>, D),
               jsxd:get(<<"os">>, <<"-">>, D),
               jsxd:get(<<"name">>, <<"-">>, D),
               jsxd:get(<<"version">>, <<"-">>, D),
               jsxd:get(<<"imported">>, 1, D) * 100,
               jsxd:get(<<"description">>, <<"-">>, D)]).
