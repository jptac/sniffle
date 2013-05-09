%% @doc Interface for sniffle-admin commands.
-module(sniffle_console_datasets).
-export([command/2, help/0]).

help() ->
    io:format("Usage~n"),
    io:format("  list [-j]~n"),
    io:format("  get [-j] <uuid>~n"),
    io:format("  import <manifest> <data file>~n"),
    io:format("  export <uuid> <directory>~n"),
    io:format("  delete <uuid>~n").

-define(CHUNK_SIZE, 1024*1024).

read_image(UUID, File, TotalSize, LastDone, Idx) ->
    case file:read(File, 1024*1024) of
        eof ->
            sniffle_dataset:set(UUID, <<"imported">>, 1),
            Done = ((Idx +1) * 1024*1024) / TotalSize,
            io:format(string:copies("=", trunc((Done - LastDone) / 0.02))),
            io:format("|~n"),
            ok;
        {ok, B} ->
            sniffle_img:create(UUID, Idx, binary:copy(B)),
            Idx1 = Idx + 1,
            Done = (Idx1 * 1024*1024) / TotalSize,
            sniffle_dataset:set(UUID, <<"imported">>, Done),
            case trunc((Done - LastDone) / 0.02) of
                X when X >= 1 ->
                    io:format(string:copies("=", X)),
                    read_image(UUID, File, TotalSize, Done, Idx1);
                _ ->
                    read_image(UUID, File, TotalSize, LastDone, Idx1)
            end
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
            ok = file:write(File, B),
            write_image(File, UUID, R, 0);
        _ ->
            write_image(File, UUID, [Idx | R], Retries + 1)
    end.

command(text, ["export", UUIDS, Path]) ->
    UUID = list_to_binary(UUIDS),
    case filelib:is_dir(Path) of
        false ->
            io:format("Export directory '~s' could not be found.", [Path]),
            error;
        _ ->
            case sniffle_dataset:get(UUID) of
                {ok, Obj} ->
                    ok = file:write_file([Path, "/", UUIDS, ".json"], jsx:encode(Obj)),
                    {ok, IDXs} = sniffle_img:list(UUID),
                    {ok, File} = file:open([Path, "/", UUIDS, ".gz"], [write, raw]),
                    write_image(File, UUID, lists:sort(IDXs), 0);
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
            {ok, TotalSize} = jsxd:get([<<"files">>, 0, <<"size">>], JSON),
            sniffle_dataset:create(UUID),
            sniffle_dataset:set(UUID, Dataset),
            sniffle_dataset:set(UUID, <<"imported">>, 0),
            io:format("|0-----------------------50---------------------100|%~n|"),
            {ok, F} = file:open(DataFile, [read, raw, binary]),
            read_image(UUID, F, TotalSize, 0, 0),
            ok
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
            sniffle_console:pp_json(lists:map(fun (ID) ->
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
    io:format("UUID                                 OS      Name            Version  Desc~n"),
    io:format("------------------------------------ ------- --------------- -------- --------------~n", []).

print(D) ->
    io:format("~36s ~7s ~15s ~8s ~s~n",
              [jsxd:get(<<"dataset">>, <<"-">>, D),
               jsxd:get(<<"os">>, <<"-">>, D),
               jsxd:get(<<"name">>, <<"-">>, D),
               jsxd:get(<<"version">>, <<"-">>, D),
               jsxd:get(<<"description">>, <<"-">>, D)]).
