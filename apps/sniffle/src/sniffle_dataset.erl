-module(sniffle_dataset).
-include("sniffle.hrl").

-define(MASTER, sniffle_dataset_vnode_master).
-define(VNODE, sniffle_dataset_vnode).
-define(SERVICE, sniffle_dataset).

-export([
         create/1,
         delete/1,
         get/1,
         list/0,
         list/2,
         import/1,
         read_image/7,
         wipe/1,
         sync_repair/2,
         list_/0
        ]).

-ignore_xref([
              sync_repair/2,
              list_/0,
              get_/1,
              wipe/1
              ]).
-export([
         set_metadata/2,
         description/2,
         disk_driver/2,
         homepage/2,
         image_size/2,
         name/2,
         type/2,
         networks/2,
         nic_driver/2,
         os/2,
         users/2,
         status/2,
         imported/2,
         version/2
        ]).


wipe(UUID) ->
    sniffle_coverage:start(?MASTER, ?SERVICE, {wipe, UUID}).

sync_repair(UUID, Obj) ->
    do_write(UUID, sync_repair, Obj).

list_() ->
    {ok, Res} = sniffle_full_coverage:start(
                  ?MASTER, ?SERVICE, {list, [], true, true}),
    Res1 = [R || {_, R} <- Res],
    {ok,  Res1}.

-spec create(UUID::fifo:dataset_id()) ->
                    duplicate | ok | {error, timeout}.
create(UUID) ->
    case sniffle_dataset:get(UUID) of
        not_found ->
            do_write(UUID, create, []);
        {ok, _RangeObj} ->
            duplicate
    end.

-spec delete(UUID::fifo:dataset_id()) ->
                    not_found | {error, timeout} | ok.
delete(UUID) ->
    case do_write(UUID, delete) of
        ok ->
            sniffle_img:delete(UUID);
        E ->
            E
    end.

-spec get(UUID::fifo:dtrace_id()) ->
                  not_found | {ok, Dataset::fifo:dataset()} | {error, timeout}.
get(UUID) ->
    sniffle_entity_read_fsm:start({?VNODE, ?SERVICE}, get, UUID).

-spec list() ->
                  {ok, [UUID::fifo:dataset_id()]} | {error, timeout}.
list() ->
    sniffle_coverage:start(?MASTER, ?SERVICE, list).

%%--------------------------------------------------------------------
%% @doc Lists all vm's and fiters by a given matcher set.
%% @end
%%--------------------------------------------------------------------
-spec list([fifo:matcher()], boolean()) -> {error, timeout} | {ok, [fifo:uuid()]}.

list(Requirements, true) ->
    {ok, Res} = sniffle_full_coverage:start(
                  ?MASTER, ?SERVICE, {list, Requirements, true}),
    Res1 = lists:sort(rankmatcher:apply_scales(Res)),
    {ok,  Res1};

list(Requirements, false) ->
    {ok, Res} = sniffle_coverage:start(
                  ?MASTER, ?SERVICE, {list, Requirements}),
    Res1 = rankmatcher:apply_scales(Res),
    {ok,  lists:sort(Res1)}.

import(URL) ->
    case hackney:request(get, URL, [], <<>>, http_opts()) of
    {ok, 200, _, Client} ->
            {ok, Body, Client1} = hackney:body(Client),
            hackney:close(Client1),
            JSON = jsxd:from_list(jsx:decode(Body)),
            {ok, UUID} = jsxd:get([<<"uuid">>], JSON),
            {ok, ImgURL} = jsxd:get([<<"files">>, 0, <<"url">>], JSON),
            {ok, TotalSize} = jsxd:get([<<"files">>, 0, <<"size">>], JSON),
            sniffle_dataset:create(UUID),
            import_manifest(UUID, JSON),
            imported(UUID, 0),
            case sniffle_img:backend() of
                s3 ->
                    {Host, Port, AKey, SKey, Bucket} =
                        {sniffle_s3:get_host(), sniffle_s3:get_port(),
                         sniffle_s3:get_access_key(), sniffle_s3:get_secret_key(),
                         sniffle_s3:get_bucket(image)},
                    ChunkSize = application:get_env(sniffle, image_chunk_size, 5*1024*1024),
                    {ok, U} = fifo_s3_upload:new(AKey, SKey, Host, Port, Bucket, UUID),
                    spawn(?MODULE, read_image, [UUID, TotalSize, ImgURL, <<>>, 0, U, ChunkSize]);
                internal ->
                    spawn(?MODULE, read_image, [UUID, TotalSize, ImgURL, <<>>, 0, undefined, 1024*1024])
            end,
            {ok, UUID};
        {ok, E, _, _} ->
            {error, E}
    end.

?SET(set_metadata).
?SET(description).
?SET(disk_driver).
?SET(homepage).
?SET(image_size).
?SET(name).
?SET(networks).
?SET(nic_driver).
?SET(os).
?SET(type).
?SET(users).
?SET(version).
?SET(status).
?SET(imported).

%%%===================================================================
%%% Internal Functions
%%%===================================================================

do_import([], _UUID, _O) ->
    ok;

do_import([{K, F} | R], UUID, O) ->
    case jsxd:get([K], O) of
        {ok, V}  ->
            F(UUID, V);
        _ ->
            ok
    end,
    do_import(R, UUID, O).

import_manifest(UUID, D1) ->
    do_import(
      [
       {<<"metadata">>, fun set_metadata/2},
       {<<"name">>, fun name/2},
       {<<"version">>, fun version/2},
       {<<"description">>, fun description/2},
       {<<"disk_driver">>, fun disk_driver/2},
       {<<"nic_driver">>, fun nic_driver/2},
       {<<"users">>, fun users/2}
      ], UUID, D1),
    image_size(
      UUID,
      ensure_integer(
        jsxd:get(<<"image_size">>,
                 jsxd:get([<<"files">>, 0, <<"size">>], 0, D1), D1))),
    networks(
      UUID,
      jsxd:get([<<"requirements">>, <<"networks">>], [], D1)),
    case jsxd:get(<<"homepage">>, D1) of
        {ok, HomePage} ->
            set_metadata(
              UUID,
              [{<<"homepage">>, HomePage}]);
        _ ->
            ok
    end,
    case jsxd:get(<<"os">>, D1) of
        {ok, <<"smartos">>} ->
            os(UUID, <<"smartos">>),
            type(UUID, <<"zone">>);
        {ok, OS} ->
            os(UUID, OS),
            type(UUID, <<"kvm">>)
    end.

ensure_integer(I) when is_integer(I) ->
    I;
ensure_integer(L) when is_list(L) ->
    list_to_integer(L);
ensure_integer(B) when is_binary(B) ->
    list_to_integer(binary_to_list(B)).

do_write(Dataset, Op) ->
    sniffle_entity_write_fsm:write({?VNODE, ?SERVICE}, Dataset, Op).

do_write(Dataset, Op, Val) ->
    sniffle_entity_write_fsm:write({?VNODE, ?SERVICE}, Dataset, Op, Val).

%% If more then one MB is in the accumulator read store it in 1MB chunks
read_image(UUID, TotalSize, Url, Acc, Idx, Ref, ChunkSize) when is_binary(Url) ->
    case hackney:request(get, Url, [], <<>>, http_opts()) of
        {ok, 200, _, Client} ->
            status(UUID, <<"importing">>),
            read_image(UUID, TotalSize, Client, Acc, Idx, Ref, ChunkSize);
        {ok, Reason, _, _} ->
            fail_import(UUID, Reason, 0)
    end;

%% If we're done (and less then one MB is left, store the rest)



read_image(UUID, TotalSize, Client, All, Idx, Ref, ChunkSize)
  when byte_size(All) >= ChunkSize ->
    <<MB:ChunkSize/binary, Acc/binary>> = All,
    {ok, Ref1} = case sniffle_img:backend() of
                     internal ->
                         sniffle_img:create(UUID, Idx, binary:copy(MB), Ref);
                     s3 ->
                         case  fifo_s3_upload:part(Ref, binary:copy(MB)) of
                             ok ->
                                 {ok, Ref};
                             E ->
                                 fail_import(UUID, E, Idx, Ref),
                                 E
                         end
                 end,
    Idx1 = Idx + 1,
    Done = (Idx1 * ChunkSize) / TotalSize,
    imported(UUID, Done),
    libhowl:send(UUID,
                 [{<<"event">>, <<"progress">>}, {<<"data">>, [{<<"imported">>, Done}]}]),
    read_image(UUID, TotalSize, Client, binary:copy(Acc), Idx1, Ref1, ChunkSize);

read_image(UUID, _TotalSize, done, Acc, Idx, Ref, _) ->
    libhowl:send(UUID,
                 [{<<"event">>, <<"progress">>}, {<<"data">>, [{<<"imported">>, 1}]}]),
    status(UUID, <<"imported">>),
    imported(UUID, 1),
    case sniffle_img:backend() of
        internal ->
            {ok, Ref1} = sniffle_img:create(UUID, Idx, Acc, Ref),
            sniffle_img:create(UUID, done, <<>>, Ref1);
        s3 ->
            fifo_s3_upload:part(Ref, binary:copy(Acc)),
            fifo_s3_upload:done(Ref)
    end;

read_image(UUID, TotalSize, Client, Acc, Idx, Ref, ChunkSize) ->
    case hackney:stream_body(Client) of
        {ok, Data, Client1} ->
            read_image(UUID, TotalSize, Client1,
                       binary:copy(<<Acc/binary, Data/binary>>), Idx, Ref, ChunkSize);
        {done, Client2} ->
            hackney:close(Client2),
            read_image(UUID, TotalSize, done, Acc, Idx, Ref, ChunkSize);
        {error, Reason} ->
            fail_import(UUID, Reason, Idx, Ref)
    end.

fail_import(UUID, Reason, Idx) ->
    lager:error("[~s] Could not import dataset: ~p", [UUID, Reason]),
    libhowl:send(UUID,
                 [{<<"event">>, <<"error">>},
                  {<<"data">>, [{<<"message">>, Reason},
                                {<<"index">>, Idx}]}]),
    status(UUID, <<"failed">>).

fail_import(UUID, Reason, Idx, Ref) ->
    fail_import(UUID, Reason, Idx),
    case sniffle_img:backend() of
        internal ->
            ok;
        s3 ->
            fifo_s3_upload:abort(Ref)
    end.

http_opts() ->
    case os:getenv("https_proxy") of
        false ->
            case os:getenv("HTTPS_PROXY") of
                false ->
                    case os:getenv("http_proxy") of
                        false ->
                            case os:getenv("HTTP_PROXY") of
                                false -> [];
                                P     -> [{proxy, P}]
                                end;
                        P -> [{proxy, P}]
                    end;
                P -> [{proxy, P}]
            end;
        P -> [{proxy, P}]
    end.
