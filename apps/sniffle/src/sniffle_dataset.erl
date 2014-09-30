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
         read_image/9,
         wipe/1,
         sync_repair/2,
         list_/0,
         remove_requirement/2,
         add_requirement/2
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
         zone_type/2,
         networks/2,
         nic_driver/2,
         os/2,
         sha1/2,
         users/2,
         status/2,
         imported/2,
         version/2,
         kernel_version/2
        ]).

wipe(UUID) ->
    sniffle_coverage:start(?MASTER, ?SERVICE, {wipe, UUID}).

sync_repair(UUID, Obj) ->
    do_write(UUID, sync_repair, Obj).

list_() ->
    {ok, Res} = sniffle_full_coverage:raw(
                  ?MASTER, ?SERVICE, []),
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
    {ok, Res} = sniffle_full_coverage:list(
                  ?MASTER, ?SERVICE, Requirements),
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
            {ok, SHA1} = jsxd:get([<<"files">>, 0, <<"sha1">>], JSON),
            sniffle_dataset:create(UUID),
            import_manifest(UUID, JSON),
            imported(UUID, 0),
            case sniffle_img:backend() of
                s3 ->
                    {ok, {Host, Port, AKey, SKey, Bucket}} =
                        sniffle_s3:config(image),
                    ChunkSize = application:get_env(sniffle, image_chunk_size,
                                                    5*1024*1024),
                    {ok, U} = fifo_s3_upload:new(AKey, SKey, Host, Port, Bucket,
                                                 UUID),
                    spawn(?MODULE, read_image, [UUID, SHA1, TotalSize, ImgURL,
                                                <<>>, 0, U, ChunkSize, s3]);
                internal ->
                    spawn(?MODULE, read_image, [UUID, SHA1, TotalSize, ImgURL,
                                                <<>>, 0, undefined, 1024*1024,
                                                internal])
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
?SET(zone_type).
?SET(users).
?SET(version).
?SET(kernel_version).
?SET(sha1).
?SET(status).
?SET(imported).
?SET(remove_requirement).
?SET(add_requirement).

%%%===================================================================
%%% Internal Functions
%%%===================================================================

do_import([], _UUID, _O) ->
    ok;

do_import([{K, F} | R], UUID, O) ->
    case jsxd:get(K, O) of
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
       {<<"users">>, fun users/2},
       {[<<"tags">>, <<"kernel_version">>], fun kernel_version/2}
      ], UUID, D1),
    image_size(
      UUID,
      ensure_integer(
        jsxd:get(<<"image_size">>,
                 jsxd:get([<<"files">>, 0, <<"size">>], 0, D1), D1))),
    RS = jsxd:get(<<"requirements">>, [], D1),
    networks(UUID, jsxd:get(<<"networks">>, [], RS)),
    case jsxd:get(<<"homepage">>, D1) of
        {ok, HomePage} ->
            set_metadata(
              UUID,
              [{<<"homepage">>, HomePage}]);
        _ ->
            ok
    end,
    case jsxd:get(<<"min_platform">>, RS) of
        {ok, Min} ->
            Min1 = [V || {_, V} <- Min],
            [M | _] = lists:sort(Min1),
            R = {must, <<"sysinfo.Live Image">>, '>=', M},
            add_requirement(UUID, R);
        _ ->
            ok
    end,
    case jsxd:get(<<"os">>, D1) of
        {ok, <<"smartos">>} ->
            os(UUID, <<"smartos">>),
            type(UUID, <<"zone">>);
        {ok, OS} ->
            case jsxd:get(<<"type">>, D1) of
                {ok, "lx-dataset"} ->
                    os(UUID, OS),
                    type(UUID, <<"zone">>),
                    zone_type(UUID, <<"lx">>);
                _ ->
                    os(UUID, OS),
                    type(UUID, <<"kvm">>)
            end
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
read_image(UUID, Hash, TotalSize, Url, Acc, Idx, Ref, ChunkSize, Backend)
  when is_binary(Url) ->
    case hackney:request(get, Url, [], <<>>, http_opts()) of
        {ok, 200, _, Client} ->
            status(UUID, <<"importing">>),
            SHA1 = crypto:hash_init(sha),
            read_image(UUID, Hash, TotalSize, Client, Acc, Idx, Ref, ChunkSize,
                       SHA1, Backend);
        {ok, Reason, _, _} ->
            fail_import(UUID, Reason, 0)
    end.

%% If we're done (and less then one MB is left, store the rest)


read_image(UUID, Hash, TotalSize, Client, All, Idx, Ref, ChunkSize, SHA1, Backend)
  when byte_size(All) >= ChunkSize ->
    <<MB:ChunkSize/binary, Acc/binary>> = All,
    {ok, Ref1} = case Backend of
                     internal ->
                         sniffle_img:create(UUID, Idx, binary:copy(MB), Ref);
                     s3 ->
                         case  fifo_s3_upload:part(Ref, binary:copy(MB)) of
                             ok ->
                                 {ok, Ref};
                             E ->
                                 fail_import(UUID, E, Idx, Ref, Backend),
                                 E
                         end
                 end,
    Idx1 = Idx + 1,
    Done = (Idx1 * ChunkSize) / TotalSize,
    imported(UUID, Done),
    libhowl:send(UUID,
                 [{<<"event">>, <<"progress">>},
                  {<<"data">>, [{<<"imported">>, Done}]}]),
    read_image(UUID, Hash, TotalSize, Client, binary:copy(Acc), Idx1, Ref1,
               ChunkSize,
               SHA1, Backend);

read_image(UUID, Hash, _TotalSize, done, Acc, Idx, Ref, _, SHA1, Backend) ->
    libhowl:send(UUID,
                 [{<<"event">>, <<"progress">>},
                  {<<"data">>, [{<<"imported">>, 1}]}]),
    status(UUID, <<"imported">>),
    case base16:encode(crypto:hash_final(SHA1)) of
        Digest when Digest == Hash ->
            sha1(UUID, Digest),
            imported(UUID, 1),
            case Backend of
                internal ->
                    {ok, Ref1} = sniffle_img:create(UUID, Idx, Acc, Ref),
                    sniffle_img:create(UUID, done, <<>>, Ref1);
                s3 ->
                    fifo_s3_upload:part(Ref, binary:copy(Acc)),
                    fifo_s3_upload:done(Ref)
            end;
        Digest ->
            lager:error("[dataset] import failed. Sha1 as ~p but expected ~p",
                        [Digest, Hash]),
            fail_import(UUID, {sha1, Digest, Hash}, Idx, Ref, Backend)
    end;


read_image(UUID, Hash, TotalSize, Client, Acc, Idx, Ref, ChunkSize, SHA1, Backend) ->
    case hackney:stream_body(Client) of
        {ok, Data, Client1} ->
            SHA11 = crypto:hash_update(SHA1, Data),
            read_image(UUID, Hash, TotalSize, Client1, <<Acc/binary, Data/binary>>,
                       Idx, Ref, ChunkSize, SHA11, Backend);
        {done, Client2} ->
            hackney:close(Client2),
            read_image(UUID, Hash, TotalSize, done, Acc, Idx, Ref, ChunkSize, SHA1,
                       Backend);
        {error, Reason} ->
            fail_import(UUID, Reason, Idx, Ref, Backend)
    end.

fail_import(UUID, Reason, Idx) ->
    lager:error("[~s] Could not import dataset: ~p", [UUID, Reason]),
    libhowl:send(UUID,
                 [{<<"event">>, <<"error">>},
                  {<<"data">>, [{<<"message">>, Reason},
                                {<<"index">>, Idx}]}]),
    status(UUID, <<"failed">>).

fail_import(UUID, Reason, Idx, Ref, Backend) ->
    fail_import(UUID, Reason, Idx),
    case Backend of
        internal ->
            ok;
        s3 ->
            fifo_s3_upload:abort(Ref)
    end.

http_opts() ->
    case sniffle_opt:get(network, http, proxy, http_proxy, undefined) of
        undefined ->
            [];
        P ->
            [{proxy, P}]
    end.
