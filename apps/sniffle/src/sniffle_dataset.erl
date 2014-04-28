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
         set/2,
         set/3,
         import/1,
         read_image/6,
         wipe/1,
         sync_repair/2,
         list_/0
        ]).

wipe(UUID) ->
    sniffle_coverage:start(?MASTER, ?VNODE, {wipe, UUID}).

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
    Res1 = rankmatcher:apply_scales(Res),
    {ok,  lists:sort(Res1)};

list(Requirements, false) ->
    {ok, Res} = sniffle_coverage:start(
                  ?MASTER, ?SERVICE, {list, Requirements}),
    Res1 = rankmatcher:apply_scales(Res),
    {ok,  lists:sort(Res1)}.

-spec set(UUID::fifo:dataset_id(),
          Attribute::fifo:keys(),
          Value::fifo:value()) ->
                 ok | {error, timeout}.
set(UUID, Attribute, Value) ->
    do_write(UUID, set, [{Attribute, Value}]).

-spec set(UUID::fifo:dataset_id(),
          Attributes::fifo:attr_list()) ->
                 ok | {error, timeout}.
set(UUID, Attributes) ->
    do_write(UUID, set, Attributes).

import(URL) ->
    case hackney:request(get, URL, [], <<>>, http_opts()) of
    {ok, 200, _, Client} ->
            {ok, Body, Client1} = hackney:body(Client),
            hackney:close(Client1),
            JSON = jsxd:from_list(jsx:decode(Body)),
            Dataset = transform_dataset(JSON),
            {ok, UUID} = jsxd:get([<<"dataset">>], Dataset),
            {ok, ImgURL} = jsxd:get([<<"files">>, 0, <<"url">>], JSON),
            {ok, TotalSize} = jsxd:get([<<"files">>, 0, <<"size">>], JSON),
            sniffle_dataset:create(UUID),
            sniffle_dataset:set(UUID, Dataset),
            sniffle_dataset:set(UUID, <<"imported">>, 0),
            case sniffle_img:backend() of
                s3 ->
                    {Host, Port, AKey, SKey, Bucket} =
                        {sniffle_s3:get_host(), sniffle_s3:get_port(),
                         sniffle_s3:get_access_key(), sniffle_s3:get_secret_key(),
                         sniffle_s3:get_bucket(image)},
                    {ok, U} = fifo_s3_upload:new(AKey, SKey, Host, Port, Bucket, UUID),
                    spawn(?MODULE, read_image, [UUID, TotalSize, ImgURL, <<>>, 0, U]);
                internal ->
                    spawn(?MODULE, read_image, [UUID, TotalSize, ImgURL, <<>>, 0, undefined])
            end,
            {ok, UUID};
        {ok, E, _, _} ->
            {error, E}
    end.

%%%===================================================================
%%% Internal Functions
%%%===================================================================

transform_dataset(D1) ->
    case jsxd:get([<<"sniffle_version">>], D1) of
        undefined ->
            {ok, ID} = jsxd:get(<<"uuid">>, D1),
            D2 = jsxd:thread(
                   [{select,[<<"os">>, <<"metadata">>, <<"name">>,
                             <<"version">>, <<"description">>,
                             <<"disk_driver">>, <<"nic_driver">>,
                             <<"users">>]},
                    {set, <<"dataset">>, ID},
                    {set, <<"image_size">>,
                     ensure_integer(jsxd:get(<<"image_size">>, 0, D1))},
                    {set, <<"networks">>,
                     jsxd:get(<<"requirements.networks">>, [], D1)}],
                   D1),
            D3 = case jsxd:get(<<"homepage">>, D1) of
                     {ok, HomePage} ->
                         jsxd:set([<<"metadata">>, <<"homepage">>], HomePage, D2);
                     _ ->
                         D2
                 end,
            case jsxd:get(<<"os">>, D1) of
                {ok, <<"smartos">>} ->
                    jsxd:set(<<"type">>, <<"zone">>, D3);
                {ok, _} ->
                    jsxd:set(<<"type">>, <<"kvm">>, D3)
            end;
        _ ->
            jsxd:set(<<"image_size">>,
                     ensure_integer(jsxd:get(<<"image_size">>, 0, D1)),
                     D1)
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
read_image(UUID, TotalSize, Url, Acc, Idx, Ref) when is_binary(Url) ->
    case hackney:request(get, Url, [], <<>>, http_opts()) of
        {ok, 200, _, Client} ->
            sniffle_dataset:set(UUID, <<"status">>, <<"importing">>),
            read_image(UUID, TotalSize, Client, Acc, Idx, Ref);
        {ok, Reason, _, _} ->
            fail_import(UUID, Reason, 0)
    end;

read_image(UUID, TotalSize, Client, <<MB:1048576/binary, Acc/binary>>, Idx, Ref) ->
    {ok, Ref1} = case sniffle_img:backend() of
                     internal ->
                         sniffle_img:create(UUID, Idx, binary:copy(MB), Ref);
                     s3 ->
                         case  fifo_s3_upload:part(Ref, binary:copy(MB)) of
                             ok ->
                                 {ok, Ref};
                             E ->
                                 fail_import(UUID, E, Idx),
                                 E
                         end
                 end,
    Idx1 = Idx + 1,
    Done = (Idx1 * 1024*1024) / TotalSize,
    sniffle_dataset:set(UUID, <<"imported">>, Done),
    libhowl:send(UUID,
                 [{<<"event">>, <<"progress">>}, {<<"data">>, [{<<"imported">>, Done}]}]),
    read_image(UUID, TotalSize, Client, binary:copy(Acc), Idx1, Ref1);

%% If we're done (and less then one MB is left, store the rest)
read_image(UUID, _TotalSize, done, Acc, Idx, Ref) ->
    libhowl:send(UUID,
                 [{<<"event">>, <<"progress">>}, {<<"data">>, [{<<"imported">>, 1}]}]),
    sniffle_dataset:set(UUID, <<"status">>, <<"imported">>),
    sniffle_dataset:set(UUID, <<"imported">>, 1),
    {ok, Ref1} = sniffle_img:create(UUID, Idx, Acc, Ref),
    io:format("~p~n", [Ref1]),
    sniffle_img:create(UUID, done, <<>>, Ref1);

read_image(UUID, TotalSize, Client, Acc, Idx, Ref) ->
    case hackney:stream_body(Client) of
        {ok, Data, Client1} ->
            read_image(UUID, TotalSize, Client1,
                       binary:copy(<<Acc/binary, Data/binary>>), Idx, Ref);
        {done, Client2} ->
            hackney:close(Client2),
            read_image(UUID, TotalSize, done, Acc, Idx, Ref);
        {error, Reason} ->
            fail_import(UUID, Reason, Idx)
    end.

fail_import(UUID, Reason, Idx) ->
    lager:error("[~s] Could not import dataset: ~p", [UUID, Reason]),
    libhowl:send(UUID,
                 [{<<"event">>, <<"error">>},
                  {<<"data">>, [{<<"message">>, Reason},
                                {<<"index">>, Idx}]}]),
    sniffle_dataset:set(UUID, <<"status">>, <<"failed">>).

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
