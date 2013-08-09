-module(sniffle_dataset).
-include("sniffle.hrl").
%%-include_lib("riak_core/include/riak_core_vnode.hrl").

-export([
         create/1,
         delete/1,
         get/1,
         list/0,
         list/1,
         set/2,
         set/3,
         import/1,
         read_image/5,
         transform_dataset/1
        ]).

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
    sniffle_entity_read_fsm:start(
      {sniffle_dataset_vnode, sniffle_dataset},
      get, UUID
     ).

-spec list() ->
                  {ok, [UUID::fifo:dataset_id()]} | {error, timeout}.
list() ->
    sniffle_entity_coverage_fsm:start(
      {sniffle_dataset_vnode, sniffle_dataset},
      list
     ).

-spec list(Reqs::[fifo:matcher()]) ->
                  {ok, [UUID::fifo:dataset_id()]} | {error, timeout}.
list(Requirements) ->
    {ok, Res} = sniffle_entity_coverage_fsm:start(
                  {sniffle_dataset_vnode, sniffle_dataset},
                  list, Requirements
                 ),
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
            spawn(?MODULE, read_image, [UUID, TotalSize, ImgURL, <<>>, 0]),
            {ok, UUID};
        {ok, E, _, _} ->
            {error, E}
    end.


%%%===================================================================
%%% Internal Functions
%%%===================================================================

transform_dataset(D1) ->
    case jsxd:get([<<"urn">>], D1) of
        undefined ->
            jsxd:set(<<"image_size">>,
                     ensure_integer(jsxd:get(<<"image_size">>, 0, D1)),
                     D1);
        _ ->
            {ok, ID} = jsxd:get(<<"uuid">>, D1),
            D2 = jsxd:thread(
                   [{select,[<<"os">>, <<"metadata">>, <<"name">>,
                             <<"version">>, <<"description">>,
                             <<"disk_driver">>, <<"nic_driver">>]},
                    {set, <<"dataset">>, ID},
                    {set, <<"image_size">>,
                     ensure_integer(jsxd:get(<<"image_size">>, 0, D1))},
                    {set, <<"networks">>,
                     jsxd:get(<<"requirements.networks">>, [], D1)}],
                   D1),
            case jsxd:get(<<"os">>, D1) of
                {ok, <<"smartos">>} ->
                    jsxd:set(<<"type">>, <<"zone">>, D2);
                {ok, _} ->
                    jsxd:set(<<"type">>, <<"kvm">>, D2)
            end
    end.

ensure_integer(I) when is_integer(I) ->
    I;
ensure_integer(L) when is_list(L) ->
    list_to_integer(L);
ensure_integer(B) when is_binary(B) ->
    list_to_integer(binary_to_list(B)).

do_write(Dataset, Op) ->
    sniffle_entity_write_fsm:write({sniffle_dataset_vnode, sniffle_dataset}, Dataset, Op).

do_write(Dataset, Op, Val) ->
    sniffle_entity_write_fsm:write({sniffle_dataset_vnode, sniffle_dataset}, Dataset, Op, Val).

%% If more then one MB is in the accumulator read store it in 1MB chunks
read_image(UUID, TotalSize, Url, Acc, Idx) when is_binary(Url) ->
    case hackney:request(get, Url, [], <<>>, http_opts()) of
        {ok, 200, _, Client} ->
            read_image(UUID, TotalSize, Client, Acc, Idx);
        {ok, E, _, _} ->
            libhowl:send(UUID,
                         [{<<"event">>, <<"error">>}, {<<"data">>, [{<<"message">>, E},
                                                                    {<<"index">>, 0}]}])
    end;

read_image(UUID, TotalSize, Client, <<MB:1048576/binary, Acc/binary>>, Idx) ->
    sniffle_img:create(UUID, Idx, binary:copy(MB)),
    Idx1 = Idx + 1,
    Done = (Idx1 * 1024*1024) / TotalSize,
    sniffle_dataset:set(UUID, <<"imported">>, Done),
    libhowl:send(UUID,
                 [{<<"event">>, <<"progress">>}, {<<"data">>, [{<<"imported">>, Done}]}]),
    read_image(UUID, TotalSize, Client, binary:copy(Acc), Idx1);

%% If we're done (and less then one MB is left, store the rest)
read_image(UUID, _TotalSize, done, Acc, Idx) ->
    libhowl:send(UUID,
                 [{<<"event">>, <<"progress">>}, {<<"data">>, [{<<"imported">>, 1}]}]),
    sniffle_dataset:set(UUID, <<"imported">>, 1),
    sniffle_img:create(UUID, Idx, Acc);

read_image(UUID, TotalSize, Client, Acc, Idx) ->
    case hackney:stream_body(Client) of
        {ok, Data, Client1} ->
            read_image(UUID, TotalSize, Client1, binary:copy(<<Acc/binary, Data/binary>>), Idx);
        {done, Client2} ->
            hackney:close(Client2),
            read_image(UUID, TotalSize, done, Acc, Idx);
        {error, Reason} ->
            libhowl:send(UUID,
                         [{<<"event">>, <<"error">>}, {<<"data">>, [{<<"message">>, <<"failed">>},
                                                                    {<<"index">>, Idx}]}]),
            libhowl:send(UUID,
                         [{<<"event">>, <<"progress">>}, {<<"data">>, [{<<"imported">>, 0}]}]),
            lager:error("Error importing image ~s: ~p", [UUID, Reason])
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
