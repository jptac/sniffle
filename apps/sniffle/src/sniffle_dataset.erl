-module(sniffle_dataset).
-include("sniffle.hrl").
                                                %-include_lib("riak_core/include/riak_core_vnode.hrl").

-export([
         create/1,
         delete/1,
         get/1,
         list/0,
         list/1,
         set/2,
         set/3,
         import/1,
         read_image/5
        ]).

create(Dataset) ->
    case sniffle_dataset:get(Dataset) of
        {ok, not_found} ->
            do_write(Dataset, create, []);
        {ok, _RangeObj} ->
            duplicate
    end.

delete(Dataset) ->
    do_write(Dataset, delete).

get(Dataset) ->
    sniffle_entity_read_fsm:start(
      {sniffle_dataset_vnode, sniffle_dataset},
      get, Dataset
     ).

list() ->
    sniffle_entity_coverage_fsm:start(
      {sniffle_dataset_vnode, sniffle_dataset},
      list
     ).

list(Requirements) ->
    sniffle_entity_coverage_fsm:start(
      {sniffle_dataset_vnode, sniffle_dataset},
      list, Requirements
     ).

set(Dataset, Attribute, Value) ->
    do_write(Dataset, set, [{Attribute, Value}]).


set(Dataset, Attributes) ->
    do_write(Dataset, set, Attributes).

import(URL) ->
    {ok, 200, _, Client} = hackney:request(get, URL, [], <<>>, []),
    {ok, Body, Client1} = hackney:body(Client),
    hackney:close(Client1),
    Dataset = jsxd:from_list(jsx:decode(Body)),
    {ok, UUID} = jsxd:get([<<"uuid">>], Dataset),
    {ok, ImgURL} = jsxd:get([<<"files">>, 0, <<"url">>], Dataset),
    {ok, TotalSize} = jsxd:get([<<"files">>, 0, <<"size">>], Dataset),
    sniffle_dataset:create(UUID),
    sniffle_dataset:set(UUID, Dataset),
    sniffle_dataset:set(UUID, <<"imported">>, 0),
    {ok, 200, _, ImgClient} = hackney:request(get, ImgURL, [], <<>>, []),
    spawn(?MODULE, read_image, [UUID, TotalSize, ImgClient, <<>>, 0]),
    {ok, UUID}.


%%%===================================================================
%%% Internal Functions
%%%===================================================================

do_write(Dataset, Op) ->
    case sniffle_entity_write_fsm:write({sniffle_dataset_vnode, sniffle_dataset}, Dataset, Op) of
        {ok, not_found} ->
            not_found;
        R ->
            R
    end.

do_write(Dataset, Op, Val) ->
    case sniffle_entity_write_fsm:write({sniffle_dataset_vnode, sniffle_dataset}, Dataset, Op, Val) of
        {ok, not_found} ->
            not_found;
        R ->
            R
    end.

%% If more then one MB is in the accumulator read store it in 1MB chunks
read_image(UUID, TotalSize, Client, <<MB:1048576/binary, Acc/binary>>, Idx) ->
    sniffle_img:create(UUID, Idx, MB),
    Idx1 = Idx + 1,
    Done = (Idx1 * 1024*1024) / TotalSize,
    sniffle_dataset:set(UUID, <<"imported">>, Done),
    libhowl:send(UUID, [{<<"imported">>, Done}]),
    read_image(UUID, TotalSize, Client, Acc, Idx1);

%% If we're done (and less then one MB is left, store the rest)
read_image(UUID, _TotalSize, done, Acc, Idx) ->
    sniffle_dataset:set(UUID, <<"imported">>, 1),
    sniffle_img:create(UUID, Idx, Acc);

read_image(UUID, TotalSize, Client, Acc, Idx) ->
    case hackney:stream_body(Client) of
        {ok, Data, Client1} ->
            read_image(UUID, TotalSize, Client1, <<Acc/binary, Data/binary>>, Idx);
        {done, Client2} ->
            hackney:close(Client2),
            read_image(UUID, TotalSize, done, Acc, Idx);
        {error, Reason} ->
            lager:error("Error importing image ~s: ~p", [UUID, Reason])
    end.

