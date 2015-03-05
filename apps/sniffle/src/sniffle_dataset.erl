-module(sniffle_dataset).
-include("sniffle.hrl").

-define(MASTER, sniffle_dataset_vnode_master).
-define(VNODE, sniffle_dataset_vnode).
-define(SERVICE, sniffle_dataset).

-define(FM(Met, Mod, Fun, Args),
        folsom_metrics:histogram_timed_update(
          {sniffle, dataset, Met},
          Mod, Fun, Args)).

-export([
         create/1,
         delete/1,
         get/1,
         list/0,
         list/2,
         import/1,
         wipe/1,
         sync_repair/2,
         list_/0,
         remove_requirement/2,
         add_requirement/2,
         remove_network/2,
         add_network/2
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
    ?FM(wipe, sniffle_coverage, start, [?MASTER, ?SERVICE, {wipe, UUID}]).

sync_repair(UUID, Obj) ->
    do_write(UUID, sync_repair, Obj).

list_() ->
    {ok, Res} = ?FM(list_all,sniffle_full_coverage, raw,
                    [?MASTER, ?SERVICE, []]),
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
            sniffle_s3:delete(image, binary_to_list(UUID));
        E ->
            E
    end.

-spec get(UUID::fifo:dtrace_id()) ->
                 not_found | {ok, Dataset::fifo:dataset()} | {error, timeout}.
get(UUID) ->
    ?FM(get, sniffle_entity_read_fsm, start, [{?VNODE, ?SERVICE}, get, UUID]).

-spec list() ->
                  {ok, [UUID::fifo:dataset_id()]} | {error, timeout}.
list() ->
    ?FM(list, sniffle_coverage, start, [?MASTER, ?SERVICE, list]).

%%--------------------------------------------------------------------
%% @doc Lists all vm's and fiters by a given matcher set.
%% @end
%%--------------------------------------------------------------------
-spec list([fifo:matcher()], boolean()) ->
                  {error, timeout} | {ok, [fifo:uuid()]}.

list(Requirements, true) ->
    {ok, Res} = ?FM(list_all, sniffle_full_coverage, list,
                    [?MASTER, ?SERVICE, Requirements]),
    Res1 = lists:sort(rankmatcher:apply_scales(Res)),
    {ok,  Res1};

list(Requirements, false) ->
    {ok, Res} = ?FM(list_all, sniffle_coverage, start,
                    [?MASTER, ?SERVICE, {list, Requirements}]),
    Res1 = rankmatcher:apply_scales(Res),
    {ok,  lists:sort(Res1)}.

import(URL) ->
    sniffle_dataset_download_fsm:download(URL).

?SET(set_metadata).
?SET(description).
?SET(disk_driver).
?SET(homepage).
?SET(image_size).
?SET(name).
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
?SET(remove_network).
?SET(add_network).

%%%===================================================================
%%% Internal Functions
%%%===================================================================

do_write(Dataset, Op) ->
    ?FM(Op, sniffle_entity_write_fsm, write, [{?VNODE, ?SERVICE}, Dataset, Op]).

do_write(Dataset, Op, Val) ->
    ?FM(Op, sniffle_entity_write_fsm, write,
        [{?VNODE, ?SERVICE}, Dataset, Op, Val]).
