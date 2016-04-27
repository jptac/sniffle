-module(sniffle_dataset_cmd).

-define(BUCKET, <<"dataset">>).
-include("sniffle.hrl").

-export([
         repair/4,
         sync_repair/4,
         get/3,
         create/4,
         delete/3
        ]).

-ignore_xref([
              sync_repair/4,
              create/4,
              delete/3,
              get/3,
              repair/4,
              sync_repair/4
             ]).

-export([
         add_requirement/4,
         dataset/4,
         description/4,
         disk_driver/4,
         homepage/4,
         image_size/4,
         imported/4,
         kernel_version/4,
         name/4,
         nic_driver/4,
         os/4,
         remove_requirement/4,
         add_network/4,
         remove_network/4,
         set_metadata/4,
         sha1/4,
         status/4,
         type/4,
         users/4,
         version/4,
         zone_type/4
        ]).

-ignore_xref([
              add_requirement/4,
              dataset/4,
              description/4,
              disk_driver/4,
              homepage/4,
              image_size/4,
              imported/4,
              kernel_version/4,
              name/4,
              remove_network/4,
              nic_driver/4,
              os/4,
              remove_requirement/4,
              add_network/4,
              remove_network/4,
              set_metadata/4,
              sha1/4,
              status/4,
              type/4,
              users/4,
              version/4,
              zone_type/4
             ]).


%%%===================================================================
%%% API
%%%===================================================================

repair(Preflist, UUID, VClock, Obj) ->
    riak_core_vnode_master:command(Preflist,
                                   ?REQ({repair, UUID, VClock, Obj}),
                                   ignore,
                                   ?MASTER).



%%%===================================================================
%%% API - reads
%%%===================================================================

get(Preflist, ReqID, UUID) ->
    ?REQUEST(Preflist, ReqID, {get, UUID}).

%%%===================================================================
%%% API - writes
%%%===================================================================

sync_repair(Preflist, ReqID, UUID, Obj) ->
    ?REQUEST(Preflist, ReqID, {sync_repair, UUID, Obj}).

create_fn({_, Coordinator} = ReqID, not_found, [UUID]) ->
    I0 = ft_dataset:new(ReqID),
    I1 = ft_dataset:uuid(ReqID, UUID, I0),
    {write, ok, ft_obj:new(I1, Coordinator)};

create_fn(_, {ok, _}, _) ->
    {reply, ok}.

create(Preflist, ReqID, UUID, _) ->
    ?REQUEST(Preflist, ReqID, {apply, UUID, fun create_fn/3, [UUID]}).

delete(Preflist, ReqID, UUID) ->
    ?REQUEST(Preflist, ReqID, {delete, UUID}).

?VSET(set_metadata).
?VSET(dataset).
?VSET(description).
?VSET(disk_driver).
?VSET(homepage).
?VSET(image_size).
?VSET(name).
?VSET(nic_driver).
?VSET(os).
?VSET(type).
?VSET(zone_type).
?VSET(users).
?VSET(version).
?VSET(kernel_version).
?VSET(sha1).
?VSET(status).
?VSET(imported).
?VSET(remove_requirement).
?VSET(add_requirement).
?VSET(remove_network).
?VSET(add_network).
