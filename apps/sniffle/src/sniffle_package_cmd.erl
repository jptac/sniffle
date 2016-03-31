-module(sniffle_package_cmd).
-define(BUCKET, <<"package">>).
-include("sniffle.hrl").

-export([repair/4,
         sync_repair/4,
         get/3,
         create/4,
         delete/3]).

-ignore_xref([create/4,
              delete/3,
              get/3,
              repair/4,
              start_vnode/1,
              handle_info/2,
              sync_repair/4]).

-export([
         org_resource_inc/4, org_resource_dec/4, org_resource_remove/4,
         set_metadata/4,
         blocksize/4,
         compression/4,
         cpu_cap/4,
         cpu_shares/4,
         max_swap/4,
         name/4,
         quota/4,
         ram/4,
         uuid/4,
         zfs_io_priority/4,
         remove_requirement/4,
         add_requirement/4
        ]).

-ignore_xref([
              org_resource_inc/4, org_resource_dec/4, org_resource_remove/4,
              set_metadata/4,
              blocksize/4,
              compression/4,
              cpu_cap/4,
              cpu_shares/4,
              max_swap/4,
              name/4,
              quota/4,
              ram/4,
              uuid/4,
              zfs_io_priority/4,
              remove_requirement/4,
              add_requirement/4
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


create_fn({_, Coordinator} = ReqID, not_found, [UUID, Package]) ->
    I0 = ft_package:new(ReqID),
    I1 = ft_package:uuid(ReqID, UUID, I0),
    I2 = ft_package:name(ReqID, Package, I1),
    {write, ok, ft_obj:new(I2, Coordinator)};

create_fn({_, Coordinator} = ReqID, {ok, O}, [UUID, Package]) ->
    I0 = ft_obj:val(O),
    I1 = ft_package:uuid(ReqID, UUID, I0),
    I2 = ft_package:name(ReqID, Package, I1),
    {write, ok, ft_obj:update(I2, Coordinator, O)}.

create(Preflist, ReqID, UUID, Args) ->
    ?REQUEST(Preflist, ReqID, {apply, UUID, fun create_fn/3, [UUID | Args]}).

sync_repair(Preflist, ReqID, UUID, Obj) ->
    ?REQUEST(Preflist, ReqID, {sync_repair, UUID, Obj}).

delete(Preflist, ReqID, UUID) ->
    ?REQUEST(Preflist, ReqID, {delete, UUID}).

?VSET(org_resource_inc).
?VSET(org_resource_dec).
?VSET(org_resource_remove).
?VSET(set_metadata).
?VSET(blocksize).
?VSET(compression).
?VSET(cpu_cap).
?VSET(cpu_shares).
?VSET(max_swap).
?VSET(name).
?VSET(quota).
?VSET(ram).
?VSET(uuid).
?VSET(zfs_io_priority).
?VSET(remove_requirement).
?VSET(add_requirement).
