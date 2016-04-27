-module(sniffle_dtrace_cmd).
-define(BUCKET, <<"dtrace">>).
-include("sniffle.hrl").

-export([
         sync_repair/4,
         repair/4,
         get/3,
         create/4,
         delete/3
        ]).


-ignore_xref([
              create/4,
              delete/3,
              get/3,
              repair/4,
              start_vnode/1,
              handle_info/2,
              sync_repair/4
             ]).

-export([
         name/4,
         uuid/4,
         script/4,
         set_metadata/4,
         set_config/4
        ]).

-ignore_xref([
         name/4,
         uuid/4,
         script/4,
         set_metadata/4,
         set_config/4
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
create_fn({_, Coordinator} = ReqID, not_found, [UUID, Name, Script]) ->
    I0 = ft_dtrace:new(ReqID),
    I1 = ft_dtrace:uuid(ReqID, UUID, I0),
    I2 = ft_dtrace:name(ReqID, Name, I1),
    I3 = ft_dtrace:script(ReqID, Script, I2),
    {write, ok, ft_obj:new(I3, Coordinator)};

create_fn({_, Coordinator} = ReqID, {ok, O}, [UUID, Name, Script]) ->
    I0 = ft_obj:val(O),
    I1 = ft_dtrace:uuid(ReqID, UUID, I0),
    I2 = ft_dtrace:name(ReqID, Name, I1),
    I3 = ft_dtrace:script(ReqID, Script, I2),
    {write, ok, ft_obj:update(I3, Coordinator, O)}.

create(Preflist, ReqID, UUID, Args) ->
    ?REQUEST(Preflist, ReqID, {apply, UUID, fun create_fn/3, [UUID | Args]}).

sync_repair(Preflist, ReqID, UUID, Obj) ->
    ?REQUEST(Preflist, ReqID, {sync_repair, UUID, Obj}).

delete(Preflist, ReqID, UUID) ->
    ?REQUEST(Preflist, ReqID, {delete, UUID}).

?VSET(name).
?VSET(uuid).
?VSET(script).
?VSET(set_metadata).
?VSET(set_config).
