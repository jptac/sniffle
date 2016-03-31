-module(sniffle_2i_cmd).
-define(BUCKET, <<"2i">>).
-include("sniffle.hrl").


%% Reads
-export([get/3]).

%% Writes
-export([add/4,
         delete/3,
         repair/4, sync_repair/4]).

-ignore_xref([
              get/3,
              add/4,
              delete/3,
              repair/4, sync_repair/4
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

add_fn({_, Coordinator} = ReqID, not_found, [Target]) ->
    S2i = sniffle_2i_state:new(ReqID),
    S2i1 = sniffle_2i_state:target(ReqID, Target, S2i),
    {write, ok, ft_obj:new(S2i1, Coordinator)};
add_fn({_, Coordinator} = ReqID, {ok, O}, [Target]) ->
    S2i = ft_obj:val(O),
    S2i1 = sniffle_2i_state:target(ReqID, Target, S2i),
    {write, ok, ft_obj:update(S2i1, Coordinator, O)}.

add(Preflist, ReqID, UUID, Target) ->
    ?REQUEST(Preflist, ReqID, {apply, UUID, fun add_fn/3, [Target]}).

delete(Preflist, ReqID, UUID) ->
    ?REQUEST(Preflist, ReqID, {delete, UUID}).
