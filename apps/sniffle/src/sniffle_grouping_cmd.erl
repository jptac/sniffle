-module(sniffle_grouping_cmd).

-define(BUCKET, <<"grouping">>).
-include("sniffle.hrl").

-export([
         repair/4,
         get/3,
         create/4,
         delete/3,
         add_element/4,
         remove_element/4,
         add_grouping/4,
         remove_grouping/4,
         sync_repair/4,
         set_metadata/4,
         set_config/4
        ]).

-ignore_xref([
              create/4,
              add_element/4,
              remove_element/4,
              add_grouping/4,
              remove_grouping/4,
              delete/3,
              get/3,
              repair/4,
              start_vnode/1,
              handle_info/2,
              set_config/4,
              set_metadata/4,
              sync_repair/4
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

create_fn({_, Coordinator} = ReqID, not_found, [UUID, Name, Type]) ->
    G0 = ft_grouping:new(ReqID),
    G1 = ft_grouping:uuid(ReqID, UUID, G0),
    G2 = ft_grouping:name(ReqID, Name, G1),
    G3 = ft_grouping:type(ReqID, Type, G2),
    {write, ok, ft_obj:new(G3, Coordinator)};

create_fn({_, Coordinator} = ReqID, {ok, O}, [UUID, Name, Type]) ->
    G0 = ft_obj:val(O),
    G1 = ft_grouping:uuid(ReqID, UUID, G0),
    G2 = ft_grouping:name(ReqID, Name, G1),
    G3 = ft_grouping:type(ReqID, Type, G2),
    {write, ok, ft_obj:update(G3, Coordinator, O)}.

create(Preflist, ReqID, UUID, Args) ->
    ?REQUEST(Preflist, ReqID, {apply, UUID, fun create_fn/3, [UUID | Args]}).


sync_repair(Preflist, ReqID, UUID, Obj) ->
    ?REQUEST(Preflist, ReqID, {sync_repair, UUID, Obj}).

delete(Preflist, ReqID, UUID) ->
    ?REQUEST(Preflist, ReqID, {delete, UUID}).

?VSET(add_element).
?VSET(remove_element).
?VSET(add_grouping).
?VSET(remove_grouping).
?VSET(set_config).
?VSET(set_metadata).
