-module(sniffle_network_cmd).
-define(BUCKET, <<"network">>).
-include("sniffle.hrl").

-export([
         repair/4,
         sync_repair/4,
         get/3,
         create/4,
         delete/3,
         add_iprange/4,
         remove_iprange/4,
         add_resolver/4,
         remove_resolver/4
        ]).

-ignore_xref([
         repair/4,
         sync_repair/4,
         get/3,
         create/4,
         delete/3,
         add_iprange/4,
         remove_iprange/4
        ]).

-export([name/4, uuid/4, set_metadata/4]).
-ignore_xref([name/4, uuid/4, set_metadata/4]).


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

delete(Preflist, ReqID, UUID) ->
    ?REQUEST(Preflist, ReqID, {delete, UUID}).

create_fn({_, Coordinator} = ReqID, not_found, [UUID, Name]) ->
    V0 = ft_network:new(ReqID),
    V1 = ft_network:uuid(ReqID, UUID, V0),
    V2 = ft_network:name(ReqID, Name, V1),
    {write, ok, ft_obj:new(V2, Coordinator)};

create_fn({_, Coordinator} = ReqID, {ok, O}, [UUID, Name]) ->
    V0 = ft_obj:val(O),
    V1 = ft_network:uuid(ReqID, UUID, V0),
    V2 = ft_network:name(ReqID, Name, V1),
    {write, ok, ft_obj:update(V2, Coordinator, O)}.

create(Preflist, ReqID, UUID, Args) ->
    ?REQUEST(Preflist, ReqID, {apply, UUID, fun create_fn/3, [UUID | Args]}).

?VSET(add_iprange).
?VSET(remove_iprange).
?VSET(add_resolver).
?VSET(remove_resolver).
?VSET(uuid).
?VSET(name).
?VSET(set_metadata).

