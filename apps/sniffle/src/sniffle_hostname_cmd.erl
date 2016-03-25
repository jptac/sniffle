-module(sniffle_hostname_cmd).
-define(BUCKET, <<"hostname">>).
-include("sniffle.hrl").

-export([repair/4,
         sync_repair/4,
         get/3,
         delete/3,
         add_a/4,
         remove_a/4
        ]).

-ignore_xref([repair/4,
              sync_repair/4,
              get/3,
              delete/3,
              add_a/4,
              remove_a/4
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

delete(Preflist, ReqID, UUID) ->
    ?REQUEST(Preflist, ReqID, {delete, UUID}).

?VSET(add_a).
?VSET(remove_a).
