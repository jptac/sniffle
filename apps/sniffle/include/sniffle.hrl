-record(vstate, {
          db,
          partition,
          service,
          bucket,
          service_bin,
          node,
          hashtrees,
          internal,
          state,
          vnode
         }).


-define(DEFAULT_TIMEOUT, 1000).

-type val() ::  statebox:statebox().

-type idx_node() :: {integer(), node()}.

-type vnode_reply() :: {idx_node(), any() | not_found}.


-define(SET(T),
        T(UUID, V) ->
               do_write(UUID, T, [V])).

-define(VNODE, sniffle_vnode).
-define(MASTER, sniffle_vnode_master).

-include_lib("sniffle_req/include/req.hrl").

-define(REQUEST(Preflist, ReqID, Request),
        riak_core_vnode_master:command(Preflist,
                                       ?REQ(ReqID, Request),
                                       {fsm, undefined, self()},
                                       ?MASTER)).
-define(VSET(Field),
        Field(Preflist, ReqID, UUID, Vals) ->
               ?REQUEST(Preflist, ReqID, {change, Field, UUID, Vals})).

