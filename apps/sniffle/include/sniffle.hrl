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
               do_write(UUID, T, V)).

-define(VSET(Field),
        Field(Preflist, ReqID, Vm, Val) ->
               riak_core_vnode_master:command(Preflist,
                                              {Field, ReqID, Vm, Val},
                                              {fsm, undefined, self()},
                                              ?MASTER)).
