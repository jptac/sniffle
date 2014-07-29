-define(ENV(K, D),
        (case application:get_env(sniffle, K) of
             undefined ->
                 D;
             {ok, EnvValue} ->
                 EnvValue
         end)).

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

-define(N, ?ENV(n, 3)).
-define(R, ?ENV(r, 2)).
-define(W, ?ENV(w, 3)).
-define(NRW(System),
        (case application:get_env(sniffle, System) of
             {ok, Res} ->
                 Res;
             undefined ->
                 {?N, ?R, ?W}
         end)).
-define(STATEBOX_EXPIRE, 60000).
-define(STATEBOX_TRUNCATE, 240).

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
