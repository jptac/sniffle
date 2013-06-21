-define(ENV(K, D),
        (case application:get_env(sniffle, K) of
             undefined ->
                 D;
             {ok, EnvValue} ->
                 EnvValue
         end)).

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

-record(sniffle_obj, {val    :: val(),
                      vclock :: vclock:vclock()}).

-type sniffle_obj() :: #sniffle_obj{} | not_found.

-type idx_node() :: {integer(), node()}.

-type vnode_reply() :: {idx_node(), sniffle_obj() | not_found}.

-record(hypervisor,
        {
          name :: binary(),
          host :: inet:ip_address() | inet:hostname(),
          port :: inet:port_number(),
          resources :: dict()
        }).

-record(vm,
        {
          uuid :: fifo:uuid(),
          alias :: binary(),
          hypervisor :: binary(),
          log :: [fifo:log()],
          attributes :: dict()
        }).

-record(iprange,
        {
          name :: binary(),
          network :: integer(),
          gateway :: integer(),
          netmask :: integer(),
          first :: integer(),
          last :: integer(),
          current :: integer(),
          tag :: binary(),
          free :: [integer()]
        }).

-record(package,
        {
          uuid :: fifo:uuid(),
          name :: binary(),
          attributes :: dict()
        }).

-record(dataset,
        {
          uuid :: fifo:uuid(),
          name :: binary(),
          attributes :: dict()
        }).
