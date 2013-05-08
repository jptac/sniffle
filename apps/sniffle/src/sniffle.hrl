-define(PRINT(Var), 1==1 orelse io:format("DEBUG: ~p:~p - ~p~n~n ~p~n~n", [?MODULE, ?LINE, ??Var, Var])).

-define(N, 3).
-define(R, 2).
-define(W, 3).
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
