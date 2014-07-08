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

-record(hypervisor_0_1_0,
        {
          characteristics = riak_dt_map:new()    :: riak_dt_map:map(),
          etherstubs      = riak_dt_lwwreg:new() :: riak_dt_lwwreg:orswot(),
          host            = riak_dt_lwwreg:new() :: riak_dt_lwwreg:lwwreg(),
          metadata        = riak_dt_map:new()    :: riak_dt_map:map(),
          alias           = riak_dt_lwwreg:new() :: riak_dt_lwwreg:lwwreg(),
          networks        = riak_dt_lwwreg:new() :: riak_dt_lwwreg:lwwreg(),
          path            = riak_dt_lwwreg:new() :: riak_dt_lwwreg:orswot(),
          pools           = riak_dt_map:new()    :: riak_dt_map:map(),
          port            = riak_dt_lwwreg:new() :: riak_dt_lwwreg:lwwreg(),
          resources       = riak_dt_map:new()    :: riak_dt_map:map(),
          services        = riak_dt_map:new()    :: riak_dt_map:map(),
          sysinfo         = riak_dt_lwwreg:new() :: riak_dt_lwwreg:lwwreg(),
          uuid            = riak_dt_lwwreg:new() :: riak_dt_lwwreg:lwwreg(),
          version         = riak_dt_lwwreg:new() :: riak_dt_lwwreg:lwwreg(),
          virtualisation  = riak_dt_lwwreg:new() :: riak_dt_lwwreg:lwwreg()
        }).

-record(grouping_0_1_0,
        {
          uuid           = riak_dt_lwwreg:new() :: riak_dt_lwwreg:lwwreg(),
          name           = riak_dt_lwwreg:new() :: riak_dt_lwwreg:lwwreg(),
          type           = riak_dt_lwwreg:new() :: riak_dt_lwwreg:lwwreg(),
          groupings      = riak_dt_orswot:new() :: riak_dt_orswot:orswot(),
          elements       = riak_dt_orswot:new() :: riak_dt_orswot:orswot(),
          metadata       = riak_dt_map:new()    :: riak_dt_map:map()
        }).

-record(dataset_0_1_0,
        {
          uuid           = riak_dt_lwwreg:new() :: riak_dt_lwwreg:lwwreg(),
          status         = riak_dt_lwwreg:new() :: riak_dt_lwwreg:lwwreg(),
          imported       = riak_dt_lwwreg:new() :: riak_dt_lwwreg:lwwreg(),
          metadata       = riak_dt_map:new()    :: riak_dt_map:map(),

          dataset        = riak_dt_lwwreg:new() :: riak_dt_lwwreg:lwwreg(),
          description    = riak_dt_lwwreg:new() :: riak_dt_lwwreg:lwwreg(),
          disk_driver    = riak_dt_lwwreg:new() :: riak_dt_lwwreg:lwwreg(),
          homepage       = riak_dt_lwwreg:new() :: riak_dt_lwwreg:lwwreg(),
          image_size     = riak_dt_lwwreg:new() :: riak_dt_lwwreg:lwwreg(),
          name           = riak_dt_lwwreg:new() :: riak_dt_lwwreg:lwwreg(),
          networks       = riak_dt_lwwreg:new() :: riak_dt_lwwreg:lwwreg(),
          nic_driver     = riak_dt_lwwreg:new() :: riak_dt_lwwreg:lwwreg(),
          os             = riak_dt_lwwreg:new() :: riak_dt_lwwreg:lwwreg(),
          users          = riak_dt_lwwreg:new() :: riak_dt_lwwreg:lwwreg(),
          version        = riak_dt_lwwreg:new() :: riak_dt_lwwreg:lwwreg()

        }).

-record(network_0_1_0,
        {
          uuid           = riak_dt_lwwreg:new() :: riak_dt_lwwreg:lwwreg(),
          name           = riak_dt_lwwreg:new() :: riak_dt_lwwreg:lwwreg(),
          ipranges       = riak_dt_orswot:new() :: riak_dt_orswot:orswot(),
          metadata       = riak_dt_map:new()    :: riak_dt_map:map()
        }).


-define(PACKAGE, package).
-define(IPRANGE, iprange).
-define(VM, vm).
-define(HYPERVISOR, hypervisor_0_1_0).
-define(GROUPING, grouping_0_1_0).
-define(DATASET, dataset_0_1_0).
-define(NETWORK, network_0_1_0).

-define(NEW_LWW(V, T), riak_dt_lwwreg:update(
                         {assign, V, T}, none,
                         riak_dt_lwwreg:new())).
