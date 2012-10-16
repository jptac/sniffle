-define(PRINT(Var), io:format("DEBUG: ~p:~p - ~p~n~n ~p~n~n", [?MODULE, ?LINE, ??Var, Var])).

-define(N, 3).
-define(R, 2).
-define(W, 3).
-define(STATEBOX_EXPIRE, 60000).
-define(DEFAULT_TIMEOUT, 10000).


-type val() ::  statebox:statebox().

-record(sniffle_obj, {val    :: val(),
		    vclock :: vclock:vclock()}).

-type sniffle_obj() :: #sniffle_obj{} | not_found.

-type idx_node() :: {integer(), node()}.

-type vnode_reply() :: {idx_node(), sniffle_obj() | not_found}.

-record(hypervisor,
	{
	  name :: string(),
	  host :: string(),
	  port :: pos_integer(),
	  resources :: dict()
	}).

-record(vm,
	{
	  uuid :: string(),
	  alias :: string(),
	  hypervisor :: string(),
	  attributes :: dict()
	}).

-record(iprange,
	{
	  name :: string(),
	  network :: integer(),
	  gateway :: integer(),
	  netmask :: integer(),
	  first :: integer(),
	  last :: integer(),
	  current :: integer(),
	  tag :: string(),
	  free :: [integer()]
	}).

-record(package,
	{
	  uuid :: string(),
	  name :: string(),
	  attributes :: dict()
	}).

-record(dataset,
	{
	  uuid :: string(),
	  name :: string(),
	  attributes :: dict()
	}).
