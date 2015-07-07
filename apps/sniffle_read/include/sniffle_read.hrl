-define(DEFAULT_TIMEOUT, 1000).

-type idx_node() :: {integer(), node()}.
-type vnode_reply() :: {idx_node(), any() | not_found}.
