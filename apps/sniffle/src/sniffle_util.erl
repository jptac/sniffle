-module(sniffle_util).
-export([node_for/1]).

-ignore_xref([node_for/1]).

node_for(Key) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    CHash = riak_core_ring:chash(Ring),
    [{_, Node}] = chash:successors(chash:key_of(Key), CHash, 1),
    Node.
