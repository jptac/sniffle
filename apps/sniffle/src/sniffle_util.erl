-module(sniffle_util).
-export([ensure_str/1, node_for/1]).

-ignore_xref([node_for/1]).

ensure_str(V) when is_atom(V) ->
    atom_to_list(V);
ensure_str(V) when is_binary(V) ->
    binary_to_list(V);
ensure_str(V) when is_integer(V) ->
    integer_to_list(V);
ensure_str(V) when is_list(V) ->
    V.

node_for(Key) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    CHash = riak_core_ring:chash(Ring),
    [{_, Node}] = chash:successors(chash:key_of(Key), CHash, 1),
    Node.
