-module(sniffle_util).
-export([ensure_str/1]).

ensure_str(V) when is_atom(V) ->
    atom_to_list(V);
ensure_str(V) when is_binary(V) ->
    binary_to_list(V);
ensure_str(V) when is_integer(V) ->
    integer_to_list(V);
ensure_str(V) when is_list(V) ->
    V.
