-module(sniffle_opt).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([get/5, set/2]).

get(Prefix, SubPrefix, Key, EnvKey, Dflt) ->
    case riak_core_metadata:get({Prefix, SubPrefix}, Key) of
        undefined ->
            V = application:get_env(sniffle, EnvKey, Dflt),
            set(Prefix, SubPrefix, Key, V),
            V;
        V ->
            V
    end.

set(Ks, Val) ->
    case is_valid(Ks, Val) of
        {true, V1} ->
            [Prefix, SubPrefix, Key] =
                [list_to_atom(K) || K <- Ks],
            set(Prefix, SubPrefix, Key, V1)
    end.

set(Prefix, SubPrefix, Key, Val) ->
    riak_core_metadata:put({Prefix, SubPrefix}, Key, Val).

is_valid(Ks, V) ->
    Ks1 = [sniffle_util:ensure_str(K) || K <- Ks],
    case get_type(Ks1) of
        {ok, Type} ->
            case  valid_type(Type, V) of
                {true, V1} ->
                    {true, V1};
                false ->
                    {invalid, type, Type}
            end;
        E ->
            E
    end.

%%%===================================================================
%%% Internal Functions
%%%===================================================================

get_type(Ks) ->
    get_type(Ks, opts()).

get_type([], _) ->
    {invalid, path};

get_type([K], Os) ->
    case proplists:get_value(K, Os) of
        undefined ->
            {invalid, key, K};
        Type ->
            {ok, Type}
    end;

get_type([K|R], Os) ->
    case proplists:get_value(K, Os) of
        undefined ->
            {invalid, key, K};
        Os1 ->
            get_type(R, Os1)
    end.

opts() ->
    [{"storage",
      [{"general", [{"backend", {enum, ["internal", "s3"]}}]},
       {"s3", [{"image_bucket", string}, {"snapshot_bucket", string},
               {"general_bucket", string}, {"access_key", string},
               {"secret_key", string}, {"host", string},
               {"port", integer}]}]}].

valid_type(integer, I) when is_list(I) ->
    try list_to_integer(I) of
        V ->
            {true, V}
    catch
        _:_ ->
            false
    end;
valid_type(integer, I) when is_integer(I) ->
    {true, I};

valid_type(string, L) when is_binary(L) ->
    {true, binary_to_list(L)};
valid_type(string, L) when is_list(L) ->
    {true, L};

valid_type(binary, B) when is_binary(B) ->
    {true, B};
valid_type(binary, L) when is_list(L) ->
    {true, list_to_binary(L)};

valid_type({enum, Vs}, V) when is_atom(V)->
    valid_type({enum, Vs}, atom_to_list(V));
valid_type({enum, Vs}, V) when is_binary(V)->
    valid_type({enum, Vs}, binary_to_list(V));
valid_type({enum, Vs}, V) ->
    case lists:member(V, Vs) of
        true ->
            {true, list_to_atom(V)};
        false ->
            false
    end;

valid_type(_, _) ->
    false.


%%%===================================================================
%%% Tests
%%%===================================================================

-ifdef(TEST).

valid_integer_test() ->
    ?assertEqual({true, 1}, valid_type(integer, 1)),
    ?assertEqual({true, 42}, valid_type(integer, "42")),
    ?assertEqual(false, valid_type(integer, "42a")),
    ?assertEqual(false, valid_type(integer, "a")),
    ?assertEqual(false, valid_type(integer, a)).

valid_string_test() ->
    ?assertEqual({true, "abc"}, valid_type(string, "abc")),
    ?assertEqual({true, "abc"}, valid_type(string, <<"abc">>)),
    ?assertEqual(false, valid_type(string, 42)),
    ?assertEqual(false, valid_type(string, a)).

valid_binary_test() ->
    ?assertEqual({true, <<"abc">>}, valid_type(binary, "abc")),
    ?assertEqual({true, <<"abc">>}, valid_type(binary, <<"abc">>)),
    ?assertEqual(false, valid_type(binary, 42)),
    ?assertEqual(false, valid_type(binary, a)).

valid_enum_test() ->
    ?assertEqual({true, abc}, valid_type({enum, ["abc", "bcd"]}, "abc")),
    ?assertEqual({true, abc}, valid_type({enum, ["abc", "bcd"]}, "abc")),
    ?assertEqual({true, abc}, valid_type({enum, ["abc", "bcd"]}, <<"abc">>)),
    ?assertEqual(false, valid_type({enum, ["bc", "bcd"]}, "abc")),
    ?assertEqual(false, valid_type({enum, ["bc", "bcd"]}, "abc")),
    ?assertEqual(false, valid_type({enum, ["bc", "bcd"]}, <<"abc">>)),
    ?assertEqual(false, valid_type({enum, ["abc", "bcd"]}, 42)).

-endif.
