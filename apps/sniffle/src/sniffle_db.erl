-module(sniffle_db).

%% API
-export([start/1,
         get/3,
         transact/2,
         delete/3,
         put/4,
         fold/4,
         fold_keys/4]).

-ignore_xref([start_link/1, fold_keys/4]).

-define(BACKEND, "hanoidb").

%%%===================================================================
%%% API
%%%===================================================================
start(Partition) ->
    case erlang:whereis(Partition) of
        undefined ->
            sniffle_db_sup:start_child(Partition);
        _ ->
            ok
    end.

transact(Partition, Transaction) ->
    gen_server:call(Partition, {transact, Transaction}).

put(Partition, Bucket, Key, Value) ->
    gen_server:call(Partition, {put, Bucket, Key, Value}).

get(Partition, Bucket, Key) ->
    gen_server:call(Partition, {get, Bucket, Key}).

delete(Partition, Bucket, Key) ->
    gen_server:call(Partition, {delete, Bucket, Key}).

fold(Partition, Bucket, FoldFn, Acc0) ->
    gen_server:call(Partition, {fold, Bucket, FoldFn, Acc0}).

fold_keys(Partition, Bucket, FoldFn, Acc0) ->
    gen_server:call(Partition, {fold_keys, Bucket, FoldFn, Acc0}).

%%%===================================================================
%%% Internal functions
%%%===================================================================
