-module(sniffle_opt).


-export([get/3, set/2, unset/1, update/0]).
-ignore_xref([update/0]).

get(Prefix, SubPrefix, Key) ->
    fifo_opt:get(Prefix, SubPrefix, Key).

set(Ks, Val) ->
    fifo_opt:set(opts(), Ks, Val).

unset(Ks) ->
    fifo_opt:unset(opts(), Ks).


update() ->
    Opts =
        [
         {network, http, proxy},
         {storage, general, backend},
         {storage, s3, image_bucket},
         {storage, s3, snapshot_bucket},
         {storage, s3, general_bucket},
         {storage, s3, access_key},
         {storage, s3, secret_key},
         {storage, s3, host},
         {storage, s3, backup_host},
         {storage, s3, port}
        ],
    [update(A, B, C) || {A, B, C} <- Opts].

update(A, B, C) ->
    case riak_core_metadata:get({A, B}, C) of
        undefined ->
            ok;
        V ->
            riak_core_metadata:delete({A, B}, C),
            set([A, B, C], V)
    end.


%%%===================================================================
%%% Internal Functions
%%%===================================================================

opts() ->
    [{"network",
      [{"http", [{"proxy", string}]}]},
     {"endpoints",
      [{"datasets",
        [{"servers", {list, binary}}]}]},
     {"storage",
      [{"s3",
        [{"image_bucket", string}, {"snapshot_bucket", string},
         {"general_bucket", string},
         {"access_key", string}, {"secret_key", string},
         {"host", string}, {"port", integer},
         {"backup_host", string}, {"backup_port", integer},
         {"backup_access_key", string}, {"backup_secret_key", string}]}]}].
