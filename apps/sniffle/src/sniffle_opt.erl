-module(sniffle_opt).


-export([get/5, set/2, unset/1]).

get(Prefix, SubPrefix, Key, EnvKey, Dflt) ->
    fifo_opt:get(opts(), Prefix, SubPrefix, Key, {sniffle, EnvKey}, Dflt).

set(Ks, Val) ->
    fifo_opt:set(opts(), Ks, Val).

unset(Ks) ->
    fifo_opt:unset(opts(), Ks).

%%%===================================================================
%%% Internal Functions
%%%===================================================================

opts() ->
    [{"network",
      [{"http", [{"proxy", string}]}]},
     {"storage",
      [{"general", [{"backend", {enum, ["internal", "s3"]}}]},
       {"s3", [{"image_bucket", string}, {"snapshot_bucket", string},
               {"general_bucket", string}, {"access_key", string},
               {"secret_key", string}, {"host", string},
               {"port", integer}]}]}].
