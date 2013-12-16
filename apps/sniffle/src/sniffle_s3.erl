-module(sniffle_s3).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([
         delete/2,
         list/1
        ]).

-export([
         new_stream/2
        ]).

-export([
         new_upload/2
        ]).

list(Type) ->
    fifo_s3:list(get_bucket(Type), get_config()).

delete(Type, Key) ->
    fifo_s3:delete(get_bucket(Type), Key, get_config()).

new_stream(Type, Key) ->
    fifo_s3:new_stream(get_bucket(Type), Key, get_config()).

new_upload(Type, Key) ->
    fifo_s3:new_upload(get_bucket(Type), Key, get_config()).

%%%===================================================================
%%% Internal functions
%%%===================================================================

get_bucket(image) ->
    get_opt(image_bucket, "fifo_images");
get_bucket(snapshot) ->
    get_opt(snapshot_bucket, "fifo_snapshots");
get_bucket(_) ->
    get_opt(general_bucket, "fifo").

get_config() ->
    erlcloud_s3:new(get_access_key(), get_secret_key(),
                    get_host(), get_port()).

get_access_key() ->
    get_opt(access_key, s3_access_key, "").
get_secret_key() ->
    get_opt(secret_key, s3_secret_key, "").
get_host() ->
    get_opt(host, s3_host, "").
get_port() ->
    get_opt(port, s3_port, "").

get_opt(Key, Dflt) ->
    sniffle_opt:get(storage, s3, Key, Key, Dflt).

get_opt(Key, EnvKey, Dflt) ->
    sniffle_opt:get(storage, s3, Key, EnvKey, Dflt).
