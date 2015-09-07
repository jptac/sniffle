-module(sniffle_s3).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([
         delete/2,
         list/1,
         config/1
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

-spec new_upload(Type :: atom(), Key :: binary() | string()) ->
                        {ok, term()} | {error, term()}.

new_upload(Type, Key) ->
    fifo_s3:new_upload(get_bucket(Type), Key, get_config()).

%%%===================================================================
%%% Internal functions
%%%===================================================================

get_bucket(image) ->
    get_opt(image_bucket);
get_bucket(snapshot) ->
    get_opt(snapshot_bucket);
get_bucket(_) ->
    get_opt(general_bucket).

get_config() ->
    erlcloud_s3:new(get_access_key(), get_secret_key(),
                    get_host(), get_port()).

get_access_key() ->
    get_opt(access_key).
get_secret_key() ->
    get_opt(secret_key).
get_host() ->
    get_opt(host).
get_port() ->
    get_opt(port).

get_opt(Key) ->
    sniffle_opt:get(storage, s3, Key).

config(Type) ->
    R = {get_host(), get_port(), get_access_key(), get_secret_key(),
         get_bucket(Type)},
    case R of
        {"", _, _, _, _} ->
            error;
        {_, "", _, _, _} ->
            error;
        {_, _, "", _, _} ->
            error;
        {_, _, _, "", _} ->
            error;
        {_, _, _, _, ""} ->
            error;
        _ ->
            {ok, R}
    end.
