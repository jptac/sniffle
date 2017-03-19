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
    fifo_s3:list(get_bucket(Type), get_config(Type)).

delete(Type, Key) ->
    case get_host(Type) of
        "" ->
            ok;
        _ ->
            fifo_s3:delete(get_bucket(Type), Key, get_config(Type))
    end.

new_stream(Type, Key) ->
    fifo_s3:new_stream(get_bucket(Type), Key, get_config(Type)).

-spec new_upload(Type :: atom(), Key :: binary() | string()) ->
                        {ok, term()} | {error, term()}.

new_upload(Type, Key) ->
    fifo_s3:new_upload(get_bucket(Type), Key, get_config(Type)).

%%%===================================================================
%%% Internal functions
%%%===================================================================

get_bucket(image) ->
    get_opt(image_bucket);
get_bucket(snapshot) ->
    get_opt(snapshot_bucket);
get_bucket(_) ->
    get_opt(general_bucket).

get_config(Type) ->
    erlcloud_s3:new(get_access_key(Type), get_secret_key(Type),
                    get_host(Type), get_port(Type)).

get_access_key(backup) ->
    get_opt(backup_access_key, access_key);
get_access_key(_Type) ->
    get_opt(access_key).

get_secret_key(backup) ->
    get_opt(backup_secret_key, secret_key);
get_secret_key(_Type) ->
    get_opt(secret_key).

get_host(backup) ->
    get_opt(backup_host, host);
get_host(_Type) ->
    get_opt(host).


get_port(backup) ->
    get_opt(backup_port, port);
get_port(_Type) ->
    get_opt(port).

get_opt(Key, Secondary) ->
    case get_opt(Key) of
        "" ->
            get_opt(Secondary);
        Result ->
            Result
    end.

get_opt(Key) ->
    sniffle_opt:get(storage, s3, Key).

config(Type) ->
    R = {get_host(Type), get_port(Type), get_access_key(Type),
         get_secret_key(Type), get_bucket(Type)},
    case R of
        %% @TODO: this is a ugly hack!
        {"no_s3", _, _, _, _} ->
            {ok, no_s3};
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
