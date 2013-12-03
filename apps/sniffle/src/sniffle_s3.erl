-module(sniffle_s3).

-export([
         get_config/0,
         delete/2,
         list/1
        ]).

-export([
         new_stream/2,
         new_stream/3,
         stream_length/1,
         get_part/2,
         get_stream/1
        ]).

-export([
         new_upload/2,
         new_upload/3,
         put_upload/3,
         put_upload/2,
         complete_upload/1
        ]).

-ignore_xref([
              get_config/0,
              new_stream/2,
              new_stream/3,
              get_part/2,
              put_upload/3,
              stream_length/1,
              get_stream/1,
              new_upload/2,
              new_upload/3,
              put_upload/2,
              complete_upload/1
             ]).

-record(download, {
          bucket            :: string(),
          key               :: string(),
          conf              :: term(),
          size              :: pos_integer(),
          part  = 0         :: non_neg_integer(),
          chunk = 1024*1024 :: pos_integer()
         }).

-record(upload, {
          bucket            :: string(),
          key               :: string(),
          conf              :: term(),
          id                :: string(),
          etags = []        :: [{non_neg_integer(), string()}],
          part = 1          :: non_neg_integer()
         }).

list(Type) ->
    try erlcloud_s3:list_objects(get_bucket(Type), get_config()) of
        List ->
            case proplists:get_value(contents, List) of
                undefined ->
                    {ok, []};
                C ->
                    [proplists:get_value(key, O) || O <- C]
            end
    catch
        _:E ->
            lager:error("Metadata fetch error: ~p", [E]),
            {error, E}
    end.

delete(Type, Key) ->
    erlcloud_s3:delete_object(get_bucket(Type), Key, get_config()).
new_stream(Type, Key) ->
    new_stream(get_bucket(Type), Key, get_config()).

new_stream(Bucket, Key, Config) ->
    try erlcloud_s3:list_objects(Bucket, Config) of
        List ->
            case proplists:get_value(contents, List) of
                undefined ->
                    {error, not_found};
                Content ->
                    case find_size(Content, Key) of
                        not_found ->
                            {error, not_found};
                        {ok, Size} ->
                            D = #download{
                                   bucket = Bucket,
                                   key = Key,
                                   conf = Config,
                                   size = Size
                                  },
                            {ok, D}
                    end
            end
    catch
        _:E ->
            lager:error("Metadata fetch error: ~p", [E]),
            {error, E}
    end.

stream_length(#download{size=S, chunk=C}) ->
    X = S / C,
    T = erlang:trunc(X),
    case (X - T) of
        Neg when Neg < 0 -> T;
        Pos when Pos > 0 -> T + 1;
        _ -> T
    end.

get_part(P, #download{bucket=B, key=K, conf=Conf, chunk=C, size=Size}) ->
    Start = P*C,
    Last = Size - 1,
    End = case (P+1)*C of
              EndX when EndX > Last ->
                  Last;
              EndX ->
                  EndX
          end,
    Range = build_range(Start, End),
    try erlcloud_s3:get_object(B, K, [{range, Range}], Conf) of
        Data ->
            case proplists:get_value(content, Data) of
                undefined ->
                    {error, content};
                D ->
                    {ok, D}
            end
    catch
        _:E ->
            {error, E}
    end.


get_stream(#download{part=P, size=S, chunk=C})
  when P*C >=  S ->
    {ok, done};

get_stream(St = #download{part=P}) ->
    case get_part(P, St) of
        {ok, D} ->
            {ok, D, St#download{part = P+1}};
        E ->
            E
    end.

new_upload(Type, Key) ->
    new_upload(get_bucket(Type), Key, get_config()).

new_upload(Bucket, Key, Config) ->
    case erlcloud_s3:start_multipart(Bucket, Key, [], [], Config) of
        {ok, [{uploadId,Id}]} ->
            U = #upload{
                   bucket = Bucket,
                   key = Key,
                   conf = Config,
                   id = Id
                  },
            {ok, U};
        {error, Error} ->
            {error, Error}
    end.

put_upload(P, V, U = #upload{bucket=B, key=K, conf=C, id=Id, etags=Ts}) ->
    case erlcloud_s3:upload_part(B, K, Id, P, V, [], C) of
        {ok, [{etag, ETag}]} ->
            {ok, U#upload{etags = [{P, ETag} | Ts]}};
        {error, Error} ->
            {error, Error}
    end.

put_upload(V, U = #upload{bucket=B, key=K, conf=C, part=P, id=Id, etags=Ts}) ->
    case erlcloud_s3:upload_part(B, K, Id, P, V, [], C) of
        {ok, [{etag, ETag}]} ->
            {ok, U#upload{part = P+1, etags = [{P, ETag} | Ts]}};
        {error, Error} ->
            {error, Error}
    end.

complete_upload(#upload{bucket=B, key=K, conf=C, id=Id, etags=Ts}) ->
    erlcloud_s3:complete_multipart(B, K, Id, lists:sort(Ts), [], C).

%%%===================================================================
%%% Internal functions
%%%===================================================================

build_range(Start, Stop) ->
	lists:flatten(io_lib:format("bytes=~p-~p", [Start, Stop])).

find_size([], _) ->
    not_found;
find_size([O|R], File) ->
    case proplists:get_value(key, O) of
        Name when Name =:= File ->
            {ok, proplists:get_value(size, O)};
        _ ->
            find_size(R, File)
    end.

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
