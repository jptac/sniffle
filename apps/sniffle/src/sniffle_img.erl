-module(sniffle_img).
-include("sniffle.hrl").

-define(MASTER, sniffle_img_vnode_master).
-define(VNODE, sniffle_img_vnode).
-define(SERVICE, sniffle_img).

-export([
         create/4,
         delete/1,
         delete/2,
         get/2,
         get/1,
         list/0,
         list/1,
         backend/0,
         sync_repair/2,
         wipe/1,
         list_/1
        ]).

-ignore_xref([
              sync_repair/2,
              list_/1,
              wipe/1
              ]).

wipe(UUID) ->
    sniffle_coverage:start(?MASTER, ?SERVICE, {wipe, UUID}).

sync_repair(<<_Img:36/binary, _Part:32/integer>>=UUID, Obj) ->
    do_write(UUID, sync_repair, Obj).

list_(Base) ->
    {ok, Res} = sniffle_full_coverage:start(
                  ?MASTER, ?SERVICE, {list, Base, true, true}),
    Res1 = [R || {_, R} <- Res],
    {ok,  Res1}.

-spec create(Img::fifo:dataset_id(),
             Idx::integer(),
             Data::binary(),
             Ref::term()) ->
                    {error, timeout} | ok.

create(_Img, done, _, Ref) ->
    case backend() of
        s3 ->
            fifo_s3:complete_upload(Ref);
        internal ->
            ok
    end;

create(Img, Idx, Data, Ref) ->
    case backend() of
        s3 ->
            ImgS = binary_to_list(Img),
            U = case Ref of
                    undefined ->
                        {ok, U1} = sniffle_s3:new_upload(image, ImgS),
                        U1;
                    _ ->
                        Ref
                end,
            fifo_s3:put_upload(Idx+1, Data, U);
        internal ->
            {do_write({Img, Idx}, create, Data), undefined}
    end.

-spec delete(Img::fifo:dataset_id(),
             Idx::integer()) ->
                    not_found | {error, timeout} | ok.
delete(Img, Idx) ->
    case backend() of
        s3 ->
            ok;
        internal ->
            do_write({Img, Idx}, delete)
    end.

-spec delete(Img::fifo:dataset_id()) ->
                    not_found | {error, timeout} | ok.
delete(Img) ->
    case backend() of
        s3 ->
            sniffle_s3:delete(image, binary_to_list(Img)),
            ok;
        internal ->
            {ok, Idxs} = list(Img),
            [do_write({Img, Idx}, delete) || Idx <- Idxs],
            ok
    end.

-spec get(Img::fifo:dataset_id(),
          Idx::integer()) ->
                 not_found | {error, timeout} | {ok, Data::binary()}.
get(Img, Idx) ->
    case backend() of
        s3 ->
            {ok, S} = sniffle_s3:new_stream(image, binary_to_list(Img)),
            fifo_s3:get_part(Idx, S);
        internal ->
            lager:debug("<IMG> ~s[~p]", [Img, Idx]),
            sniffle_entity_read_fsm:start(
              {?VNODE, ?SERVICE},
              get, {Img, Idx})
    end.

get({Img, Idx}) ->
    get(Img, Idx);

get(<<Img:36/binary, Idx:32/integer>>) ->
    get(Img, Idx).

-spec list() ->
                  {ok, [Img::fifo:dataset_id()]} | {error, timeout}.
list() ->
    case backend() of
        s3 ->
            case sniffle_s3:list(image) of
                Is when is_list(Is) ->
                    {ok, [list_to_binary(I) || I <- Is]};
                R ->
                    R
            end;
        internal ->
            sniffle_coverage:start(
              ?MASTER, ?SERVICE,
              list)
    end.

-spec list(Img::fifo:dataset_id()) ->
                  {ok, [Idx::integer()]} | {error, timeout}.
list(Img) ->
    case backend() of
        s3 ->

            {ok,{S3Host, S3Port, AKey, SKey, Bucket}} = sniffle_s3:config(image),
            {ok, AKey, SKey, S3Host, S3Port, Bucket, Img};
        internal ->
            sniffle_coverage:start(
              ?MASTER, ?SERVICE,
              {list, Img})
    end.

%%%===================================================================
%%% Internal Functions
%%%===================================================================

do_write(Img, Op) ->
    sniffle_entity_write_fsm:write({?VNODE, ?SERVICE}, Img, Op).

do_write(Img, Op, Val) ->
    sniffle_entity_write_fsm:write({?VNODE, ?SERVICE}, Img, Op, Val).

backend() ->
    sniffle_opt:get(storage, general, backend, large_data_backend, internal).
