-module(sniffle_img).
-include("sniffle.hrl").
                                                %-include_lib("riak_core/include/riak_core_vnode.hrl").

-export([
         create/3,
         delete/1,
         delete/2,
         get/2,
         list/0,
         list/1
        ]).

-ignore_xref([read_image/5]).

-spec create(Img::fifo:dataset_id(),
             Idx::integer(),
             Data::binary()) ->
                    {error, timeout} | ok.
create(Img, Idx, Data) ->
    do_write({Img, Idx}, create, Data).

-spec delete(Img::fifo:dataset_id(),
             Idx::integer()) ->
                    not_found | {error, timeout} | ok.
delete(Img, Idx) ->
    do_write({Img, Idx}, delete).

-spec delete(Img::fifo:dataset_id()) ->
                    not_found | {error, timeout} | ok.
delete(Img) ->
    do_write(Img, delete).

-spec get(Img::fifo:dataset_id(),
          Idx::integer()) ->
                 not_found | {error, timeout} | {ok, Data::binary()}.
get(Img, Idx) ->
    lager:debug("<IMG> ~s[~p]", [Img, Idx]),
    sniffle_entity_read_fsm:start(
      {sniffle_img_vnode, sniffle_img},
      get, {Img, Idx}
     ).

-spec list() ->
                  {ok, [Img::fifo:dataset_id()]} | {error, timeout}.
list() ->
    sniffle_entity_coverage_fsm:start(
      {sniffle_img_vnode, sniffle_img},
      list
     ).

-spec list(Img::fifo:dataset_id()) ->
                  {ok, [Idx::integer()]} | {error, timeout}.
list(Img) ->
    sniffle_entity_coverage_fsm:start(
      {sniffle_img_vnode, sniffle_img},
      list, Img
     ).


%%%===================================================================
%%% Internal Functions
%%%===================================================================

do_write(Img, Op) ->
    sniffle_entity_write_fsm:write({sniffle_img_vnode, sniffle_img}, Img, Op).

do_write(Img, Op, Val) ->
    sniffle_entity_write_fsm:write({sniffle_img_vnode, sniffle_img}, Img, Op, Val).
