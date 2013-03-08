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

create(Img, Idx, Data) ->
    do_write({Img, Idx}, create, Data).

delete(Img, Idx) ->
    do_write({Img, Idx}, delete).

delete(Img) ->
    do_write(Img, delete).

get(Img, Idx) ->
    lager:debug("<IMG> ~s[~p]", [Img, Idx]),
    sniffle_entity_read_fsm:start(
      {sniffle_img_vnode, sniffle_img},
      get, {Img, Idx}
     ).

list() ->
    sniffle_entity_coverage_fsm:start(
      {sniffle_img_vnode, sniffle_img},
      list
     ).

list(Img) ->
    sniffle_entity_coverage_fsm:start(
      {sniffle_img_vnode, sniffle_img},
      list, Img
     ).


%%%===================================================================
%%% Internal Functions
%%%===================================================================

do_write(Img, Op) ->
    case sniffle_entity_write_fsm:write({sniffle_img_vnode, sniffle_img}, Img, Op) of
        {ok, not_found} ->
            not_found;
        R ->
            R
    end.

do_write(Img, Op, Val) ->
    case sniffle_entity_write_fsm:write({sniffle_img_vnode, sniffle_img}, Img, Op, Val) of
        {ok, not_found} ->
            not_found;
        R ->
            R
    end.
