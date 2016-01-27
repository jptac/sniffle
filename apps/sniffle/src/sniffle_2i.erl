-module(sniffle_2i).

-include_lib("riak_core/include/riak_core_vnode.hrl").

-export([
         sync_repair/2,
         list/0, list_/0,
         get/1, get/2, raw/1, raw/2,
         add/3, delete/2,
         wipe/2, reindex/1
        ]).


-define(TIMEOUT, 5000).
-define(MASTER, sniffle_2i_vnode_master).
-define(SERVICE, sniffle_2i).


-define(FM(Met, Mod, Fun, Args),
        folsom_metrics:histogram_timed_update(
          {sniffle, s2i, Met},
          Mod, Fun, Args)).

%% Public API

reindex(_) -> ok.

wipe(Type, Key) ->
    TK = term_to_binary({Type, Key}),
    ?FM(wipe, sniffle_coverage, start,
        [sniffle_2i_vnode_master, sniffle_2i, {wipe, TK}]).

sync_repair(TK, Obj) ->
    do_write(TK, sync_repair, Obj).


get(Type, Key) ->
    ?MODULE:get(term_to_binary({Type, Key})).

-spec get(TK::binary()) ->
                 not_found |
                 {error, timeout} |
                 {ok, Target::fifo:uuid()}.
get(TK) ->
    case ?FM(get, sniffle_entity_read_fsm, start,
             [{sniffle_2i_vnode, sniffle_2i}, get, TK]) of
        not_found ->
            not_found;
        {ok, R} ->
            case sniffle_2i_state:target(R) of
                not_found ->
                    not_found;
                UUID ->
                    {ok, UUID}
            end
    end.

raw(Type, Key) ->
    raw(term_to_binary({Type, Key})).

raw(TK) ->
    case ?FM(get, sniffle_entity_read_fsm, start,
             [{sniffle_2i_vnode, sniffle_2i}, get, TK, undefined, true]) of
         not_found ->
            not_found;
        R ->
            R
    end.


list() ->
    {ok, Res} = ?FM(list, sniffle_coverage, start,
                    [?MASTER, ?SERVICE, list]),
    Res1 = [binary_to_term(R) || R <- Res],
    {ok,  Res1}.

list_() ->
    {ok, Res} =
        ?FM(list, sniffle_coverage, raw,
            [?MASTER, ?SERVICE, []]),
    Res1 = [binary_to_term(R) || {_, R} <- Res],
    {ok,  Res1}.


-spec add(Type::term(), K::binary(), Target::fifo:uuid()) ->
                 {ok, UUID::binary()} |
                 douplicate |
                 {error, timeout}.

add(Type, Key, Target) ->
    lager:info("[2i] Adding 2i key ~s/~p-~p -> ~s", [Type, Key, Target]),
    TK = term_to_binary({Type, Key}),
    Res = case sniffle_2i:get(TK) of
              not_found ->
                  ok;
              {ok, not_found} ->
                  ok;
              {ok, _} ->
                  douplicate
          end,
    case Res of
        ok ->
            do_write(TK, add, Target);
        E ->
            {error, E}
    end.

-spec delete(Type::term(), Key::binary()) ->
                    ok |
                    not_found|
                    {error, timeout}.

delete(Type, Key) ->
    TK = term_to_binary({Type, Key}),
    do_write(TK, delete).

%%%===================================================================
%%% Internal Functions
%%%===================================================================

do_write(Key, Op) ->
    ?FM(Op, sniffle_entity_write_fsm, write,
        [{sniffle_2i_vnode, sniffle_2i}, Key, Op]).

do_write(Key, Op, Val) ->
    ?FM(Op, sniffle_entity_write_fsm, write,
        [{sniffle_2i_vnode, sniffle_2i}, Key, Op, Val]).
