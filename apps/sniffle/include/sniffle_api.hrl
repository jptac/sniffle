%% Commands that are the same in every api module

-export([wipe/1, sync_repair/2, get/1, list/0, list/2, list/3, list_/0]).

wipe(UUID) ->
    ?FM(wipe, sniffle_coverage, fold, [?REQ({wipe, UUID})]).

sync_repair(UUID, Obj) ->
    do_write(UUID, sync_repair, Obj).


get(UUID) ->
    ?FM(get, sniffle_entity_read_fsm, start,
        [{?CMD, ?MODULE}, get, UUID]).

list_() ->
    {ok, Res} = ?FM(list_all, sniffle_coverage, fold,
                    [?REQ({list, [], true})]),
    Res1 = [R || {_, R} <- Res],
    {ok,  Res1}.

%%--------------------------------------------------------------------
%% @doc Lists all vm's and fiters by a given matcher set.
%% @end
%%--------------------------------------------------------------------
list() ->
    ?FM(list_all, sniffle_coverage, fold,
        [?REQ(list)]).

list(Requirements, Full) ->
    {ok, Res} = ?FM(list_all, sniffle_coverage, fold,
                    [?REQ({list, Requirements, false})]),
    Res1 = lists:sort(rankmatcher:apply_scales(Res)),
    Res2 = case Full of
               true ->
                   Res1;
               false ->
                   [{P, ?S:uuid(O)} || {P, O} <- Res1]
           end,
    {ok, Res2}.

list(Requirements, FoldFn, Acc0) ->
    ?FM(list_all, sniffle_coverage, fold,
        [?REQ({list, Requirements, false}), FoldFn, Acc0]).

%%%===================================================================
%%% Internal Functions
%%%===================================================================

do_write(User, Op) ->
    ?FM(Op, sniffle_entity_write_fsm, write, [{?CMD, ?MODULE}, User, Op]).

do_write(User, Op, Val) ->
    ?FM(Op, sniffle_entity_write_fsm, write,
        [{?CMD, ?MODULE}, User, Op, Val]).
