-module(sniffle_coverage).
-include_lib("sniffle_req/include/req.hrl").
-behaviour(riak_core_coverage_fsm).

-export([
         init/2,
         process_results/2,
         finish/2,
         start/1,
         mk_reqid/0,
         fold/1, fold/3,
         list/1, raw/1,
         list/3, raw/3
        ]).

-ignore_xref([list/5, raw/5]).

-type merge_fn() ::  fun(([term()]) -> term()).

-record(state, {replies = #{} :: #{binary() => pos_integer()},
                seen = btrie:new() :: btrie:btrie(),
                r = 1 :: pos_integer(),
                reqid :: integer(),
                from :: pid(),
                reqs :: list(),
                raw :: boolean(),
                merge_fn = fun([E | _]) -> E  end :: merge_fn(),
                completed = [] :: [binary()]}).

-define(PARTIAL_SIZE, 10).

concat(Es, Acc) ->
    Es ++ Acc.

raw(Requirements) ->
    raw(Requirements, fun concat/2, []).

raw(Requirements, FoldFn, Acc0) ->
    fold(#req{request = {list, Requirements, true}}, FoldFn, Acc0).

list(Requirements) ->
    list(Requirements, fun concat/2, []).

list(Requirements, FoldFn, Acc0) ->
    fold(#req{request = {list, Requirements, false}}, FoldFn, Acc0).

start(Request) ->
    fold(Request, fun concat/2, []).

fold(Request) ->
    fold(Request, fun concat/2, []).

fold(Request, FoldFn, Acc0) ->
    ReqID = mk_reqid(),
    sniffle_coverage_sup:start_coverage(
      ?MODULE, {self(), ReqID, something_else}, Request#req{id = ReqID}),
    wait(ReqID, FoldFn, Acc0).

-spec wait(term(), fun(), term()) ->
    ok | {ok, term()} | {error, term()}.
wait(ReqID, FoldFn, Acc) ->
    receive
        {ok, ReqID} ->
            ok;
        {partial, ReqID, Result1} ->
            wait(ReqID, FoldFn, FoldFn(Result1, Acc));
        {ok, ReqID, Result1} ->
            {ok, FoldFn(Result1, Acc)}
    after 10000 ->
            {error, timeout}
    end.

%% The first is the vnode service used
init(Req, RequestIn = #req{request = {list, Requirements, Raw}}) ->
    {Request, VNodeSelector, N, PrimaryVNodeCoverage,
     sniffle, sniffle_vnode_master, Timeout, State1} =
        base_init(Req, RequestIn#req{request = {list, Requirements, true}}),
    Merge = case Raw of
                true ->
                    fun raw_merge/1;
                false ->
                    fun merge/1
            end,
    State2 = State1#state{reqs = Requirements, raw = Raw, merge_fn = Merge},
    {Request, VNodeSelector, N, PrimaryVNodeCoverage,
     sniffle, sniffle_vnode_master, Timeout, State2};

init(Req, Request) ->
    base_init(Req, Request).

base_init({From, ReqID, _}, Request) ->
    {ok, N} = application:get_env(sniffle, n),
    {ok, R} = application:get_env(sniffle, r),
    %% all - full coverage; allup - partial coverage
    VNodeSelector = allup,
    PrimaryVNodeCoverage = R,
    %% We timeout after 10s or whatever is configured
    Timeout = application:get_env(sniffle, coverage_timeout, 10000),
    State = #state{r = R, from = From, reqid = ReqID},
    {Request, VNodeSelector, N, PrimaryVNodeCoverage,
     sniffle, sniffle_vnode_master, Timeout, State}.

update(Key, State) when is_binary(Key) ->
    update({Key, Key}, State);

update(Key, State) when is_atom(Key) ->
    update({Key, Key}, State);

update({Pts, {Key, V}}, State) when not is_binary(Pts) ->
    update({Key, {Pts, V}}, State);

update({Key, Value}, State = #state{seen = Seen}) ->
    case btrie:is_key(Key, Seen) of
        true ->
            State;
        false ->
            update1({Key, Value}, State)
    end.

update1({Key, Value}, State = #state{r = R, completed = Competed, seen = Seen})
  when R < 2 ->
    Seen1 = btrie:store(Key, Seen),
    State#state{seen = Seen1, completed = [Value | Competed]};

update1({Key, Value},
        State = #state{r = R, completed = Competed, seen = Seen,
                       merge_fn = Merge, replies = Replies}) ->
    case maps:find(Key, Replies) of
        error ->
            Replies1 = maps:put(Key, [Value], Replies),
            State#state{replies = Replies1};
        {ok, Vals} when length(Vals) >= R - 1 ->
            Merged = Merge([Value | Vals]),
            Seen1 = btrie:store(Key, Seen),
            Replies1 = maps:remove(Key, Replies),
            State#state{seen = Seen1, completed = [Merged | Competed],
                        replies = Replies1};
        {ok, Vals} ->
            Replies1 = maps:put(Key, [Value | Vals], Replies),
            State#state{replies = Replies1}
    end.

process_results({Type, _ReqID, _IdxNode, Obj},
                State = #state{reqid = ReqID, from = From})
  when Type =:= partial;
       Type =:= ok
       ->
    State1 = lists:foldl(fun update/2, State, Obj),
    State2 = case length(State1#state.completed) of
                 L when L >= ?PARTIAL_SIZE ->
                     From ! {partial, ReqID, State1#state.completed},
                     State1#state{completed = []};
                 _ ->
                     State1
             end,
    %% If we return ok and not done this vnode will be considered
    %% to keep sending data.
    %% So we translate the reply type here
    ReplyType = case Type of
                    ok -> done;
                    partial -> ok
                end,
    {ReplyType, State2};

process_results({ok, _}, State) ->
    {done, State};

process_results(Result, State) ->
    lager:error("[coverage] Unknown process results call: ~p ~p",
                [Result, State]),
    {done, State}.

finish(clean, State = #state{completed = Completed, reqid = ReqID,
                             from = From}) ->
    From ! {ok, ReqID, Completed},
    {stop, normal, State};

finish({error, E}, State = #state{completed = Completed, reqid = ReqID,
                             from = From}) ->
    lager:error("[coverage] Finished with error: ~p", [E]),
    From ! {ok, ReqID, Completed},
    {stop, normal, State};

finish(How, State) ->
    lager:error("[coverage] Unknown finish call: ~p ~p", [How, State]),
    {error, failed}.

%%%===================================================================
%%% Internal Functions
%%%===================================================================

mk_reqid() ->
    erlang:unique_integer().


raw_merge([{Score, V} | R]) ->
    raw_merge(R, Score, [V]).

raw_merge([], recalculate, Vs) ->
    {0, ft_obj:merge(sniffle_entity_read_fsm, Vs)};

raw_merge([], Score, Vs) ->
    {Score, ft_obj:merge(sniffle_entity_read_fsm, Vs)};

raw_merge([{Score, V} | R], Score, Vs) ->
    raw_merge(R, Score, [V | Vs]);

raw_merge([{_Score1, V} | R], _Score2, Vs) when _Score1 =/= _Score2->
    raw_merge(R, recalculate, [V | Vs]).

merge(Vs) ->
    {Score, Obj} = raw_merge(Vs),
    {Score, ft_obj:val(Obj)}.
