-module(sniffle_coverage).

-behaviour(riak_core_coverage_fsm).

-export([
         init/2,
         process_results/2,
         finish/2,
         start/3,
         mk_reqid/0,
         list/3, raw/3
        ]).

-record(state, {replies = #{} :: #{binary() => pos_integer()},
                seen = sets:new() :: sets:set(),
                r = 1 :: pos_integer(),
                reqid :: integer(),
                from :: pid(),
                reqs :: list(),
                raw :: boolean(),
                merge_fn = fun([E | _]) -> E  end :: fun( ([Type]) -> Type),
                completed = [] :: [binary()]}).

-define(PARTIAL_SIZE, 10).

raw(VNodeMaster, NodeCheckService, Requirements) ->
    start(VNodeMaster, NodeCheckService,
          {list, Requirements, true}).

list(VNodeMaster, NodeCheckService, Requirements) ->
    start(VNodeMaster, NodeCheckService,
          {list, Requirements, false}).

start(VNodeMaster, NodeCheckService, Request) ->
    FoldFn = fun(Es, Acc) ->
                     Es ++ Acc
             end,
    fold(VNodeMaster, NodeCheckService, Request, FoldFn, []).

fold(VNodeMaster, NodeCheckService, Request, FoldFn, Acc0) ->
    ReqID = mk_reqid(),
    sniffle_coverage_sup:start_coverage(
      ?MODULE, {self(), ReqID, something_else},
      {VNodeMaster, NodeCheckService, Request}),
    wait(ReqID, FoldFn, Acc0).

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
init(Req,
     {VNodeMaster, NodeCheckService, {list, Requirements, Raw}}) ->
    {Request, VNodeSelector, N, PrimaryVNodeCoverage,
     NodeCheckService, VNodeMaster, Timeout, State1} =
        base_init(Req, {VNodeMaster, NodeCheckService,
                        {list, Requirements, true}}),
    Merge = case Raw of
                true ->
                    fun raw_merge/1;
                false ->
                    fun merge/1
            end,
    State2 = State1#state{reqs = Requirements, raw = Raw, merge_fn = Merge},
    {Request, VNodeSelector, N, PrimaryVNodeCoverage,
     NodeCheckService, VNodeMaster, Timeout, State2};

init(Req, {VNodeMaster, NodeCheckService, Request}) ->
    base_init(Req, {VNodeMaster, NodeCheckService, Request}).

base_init({From, ReqID, _}, {VNodeMaster, NodeCheckService, Request}) ->
    {ok, N} = application:get_env(sniffle, n),
    {ok, R} = application:get_env(sniffle, r),
    %% all - full coverage; allup - partial coverage
    VNodeSelector = allup,
    PrimaryVNodeCoverage = R,
    %% We timeout after 5s
    Timeout = 5000,
    State = #state{r = R, from = From, reqid = ReqID},
    {Request, VNodeSelector, N, PrimaryVNodeCoverage,
     NodeCheckService, VNodeMaster, Timeout, State}.

update(Key, State) when is_binary(Key) ->
    update({Key, Key}, State);

update({Pts, {Key, V}}, State) when not is_binary(Pts) ->
    update({Key, {Pts, V}}, State);

update({Key, Value}, State = #state{seen = Seen}) ->
    case sets:is_element(Key, Seen) of
        true ->
            State;
        false ->
            update1({Key, Value}, State)
    end.

update1({Key, Value}, State = #state{r = R, completed = Competed, seen = Seen})
  when R < 2 ->
    Seen1 = sets:add_element(Key, Seen),
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
            Seen1 = sets:add_element(Key, Seen),
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
    lager:error("Unknown process results call: ~p ~p", [Result, State]),
    {done, State}.

finish(clean, State = #state{completed = Completed, reqid = ReqID,
                             from = From}) ->
    From ! {ok, ReqID, Completed},
    {stop, normal, State};

finish(How, State) ->
    lager:error("Unknown process results call: ~p ~p", [How, State]),
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
