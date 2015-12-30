-module(sniffle_coverage).

-behaviour(riak_core_coverage_fsm).

-export([
         init/2,
         process_results/2,
         finish/2,
         start/3,
         mk_reqid/0
        ]).

-record(state, {replies = #{} :: #{binary() => pos_integer()},
                seen = sets:new() :: sets:set(),
                r = 1 :: pos_integer(),
                reqid :: integer(),
                from :: pid(),
                reqs :: list(),
                raw :: boolean(),
                completed = [] :: [binary()]}).

-define(PARTIAL_SIZE, 10).

start(VNodeMaster, NodeCheckService, Request) ->
    ReqID = mk_reqid(),
    sniffle_coverage_sup:start_coverage(
      ?MODULE, {self(), ReqID, something_else},
      {VNodeMaster, NodeCheckService, Request}),
    wait(ReqID).

wait(ReqID) ->
    receive
        {ok, ReqID} ->
            ok;
        {partial, ReqID, Result} ->
            wait(ReqID, Result);
        {ok, ReqID, Result} ->
            {ok, Result}
    after 10000 ->
            {error, timeout}
    end.

wait(ReqID, Result) ->
    receive
        {ok, ReqID} ->
            ok;
        {partial, ReqID, Result1} ->
            wait(ReqID, Result1 ++ Result);
        {ok, ReqID, Result1} ->
            {ok, Result1 ++ Result}
    after 10000 ->
            {error, timeout}
    end.

%% The first is the vnode service used
init(Req,
     {VNodeMaster, NodeCheckService, {list, Requirements, Full, Raw}}) ->
    {Request, VNodeSelector, N, PrimaryVNodeCoverage,
     NodeCheckService, VNodeMaster, Timeout, State1} =
        init(Req, {VNodeMaster, NodeCheckService, {list, Requirements, Full}}),
    State2 = State1#state{reqs = Requirements, raw = Raw},
    {Request, VNodeSelector, N, PrimaryVNodeCoverage,
     NodeCheckService, VNodeMaster, Timeout, State2};

init({From, ReqID, _}, {VNodeMaster, NodeCheckService, Request}) ->
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


update(Key, State = #state{seen = Seen}) ->
    case sets:is_element(Key, Seen) of
        true ->
            State;
        false ->
            update1(Key, State)
    end.

update1(Key, State = #state{r = R, completed = Competed, seen = Seen})
  when R < 2 ->
    Seen1 = sets:add_element(Key, Seen),
    State#state{seen = Seen1, completed = [Key | Competed]};

update1(Key, State = #state{r = R, completed = Competed, seen = Seen,
                            replies = Replies}) ->
    case maps:find(Key, Replies) of
        error ->
            Replies1 = maps:put(Key, 1, Replies),
            State#state{replies = Replies1};
        {ok, Count} when Count =:= R - 1 ->
            Seen1 = sets:add_element(Key, Seen),
            Replies1 = maps:remove(Key, Replies),
            State#state{seen = Seen1, completed = [Key | Competed],
                        replies = Replies1};
        {ok, Count} ->
            Replies1 = maps:put(Key, Count + 1, Replies),
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
