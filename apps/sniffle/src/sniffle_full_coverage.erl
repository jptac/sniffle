-module(sniffle_full_coverage).

-include("sniffle.hrl").
-include_lib("fifo_dt/include/ft.hrl").

-behaviour(riak_core_coverage_fsm).

-export([
         init/2,
         process_results/2,
         finish/2,
         start/3
        ]).

-record(state, {replies, r, reqid, from, reqs, raw=false}).

start(VNodeMaster, NodeCheckService, {list, Requirements, true}) ->
    start(VNodeMaster, NodeCheckService, {list, Requirements, true, false});

start(VNodeMaster, NodeCheckService, Request = {list, Requirements, true, _}) ->
    ReqID = mk_reqid(),
    sniffle_coverage_sup:start_coverage(
      ?MODULE, {self(), ReqID, Requirements},
      {VNodeMaster, NodeCheckService, Request}),
    receive
        ok ->
            ok;
        {ok, Result} ->
            {ok, Result}
    after 10000 ->
            {error, timeout}
    end.

%% The first is the vnode service used
init({From, ReqID, Requirements},
     {VNodeMaster, NodeCheckService, {list, Requirements, Full, Raw}}) ->
    {NVal, R, _W} = ?NRW(NodeCheckService),
    %% all - full coverage; allup - partial coverage
    VNodeSelector = allup,
    %% Same as R value here, TODO: Make this dynamic
    PrimaryVNodeCoverage = R,
    %% We timeout after 5s
    Timeout = 5000,
    Request = {list, Requirements, Full},
    State = #state{replies = dict:new(), r = R,
                   from = From, reqid = ReqID,
                   reqs = Requirements, raw = Raw},
    {Request, VNodeSelector, NVal, PrimaryVNodeCoverage,
     NodeCheckService, VNodeMaster, Timeout, State}.

process_results({ok, _ReqID, _IdxNode, Obj},
                State = #state{replies = Replies}) ->
    Replies1 = lists:foldl(fun ({Pts, {Key, V}}, D) ->
                                   dict:append(Key, {Pts, V}, D)
                           end, Replies, Obj),
    {done, State#state{replies = Replies1}};

process_results(Result, State) ->
    lager:error("Unknown process results call: ~p ~p", [Result, State]),
    {done, State}.

finish(clean, State = #state{replies = Replies,
                             from = From, r = R}) ->
    MergedReplies = dict:fold(fun(_Key, Es, Res)->
                                      case length(Es) of
                                          _L when _L < R ->
                                              Res;
                                          _ ->
                                              Mgd = case State#state.raw of
                                                        false ->
                                                            merge(Es);
                                                        true ->
                                                            raw_merge(Es)
                                                    end,
                                              [Mgd | Res]
                                      end
                              end, [], Replies),
    From ! {ok, MergedReplies},
    {stop, normal, State};

finish(How, State) ->
    lager:error("Unknown finish results call: ~p ~p", [How, State]),
    {error, failed}.

%%%===================================================================
%%% Internal Functions
%%%===================================================================

mk_reqid() ->
    {MegaSecs,Secs,MicroSecs} = erlang:now(),
    (MegaSecs*1000000 + Secs)*1000000 + MicroSecs.

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


merge([{Score, V} | R]) ->
    merge(R, Score, [V]).

merge([], recalculate, Vs) ->
    {0, merge_obj(Vs)};

merge([], Score, Vs) ->
    {Score, merge_obj(Vs)};

merge([{Score, V} | R], Score, Vs) ->
    merge(R, Score, [V | Vs]);

merge([{_Score1, V} | R], _Score2, Vs) when _Score1 =/= _Score2->
    merge(R, recalculate, [V | Vs]).

merge_obj(Vs) ->
    case ft_obj:merge(sniffle_entity_read_fsm, Vs) of
        O ->
            V = ft_obj:val(O),
            case statebox:is_statebox(V) of
                true ->
                    statebox:value(V);
                false ->
                    V
            end
    end.
