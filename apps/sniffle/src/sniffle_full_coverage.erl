-module(sniffle_full_coverage).

-include("sniffle.hrl").

-behaviour(riak_core_coverage_fsm).

-export([
         init/2,
         process_results/2,
         finish/2,
         start/3
        ]).

-record(state, {replies, r, reqid, from, reqs}).


start(VNodeMaster, NodeCheckService, Request = {list, Requirements, true}) ->
    ReqID = mk_reqid(),
    sniffle_coverage_sup:start_coverage(
      ?MODULE, {self(), ReqID, Requirements},
      {VNodeMaster, NodeCheckService, Request}),
    receive
        ok ->
            ok;
        {ok, Result} ->
            {ok, Result}
            %%;
            %%Else ->
            %%lager:error("Unknown coverage reply: ~p", [Else]),
            %%{error, unknown_reply}
    after 10000 ->
            {error, timeout}
    end.

%% The first is the vnode service used
init({From, ReqID, Requirements}, {VNodeMaster, NodeCheckService, Request}) ->
    {NVal, R, _W} = ?NRW(NodeCheckService),
    %% all - full coverage; allup - partial coverage
    VNodeSelector = allup,
    %% Same as R value here, TODO: Make this dynamic
    PrimaryVNodeCoverage = R,
    %% We timeout after 5s
    Timeout = 5000,
    State = #state{replies = dict:new(), r = R,
                   from = From, reqid = ReqID,
                   reqs = Requirements},
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
                                              Mgd = merge(Es),
                                              [Mgd | Res]
                                      end
                              end, [], Replies),
    %%    statman_histogram:record_value(
    %%      {list_to_binary(stat_name(SD0#state.vnode) ++ "/list"), total},
    %%      SD0#state.start),
    From ! {ok, MergedReplies},
    {stop, normal, State};

finish(How, State) ->
    lager:error("Unknown process results call: ~p ~p", [How, State]),
    {error, failed}.

%%%===================================================================
%%% Internal Functions
%%%===================================================================

mk_reqid() ->
    {MegaSecs,Secs,MicroSecs} = erlang:now(),
    (MegaSecs*1000000 + Secs)*1000000 + MicroSecs.


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
    case sniffle_obj:merge(sniffle_entity_read_fsm, Vs) of
        #sniffle_obj{val = V} ->
            statebox:value(V)
    end.
