-module(sniffle_coverage).

-behaviour(riak_core_coverage_fsm).

-include("sniffle.hrl").

-export([
         init/2,
         process_results/2,
         finish/2,
         start/3,
         mk_reqid/0
        ]).

-record(state, {replies, r, reqid, from}).

start(VNodeMaster, NodeCheckService, Request) ->
    ReqID = mk_reqid(),
    sniffle_coverage_sup:start_coverage(
      ?MODULE, {self(), ReqID, something_else},
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
init({From, ReqID, _}, {VNodeMaster, NodeCheckService, Request}) ->
    {NVal, R, _W} = ?NRW(NodeCheckService),
    %% all - full coverage; allup - partial coverage
    VNodeSelector = allup,
    PrimaryVNodeCoverage = R,
    %% We timeout after 5s
    Timeout = 5000,
    State = #state{replies = dict:new(), r = R,
                   from = From, reqid = ReqID},
    {Request, VNodeSelector, NVal, PrimaryVNodeCoverage,
     NodeCheckService, VNodeMaster, Timeout, State}.

process_results({ok, _ReqID, _IdxNode, Obj},
                State = #state{replies = Replies}) ->
    Replies1 = lists:foldl(fun (Key, D) ->
                                  dict:update_counter(Key, 1, D)
                          end, Replies, Obj),
    {done, State#state{replies = Replies1}};

process_results({ok, _ReqID, _IdxNode}, State) ->
    {done, State};

process_results(Result, State) ->
    lager:error("Unknown process results call: ~p ~p", [Result, State]),
    {done, State}.

finish(clean, State = #state{replies = Replies,
                             from = From, r = R}) ->
    MergedReplies = dict:fold(fun(_Key, Count, Keys) when Count < R->
                                      Keys;
                                 (Key, _Count, Keys) ->
                                      [Key | Keys]
                              end, [], Replies),
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
