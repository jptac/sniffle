-module(sniffle_cloud_status).

-behaviour(riak_core_coverage_fsm).

-include("sniffle.hrl").

-export([
         init/2,
         process_results/2,
         finish/2,
         start/0
        ]).

-record(state, {resources, r, reqid, from, warnings = []}).

start() ->
    ReqID = sniffle_coverage:mk_reqid(),
    sniffle_coverage_sup:start_coverage(
      ?MODULE, {self(), ReqID, something_else},
      {sniffle_hypervisor_vnode_master, sniffle_hypervisor, status}),
    receive
        ok ->
            ok;
        {ok, Result} ->
            {ok, Result};
        Else ->
            lager:error("Unknown coverage reply: ~p", [Else]),
            {error, unknown_reply}
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
    State = #state{resources = orddict:new(),
                   from = From, reqid = ReqID,
                   r = R},
    {Request, VNodeSelector, NVal, PrimaryVNodeCoverage,
     NodeCheckService, VNodeMaster, Timeout, State}.

process_results({ok, _ReqID, _IdxNode, {Res, Warnings}},
                State = #state{resources = Resources,
                               warnings = Warnings0}) ->
    Resources1 = lists:foldl(fun({H, ResIn}, Acc) ->
                                     orddict:append(H, ResIn, Acc)
                             end, Resources, Res),
    {done, State#state{resources = Resources1,
                       warnings = ordsets:union(Warnings0, Warnings)}};

process_results(Result, State) ->
    lager:error("Unknown process results call: ~p ~p", [Result, State]),
    {done, State}.

finish(clean, State = #state{resources = Resources,
                             warnings = Warnings,
                             r = R,
                             from = From}) ->
    Resources1 =
        orddict:fold(
          fun (H, [Res | Resr], Acc) when length(Resr) >= (R-1) ->
                  Res1 = merge_numbers(Resr, Res),
                  Acc1 = jsxd:merge(fun(_K, V1, V2) when
                                              is_list(V1),
                                              is_list(V2) ->
                                            V1 ++ V2;
                                       (_K, V1, V2) when
                                              is_number(V1),
                                              is_number(V2) ->
                                            V1 + V2
                                    end, Acc, Res1),
                  jsxd:update(<<"hypervisors">>,
                              fun(Current)->
                                      ordsets:add_element(H, Current)
                              end, [H], Acc1);
              (_,_, Acc) ->
                  Acc
          end, [], Resources),
    From ! {ok, {Resources1, Warnings}},
    {stop, normal, State};

finish(How, State) ->
    lager:error("Unknown process results call: ~p ~p", [How, State]),
    {error, failed}.

%%%===================================================================
%%% Internal Functions
%%%===================================================================

merge_numbers([], Acc) ->
    Acc;
merge_numbers([R | Rr], Acc) ->
    Acc1 = jsxd:merge(fun(_K, V1, V2) when is_list(V1), is_list(V2) ->
                              ordsets:union(ordsets:from_list(V1),
                                            ordsets:from_list(V2));
                         (_K, V1, _V2) when is_number(V1), is_number(_V2),
                                            V1 > _V2 ->
                              V1;
                         (_K, _V1, V2) when is_number(_V1), is_number(V2) ->
                              V2
                      end, R, Acc),
    merge_numbers(Rr, Acc1).
