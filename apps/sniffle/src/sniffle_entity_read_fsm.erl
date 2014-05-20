%% @doc The coordinator for stat get operations.  The key here is to
%% generate the preflist just like in wrtie_fsm and then query each
%% replica and wait until a quorum is met.
-module(sniffle_entity_read_fsm).
-behavior(gen_fsm).
-include("sniffle.hrl").

%% API
-export([start_link/6, start/2, start/3, start/4]).

-export([reconcile/1, different/1, needs_repair/2, repair/4, unique/1]).

%% Callbacks
-export([init/1, code_change/4, handle_event/3, handle_info/3,
         handle_sync_event/4, terminate/3]).

%% States
-export([prepare/2, execute/2, waiting/2, wait_for_n/2, finalize/2]).

-record(state,
        {
          start,
          req_id,
          from,
          entity,
          op,
          r,
          n,
          preflist,
          num_r=0,
          size,
          timeout=?DEFAULT_TIMEOUT,
          val,
          vnode,
          system,
          replies=[]}).

-ignore_xref([
              code_change/4,
              different/1,
              execute/2,
              finalize/2,
              handle_event/3,
              handle_info/3,
              handle_sync_event/4,
              init/1,
              needs_repair/2,
              prepare/2,
              reconcile/1,
              repair/4,
              start/2,
              start/4,
              start_link/6,
              terminate/3,
              unique/1,
              wait_for_n/2,
              waiting/2
             ]).

%%%===================================================================
%%% API
%%%===================================================================

start_link(ReqID, {VNode, System}, Op, From, Entity, Val) ->
    gen_fsm:start_link(?MODULE, [ReqID, {VNode, System}, Op, From, Entity, Val], []).

start(VNodeInfo, Op) ->
    start(VNodeInfo, Op, undefined).

start(VNodeInfo, Op, User) ->
    start(VNodeInfo, Op, User, undefined).

-spec start(VNodeInfo::term(), Op::atom(), Entity::term() | undefined, Val::term() | undefined) ->
                   ok | not_found | {ok, Res::term()} | {error, timeout}.
start(VNodeInfo, Op, Entity, Val) ->
    ReqID = mk_reqid(),
    sniffle_entity_read_fsm_sup:start_read_fsm(
      [ReqID, VNodeInfo, Op, self(), Entity, Val]
     ),
    receive
        {ReqID, ok} ->
            ok;
        {ReqID, not_found} ->
            not_found;
        {ReqID, ok, Result} ->
            {ok, Result}
    after ?DEFAULT_TIMEOUT ->
            lager:error("[read] timeout"),
            {error, timeout}
    end.

%%%===================================================================
%%% States
%%%===================================================================

%% Intiailize state data.
init([ReqId, {VNode, System}, Op, From]) ->
    init([ReqId, {VNode, System}, Op, From, undefined, undefined]);

init([ReqId, {VNode, System}, Op, From, Entity]) ->
    init([ReqId, {VNode, System}, Op, From, Entity, undefined]);

init([ReqId, {VNode, System}, Op, From, Entity, Val]) ->
    {N, R, _W} = ?NRW(System),
    SD = #state{
            start = now(),
            req_id=ReqId,
            r=R,
            n=N,
            from=From,
            op=Op,
            val=Val,
            vnode=VNode,
            system=System,
            entity=Entity},
    {ok, prepare, SD, 0}.

%% @doc Calculate the Preflist.
prepare(timeout, SD0=#state{entity=Entity,
                            n=N,
                            system=System}) ->
    Bucket = list_to_binary(atom_to_list(System)),
    DocIdx = riak_core_util:chash_key({Bucket, Entity}),
    Prelist = riak_core_apl:get_apl(DocIdx, N, System),
    SD = SD0#state{preflist=Prelist},
    {next_state, execute, SD, 0}.

%% @doc Execute the get reqs.
execute(timeout, SD0=#state{req_id=ReqId,
                            entity=Entity,
                            op=Op,
                            val=Val,
                            vnode=VNode,
                            preflist=Prelist}) ->
    case Entity of
        undefined ->
            VNode:Op(Prelist, ReqId);
        _ ->
            case Val of
                undefined ->
                    VNode:Op(Prelist, ReqId, Entity);
                _ ->
                    VNode:Op(Prelist, ReqId, Entity, Val)
            end
    end,
    {next_state, waiting, SD0}.

%% @doc Wait for R replies and then respond to From (original client
%% that called `get/2').
%% TODO: read repair...or another blog post?

waiting({ok, ReqID, IdxNode, Obj},
        SD0=#state{from=From, num_r=NumR0, replies=Replies0,
                   r=R, n=N, timeout=Timeout}) ->
    NumR = NumR0 + 1,
    Replies = [{IdxNode, Obj}|Replies0],
    SD = SD0#state{num_r=NumR,replies=Replies},
    if
        NumR =:= R ->
            case merge(Replies) of
                not_found ->
                    From ! {ReqID, not_found};
                Merged ->
                    Reply = sniffle_obj:val(Merged),
                    case statebox:is_statebox(Reply) of
                        true ->
                            From ! {ReqID, ok, statebox:value(Reply)};
                        false  ->
                            From ! {ReqID, ok, Reply}
                    end
            end,
            if
                NumR =:= N ->
                    {next_state, finalize, SD, 0};
                true ->
                    {next_state, wait_for_n, SD, Timeout}
            end;
        true ->
            {next_state, waiting, SD}
    end.

wait_for_n({ok, _ReqID, IdxNode, Obj},
           SD0=#state{n=N, num_r=NumR, replies=Replies0}) when NumR == N-1 ->
    Replies = [{IdxNode, Obj}|Replies0],
    {next_state, finalize, SD0#state{num_r=N, replies=Replies}, 0};

wait_for_n({ok, _ReqID, IdxNode, Obj},
           SD0=#state{num_r=NumR0, replies=Replies0, timeout=Timeout}) ->
    NumR = NumR0 + 1,
    Replies = [{IdxNode, Obj}|Replies0],
    {next_state, wait_for_n, SD0#state{num_r=NumR, replies=Replies}, Timeout};

%% TODO partial repair?
wait_for_n(timeout, SD) ->
    {stop, timeout, SD}.

finalize(timeout, SD=#state{
                        vnode=VNode,
                        replies=Replies,
                        entity=Entity}) ->
    MObj = merge(Replies),
    case needs_repair(MObj, Replies) of
        true ->
            lager:warning("[~p] performing read repair on '~p'.",
                          [SD#state.vnode, Entity]),
            repair(VNode, Entity, MObj, Replies),
            {stop, normal, SD};
        false ->
            {stop, normal, SD}
    end.

handle_info(_Info, _StateName, StateData) ->
    {stop,badmsg,StateData}.

handle_event(_Event, _StateName, StateData) ->
    {stop,badmsg,StateData}.

handle_sync_event(_Event, _From, _StateName, StateData) ->
    {stop,badmsg,StateData}.

code_change(_OldVsn, StateName, State, _Extra) -> {ok, StateName, State}.

terminate(_Reason, _SN, _SD) ->
    ok.

%%%===================================================================
%%% Internal Functions
%%%===================================================================

%% @pure
%%
%% @doc Given a list of `Replies' return the merged value.
-spec merge([vnode_reply()]) -> sniffle_obj() | not_found.
merge(Replies) ->
    Objs = [Obj || {_,Obj} <- Replies],
    sniffle_obj:merge(sniffle_entity_read_fsm, Objs).

%% @pure
%%
%% @doc Reconcile conflicts among conflicting values.
-spec reconcile([A :: statebox:statebox()]) -> A :: statebox:statebox().

reconcile([V = #?HYPERVISOR{} | Vs]) ->
    reconcile_hypervisor(Vs, V);

reconcile([V = #?GROUPING{} | Vs]) ->
    reconcile_grouping(Vs, V);

reconcile([V0 | _ ] = Vals) ->
    case statebox:is_statebox(V0) of
        true ->
            statebox:merge(Vals);
        false ->
            V0
    end.

reconcile_hypervisor([H | R], Acc) ->
    reconcile_hypervisor(R, sniffle_hypervisor_state:merge(Acc, H));
reconcile_hypervisor(_, Acc) ->
    Acc.

reconcile_grouping([H | R], Acc) ->
    reconcile_grouping(R, sniffle_grouping_state:merge(Acc, H));
reconcile_grouping(_, Acc) ->
    Acc.

%% @pure
%%
%% @doc Given the merged object `MObj' and a list of `Replies'
%% determine if repair is needed.
-spec needs_repair(any(), [vnode_reply()]) -> boolean().
needs_repair(MObj, Replies) ->
    Objs = [Obj || {_,Obj} <- Replies],
    lists:any(different(MObj), Objs).

%% @pure
different(A) -> fun(B) -> not sniffle_obj:equal(A,B) end.

%% @impure
%%
%% @doc Repair any vnodes that do not have the correct object.
-spec repair(atom(), string(), sniffle_obj(), [vnode_reply()]) -> io.
repair(_, _, _, []) -> io;

repair(VNode, StatName, MObj, [{IdxNode,Obj}|T]) ->
    case sniffle_obj:equal(MObj, Obj) of
        true ->
            repair(VNode, StatName, MObj, T);
        false ->
            case Obj of
                not_found ->
                    VNode:repair(IdxNode, StatName, not_found, MObj);
                _ ->
                    VNode:repair(IdxNode, StatName, Obj#sniffle_obj.vclock, MObj)
            end,
            repair(VNode, StatName, MObj, T)
    end.

%% pure
%%
%% @doc Given a list return the set of unique values.
-spec unique([A::any()]) -> [A::any()].
unique(L) ->
    sets:to_list(sets:from_list(L)).

mk_reqid() ->
    erlang:phash2(erlang:now()).

%%stat_name(sniffle_dtrace_vnode) ->
%%    "dtrace";
%%stat_name(sniffle_vm_vnode) ->
%%    "vm";
%%stat_name(sniffle_hypervisor_vnode) ->
%%    "hypervisor";
%%stat_name(sniffle_package_vnode) ->
%%    "package";
%%stat_name(sniffle_dataset_vnode) ->
%%    "dataset";
%%stat_name(sniffle_img_vnode) ->
%%    "img";
%%stat_name(sniffle_network_vnode) ->
%%    "network";
%%stat_name(sniffle_iprange_vnode) ->
%%    "iprange".
