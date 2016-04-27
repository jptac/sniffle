%% @doc The coordinator for stat get operations.  The key here is to
%% generate the preflist just like in wrtie_fsm and then query each
%% replica and wait until a quorum is met.
-module(sniffle_entity_read_fsm).
-behavior(gen_fsm).
-include("sniffle_read.hrl").

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
          req_id,
          from,
          entity,
          op,
          r,
          n,
          preflist,
          num_r=0,
          size,
          timeout = 10000,
          val,
          cmd,
          system,
          replies=[]}).

-type state() :: #state{}.

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

start_link(ReqID, {Cmd, System}, Op, From, Entity, Val) ->
    gen_fsm:start_link(?MODULE,
                       [ReqID, {Cmd, System}, Op, From, Entity, Val], []).

start(CmdInfo, Op) ->
    start(CmdInfo, Op, undefined).

start(CmdInfo, Op, User) ->
    start(CmdInfo, Op, User, undefined).

-spec start(CmdInfo::term(), Op::atom(), Entity::term() | undefined,
            Val::term() | undefined) ->
                   ok | not_found | {ok, Res::term()} | {error, timeout}.
start(CmdInfo, Op, Entity, Val) ->
    ReqID = sniffle_vnode:mk_reqid(),
    sniffle_entity_read_fsm_sup:start_read_fsm(
      [ReqID, CmdInfo, Op, self(), Entity, Val]
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
-spec init(_) -> {ok, prepare, state(), 0}.

init([ReqId, {Cmd, System}, Op, From]) ->
    init([ReqId, {Cmd, System}, Op, From, undefined, undefined]);

init([ReqId, {Cmd, System}, Op, From, Entity]) ->
    init([ReqId, {Cmd, System}, Op, From, Entity, undefined]);

init([ReqId, {Cmd, System}, Op, From, Entity, Val]) ->
    {ok, N} = application:get_env(sniffle, n),
    {ok, R} = application:get_env(sniffle, r),
    SD = #state{
            req_id=ReqId,
            r=R,
            n=N,
            from=From,
            op=Op,
            val=Val,
            cmd=Cmd,
            system=System,
            entity=Entity},
    {ok, prepare, SD, 0}.

%% @doc Calculate the Preflist.
prepare(timeout, SD0=#state{entity=Entity,
                            n=N,
                            system=System}) ->
    Bucket = list_to_binary(atom_to_list(System)),
    DocIdx = riak_core_util:chash_key({Bucket, Entity}),
    Prelist = riak_core_apl:get_apl(DocIdx, N, sniffle),
    SD = SD0#state{preflist=Prelist},
    {next_state, execute, SD, 0}.

%% @doc Execute the get reqs.
execute(timeout, SD0=#state{req_id=ReqId,
                            entity=Entity,
                            op=Op,
                            val=Val,
                            cmd=Cmd,
                            preflist=Prelist}) ->
    case Entity of
        undefined ->
            Cmd:Op(Prelist, ReqId);
        _ ->
            case Val of
                undefined ->
                    Cmd:Op(Prelist, ReqId, Entity);
                _ ->
                    Cmd:Op(Prelist, ReqId, Entity, Val)
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
    SD = SD0#state{num_r=NumR, replies=Replies},
    case NumR of
        R ->
            case merge(Replies) of
                not_found ->
                    From ! {ReqID, not_found};
                Merged ->
                    Reply = ft_obj:val(Merged),
                    From ! {ReqID, ok, Reply}
            end,
            case NumR of
                N ->
                    {next_state, finalize, SD, 0};
                _ ->
                    {next_state, wait_for_n, SD, Timeout}
            end;
        _ ->
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
                        cmd=Cmd,
                        replies=Replies,
                        entity=Entity}) ->
    MObj = merge(Replies),
    case needs_repair(MObj, Replies) of
        true ->
            lager:warning("[~p] performing read repair on '~p'.",
                          [SD#state.cmd, Entity]),
            repair(Cmd, Entity, MObj, Replies),
            {stop, normal, SD};
        false ->
            {stop, normal, SD}
    end.

handle_info(_Info, _StateName, StateData) ->
    {stop, badmsg, StateData}.

handle_event(_Event, _StateName, StateData) ->
    {stop, badmsg, StateData}.

handle_sync_event(_Event, _From, _StateName, StateData) ->
    {stop, badmsg, StateData}.

code_change(_OldVsn, StateName, State, _Extra) -> {ok, StateName, State}.

terminate(_Reason, _SN, _SD) ->
    ok.

%%%===================================================================
%%% Internal Functions
%%%===================================================================

%%
%% @doc Given a list of `Replies' return the merged value.
-spec merge([vnode_reply()]) -> fifo:obj() | not_found.
merge(Replies) ->
    Objs = [Obj || {_, Obj} <- Replies],
    ft_obj:merge(sniffle_entity_read_fsm, Objs).

%%
%% @doc Reconcile conflicts among conflicting values.
-spec reconcile([A]) -> A.

reconcile([V | Vs]) ->
    reconcile(fifo_dt:type(V), Vs, V).

reconcile(M, [H | R], Acc) when M /= undefined ->
    reconcile(M, R, M:merge(Acc, H));
reconcile(_M, _, Acc) ->
    Acc.

%%
%% @doc Given the merged object `MObj' and a list of `Replies'
%% determine if repair is needed.
-spec needs_repair(any(), [vnode_reply()]) -> boolean().
needs_repair(MObj, Replies) ->
    Objs = [Obj || {_, Obj} <- Replies],
    lists:any(different(MObj), Objs).

different(A) -> fun(B) -> not ft_obj:equal(A, B) end.

%%
%% @doc Repair any cmds that do not have the correct object.
-spec repair(atom(), string(), fifo:obj(), [vnode_reply()]) -> io.
repair(_, _, _, []) -> io;

repair(Cmd, StatName, MObj, [{IdxNode, Obj}|T]) ->
    case ft_obj:equal(MObj, Obj) of
        true ->
            repair(Cmd, StatName, MObj, T);
        false ->
            case Obj of
                not_found ->
                    Cmd:repair(IdxNode, StatName, not_found, MObj);
                _ ->
                    Cmd:repair(IdxNode, StatName, ft_obj:vclock(Obj), MObj)
            end,
            repair(Cmd, StatName, MObj, T)
    end.

%%
%% @doc Given a list return the set of unique values.
-spec unique([A::any()]) -> [A::any()].
unique(L) ->
    sets:to_list(sets:from_list(L)).

%%stat_name(sniffle_dtrace_cmd) ->
%%    "dtrace";
%%stat_name(sniffle_vm_cmd) ->
%%    "vm";
%%stat_name(sniffle_hypervisor_cmd) ->
%%    "hypervisor";
%%stat_name(sniffle_package_cmd) ->
%%    "package";
%%stat_name(sniffle_dataset_cmd) ->
%%    "dataset";
%%stat_name(sniffle_network_cmd) ->
%%    "network";
%%stat_name(sniffle_iprange_cmd) ->
%%    "iprange".
