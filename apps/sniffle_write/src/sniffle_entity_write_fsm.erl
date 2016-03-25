%% @doc The coordinator for stat write opeartions.  This example will
%% show how to properly replicate your data in Riak Core by making use
%% of the _preflist_.
-module(sniffle_entity_write_fsm).
-behavior(gen_fsm).
-include("sniffle_write.hrl").

%% API
-export([start_link/5, start_link/6, write/3, write/4]).

%% Callbacks
-export([init/1, code_change/4, handle_event/3, handle_info/3,
         handle_sync_event/4, terminate/3]).

%% States
-export([prepare/2, execute/2, waiting/2]).

%% req_id: The request id so the caller can verify the response.
%%
%% from: The pid of the sender so a reply can be made.
%%
%% entity: The entity.
%%
%% op: The stat op, one of [add, delete, grant, revoke].
%%
%% val: Additional arguments passed.
%%
%% prelist: The preflist for the given {Client, StatName} pair.
%%
%% num_w: The number of successful write replies.
-record(state, {req_id :: pos_integer(),
                from :: pid(),
                entity :: string(),
                op :: atom(),
                n,
                w,
                cmd,
                system,
                cordinator :: node(),
                val = undefined :: term() | undefined,
                preflist :: riak_core_apl:preflist(),
                num_w = 0 :: non_neg_integer()}).

-type state() :: #state{}.

-ignore_xref([
              code_change/4,
              execute/2,
              handle_event/3,
              handle_info/3,
              handle_sync_event/4,
              init/1,
              prepare/2,
              start_link/5,
              start_link/6,
              terminate/3,
              waiting/2
             ]).

%%%===================================================================
%%% API
%%%===================================================================

start_link({Cmd, System}, ReqID, From, Entity, Op) ->
    start_link({Cmd, System}, ReqID, From, Entity, Op, undefined).

start_link({Cmd, System}, ReqID, From, Entity, Op, Vals) ->
    gen_fsm:start_link(?MODULE,
                       [{Cmd, System}, ReqID, From, Entity, Op, Vals], []).

write({Cmd, System}, User, Op) ->
    write({Cmd, System}, User, Op, undefined).

write({Cmd, System}, User, Op, Vals) ->
    ReqID = sniffle_vnode:mk_reqid(),
    sniffle_entity_write_fsm_sup:start_write_fsm(
      [{Cmd, System}, ReqID, self(), User, Op, Vals]),
    receive
        {ReqID, ok} ->
            ok;
        {ReqID, ok, not_found} ->
            not_found;
        {ReqID, ok, Result} ->
            {ok, Result};
        {ReqID, error, Result} ->
            {error, Result}
    after ?DEFAULT_TIMEOUT ->
            lager:error("[~p:write(~p)] timeout on ~p", [System, ReqID, Op]),
            {error, timeout}
    end.

%%%===================================================================
%%% States
%%%===================================================================

%% @doc Initialize the state data.
-spec init(_) -> {ok, prepare, state(), 0}.
init([{Cmd, System}, ReqID, From, Entity, Op, Vals]) ->
    {ok, N} = application:get_env(sniffle, n),
    {ok, W} = application:get_env(sniffle, w),

    SD = #state{req_id=ReqID,
                from=From,
                w=W,
                n=N,
                entity=Entity,
                op=Op,
                cmd=Cmd,
                system=System,
                cordinator=node(),
                val=Vals},
    {ok, prepare, SD, 0}.

%% @doc Prepare the write by calculating the _preference list_.
prepare(timeout, SD0=#state{
                   entity=Entity,
                   system=System,
                   n=N
                  }) ->
    Bucket = list_to_binary(atom_to_list(System)),
    DocIdx = riak_core_util:chash_key({Bucket, Entity}),
    Preflist = riak_core_apl:get_apl(DocIdx, N, sniffle),
    SD = SD0#state{preflist=Preflist},
    {next_state, execute, SD, 0}.

%% @doc Execute the write request and then go into waiting state to
%% verify it has meets consistency requirements.
execute(timeout, SD0=#state{req_id=ReqID,
                            entity=Entity,
                            op=Op,
                            val=Vals,
                            cmd=Cmd,
                            cordinator=Cordinator,
                            preflist=Preflist}) ->
    case Vals of
        undefined ->
            Cmd:Op(Preflist, {ReqID, Cordinator}, Entity);
        _ ->
            Cmd:Op(Preflist, {ReqID, Cordinator}, Entity, Vals)
    end,
    {next_state, waiting, SD0}.

%% @doc Wait for W write reqs to respond.
waiting({ok, ReqID}, SD0=#state{from=From, num_w=NumW0, req_id=ReqID, w=W}) ->
    NumW = NumW0 + 1,
    SD = SD0#state{num_w=NumW},
    case NumW of
        W ->
            From ! {ReqID, ok},
            {stop, normal, SD};
        _ -> {next_state, waiting, SD}
    end;

waiting({error, ReqID, Reply},
        SD=#state{from=From,
                  req_id=ReqID}) ->
    From ! {ReqID, error, Reply},
    {stop, normal, SD};

waiting({ok, ReqID, Reply}, SD0=#state{from=From, num_w=NumW0, req_id=ReqID,
                                       w=W}) ->
    NumW = NumW0 + 1,
    SD = SD0#state{num_w=NumW},
    case NumW of
        W ->
            From ! {ReqID, ok, Reply},
            {stop, normal, SD};
        _ ->
            {next_state, waiting, SD}
    end.

handle_info(_Info, _StateName, StateData) ->
    {stop, badmsg, StateData}.

handle_event(_Event, _StateName, StateData) ->
    {stop, badmsg, StateData}.

handle_sync_event(_Event, _From, _StateName, StateData) ->
    {stop, badmsg, StateData}.

code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

terminate(_Reason, _SN, _SD) ->
    ok.

%%%===================================================================
%%% Internal Functions
%%%===================================================================
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
