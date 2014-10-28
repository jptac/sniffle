%%%-------------------------------------------------------------------
%%% @author Heinz Nikolaus Gies <heinz@licenser.net>
%%% @copyright (C) 2014, Heinz Nikolaus Gies
%%% @doc
%%%
%%% @end
%%% Created : 27 Oct 2014 by Heinz Nikolaus Gies <heinz@licenser.net>
%%%-------------------------------------------------------------------
-module(sniffle_watchdog).

-behaviour(gen_server).

%% API
-export([start_link/0, alerts/0]).
-ignore_xref([start_link/0, alerts/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

-define(TICK, 1000).
-define(FIRST_TICK, (60*1000)).
-define(NODE_LIST_TIME, 120).
-define(PING_CONCURRENCY, 5).
-define(PING_THRESHOLD, 5).

-record(state, {
          ensemble = root,
          tick = ?TICK,
          count = 0,
          hypervisors = [],
          alerts = sets:new(),
          ping_concurrency = ?PING_CONCURRENCY,
          ping_threshold = ?PING_THRESHOLD

         }).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

alerts() ->
    gen_server:call(?SERVER, alerts).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
init([]) ->
    erlang:send_after(?FIRST_TICK, self(), tick),
    {ok, #state{}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @spec handle_call(Request, From, State) ->
%%                                   {reply, Reply, State} |
%%                                   {reply, Reply, State, Timeout} |
%%                                   {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, Reply, State} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_call(alerts, _From,
            State = #state{ensemble = Ensemble, alerts = Alerts}) ->
    case riak_ensemble_manager:get_leader(Ensemble) of
        {_Ensamble, Leader} when Leader == node() ->
            Reply = sets:to_list(Alerts),
            {reply, {ok, Reply}, State};
        _ ->
            {reply, {error, wrong_node}, State}
    end;

handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%                                  {noreply, State, Timeout} |
%%                                  {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_cast(_Msg, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_info(tick, State = #state{ensemble = Ensemble, tick = Tick}) ->
    erlang:send_after(Tick, self(), tick),
    case riak_ensemble_manager:get_leader(Ensemble) of
        {_Ensamble, Leader} when Leader == node() ->
            State1 = run_check(State),
            {noreply, State1};
        _ ->
            %% We only want to run this on the leader.
            {noreply, State#state{count = 0, hypervisors = [], alerts = sets:new()}}
    end;

handle_info(_Info, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

run_check(State = #state{count = 0}) ->
    {ok, HVs} = sniffle_hypervisor:list([], true),
    HVs1 = [{ft_hypervisor:uuid(H), ft_hypervisor:alias(H),
             ft_hypervisor:endpoint(H), 0} || {_, H} <- HVs],
    run_check(State#state{count = ?NODE_LIST_TIME, hypervisors = HVs1});

run_check(State = #state{alerts = Alerts, hypervisors = HVs, count = Cnt,
                         ping_threshold = Threshold}) ->
    Alerts1 = check_riak_core(),
    HVs1 = ping_test(HVs, [], State#state.ping_concurrency),
    Alerts2 = ping_to_alerts(HVs1, Alerts1, Threshold),
    Raised = sets:subtract(Alerts2, Alerts),
    Cleared = sets:subtract(Alerts, Alerts2),
    clear(Cleared),
    raise(Raised),
    State#state{alerts = Alerts2, count = Cnt - 1, hypervisors = HVs1}.

ping_to_alerts(HVs, Alerts, Threshold) ->
    lists:foldl(fun({UUID, Alias, _, _N}, Acc) when _N >= Threshold ->
                        E = {chunter_down, UUID, Alias},
                        sets:add_element(E, Acc);
                   (_, Acc) ->
                        Acc
                end, HVs, Alerts).

check_riak_core() ->
    {Down, Handoffs} = riak_core_status:transfers(),
    Alerts1 = lists:foldl(
                fun({waiting_to_handoff, Node, _}, As) ->
                        sets:add_element({handoff, Node}, As);
                   ({stopped, Node, _}, As) ->
                        sets:add_element({stopped, Node}, As)
                end, sets:new(), Handoffs),
    Down1 = sets:from_list([{down, N} || N <- Down]),
    sets:union(Alerts1, Down1).

ping_test(Hs, HIn, Concurrency) when length(Hs) =< Concurrency ->
    Hs1 = [ping(H) || H <- Hs],
    lists:foldl(fun (H, HSAcc) ->
                        [read_ping(H) | HSAcc]
                end, HIn, Hs1);

ping_test(Hs, HIn, Concurrency) ->
    {T1, HsRest} = lists:split(Concurrency, Hs),
    Hs1 = [ping(H) || H <- T1],
    Hs2 = lists:foldl(fun (H, HSAcc) ->
                              [read_ping(H) | HSAcc]
                      end, HIn, Hs1),
    ping_test(HsRest, Hs2, Concurrency).

ping({UUID, Alias, {Host, Port}, N}) ->
    Ref = make_ref(),
    Self = self(),
    spawn(fun() ->
                  case libchunter:ping(Host, Port) of
                      pong ->
                          Self ! {Ref, ok};
                      _ ->
                          Self ! {Ref, fail}
                  end
          end),
    {Ref, {UUID, Alias, {Host, Port}, N}}.

read_ping({Ref, {UUID, Alias, {Host, Port}, N}}) ->
    receive
        {Ref, ok} ->
            {UUID, Alias, {Host, Port}, 0};
        {Ref, fail} ->
            {UUID, Alias, {Host, Port}, N+1}
    after
        500 ->
            {UUID, Alias, {Host, Port}, N+1}
    end.

clear(Cleared) ->
    [libwatchdog:clear(T, A) ||
        {T, A, _S} <- [to_msg(E) || E <- sets:to_list(Cleared)]].

raise(Raised) ->
    [libwatchdog:raise(T, A, S) ||
        {T, A, S} <- [to_msg(E) || E <- sets:to_list(Raised)]].

to_msg({handoff, Node}) ->
    {sniffle_handoff, <<"Pending handoff: ", (a2b(Node))/binary>>, 1};

to_msg({stopped, Node}) ->
    {sniffle_stopped, <<"Stopped node: ", (a2b(Node))/binary>>, 5};

to_msg({down, Node}) ->
    {sniffle_down, <<"Node down: ", (a2b(Node))/binary>>, 10};

to_msg({chunter_down, UUID, Alias}) ->
    {chunter_down, <<"Chunter node ", Alias/binary,
                     "(", UUID/binary, ") down.">>, 10}.

a2b(A) ->
    list_to_binary(atom_to_list(A)).
