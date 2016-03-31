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
-export([start_link/0, status/0]).
-ignore_xref([start_link/0, status/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

-define(TICK, 10000).
-define(FIRST_TICK, (60*1000)).
-define(NODE_LIST_TIME, 120).
-define(PING_CONCURRENCY, 5).
-define(PING_THRESHOLD, 5).
-define(HV_UPDATE_BUCKETS, 10).
-define(ENSEMBLE, root).

-record(state, {
          tick = ?TICK,
          alerts = sets:new()
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

status() ->
    gen_server:call(?SERVER, status).

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
handle_call(status, _From,
            State = #state{alerts = Alerts}) ->
    Reply = sets:to_list(Alerts),
    {reply, {ok, {Reply, []}}, State};

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
handle_info(tick, State = #state{tick = Tick}) ->
    S1 = run_check(State),
    erlang:send_after(Tick, self(), tick),
    {noreply, S1};

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


run_check(State = #state{}) ->
    Alerts1 = check_riak_core(),
    %% We only grab pool stats when we run a new check so we don't need
    %% to rerun that when we already claculated it once.
    State#state{alerts = Alerts1}.


check_riak_core() ->
    {Down, Handoffs} = riak_core_status:transfers(),
    lists:foldl(
      fun({waiting_to_handoff, Node, _}, As) ->
              sets:add_element({handoff, Node}, As);
         ({stopped, Node, _}, As) ->
              sets:add_element({stopped, Node}, As)
      end, sets:from_list([{down, N} || N <- Down]), Handoffs).
