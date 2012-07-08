%%%-------------------------------------------------------------------
%%% @author Heinz N. Gies <heinz@licenser.net>
%%% @copyright (C) 2012, Heinz N. Gies
%%% @doc
%%%
%%% @end
%%% Created :  7 May 2012 by Heinz N. Gies <heinz@licenser.net>
%%%-------------------------------------------------------------------
-module(sniffle_host_srv).

-behaviour(gen_server).

%% API
-export([start_link/3, kill/1,
	 scall/3, call/4]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
	 terminate/2, code_change/3]).

-define(SERVER, ?MODULE). 

-record(state, {
	  dispatcher_state = undefined,
	  uuid,
	  dispatcher,
	  host}).

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
start_link(Dispatcher, UUID, Host) ->
    gen_server:start_link(?MODULE,  [Dispatcher, UUID, Host], []).

call(PID, From, Auth, Args) ->
    gen_server:cast(PID, {call, From, Auth, Args}).

scall(PID, Auth, Args) ->
    gen_server:call(PID, {call, Auth, Args}).


kill(PID) ->
    gen_server:cast(PID, kill).

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
init([Dispatcher, UUID, Host]) ->
    ok = backyard_srv:register_connect_handler(backyard_connect),
    case Dispatcher:init(UUID, Host) of
	{ok, State} ->
	    {ok, #state{
	       dispatcher_state = State,
	       uuid = UUID,
	       dispatcher = Dispatcher,
	       host = Host}, 500};
	{stop, Reason} ->
	    {stop, Reason}
    end.

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
    
handle_call({call, Auth, Args}, _From,
	    #state{dispatcher_state = DState,
		   dispatcher = Dispatcher} = State) ->
    case Dispatcher:handle_call(Auth, Args, self(), DState) of
	{reply, Reply, DState1} ->
	    {reply, Reply, State#state{dispatcher_state = DState1}};
	{stop, Reason, Reply, DState1} ->
	    {stop, Reason, Reply, State#state{dispatcher_state = DState1}};
	{stop, Reason, DState1} ->
	    {stop, Reason, State#state{dispatcher_state = DState1}}
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
handle_cast(backyard_connect,  #state{uuid = UUID} = State) ->
    reregister_int(UUID),
    {noreply, State};

handle_cast({call, From, Auth, Args},
	    #state{dispatcher_state = DState,
		   dispatcher = Dispatcher} = State) ->
    case Dispatcher:handle_call(Auth, Args, From, DState) of
	{reply, Reply, DState1} ->
	    gen_server:reply(From, Reply),
	    {noreply, State#state{dispatcher_state = DState1}};
	{stop, _Reason, Reply, DState1} ->
	    gen_server:reply(From, Reply),
	    {stop, Reply, State#state{dispatcher_state = DState1}};
	{stop, Reason, DState1} ->
	    {stop, Reason, State#state{dispatcher_state = DState1}}
    end;

handle_cast({cast, Auth, Args},
	    #state{dispatcher_state = DState,
		   dispatcher = Dispatcher} = State) ->
    case Dispatcher:cast(Auth, Args, DState) of
	{noreply, DState1} ->
	    {noreply, State#state{dispatcher_state = DState1}};
	{stop, Reason, DState1} ->
	    {stop, Reason, State#state{dispatcher_state = DState1}}
    end;

handle_cast(kill, State) ->
    {stop, shutdown, State};

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
handle_info({'DOWN', _Ref, process, _Pid2, Reason}, #state{uuid = UUID} = State) ->
    sniffle_server:remove_host(UUID),
    {noreply, State};    
%{stop, Reason, State};

handle_info(timeout, #state{uuid = UUID} = State) ->
    reregister_int(UUID),
    {noreply, State};

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
terminate(Reason, #state{dispatcher_state = DState,
			  dispatcher = Dispatcher}) ->
    backyard_srv:unregister_handler(),
    Dispatcher:terminate(Reason, DState).

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
reregister_int(UUID) ->
    try
	gproc:reg({p, l, {sniffle, host}}, UUID),
	gproc:reg({n, l, {host, UUID}}, self()),
	ok
    catch
	_:_ ->
	    ok
    end.
