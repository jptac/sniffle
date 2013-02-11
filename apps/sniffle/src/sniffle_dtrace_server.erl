%%%-------------------------------------------------------------------
%%% @author Heinz Nikolaus Gies <heinz@licenser.net>
%%% @copyright (C) 2013, Heinz Nikolaus Gies
%%% @doc
%%%
%%% @end
%%% Created : 10 Jan 2013 by Heinz Nikolaus Gies <heinz@licenser.net>
%%%-------------------------------------------------------------------
-module(sniffle_dtrace_server).

-behaviour(gen_server).

%% API
-export([
         run/3,
         start_link/1
        ]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-ignore_xref([start_link/1]).

-record(state, {data = [],
                servers,
                runners,
                listeners = []}).

-include("hanoidb.hrl").

%%%===================================================================
%%% API
%%%===================================================================

run(ID, Servers, Listener) ->
    case global:whereis_name({dtrace, ID}) of
        undefined ->
            sniffle_dtrace_sup:start_child([ID, Servers, Listener]);
        Pid ->
            gen_server:cast(Pid, {listen, Listener}),
            {ok, Pid}
    end.


%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link([ID, Servers, Listener]) ->
    gen_server:start_link({global, {dtrace, ID}}, ?MODULE, [ID, Servers, Listener], []).

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
init([ID, Servers, Listener]) ->
    {ok, ScriptObj} = sniffle_dtrace:get(ID),
    Script = case jsxd:get(<<"script">>, ScriptObj) of
                 {ok, S} when is_binary(S) ->
                     binary_to_list(S);
                 {ok, S} when is_list(S) ->
                     S
             end,
    Servers1 = [{binary_to_list(jsxd:get(<<"host">>, <<"">>, S1)),
                 binary_to_list(jsxd:get(<<"port">>, 4200, S1))}
                || {ok, S1} <- [sniffle_hypervisor:get(S0) || S0 <- Servers]],
    Runners = [libchunter_dtrace_server:dtrace(Host, Port, Script)
               || {Host, Port} <- Servers1],
    erlang:monitor(process, Listener),
    {ok, #state{runners = Runners, servers = Servers1, listeners = [Listener]}}.

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

handle_cast({listen, Listener}, State) ->
    erlang:monitor(process, Listener),
    {noreply, State};

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
handle_info({'DOWN', _Ref, process, _Pid, _Reason}, State = #state{ listeners = []}) ->
    {stop, normal, State};

handle_info({'DOWN', _Ref, process, Pid, _Reason}, State = #state{ listeners = [Pid]}) ->
    {stop, normal, State};

handle_info({'DOWN', _Ref, process, Pid, _Reason}, State = #state{ listeners = Ls}) ->
    {stop, normal, State#state{listeners = lists:delete(Pid, Ls)}};

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
terminate(_Reason, #state{servers = Servers} = _State) ->
    [ libchunter_dtrace_server:close(S) || S <- Servers],
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
