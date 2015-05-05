%%%-------------------------------------------------------------------
%%% @author Heinz Nikolaus Gies <heinz@licenser.net>
%%% @copyright (C) 2014, Heinz Nikolaus Gies
%%% @doc
%%%
%%% @end
%%% Created : 26 Feb 2014 by Heinz Nikolaus Gies <heinz@licenser.net>
%%%-------------------------------------------------------------------
-module(sniffle_create_pool).

-behaviour(gen_server).

%% API
-export([start_link/0, add/4]).

-ignore_xref([start_link/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

-record(state, {
          workers = [],
          reqs = [],
          rev_reqs = [],
          size = 1
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

add(UUID, Package, Dataset, Config) ->
    gen_server:cast(?MODULE, {add, UUID, Package, Dataset, Config}).

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
    case application:get_env(sniffle, create_pool_size) of
        {ok, S} ->
            {ok, #state{size=S}};
        _ ->
            {ok, #state{}}
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
handle_cast({add, UUID, Package, Dataset, Config},
            State = #state{workers = Ws, size = Size}) when length(Ws) < Size ->
    {ok, Pid} = sniffle_create_fsm:create(UUID, Package, Dataset, Config),
    Ref = erlang:monitor(process, Pid),
    lager:info("[create] Not throttling ~s", [UUID]),
    {noreply, State#state{workers = [Ref | Ws]}};

handle_cast({add, UUID, Package, Dataset, Config},
            State = #state{reqs = Rs}) ->
    lager:info("[create] Throtteling create request for ~s", [UUID]),
    {noreply, State#state{reqs = [{UUID, Package, Dataset, Config} | Rs]}}.


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
handle_info({'DOWN', Ref, process, _Pid, _Reason},
            State = #state{reqs = [], rev_reqs = [], workers = Ws}) ->
    lager:info("[create] Finished task.", []),
    {noreply, State#state{workers=lists:delete(Ref, Ws)}};

handle_info({'DOWN', Ref, process, _Pid, _Reason},
            State = #state{rev_reqs = [{UUID, Package, Dataset, Config} |Rs],
                           workers = Ws}) ->
    case lists:member(Ref, Ws) of
        true ->
            lager:info("[create] Finished task, taking ~s next.", [UUID]),
            {ok, Pid1} = sniffle_create_fsm:create(UUID, Package, Dataset, Config),
            Ref1 = erlang:monitor(process, Pid1),
            Ws1 = lists:delete(Ref, Ws),
            {noreply, State#state{workers=[Ref1 | Ws1], rev_reqs = Rs}};
        false ->
            {noreply, State}
    end;

handle_info({'DOWN', Ref, process, Pid, Reason},
            State = #state{reqs = Rs}) ->
    handle_info({'DOWN', Ref, process, Pid, Reason},
                State#state{reqs = [], rev_reqs = lists:reverse(Rs)}).

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
