%%%-------------------------------------------------------------------
%%% @author Heinz Nikolaus Gies <heinz@licenser.net>
%%% @copyright (C) 2013, Heinz Nikolaus Gies
%%% @doc
%%%
%%% @end
%%% Created : 10 Jan 2013 by Heinz Nikolaus Gies <heinz@licenser.net>
%%%-------------------------------------------------------------------
-module(sniffle_db).

-behaviour(gen_server).

%% API
-export([start/1,
         start_link/1,
         get/3,
         delete/3,
         put/4,
         fold/4]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-ignore_xref([start_link/1]).

-record(state, {db}).

-include("hanoidb.hrl").

%%%===================================================================
%%% API
%%%===================================================================

start(Partition) ->
    case global:whereis_name({db, node(), Partition}) of
        undefined ->
            sniffle_db_sup:start_child(Partition);
        _ ->
            ok
    end.

put(Partition, Bucket, Key, Value) ->
    gen_server:call({global, {db, node(), Partition}}, {put, Bucket, Key, Value}).

get(Partition, Bucket, Key) ->
    gen_server:call({global, {db, node(), Partition}}, {get, Bucket, Key}).

delete(Partition, Bucket, Key) ->
    gen_server:call({global, {db, node(), Partition}}, {delete, Bucket, Key}).

fold(Partition, Bucket, FoldFn, Acc0) ->
    gen_server:call({global, {db, node(), Partition}}, {fold, Bucket, FoldFn, Acc0}).


%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link(Partition) ->
    gen_server:start_link({global, {db, node(), Partition}}, ?MODULE, [Partition], []).

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
init([Partition]) ->
    {ok, DBLoc} = application:get_env(sniffle, db_path),
    {ok, Db} = hanoidb:open(DBLoc ++ "/" ++ integer_to_list(Partition)),
    {ok, #state{db = Db}}.

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
handle_call({put, Bucket, Key, Value}, _From, State) ->
    hanoidb:put(State#state.db, <<Bucket/binary, Key/binary>>, term_to_binary(Value)),
    {reply, ok, State};

handle_call({get, Bucket, Key}, _From, State) ->
    case hanoidb:get(State#state.db, <<Bucket/binary, Key/binary>>) of
        {ok, Bin} ->
            {reply, {ok, binary_to_term(Bin)}, State};
        not_found ->
            {reply, not_found, State}
    end;

handle_call({delete, Bucket, Key}, _From, State) ->
    Res = hanoidb:delete(State#state.db, <<Bucket/binary, Key/binary>>),
    {reply, Res, State};

handle_call({fold, Bucket, FoldFn, Acc0}, _From, State) ->
    Len = byte_size(Bucket),
    L = Len - 1,
    <<Prefix:L/binary, R>> = Bucket,
    R1 = R + 1,
    End = <<Prefix/binary, R1>>,
    Range = #key_range{
      from_key = Bucket,
      from_inclusive = false,
      to_key = End,
      to_inclusive = false
     },
    Rep = hanoidb:fold_range(State#state.db,
                             fun (<<_:Len/binary, Key/binary>>, Value, Acc) ->
                                     FoldFn(Key, binary_to_term(Value), Acc);
                                 (Key, _, Acc) ->
                                     lager:error("[db/~p] Unknown fold key '~p'.", [Bucket, Key]),
                                     Acc
                             end,
                             Acc0,
                             Range),

    {reply, Rep, State};

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
terminate(_Reason, #state{db = Db} = _State) ->
    hanoidb:close(Db),
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
