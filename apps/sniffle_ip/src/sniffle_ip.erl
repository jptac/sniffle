%%%-------------------------------------------------------------------
%%% @author Heinz Nikolaus Gies <heinz@licenser.net>
%%% @copyright (C) 2015, Heinz Nikolaus Gies
%%% @doc
%%%
%%% @end
%%% Created :  2 Feb 2015 by Heinz Nikolaus Gies <heinz@licenser.net>
%%%-------------------------------------------------------------------
-module(sniffle_ip).

-behaviour(gen_server).

%% API
-export([start_link/0, claim/1, claim/3]).

-ignore_xref([start_link/0, claim/1, claim/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

-record(state, {}).

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


claim(IPRange) ->
    gen_server:call(?SERVER, {claim, IPRange, nodes()}).

claim(_IPRange, From, []) ->
    gen_server:reply(From, {error, no_claim});

claim(IPRange, From, [This | Nodes]) ->
    case node() of
        This ->
            gen_server:cast(?SERVER, {claim, IPRange, From, Nodes});
        _ ->
            gen_server:reply(From, {error, wrong_node})
    end.

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
handle_call({claim, IPRange, Nodes}, From, State) ->
    handle_cast({claim, IPRange, From, Nodes}, State);

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
handle_cast({claim, IPRange, From, Nodes}, State) ->
    case ft_iprange:free(IPRange) of
        [] ->
            gen_server:reply(From, {error, no_claim});
         Free ->
            case lists:filter(make_is_lock(), Free) of
                [] ->
                    case next_alife_node(Nodes) of
                        no_nodes ->
                            gen_server:reply(From, {error, no_claim});
                        {Next,  Nodes1} ->
                            rpc:cast(Next, sniffle_ip, claim,
                                     [IPRange, From, Nodes1])
                    end;
                [First | _] ->
                    Reply = sniffle_iprange:claim_specific_ip(
                              ft_iprange:uuid(IPRange), First),
                    gen_server:reply(From, Reply)
            end
    end,
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

make_is_lock() ->
    Node = node(),
    {ok, State} = riak_core_ring_manager:get_my_ring(),
    CHash = riak_core_ring:chash(State),
    fun (IP) ->
            <<Hash:160>> = chash:key_of(IP),
            Idx = chash:next_index(Hash, CHash),
            riak_core_ring:index_owner(State, Idx) == Node
    end.

next_alife_node([]) ->
    no_nodes;

next_alife_node([N | Rest]) ->
    case net_adm:ping(N) of
        pong ->
            {N, Rest};
        pang ->
            next_alife_node(Rest)
    end.

