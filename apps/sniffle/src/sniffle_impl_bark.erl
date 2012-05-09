%%%-------------------------------------------------------------------
%%% @Author Heinz N. Gies <>
%%% @copyright (C) 2012, Heinz N. Gies
%%% @doc
%%%
%%% @end
%%% Created :  7 May 2012 by Heinz N. Gies <>
%%%-------------------------------------------------------------------
-module(sniffle_impl_bark).

-include_lib("alog_pt.hrl").

-behavior(sniffle_impl).
%% API

%% gen_server callbacks
-export([init/2, handle_call/4, handle_cast/3,
	 handle_info/2, terminate/2, code_change/3]).


-record(state, {uuid, host, auth}).

init(UUID, {Host, Auth}) ->
    {ok, #state{uuid=UUID,
		host=Host,
		auth=Auth}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @spec handle_call(Auth, Request, From, State) ->
%%                                   {reply, Reply, State} |
%%                                   {reply, Reply, State, Timeout} |
%%                                   {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, Reply, State} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_call(Auth, {machines, info, Auth, UUID}, _From, 
	    #state{host = Host,
		   auth=HAuth} = State) ->
    ?DBG({machines, get, Host}, [], [sniffle, sniffle_impl_bark]),
    {reply,
     bark:get_machine_info(Host, HAuth, ensure_list(UUID)), State};

handle_call(Auth, {machines, start, UUID, Image}, _From, 
	    #state{host = Host,
		   auth=HAuth} = State) ->
    ?DBG({machines, start, Host}, [], [sniffle, sniffle_impl_bark]),
    {reply,
     bark:start_machine(Host, HAuth, ensure_list(UUID), ensure_list(Image)), State};

handle_call(Auth, {images, list}, _From, 
	    #state{host = Host,
		   uuid=UUID,
		   auth=HAuth} = State) ->
    ?DBG({images, list, Host}, [], [sniffle, sniffle_impl_bark]),
    case bark:list_images(Host, HAuth) of
	{ok, Is} ->
	    sniffle_server:register_host_resource(
	      UUID, <<"datasets">>, 
	      fun (E) ->
		      proplists:get_value(<<"id">>, E)
	      end, Is),
	    {reply, {ok, Is}, State};
	E ->
	    {reply,
	     {error, E}, State}
    end;


handle_call(Auth, Call, From, State) ->
    sniffle_impl_cloudapi:handle_call(Auth, Call, From, State).

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
handle_cast(_Auth, _Msg, State) ->
    {noreply, State}.

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

ensure_list(B) when is_binary(B) ->
    binary_to_list(B);
ensure_list(L) when is_list(L) ->
    L.
