%%%-------------------------------------------------------------------
%%% @Author Heinz N. Gies <>
%%% @copyright (C) 2012, Heinz N. Gies
%%% @doc
%%%
%%% @end
%%% Created :  7 May 2012 by Heinz N. Gies <>
%%%-------------------------------------------------------------------
-module(sniffle_impl_cloudapi).

-include_lib("alog_pt.hrl").

-behavior(sniffle_impl).
%% API

%% gen_server callbacks
-export([init/2, handle_call/4, handle_cast/3,
	 handle_info/2, terminate/2, code_change/3]).

-record(state, {uuid, host, auth}).

init(UUID, {Host, HAuth}) ->
    {ok, #state{uuid=UUID,
		host=Host,
		auth=HAuth}}.

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

handle_call(Auth, {machines, list}, _From,
	    #state{host = Host,
		   uuid=UUID,
		   auth=HAuth} = State) ->
    ?INFO({machines, list, Auth, UUID}, [], [sniffle]),
    Res = case cloudapi:list_machines(Host, HAuth) of
	      {ok, {Ms, _, _}} ->
		  ?DBG({machines, Ms}, [], [sniffle]),
		  sniffle_server:update_machines(UUID, Ms),
		  {ok, Ms};
	      E ->
		  ?WARNING({error, E}, [], [sniffle]),
		  {error, E}
	  end,
    {reply, Res, State};

handle_call(Auth, {machines, get, UUID}, _From, 
	    #state{host = Host,
		   auth=HAuth} = State) ->
    ?DBG({machines, get, Host}, [], [sniffle, sniffle_impl_cloudapi]),
    {reply,
     cloudapi:get_machine(Host, HAuth, ensure_list(UUID)),
     State};

handle_call(Auth, {machines, info, UUID}, _From, 
	    #state{host = Host,
		   auth=HAuth} = State) ->
    ?DBG({machines, info, Host}, [], [sniffle, sniffle_impl_cloudapi]),
    {reply,
     bark:get_machine_info(Host, HAuth, ensure_list(UUID)), State};
	 
handle_call(Auth, {machines, delete, UUID}, _From, 
	    #state{host = Host,
		   auth=HAuth} = State) ->
    ?DBG({machines, delete, Host}, [], [sniffle, sniffle_impl_cloudapi]),
    {reply,
     cloudapi:delete_machine(Host, HAuth, ensure_list(UUID)), State};

handle_call(Auth, {machines, start, UUID}, _From, 
	    #state{host = Host,
		   auth=HAuth} = State) ->
    ?DBG({machines, start, Host}, [], [sniffle, sniffle_impl_cloudapi]),
    {reply,
     cloudapi:start_machine(Host, HAuth, ensure_list(UUID)), State};

handle_call(Auth, {machines, start, UUID, Image}, _From, 
	    #state{host = Host,
		   auth=HAuth} = State) ->
    ?DBG({machines, start, Host}, [], [sniffle, sniffle_impl_cloudapi]),
    {reply,
     bark:start_machine(Host, HAuth, ensure_list(UUID), ensure_list(Image)), State};

handle_call(Auth, {machines, stop, UUID}, _From, 
	    #state{host = Host,
		   auth=HAuth} = State) ->
    ?DBG({machines, stop, Host}, [], [sniffle, sniffle_impl_cloudapi]),
    {reply,
     cloudapi:stop_machine(Host, HAuth, ensure_list(UUID)), State};

handle_call(Auth, {machines, reboot, UUID}, _From, 
	    #state{host = Host,
		   auth=HAuth} = State) ->
    ?DBG({machines, reboot, Host}, [], [sniffle, sniffle_impl_cloudapi]),
    {reply,
     cloudapi:reboot_machine(Host, HAuth, ensure_list(UUID)), State};

handle_call(Auth, {packages, list}, _From, 
	    #state{host=Host,
		   uuid=UUID,
		   auth=HAuth} = State) ->
    ?DBG({packages, list, Host}, [], [sniffle, sniffle_impl_cloudapi]),
    case cloudapi:list_packages(Host, HAuth) of
	{ok, Ps} ->
	    sniffle_server:register_host_resource(
	      UUID, <<"packages">>, 
	      fun (P) ->
		      proplists:get_value(<<"name">>, P)
	      end, Ps),
	    {reply, {ok, Ps}, State};
	E ->
	    {reply,
	     {error, E}, State}
    end;

handle_call(Auth, {datasets, list}, _From, 
	    #state{host = Host,
		   uuid=UUID,
		   auth=HAuth} = State) ->
    ?DBG({packages, list, Host}, [], [sniffle, sniffle_impl_cloudapi]),
    case cloudapi:list_datasets(Host, HAuth) of
	{ok, Ds} ->
	    sniffle_server:register_host_resource(
	      UUID, <<"datasets">>, 
	      fun (E) ->
		      proplists:get_value(<<"id">>, E)
	      end, Ds),
	    {reply, {ok, Ds}, State};
	E ->
	    {reply,
	     {error, E}, State}
    end;

handle_call(Auth, {keys, list}, _From, 
	    #state{host = Host,
		   uuid=UUID,
		   auth=HAuth} = State) ->
    ?DBG({keys, list, UUID, Host}, [], [sniffle, sniffle_impl_cloudapi]),
    case cloudapi:list_keys(Host, HAuth) of
	{ok, [{<<"keys">>, Ks}]} ->
	    {reply, {ok, Ks}, State};
	E ->
	    {reply,
	     {error, E}, State}
    end;

handle_call(Auth, {keys, create, Auth, Pass, KeyID, PublicKey}, _From,
	    #state{host = Host,
		   auth=HAuth} = State) ->
    case cloudapi:create_key(Host, HAuth, ensure_list(Pass), ensure_list(KeyID), PublicKey) of
	{ok, D} ->
	    {reply, {ok, D}, State};
    	E ->
	    ?WARNING({error, E}, [], [sniffle]),
	    {reply, {error, E}, State}
    end;
	
handle_call(Auth, Call, _From, State) ->
    ?ERROR({unspuorrted_handle_call, Call}, [], [sniffle, sniffle_impl_cloudapi]),
    {reply, {error, not_supported}, State}.

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
