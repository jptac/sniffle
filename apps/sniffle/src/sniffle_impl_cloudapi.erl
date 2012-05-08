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

%% API

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2,
	 terminate/2, code_change/3]).

-record(state, {uuid, host, auth}).

init([UUID, {Host, Auth}]) ->
    {ok, #state{uuid=UUID,
		host=Host,
		auth=Auth}}.

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
handle_call({machines, get, [UUID]}, _From, 
	    #state{host = Host,
		   auth = Auth} = State) ->
    ?DBG({machines, get, Host}, [], [sniffle, sniffle_impl_cloudapi]),
    {reply,
     cloudapi:get_machine(Host, Auth, ensure_list(UUID)),
     State};

handle_call({machines, info, [UUID]}, _From, 
	    #state{host = Host,
		   auth = Auth} = State) ->
    ?DBG({machines, get, Host}, [], [sniffle, sniffle_impl_cloudapi]),
    {reply,
     bark:get_machine_info(Host, Auth, ensure_list(UUID)), State};
	 
handle_call({machines, delete, [UUID]}, _From, 
	    #state{host = Host,
		   auth = Auth} = State) ->
    ?DBG({machines, delete, Host}, [], [sniffle, sniffle_impl_cloudapi]),
    {reply,
     cloudapi:delete_machine(Host, Auth, ensure_list(UUID)), State};

handle_call({machines, start, [UUID]}, _From, 
	    #state{host = Host,
		   auth = Auth} = State) ->
    ?DBG({machines, start, Host}, [], [sniffle, sniffle_impl_cloudapi]),
    {reply,
     cloudapi:start_machine(Host, Auth, ensure_list(UUID)), State};

handle_call({machines, start, [UUID, Image]}, _From, 
	    #state{host = Host,
		   auth = Auth} = State) ->
    ?DBG({machines, start, Host}, [], [sniffle, sniffle_impl_cloudapi]),
    {reply,
     bark:start_machine(Host, Auth, ensure_list(UUID), ensure_list(Image)), State};

handle_call({machines, stop, [UUID]}, _From, 
	    #state{host = Host,
		   auth = Auth} = State) ->
    ?DBG({machines, stop, Host}, [], [sniffle, sniffle_impl_cloudapi]),
    {reply,
     cloudapi:stop_machine(Host, Auth, ensure_list(UUID)), State};

handle_call({machines, reboot, [UUID]}, _From, 
	    #state{host = Host,
		   auth = Auth} = State) ->
    ?DBG({machines, reboot, Host}, [], [sniffle, sniffle_impl_cloudapi]),
    {reply,
     cloudapi:reboot_machine(Host, Auth, ensure_list(UUID)), State};

handle_call({packages, list}, _From, 
	    #state{host=Host,
		   uuid=UUID,
		   auth=Auth} = State) ->
    ?DBG({packages, list, Host}, [], [sniffle, sniffle_impl_cloudapi]),
    case cloudapi:list_packages(Host, Auth) of
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

handle_call({datasets, list}, _From, 
	    #state{host = Host,
		   uuid=UUID,
		   auth = Auth} = State) ->
    ?DBG({packages, list, Host}, [], [sniffle, sniffle_impl_cloudapi]),
    case cloudapi:list_datasets(Host, Auth) of
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

handle_call({keys, list}, _From, 
	    #state{host = Host,
		   uuid=UUID,
		   auth = Auth} = State) ->
    ?DBG({packages, list, Host}, [], [sniffle, sniffle_impl_cloudapi]),
    case cloudapi:list_keys(Host, Auth) of
	{ok, [{<<"keys">>, Ks}]} ->
	    {reply, {ok, Ks}, State};
	E ->
	    {reply,
	     {error, E}, State}
    end;

handle_call(Call, _From, State) ->
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
handle_cast(_Msg, State) ->
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
