%%%-------------------------------------------------------------------
%%% @Author Heinz N. Gies <heinz@licenser.net>
%%% @copyright (C) 2012, Heinz N. Gies
%%% @doc
%%%
%%% @end
%%% Created :  7 May 2012 by Heinz N. Gies <heinz@licenser.net>
%%%-------------------------------------------------------------------
-module(sniffle_impl_chunter).

-include_lib("alog_pt.hrl").

-behavior(sniffle_impl).
%% API

%% gen_server callbacks
-export([init/2, handle_call/4, handle_cast/3,
	 handle_info/2, terminate/2, code_change/3]).

-record(state, {uuid, host}).

init(UUID, Host) ->
    {ok, #state{uuid=UUID,
		host=Host}}.

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

handle_call(Auth, {machines, list}, _From, #state{host=Host, uuid=HUUID} = State) ->
    ?INFO({machines, list, Auth}, [], [sniffle]),
    Res = case libchunter:list_machines(Host, Auth) of
	      {ok, Ms} ->
		  ?DBG({machines, Ms}, [], [sniffle]),
		  Ms1 = [ make_frontend_json(M) || M <- Ms],
		  sniffle_server:update_machines(HUUID, Ms1),
		  {ok, Ms1};
	      E ->
		  ?WARNING({error, E}, [], [sniffle]),
		  {error, E}
	  end,
    {reply, Res, State};

handle_call(Auth, {machines, get, UUID}, _From, #state{host=Host} = State) ->
    ?DBG({machines, get, Host}, [], [sniffle, sniffle_impl_cloudapi]),
    {ok, M} = libchunter:get_machine(Host, Auth, UUID),
    {reply,
     {ok, make_frontend_json(M)},
     State};

handle_call(Auth, {machines, info, UUID}, _From, #state{host=Host} = State) ->
    ?DBG({machines, info, Host}, [], [sniffle, sniffle_impl_chunter]),
    {reply,
     libchunter:get_machine_info(Host, Auth, UUID), State};
	 
handle_call(Auth, {machines, delete, UUID}, _From, #state{host=Host} = State) ->
    ?DBG({machines, delete, Host}, [], [sniffle, sniffle_impl_chunter]),
    {reply,
     libchunter:delete_machine(Host, Auth, UUID), State};

handle_call(Auth, {machines, start, UUID}, _From, #state{host=Host} = State) ->
    ?DBG({machines, start, Host}, [], [sniffle, sniffle_impl_chunter]),
    {reply,
     libchunter:start_machine(Host, Auth, UUID), State};

handle_call(Auth, {machines, start, UUID, Image}, _From, #state{host=Host} = State) ->
    ?DBG({machines, start, Host}, [], [sniffle, sniffle_impl_chunter]),
    {reply,
     libchunter:start_machine(Host, Auth, UUID, Image), State};

handle_call(Auth, {machines, stop, UUID}, _From, #state{host=Host} = State) ->
    ?DBG({machines, stop, Host}, [], [sniffle, sniffle_impl_chunter]),
    {reply,
     libchunter:stop_machine(Host, Auth, UUID), State};

handle_call(Auth, {machines, reboot, UUID}, _From, #state{host=Host} = State) ->
    ?DBG({machines, reboot, Host}, [], [sniffle, sniffle_impl_chunter]),
    {reply,
     libchunter:reboot_machine(Host, Auth, UUID), State};

handle_call(Auth, {machines, create, Name, PackageUUID, DatasetUUID, Metadata, Tags},
	    _From, #state{host = Host,
			  uuid= HUUID} = State) ->
    ?DBG({machines, create, Host, Name, PackageUUID, DatasetUUID, Metadata, Tags}, [], [sniffle, sniffle_impl_chunter]),
    Res = case libchunter:create_machine(Host, Auth, Name, PackageUUID, DatasetUUID, Metadata, Tags) of
	      {ok, JSON} ->
		  sniffle_server:update_machines(HUUID, [JSON]),
		  {ok, make_frontend_json(JSON)};
	      E ->
		  E
	  end,
    {reply, Res, State};

handle_call(Auth, {packages, list}, _From, #state{host=Host, uuid=HUUID} = State) ->
    ?DBG({packages, list, Host}, [], [sniffle, sniffle_impl_chunter]),
    case libchunter:list_packages(Host, Auth) of
	{ok, Ps} ->
	    sniffle_server:register_host_resource(
	      HUUID, <<"packages">>, 
	      fun (P) ->
		      proplists:get_value(name, P)
	      end, Ps),
	    {reply, {ok, Ps}, State};
	E ->
	    {reply,
	     {error, E}, State}
    end;

handle_call(Auth, {datasets, list}, _From, #state{host = Host, uuid=HUUID} = State) ->
    ?DBG({packages, list, Host}, [], [sniffle, sniffle_impl_chunter]),
    case libchunter:list_datasets(Host, Auth) of
	{ok, Ds} ->
	    sniffle_server:register_host_resource(
	      HUUID, <<"datasets">>, 
	      fun (E) ->
		      proplists:get_value(id, E)
	      end, Ds),
	    {reply, {ok, Ds}, State};
	E ->
	    {reply,
	     {error, E}, State}
    end; 


handle_call(Auth, {keys, list}, _From, #state{host = Host} = State) ->
    ?DBG({keys, list, Host}, [], [sniffle, sniffle_impl_chunter]),
    case libchunter:list_keys(Host, Auth) of
	{ok, [{<<"keys">>, Ks}]} ->
	    {reply, {ok, Ks}, State};
	E ->
	    {reply,
	     {error, E}, State}
    end;

handle_call(_Auth, Call, _From, State) ->
    ?ERROR({unspuorrted_handle_call, Call}, [], [sniffle, sniffle_impl_chunter]),
    {reply, {error, not_supported}, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @spec handle_cast(Msg, #state{host=Host} = State) -> {noreply, State} |
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
%% @spec terminate(Reason, #state{host=Host} = State) -> void()
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



make_frontend_json([{nics, Ns} | R]) ->
    IPs = lists:map(fun (N) ->
			    proplists:get_value(ip, N)
		    end,Ns),
    [{ips, IPs}|make_frontend_json(R)];

make_frontend_json([{zonename, N} | R]) ->
    Rest = make_frontend_json(R),
    case proplists:get_value(name, Rest) of
	undefined ->
	    [{name, N}|make_frontend_json(R)];
	_ ->
	    Rest
    end;
make_frontend_json([{max_physical_memory, N} | R]) ->
    Rest = make_frontend_json(R),
    case proplists:get_value(memory, Rest) of
	undefined ->
	    [{memory, N/(1024*1024)}|make_frontend_json(R)];
	_ ->
	    Rest
    end;
make_frontend_json([{state, <<"installed">>} | R]) ->
    [{state, <<"stopped">>}|make_frontend_json(R)];
make_frontend_json([{alias, N} | R]) ->
    [{name, N}|make_frontend_json(R)];
make_frontend_json([{ram, R} | R]) ->
    [{memory, R}|make_frontend_json(R)];
make_frontend_json([]) ->
    [];
make_frontend_json([{K, V}|R]) ->
    [{K, V}|make_frontend_json(R)].

	
