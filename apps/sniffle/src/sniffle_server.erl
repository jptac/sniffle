%%%-------------------------------------------------------------------
%%% @author Heinz N. Gies <heinz@licenser.net>
%%% @copyright (C) 2012, Heinz N. Gies
%%% @doc
%%%
%%% @end
%%% Created :  5 May 2012 by Heinz N. Gies <heinz@licenser.net>
%%%-------------------------------------------------------------------
-module(sniffle_server).

-behaviour(gen_server).

%% API
-export([start_link/0,
	 reregister/0,
	 update_machines/2,
	 register_host_resource/4,
	 remove_host/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
	 terminate/2, code_change/3]).

-define(SERVER, ?MODULE). 

-record(state, {api_hosts=[],
		handlers=[]}).

-define(HOST_ACTION(Category, Action),
	handle_call({call, Auth, {Category, Action, UUID}}, From, #state{api_hosts=Hosts} = State) ->
	       lager:info([{fifi_component, sniffle}, {user, Auth}],
			  "~p:~p - UUID: ~s.",
			  [Category, Action, UUID]),
	       lager:debug([{fifi_component, sniffle}, {user, Auth}],
			   "~p:~p - From: ~p Hosts: ~p.", [Category, Action, From, Hosts]),
	       case get_machine_host(Auth, UUID, Hosts) of
		   {error, E} ->
		       lager:warning([{fifi_component, chunter}],
				     "~p:~p - Host not found: ~s.", [Category, Action, UUID]),
		       {reply, {error, E}, State};
		   {ok, Host} ->
		       try
			   Pid = gproc:lookup_pid({n, l, {host, Host}}),
			   case sniffle_host_srv:scall(Pid, Auth, {Category, Action, UUID}) of
			       {error, cant_call} = E1->
				   remove_host(Host),
				   {reply, {error, E1}, State};
			       Res ->
				   {reply, Res, State}
			   end
		       catch
			   T:E ->
			       lager:error([{fifi_component, sniffle}, {user, Auth}],
					   "~p:~p - Error: ~p:~p.",
					   [Category, Action, T,E]),
			       remove_host(Host),
			       {reply, {error, {host_down, E}}, State}
		       end
	       end).
-define(HOST_ACTION1(Category, Action),
	handle_call({call, Auth, {Category, Action, UUID, O1}}, From, #state{api_hosts=Hosts} = State) ->
	       lager:info([{fifi_component, sniffle}, {user, Auth}],
			  "~p:~p - UUID: ~s, O1: ~p.",
			  [Category, Action, UUID, O1]),
	       lager:debug([{fifi_component, sniffle}, {user, Auth}],
			   "~p:~p - From: ~p Hosts: ~p.", [Category, Action, From, Hosts]),
	       case get_machine_host(Auth, UUID, Hosts) of
		   {error, E} ->
		       lager:warning([{fifi_component, chunter}],
				     "~p:~p - Host not found: ~s.", [Category, Action, UUID]),
		       {reply, {error, E}, State};
		   {ok, Host} ->
		       try
			   Pid = gproc:lookup_pid({n, l, {host, Host}}),

			   case sniffle_host_srv:scall(Pid, Auth, {Category, Action, UUID, O1}) of
			       {error, cant_call} = E1 ->
				   remove_host(Host),
				   {reply, {error,  E1}, State};
			       Res ->
				   {reply, Res, State}
			   end
		       catch
			   T:E ->
			       lager:error([{fifi_component, sniffle}, {user, Auth}],
					   "~p:~p - Error: ~p:~p.",
					   [Category, Action, T,E]),
			       remove_host(Host),
			       {reply, {error, {host_down, E}}, State}
		       end
	       end).
-define(HOST_ACTION2(Category, Action),
	handle_call({call, Auth, {Category, Action, UUID, O1, O2}}, From, #state{api_hosts=Hosts} = State) ->
	       lager:info([{fifi_component, sniffle}, {user, Auth}],
			  "~p:~p - UUID: ~s, O1: ~p, O2: ~p.",
			  [Category, Action, UUID, O1, O2]),
	       lager:debug([{fifi_component, sniffle}, {user, Auth}],
			   "~p:~p - From: ~p Hosts: ~p.", [Category, Action, From, Hosts]),
	       case get_machine_host(Auth, UUID, Hosts) of
		   {error, E} ->
		       lager:warning([{fifi_component, chunter}],
				     "~p:~p - Host not found: ~s.", [Category, Action, UUID]),
		       {reply, {error, E}, State};
		   {ok, Host} ->
		       try
			   Pid = gproc:lookup_pid({n, l, {host, Host}}),
			   case sniffle_host_srv:scall(Pid, Auth, {Category, Action, UUID, O1, O2}) of
			       {error, cant_call} = E1 ->
				   remove_host(Host),
				   {reply, {error, E1}, State};
			       Res ->
				   {reply, Res, State}
			   end
		       catch
			   T:E ->
			       lager:error([{fifi_component, sniffle}, {user, Auth}],
					  "~p:~p - Error: ~p:~p.",
					  [Category, Action, T,E]),
			       remove_host(Host),
			       {reply, {error, {host_down, E}}, State}
		       end
	       end).

-define(LIST(Category),
	handle_call({call, Auth, {Category, list}}, From, #state{api_hosts=Hosts} = State) ->
	       lager:info([{fifi_component, sniffle}, {user, Auth}],
			  "~p:list.",
			  [Category]),
	       lager:debug([{fifi_component, sniffle}, {user, Auth}],
			   "~p:lust - From: ~p Hosts: ~p.", [Category, From, Hosts]),
	       Res = lists:foldl(fun (Host, List) ->
					 try
					     Pid = gproc:lookup_pid({n, l, {host, Host}}),
					     case sniffle_host_srv:scall(Pid, Auth, {Category, list}) of
						 {ok, HostRes} ->
						     List ++ HostRes;
						 {error, cant_call} ->
						     remove_host(Host),
						     List;
						 _ ->
						     List
					     end
					 catch
					     T:E ->
						 lager:error([{fifi_component, sniffle}, {user, Auth}],
							     "~p:list - Error: ~p:~p.",
							     [Category, T,E]),
						 remove_host(Host),
						 {reply, {error, {host_down, E}}, State}
					 end
				 end, [], Hosts),
	       {reply, {ok, Res}, State}).

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

reregister() ->
    gen_server:cast(?SERVER, reregister).

update_machines(Host, Ms) ->
    gen_server:cast(?SERVER, {update_machines, Host, Ms}).

register_host_resource(Host, ResourceName, IDFn, Resouces) ->
    gen_server:cast(?SERVER, {register_host_resource, Host, ResourceName, IDFn, Resouces}).
remove_host(Host) ->
    gen_server:cast(?SERVER, {remove_host, Host}).
    
    
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
-define(IMPL_PROVIDERS, [{{cloudapi, sdc}, sniffle_impl_cloudapi},
			 {chunter, sniffle_impl_chunter},
			 {{cloudapi, bark}, sniffle_impl_bark}]).
init([]) ->
    ok = backyard_srv:register_connect_handler(backyard_connect),
    Hosts = get_env_default(api_hosts, []),
    Providers = get_env_default(providers, ?IMPL_PROVIDERS),
    HostUUIDs = lists:map(fun ({Type, Spec}) ->
				      UUID = list_to_binary(uuid:to_string(uuid:uuid4())),
				      Provider=proplists:get_value(Type, Providers),
				      sniffle_host_sup:start_child(Provider, UUID, Spec),
				      UUID
			      end, Hosts),
    lager:info([{fifi_component, sniffle}],
	       "sniffle:init.",
	       []),
    lager:debug([{fifi_component, sniffle}],
	       "sniffle:init - Hosts: ~p.",
	       [Hosts]),
    {ok, #state{api_hosts=HostUUIDs}, 1000}.


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

?HOST_ACTION(machines, get);
?HOST_ACTION(machines, info);
?HOST_ACTION(machines, delete);
?HOST_ACTION(machines, start);
?HOST_ACTION1(machines, start);
?HOST_ACTION(machines, stop);
?HOST_ACTION(machines, reboot);

?LIST(machines);
?LIST(datasets);
?LIST(images);
?LIST(keys);

handle_call({call, Auth, {hosts, list}}, From, #state{api_hosts=Hosts} = State) ->
    lager:info([{fifi_component, sniffle}, {user, Auth}],
	       "hosts:list.",
	       []),
    lager:debug([{fifi_component, sniffle}, {user, Auth}],
		"hosts:list - From: ~p Hosts: ~p.", [From, Hosts]),
    {ok, AuthC} = libsnarl:user_cache(system, Auth),
    case libsnarl:allowed(system, AuthC, [host, list]) of
	false ->
	    {reply, {error, permission_denied}, State};
	true ->
	    Hosts1 =[UUID || UUID <- Hosts,
			     libsnarl:allowed(system, AuthC, [host, UUID, get]) == true],
	    {reply, {ok, Hosts1}, State}
    end;
		   
handle_call({call, Auth, {packages, list}}, From, #state{api_hosts=Hosts} = State) ->
    lager:info([{fifi_component, sniffle}, {user, Auth}],
	       "packages:list.",
	       []),
    lager:debug([{fifi_component, sniffle}, {user, Auth}],
		"packages:list - From: ~p Hosts: ~p.", [From, Hosts]),
    {ok, GlobalPackageNames} = libsnarl:option_list(system, <<"packages">>),
    GlobalPackages = 
	[P1 || {ok, P1 }<- [libsnarl:option_get(system, <<"packages">>, P) || P <- GlobalPackageNames]],
    Res = lists:foldl(fun (Host, List) ->
			      try
				  Pid = gproc:lookup_pid({n, l, {host, Host}}),
				  case sniffle_host_srv:scall(Pid, Auth, {packages, list}) of
				      {ok, HostRes} ->
					  List ++ HostRes;
				      {error, cant_call} ->
					  remove_host(Host),
					  List;
				      _ ->
					  List
				  end
			      catch
				  T:E ->
				      lager:error([{fifi_component, sniffle}, {user, Auth}],
						  "packages:list - Error: ~p:~p.",
						  [T,E]),
				      remove_host(Host),
				      {reply, {error, {host_down, E}}, State}
			      end
		      end, GlobalPackages, Hosts),
    {reply, {ok, Res}, State};

handle_call({call, Auth, {packages, create, Name, Disk, Memory, Swap}}, From, State) ->
    lager:info([{fifi_component, sniffle}, {user, Auth}],
	       "packages:create - Name: ~s, Disk: ~p, Memory: ~p, Swap: ~p.",
	       [Name, Disk, Memory, Swap]),
    lager:debug([{fifi_component, sniffle}, {user, Auth}],
		"packages:create - From: ~p.", [From]),
    case libsnarl:allowed(system, Auth, [package, Name, set]) of
	false ->
	    msg(Auth, <<"error">>, <<"Permission denied!">>),
	    {reply, {error, unauthorized}, State};
	true ->
	    Pkg = [{name, Name}, 
		   {disk, Disk}, 
		   {memory, Memory}, 
		   {swap, Swap}],
	    libsnarl:option_set(system, <<"packages">>, Name, Pkg),
	    {reply, {ok, Pkg}, State}
    end;

handle_call({call, Auth, {machines, create, Name, PackageUUID, DatasetUUID, Metadata, Tags}}, From, 
	    #state{api_hosts=Hosts} = State) ->
    lager:info([{fifi_component, sniffle}, {user, Auth}],
	       "machines:create - Name: ~s, Package, ~s, Dataset: ~s.",
	       [Name, PackageUUID, DatasetUUID]),
    lager:debug([{fifi_component, sniffle}, {user, Auth}],
		"machines:create - From: ~p Hosts: ~p.", [From, Hosts]),
    case pick_host(Hosts) of 
	{ok, Host} ->
	    lager:info([{fifi_component, sniffle}, {user, Auth}],
		       "machines:create - Autopicked Host: ~p.",
		       [Host]),
	    Pid = gproc:lookup_pid({n, l, {host, Host}}),
	    sniffle_host_srv:call(Pid, From, Auth, {machines, create, Name, PackageUUID, DatasetUUID, Metadata, Tags}),
	    {reply, ok, State};
	{error, E} ->
	    lager:debug([{fifi_component, sniffle}, {user, Auth}],
		"machines:create - pick host error: ~p.", [E]),
	    msg(Auth, <<"error">>, <<"No suitable deployment host found!">>),
	    {reply, {error, E}, State}
    end;

handle_call({call, Auth, {machines, create, Host, Name, PackageUUID, DatasetUUID, Metadata, Tags}}, From, 
	    #state{api_hosts=Hosts} = State) ->
    lager:info([{fifi_component, sniffle}, {user, Auth}],
	       "machines:create - Host: ~s, Name: ~s, Package, ~s, Dataset: ~s.",
	       [Host, Name, PackageUUID, DatasetUUID]),
    lager:debug([{fifi_component, sniffle}, {user, Auth}],
		"machines:create - From: ~p Hosts: ~p.", [From, Hosts]),
    Pid = gproc:lookup_pid({n, l, {host, Host}}),
    sniffle_host_srv:call(Pid, From, Auth, {machines, create, Name, PackageUUID, DatasetUUID, Metadata, Tags}),
    {noreply, State};

handle_call({call, Auth, {packages, delete, Name}}, From, State) ->
    lager:info([{fifi_component, sniffle}, {user, Auth}],
	       "packages:delete - Name: ~s.",
	       [Name]),
    lager:debug([{fifi_component, sniffle}, {user, Auth}],
		"packages:delete - From: ~p.", [From]),
    case libsnarl:allowed(system, Auth, [package, Name, delete]) of
	false ->
	    msg(Auth, <<"error">>, <<"Permission denied!">>),
	    {reply, {error, unauthorized}, State};
	true ->
	    libsnarl:option_delete(system, <<"packages">>, Name),
	    {reply, ok, State}
    end;

handle_call({call, Auth, {keys, create, Pass, KeyID, PublicKey}}, From, #state{api_hosts=Hosts} = State) ->
    lager:info([{fifi_component, sniffle}, {user, Auth}],
	       "keys:create - KeyID: ~s.",
	       [KeyID]),
    lager:debug([{fifi_component, sniffle}, {user, Auth}],
		"keys:create - PublicKey: ~s, From: ~p Hosts: ~p.", [PublicKey, From, Hosts]),
    Res = lists:foldl(
	    fun (Host, Res) ->
		    Pid =gproc:lookup_pid({n, l, {host, Host}}),
		    case sniffle_host_srv:scall(Pid, Auth, {keys, create, Pass, KeyID, PublicKey}) of
			{ok, D} ->
			    case Res of 
				{error, _} ->
				    Res;
				_ ->
				    {ok, D}
			    end;
			{error, cant_call} = E->
			    remove_host(Host),
			    case Res of
				{error, Es} ->
				    {error, [{Host, E}|Es]};
				_ ->
				    {error, [{Host, E}]}
			    end;
			E ->
			    case Res of
				{error, Es} ->
				    {error, [{Host, E}|Es]};
				_ ->
				    {error, [{Host, E}]}
			    end
		    end
	    end, ok, Hosts),
    {reply, {ok, Res}, State};

handle_call({call, Auth, info}, From,  #state{api_hosts=Hosts} = State) ->
    lager:info([{fifi_component, sniffle}, {user, Auth}],
	       "sniffle:info.",
	       []),
    lager:debug([{fifi_component, sniffle}, {user, Auth}],
		"sniffle:info - From: ~p Hosts: ~p.", [From, Hosts]),
    case libsnarl:allowed(system, Auth, [service, sniffle, info]) of
	false ->
	    {reply, {error, unauthorized}, State};
	true ->
	    {reply, [{<<"version">>, <<"0.1.0">>},
		     {<<"hosts">>, length(Hosts)}], State}
    end;

handle_call({call, Auth, ping}, From, State) ->
    lager:info([{fifi_component, sniffle}, {user, Auth}],
	       "sniffle:ping.",
	       []),
    lager:debug([{fifi_component, sniffle}, {user, Auth}],
		"sniffle:ping - From: ~p.", [From]),
    case libsnarl:allowed(system, Auth, [service, sniffle, info]) of
	false ->
	    {reply, {error, unauthorized}, State};
	true ->
	    {reply, pong, State}
    end;



handle_call(_Request, _From, State) ->
    Reply = {error, unknown},
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

handle_cast({cast, Auth, {register, Type, Spec}}, #state{api_hosts=HostUUIDs} = State) ->
    lager:info([{fifi_component, sniffle}, {user, Auth}],
	       "sniffle:register - Type: ~p, Spec: ~p.",
	       [Type, Spec]),
    lager:debug([{fifi_component, sniffle}, {user, Auth}],
		"sniffle:register - Hosts: ~p.", [HostUUIDs]),
    
    case libsnarl:allowed(system, Auth, [service, sniffle, host, add, Type]) of
	false ->
	    {noreply, State};
	true ->
	    Providers = get_env_default(providers, ?IMPL_PROVIDERS),
	    UUID = list_to_binary(uuid:to_string(uuid:uuid4())),
	    Provider=proplists:get_value(Type, Providers),
	    sniffle_host_sup:start_child(Provider, UUID, Spec),
	    {noreply, State#state{api_hosts=[UUID|lists:delete(UUID, HostUUIDs)]}}
    end;

handle_cast({cast, Auth, {register, Type, UUID, Spec}}, #state{api_hosts=HostUUIDs} = State) ->
    lager:info([{fifi_component, sniffle}, {user, Auth}],
	       "sniffle:register - UUID: ~s, Type: ~p, Spec: ~p.",
	       [UUID, Type, Spec]),
    lager:debug([{fifi_component, sniffle}, {user, Auth}],
		"sniffle:register - Hosts: ~p.", [HostUUIDs]),

    case libsnarl:allowed(system, Auth, [service, sniffle, host, add, Type]) of
	false ->
	    {noreply, State};
	true ->
	    try
		Pid = gproc:lookup_pid({n, l, {host, UUID}}),
		sniffle_host_srv:kill(Pid)
	    catch
		_:_ ->
		    ok
	    end,
	    Providers = get_env_default(providers, ?IMPL_PROVIDERS),
	    Provider= proplists:get_value(Type, Providers),
	    sniffle_host_sup:start_child(Provider, UUID, Spec),
	    {noreply, State#state{api_hosts=[UUID|lists:delete(UUID,HostUUIDs)]}}
    end;
	
handle_cast({update_machines, Host, Ms}, State) ->
    lager:info([{fifi_component, sniffle}],
	       "sniffle:update_machines - Host: ~s.",
	       [Host]),
    lists:map(fun (M) ->
		      register_machine(Host, M)
	      end, Ms),
    {noreply, State};

handle_cast({register_host_resource, Host, ResourceName, IDFn, Resouces}, State) ->
    Name = <<"sniffle:", ResourceName/binary, ":", Host/binary>>,
    redo:cmd([<<"DEL">>, Name]),
    lists:map(fun (P) ->
		      ID = IDFn(P),
		      redo:cmd([<<"SADD">>, Name, ID])
	      end, Resouces),
    redo:cmd([<<"TTL">>, Name, 60*60*24]),
    {noreply, State};

handle_cast({remove_host, UUID}, #state{api_hosts=Hosts} = State) ->
    lager:info([{fifi_component, sniffle}],
	       "sniffle:remove_host - Host: ~s.",
	       [UUID]),
    lager:debug([{fifi_component, sniffle}],
		"sniffle:remove_host - Hosts: ~p.", [Hosts]),
    try
	Pid =gproc:lookup_pid({n, l, {host, UUID}}),
	sniffle_host_srv:kill(Pid)
    catch
	T:E ->
	    lager:error([{fifi_component, sniffle}],
			"sniffle:remove_host - Error: ~p:~p.",
			[T,E]),
	    ok
    end,
    {noreply, State#state{api_hosts=[H||H<-Hosts,H=/=UUID]}};


handle_cast(backyard_connect, State) ->
    lager:info([{fifi_component, sniffle}],
	       "sniffle:backyard - connecting!"),
    gproc:reg({n, g, sniffle}),
    gproc:send({p,g, {sniffle,register}}, {sniffle, request, register}),
    {noreply, State};

handle_cast(reregister, State) ->
    try
%	gproc:reg({n, g, sniffle}),
%	gproc:send({p,g,{sniffle,register}}, {sniffle, request, register}),
	{noreply, State}
    catch
	T:E ->
	    lager:error([{fifi_component, sniffle}],
			"sniffle:register - Error: ~p:~p.",
			[T,E]),
	    application:stop(gproc),
	    application:start(gproc),
	    {noreply, State, 1000}
    end;

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
handle_info(timeout, State) ->
    reregister(),
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
terminate(_Reason, _State) ->
    backyard_srv:unregister_handler(),
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

get_env_default(Key, Default) ->
    case  application:get_env(Key) of
	{ok, Res} ->
	    Res;
	_ ->
	    Default
    end.

register_machine(Host, M) ->
    UUID = proplists:get_value(id, M),
    Name = <<"sniffle:machines:", UUID/binary>>,
    redo:cmd([<<"SET">>, Name, term_to_binary(Host)]),
    redo:cmd([<<"TTL">>, Name, 60*60*24]).

get_machine_host(Auth, UUID, Hosts) ->
    case get_machine_host_int(UUID) of
	{error, not_found} ->
	    lager:warning([{fifi_component, sniffle}, {user, Auth}],
			  "sniffle:host_cache_miss - ~s.",
			  [UUID]),
	    lists:foldl(fun (Host, Res) ->
				Pid = gproc:lookup_pid({n, l, {host, Host}}),
				{ok, VMs} = sniffle_host_srv:scall(Pid, Auth, {machines, list}),
				lists:foldl(fun (VM, Res1) ->
						     case lists:keyfind(id, 1, VM) of
							 {id, UUID} ->
							     Host;
							 _ ->
							     Res1
						     end
					     end, Res, VMs)
			end, {error, not_found}, Hosts);
	{ok, Host} ->
	    {ok, Host}
    end.

get_machine_host_int(UUID) ->
    Name = <<"sniffle:machines:", UUID/binary>>,
    case redo:cmd([<<"get">>, Name]) of
	undefined ->
	    lager:error([{fifi_component, sniffle}],
			"sniffle:host_not found - ~s.",
			[UUID]),
	    {error, not_found};
	Bin ->
	    {ok, binary_to_term(Bin)}
    end.


%% Wo loadbalance nodes by a very accurate measurement of random.
%% It is good practive to hope that the random node is the least 
%% loded one - not much else you can do about it.
pick_host(Hosts) ->
    {_, H} = lists:foldl(fun (UUID, {S, Res}) ->
			   try
			       Pid = gproc:lookup_pid({n, l, {host, UUID}}),
			       case sniffle_host_srv:scall(Pid, system, {info, memory}) of
				   {ok, {Used, Total}} when (Total - Used) > S ->
				       {(Total - Used), {ok, UUID}};
				   _ ->
				       {S, Res}
			       end
			   catch
			       _:_ ->
				   {S, Res}
			   end
			end, {0, {error, not_found}}, Hosts),
    H.

msg({Auth, _}, Type, Msg) ->
    msg(Auth, Type, Msg);

msg(Auth, Type, Msg) ->
    gproc:send({p, g, {user, Auth}}, {msg, Type, Msg}).
