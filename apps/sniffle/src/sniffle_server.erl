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

-include_lib("alog_pt.hrl").

%% API
-export([start_link/0,
	 reregister/0,
	 update_machines/2,
	 register_host_resource/4]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
	 terminate/2, code_change/3]).

-define(SERVER, ?MODULE). 

-record(state, {api_hosts=[],
		handlers=[]}).

-define(HOST_ACTION(Category, Action),
	handle_call({call, Auth, {Category, Action, UUID}}, From, #state{api_hosts=Hosts} = State) ->
	       ?INFO({Category, Action, Auth, UUID, Hosts}, [], [sniffle]),
	       case get_machine_host(Auth, UUID, Hosts) of
		   {error, E} ->
		       {reply, {error, E}, State};
		   {ok, Host} ->
		       try
			   Pid = gproc:lookup_pid({n, l, {host, Host}}),
			   case sniffle_host_srv:scall(Pid, From, Auth, {Category, Action, UUID}) of
			       {error, cant_call} = E1->
				   remove_host(Host),
				   {reply, {error, E1}, State};
			       Res ->
				   {reply, Res, State}
			   end
		       catch
			   _T:E ->
			       remove_host(Host),
			       {reply, {error, {host_down, E}}, State}
		       end
	       end).
-define(HOST_ACTION1(Category, Action),
	handle_call({call, Auth, {Category, Action, UUID, O1}}, From, #state{api_hosts=Hosts} = State) ->
	       ?INFO({Category, Action, Auth, UUID, Hosts}, [], [sniffle]),
	       case get_machine_host(Auth, UUID, Hosts) of
		   {error, E} ->
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
			   _T:E ->
			       remove_host(Host),
			       {reply, {error, {host_down, E}}, State}
		       end
	       end).
-define(HOST_ACTION2(Category, Action),
	handle_call({call, Auth, {Category, Action, UUID, O1, O2}}, From, #state{api_hosts=Hosts} = State) ->
	       ?INFO({Category, Action, Auth, UUID, Hosts}, [], [sniffle]),
	       case get_machine_host(Auth, UUID, Hosts) of
		   {error, E} ->
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
			   _T:E ->
			       remove_host(Host),
			       {reply, {error, {host_down, E}}, State}
		       end
	       end).

-define(LIST(Category),
	handle_call({call, Auth, {Category, list}}, From, #state{api_hosts=Hosts} = State) ->
	       ?INFO({Category, list, Auth, Hosts}, [], [sniffle]),
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
					     _T:E ->
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
    Hosts = get_env_default(api_hosts, []),
    Providers = get_env_default(providers, ?IMPL_PROVIDERS),
    HostUUIDs = lists:map(fun ({Type, Spec}) ->
				      UUID = uuid:uuid4(),
				      Provider=proplists:get_value(Type, Providers),
				      sniffle_host_sup:start_child(Provider, UUID, Spec),
				      UUID
			      end, Hosts),
    ?INFO({init, Hosts}, [], [sniffle]),
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

handle_call({call, Auth, {packages, list}}, From, #state{api_hosts=Hosts} = State) ->
    ?INFO({packages, list, Auth, Hosts}, [], [sniffle]),
    {ok, GlobalPackageNames} = libsnarl:option_list(system, packages),
    GlobalPackages = 
	[P1 || {ok, P1 }<- [libsnarl:option_get(system, packages, P) || P <- GlobalPackageNames]],
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
				  _T:E ->
				      remove_host(Host),
				      {reply, {error, {host_down, E}}, State}
			      end
		      end, GlobalPackages, Hosts),
    {reply, {ok, Res}, State};

handle_call({call, Auth, {packages, create, Name, Disk, Memory, Swap}}, _From, State) ->
    case libsnarl:allowed(system, Auth, [packages, Name, set]) of
	false ->
	    {reply, {error, unauthorized}, State};
	true ->
	    Pkg = [{name, Name}, 
		   {disk, Disk}, 
		   {memory, Memory}, 
		   {swap, Swap}],
	    libsnarl:option_set(system, packages, Name, Pkg),
	    {reply, {ok, Pkg}, State}
    end;

handle_call({call, Auth, {machines, create, Name, PackageUUID, DatasetUUID, Metadata, Tags}}, From, 
	    #state{api_hosts=Hosts} = State) ->
    Host = pick_host(Hosts),
    Pid = gproc:lookup_pid({n, l, {host, Host}}),
    sniffle_host_srv:call(Pid, From, Auth, {machines, create, Name, PackageUUID, DatasetUUID, Metadata, Tags}),
    {noreply, State};

handle_call({call, Auth, {packages, delete, Name}}, _From, State) ->
    case libsnarl:allowed(system, Auth, [packages, Name, delete]) of
	false ->
	    {reply, {error, unauthorized}, State};
	true ->
	    libsnarl:option_delete(system, packages, Name),
	    {reply, ok, State}
    end;

handle_call({call, Auth, {keys, create, Pass, KeyID, PublicKey}}, From, #state{api_hosts=Hosts} = State) ->
    ?INFO({keys, create, Auth, Pass, KeyID, PublicKey, Hosts}, [], [sniffle]),
    Res = lists:foldl(
	    fun (Host, Res) ->
		    ?DBG({Host}, [], [sniffle]),
		    Pid =gproc:lookup_pid({n, l, {host, Host}}),
		    case sniffle_host_srv:scall(Pid, Auth, {keys, create, Pass, KeyID, PublicKey}) of
			{ok, D} ->
			    ?DBG({reply, D}, [], [sniffle]),
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

handle_call({call, Auth, info}, _From,  #state{api_hosts=Hosts} = State) ->
    case libsnarl:allowed(system, Auth, [service, sniffle, info]) of
	false ->
	    {reply, {error, unauthorized}, State};
	true ->
	    ?INFO({ping}, [], [sniffle]),
	    {reply, [{<<"version">>, <<"0.1.0">>},
		     {<<"hosts">>, length(Hosts)}], State}
    end;

handle_call({call, Auth, ping}, _From, State) ->
    case libsnarl:allowed(system, Auth, [service, sniffle, info]) of
	false ->
	    {reply, {error, unauthorized}, State};
	true ->
	    ?INFO({ping}, [], [sniffle]),
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
    case libsnarl:allowed(system, Auth, [service, sniffle, hosts, add, Type]) of
	false ->
	    {noreply, State};
	true ->
	    Providers = get_env_default(providers, ?IMPL_PROVIDERS),
	    UUID = uuid:uuid4(),
	    Provider=proplists:get_value(Type, Providers),
	    ?INFO({register, Provider, UUID, Spec}, [], [sniffle]),
	    sniffle_host_sup:start_child(Provider, UUID, Spec),
	    {noreply, State#state{api_hosts=[UUID|HostUUIDs]}}
    end;
	
handle_cast({update_machines, Host, Ms}, State) ->
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
    try
	Pid =gproc:lookup_pid({n, l, {host, UUID}}),
	sniffle_host_srv:kill(Pid)
    catch
	_T:_E ->
	    ok
    end,
    {noreply, State#state{api_hosts=[H||H<-Hosts,H=/=UUID]}};

handle_cast(reregister, State) ->
    try
	gproc:reg({n, g, sniffle}),
	gproc:send({p,g,{sniffle,register}}, {sniffle, request, register}),
	{noreply, State}
    catch
	_T:_E ->
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

discover_machines(Auth, Hosts) ->
    lists:map(fun (Host) ->
		      Pid = gproc:lookup_pid({n, l, {host, Host}}),
		      sniffle_host_srv:scall(Pid, self(), Auth, {machines, list})
	      end, Hosts),
    ok.

register_machine(Host, M) ->
    UUID = proplists:get_value(id, M),
    Name = <<"sniffle:machines:", UUID/binary>>,
    redo:cmd([<<"SET">>, Name, term_to_binary(Host)]),
    redo:cmd([<<"TTL">>, Name, 60*60*24]).

get_machine_host(Auth, UUID, Hosts) ->
    case get_machine_host_int(UUID) of
	{error, not_found} ->
	    discover_machines(Auth, Hosts),
	    get_machine_host_int(UUID);
	{ok, Host} ->
	    {ok, Host}
    end.

get_machine_host_int(UUID) ->
    Name = <<"sniffle:machines:", UUID/binary>>,
    case redo:cmd([<<"get">>, Name]) of
	undefined -> 
	    {error, not_found};
	Bin ->
	    {ok, binary_to_term(Bin)}
    end.


%% Wo loadbalance nodes by a very accurate measurement of random.
%% It is good practive to hope that the random node is the least 
%% loded one - not much else you can do about it.
pick_host(Hosts) ->
    [H|_Rest] = shuffle(Hosts),
    H.

shuffle(List) ->
%% Determine the log n portion then randomize the list.
   randomize(round(math:log(length(List)) + 0.5), List).

randomize(1, List) ->
   randomize(List);
randomize(T, List) ->
   lists:foldl(fun(_E, Acc) ->
                  randomize(Acc)
               end, randomize(List), lists:seq(1, (T - 1))).

randomize(List) ->
   D = lists:map(fun(A) ->
                    {random:uniform(), A}
             end, List),
   {_, D1} = lists:unzip(lists:keysort(1, D)), 
   D1.
