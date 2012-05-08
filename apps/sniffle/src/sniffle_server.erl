%%%-------------------------------------------------------------------
%%% @author Heinz N. Gies <>
%%% @copyright (C) 2012, Heinz N. Gies
%%% @doc
%%%
%%% @end
%%% Created :  5 May 2012 by Heinz N. Gies <>
%%%-------------------------------------------------------------------
-module(sniffle_server).

-behaviour(gen_server).

-include_lib("alog_pt.hrl").

%% API
-export([start_link/0,
	reregister/0,
	 register_host_resource/4]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
	 terminate/2, code_change/3]).

-define(SERVER, ?MODULE). 

-record(state, {api_hosts=[],
		handlers=[]}).

-define(HOST_ACTION(Category, Action),
	handle_call({Category, Action, Auth, UUID}, _From, #state{api_hosts=Hosts} = State) ->
	       ?INFO({Category, Action, Auth, UUID, Hosts}, [], [sniffle]),
	       case get_machine_host(UUID) of
		   {error, not_found} ->
		       discover_machines(Auth, Hosts),
		       case get_machine_host(UUID) of
			   {error, not_found} ->
			       {reply, {error, not_found}, State};
			   {ok, Host} ->
			       Pid = gproc:lookup_pid({n, g, Host}),
			       {reply, sniffle_host_srv:call(Pid, {Category, Action, [UUID]}), State}
		       end;
		   {ok, Host} ->
		       Pid = gproc:lookup_pid({n, g, Host}),
		       {reply, sniffle_host_srv:call(Pid, {Category, Action, [UUID]}), State}
	       end).

-define(HOST_ACTION_OPTS(Category, Action),
	handle_call({Category, Action, Auth, UUID, Opts}, _From, #state{api_hosts=Hosts} = State) ->
	       ?INFO({Category, Action, Auth, UUID, Opts, Hosts}, [], [sniffle]),
	       case get_machine_host(UUID) of
		   {error, not_found} ->
		       discover_machines(Auth, Hosts),
		       case get_machine_host(UUID) of
			   {error, not_found} ->
			       {reply, {error, not_found}, State};
			   {ok, Host} ->
			       Pid =gproc:lookup_pid({n, g, Host}),
			       {reply, sniffle_host_srv:call(Pid, {Category, Action, [UUID|Opts]}), State}
		       end;
		   {ok, Host} ->
		       Pid =gproc:lookup_pid({n, g, Host}),
		       {reply, sniffle_host_srv:call(Pid, {Category, Action, [UUID|Opts]}), State}
	       end).



-define(LIST(Category),
	handle_call({Category, list, Auth}, _From, #state{api_hosts=Hosts} = State) ->
	       ?INFO({Category, list, Auth, Hosts}, [], [sniffle]),
	       Res = lists:foldl(fun (Host, List) ->
					 Pid =gproc:lookup_pid({n, g, Host}),
					 case sniffle_host_srv:call(Pid, {Category, list}) of
					     {ok, HostRes} ->
						 List ++ HostRes;
					     _ ->
						 List
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
			 {{cloudapi, bark}, sniffle_impl_bark}]).
init([]) ->
    Hosts = get_env_default(api_hosts, []),
    HostUUIDs = lists:map(fun ({Type, Spec}) ->
				      UUID = uuid:uuid4(),
				      Provider=proplists:get_value(Type, ?IMPL_PROVIDERS),
				      sniffle_host_sup:start_child(Provider, UUID, Spec),
				      UUID
			      end, Hosts),
    ?INFO({init, Hosts}, [], [sniffle]),
    {ok, #state{api_hosts=HostUUIDs}, 100}.


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
handle_call({machines, list, Auth}, _From, #state{api_hosts=Hosts} = State) ->
    ?INFO({machines, list, Auth, Hosts}, [], [sniffle]),
    Res = lists:foldl(fun ({Type, Host}, Machines) ->
			      ?DBG({Type, Host}, [], [sniffle]),
			      case cloudapi:list_machines(ensure_list(Host), Auth) of
				  {ok, {Ms, _, _}} ->
				      ?DBG({machines, Ms}, [], [sniffle]),
				      update_machines({Type, Host}, Ms),
				      Machines ++ Ms;
				  E ->
				      ?WARNING({error, E}, [], [sniffle]),
				      Machines
			      end
		     end, [], Hosts),
    {reply, {ok, Res}, State};

?HOST_ACTION(machines, get);
?HOST_ACTION(machines, delete);
?HOST_ACTION(machines, start);
?HOST_ACTION_OPTS(machines, start);
?HOST_ACTION(machines, stop);
?HOST_ACTION(machines, reboot);

?LIST(packages);
?LIST(datasets);
?LIST(images);

handle_call({keys, list, Auth}, _From, #state{api_hosts=Hosts} = State) ->
    ?INFO({keys, list, Auth, Hosts}, [], [sniffle]),
    Res = lists:foldl(fun (Host, Keys) ->
			      ?DBG({Host}, [], [sniffle]),
			      Pid = gproc:lookup_pid({n, g, Host}),
			      case sniffle_host_srv:call(Pid, {keys, list}) of
				  {ok, Ks} ->
				      ?DBG({keys, Ks}, [], [sniffle]),
				      Keys ++ Ks;
				  E ->
				      ?WARNING({error, E}, [], [sniffle]),
				      Keys
			      end
		     end, [], Hosts),
    {reply, {ok, Res}, State};


handle_call({keys, create, Auth, Pass, KeyID, PublicKey}, _From, #state{api_hosts=Hosts} = State) ->
    ?INFO({keys, create, Auth, Pass, KeyID, PublicKey, Hosts}, [], [sniffle]),
    Res = lists:foldl(fun ({Type, Host}, Res) ->
			      ?DBG({Type, Host}, [], [sniffle]),
			      case cloudapi:create_key(ensure_list(Host), Auth, ensure_list(Pass), ensure_list(KeyID), PublicKey) of
				  {ok, D} ->
				      ?DBG({reply, D}, [], [sniffle]),
				      case Res of 
					  {error, _} ->
					      Res;
					  _ ->
					      {ok, D}
				      end;
				  E ->
				      ?WARNING({error, E}, [], [sniffle]),
				      case Res of
					  {error, Es} ->
					      {error, [{Host, E}|Es]};
					  _ ->
					      {error, [{Host, E}]}
				      end
			    end
		      end, ok, Hosts),
    {reply, {ok, Res}, State};

handle_call(ping, _From, State) ->
    ?INFO({ping}, [], [sniffle]),
    {reply, pong, State};



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
handle_cast(reregister, State) ->
    gproc:reg({n, g, sniffle}),
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
    lists:map(fun ({Type, Host}) ->
		      case cloudapi:list_machines(Host, Auth) of
			  {ok, {Ms, _, _}} ->
			      update_machines({Type, Host}, Ms);
			  _ ->
			      error
		      end
	      end, Hosts),
    ok.

register_machine(Host, M) ->
    UUID = proplists:get_value(<<"id">>, M),
    Name = <<"sniffle:machines:", UUID/binary>>,
    redo:cmd([<<"SET">>, Name, term_to_binary(Host)]),
    redo:cmd([<<"TTL">>, Name, 60*60*24]).

get_machine_host(UUID) ->
    Name = <<"sniffle:machines:", UUID/binary>>,
    case redo:cmd([<<"get">>, Name]) of
	undefined ->
	    {error, not_found};
	Bin ->
	    {ok, binary_to_term(Bin)}
    end.
	    

ensure_list(B) when is_binary(B) ->
    binary_to_list(B);
ensure_list(L) when is_list(L) ->
    L.

    
