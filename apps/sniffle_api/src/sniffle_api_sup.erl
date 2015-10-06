-module(sniffle_api_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

%% Helper macro for declaring children of supervisor
-define(CHILD(I, Type), {I, {I, start_link, []}, permanent, 5000, Type, [I]}).

%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init([]) ->
    spawn(fun delay_mdns_anouncement/0),
    {ok, { {one_for_one, 5, 10}, []} }.

%% We delay the service anouncement, first we wait
%% for sniffle to start to make sure the riak
%% core services call returns all needed services
%% then we'll go through each of the services
%% wait for startup.
%% Once they are started we wait for the API to start
%% and only then enable the mdns.

delay_mdns_anouncement() ->
    riak_core:wait_for_application(sniffle),
    Services = riak_core_node_watcher:services(),
    delay_mdns_anouncement(Services).
delay_mdns_anouncement([]) ->
    riak_core:wait_for_application(sniffle_api),
    lager:info("[mdns] Enabling mDNS annoucements."),
    mdns_server_fsm:start();
delay_mdns_anouncement([S | R]) ->
    riak_core:wait_for_service(S),
    delay_mdns_anouncement(R).
