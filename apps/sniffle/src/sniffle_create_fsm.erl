%%%-------------------------------------------------------------------
%%% @author Heinz Nikolaus Gies <heinz@licenser.net>
%%% @copyright (C) 2012, Heinz Nikolaus Gies
%%% @doc
%%%
%%% @end
%%% Created : 17 Oct 2012 by Heinz Nikolaus Gies <heinz@licenser.net>
%%%-------------------------------------------------------------------
-module(sniffle_create_fsm).

-behaviour(gen_fsm).

-include("sniffle.hrl").

%% API
-export([create/4,
         start_link/4]).

%% gen_fsm callbacks
-export([
         init/1,
         handle_event/3,
         handle_sync_event/4,
         handle_info/3,
         terminate/3,
         code_change/4
        ]).

-export([
         get_package/2,
         get_dataset/2,
         create/2,
         get_server/2,
         create_permissions/2,
         get_ips/2
        ]).

-define(SERVER, ?MODULE).

-ignore_xref([
              create/2,
              get_dataset/2,
              get_package/2,
              start_link/4,
              get_server/2,
              create_permissions/2,
              get_ips/2
             ]).

-record(state, {
          uuid,
          package,
          package_name,
          dataset,
          dataset_name,
          config,
          type,
          hypervisor
         }).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Creates a gen_fsm process which calls Module:init/1 to
%% initialize. To ensure a synchronized start-up procedure, this
%% function does not return until Module:init/1 has returned.
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link(UUID, Package, Dataset, Config) ->
    gen_fsm:start_link({local, ?SERVER}, ?MODULE, [UUID, Package, Dataset, Config], []).

create(UUID, Package, Dataset, Config) ->
    supervisor:start_child(sniffle_create_fsm_sup, [UUID, Package, Dataset, Config]).


%%%===================================================================
%%% gen_fsm callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever a gen_fsm is started using gen_fsm:start/[3,4] or
%% gen_fsm:start_link/[3,4], this function is called by the new
%% process to initialize.
%%
%% @spec init(Args) -> {ok, StateName, State} |
%%                     {ok, StateName, State, Timeout} |
%%                     ignore |
%%                     {stop, StopReason}
%% @end
%%--------------------------------------------------------------------
init([UUID, Package, Dataset, Config]) ->
    process_flag(trap_exit, true),
    {ok, get_package, #state{
           uuid = UUID,
           package_name = Package,
           dataset_name = Dataset,
           config = Config
          }, 0}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% There should be one instance of this function for each possible
%% state name. Whenever a gen_fsm receives an event sent using
%% gen_fsm:send_event/2, the instance of this function with the same
%% name as the current state name StateName is called to handle
%% the event. It is also called if a timeout occurs.
%%
%% @spec state_name(Event, State) ->
%%                   {next_state, NextStateName, NextState} |
%%                   {next_state, NextStateName, NextState, Timeout} |
%%                   {stop, Reason, NewState}
%% @end
%%--------------------------------------------------------------------

get_package(_Event, State = #state{
                      uuid = UUID,
                      package_name = PackageName}) ->
    sniffle_vm:log(UUID, <<"Fetching package ", PackageName/binary>>),
    sniffle_vm:set_attribute(UUID, <<"state">>, <<"fetching_package">>),
    {ok, Package} = sniffle_package:get_attribute(PackageName),
    sniffle_vm:set_attribute(UUID, <<"package">>, PackageName),
    {next_state, get_dataset, State#state{package = Package}, 0}.

get_dataset(_Event, State = #state{
                      uuid = UUID,
                      dataset_name = DatasetName}) ->
    sniffle_vm:set_attribute(UUID, <<"state">>, <<"fetching_dataset">>),
    sniffle_vm:log(UUID, <<"Fetching dataset ", DatasetName/binary>>),
    {ok, Dataset} = sniffle_dataset:get_attribute(DatasetName),
    sniffle_vm:set_attribute(UUID, <<"dataset">>, DatasetName),
    {next_state, get_ips, State#state{dataset = Dataset}, 0}.

get_ips(_Event, State = #state{config = Config,
                               uuid = UUID,
                               dataset = Dataset}) ->
    {<<"networks">>, On} = lists:keyfind(<<"networks">>, 1, Config),
    {<<"networks">>, Ns} = lists:keyfind(<<"networks">>, 1, Dataset),
    Dataset1 = lists:keydelete(<<"networks">>, 1, Dataset),
    Ns1 = lists:foldl(fun(Nic, NsAcc) ->
                              {<<"name">>, Name} = lists:keyfind(<<"name">>, 1, Nic),
                              {Name, NicTag} = lists:keyfind(Name, 1, On),
                              sniffle_vm:log(UUID, <<"Fetching network ", NicTag/binary, " for NIC ", Name/binary>>),
                              {ok, {Tag, IP, Net, Gw}} = sniffle_iprange:claim_ip(NicTag),
                              IPb = sniffle_iprange_state:to_bin(IP),
                              Netb = sniffle_iprange_state:to_bin(Net),
                              GWb = sniffle_iprange_state:to_bin(Gw),
                              sniffle_vm:log(UUID, <<"Assigning IP ", IPb/binary,
                                                     " netmask ", Netb/binary,
                                                     " gateway ", GWb/binary,
                                                     " tag ", Tag/binary>>),
                              [[{<<"nic_tag">>, Tag},
                                {<<"ip">>, IPb},
                                {<<"netmask">>, Netb},
                                {<<"gateway">>, GWb}] | NsAcc]
                      end, [], Ns),
    {next_state, get_server, State#state{dataset = [{<<"networks">>, Ns1} | Dataset1]}, 0}.

get_server(_Event, State = #state{
                     dataset = Dataset,
                     uuid = UUID,
                     config = Config,
                     package = Package}) ->
    {<<"owner">>, Owner} = lists:keyfind(<<"owner">>, 1, Config),
    sniffle_vm:log(UUID, <<"Assigning owner ", Owner/binary>>),
    {<<"ram">>, Ram} = lists:keyfind(<<"ram">>, 1, Package),
    RamB = list_to_binary(integer_to_list(Ram)),
    sniffle_vm:log(UUID, <<"Assigning memory ", RamB/binary>>),
    sniffle_vm:set_attribute(UUID, <<"state">>, <<"fetching_server">>),
    Permission = [<<"hypervisor">>, {<<"res">>, <<"name">>}, <<"create">>],
    {<<"networks">>, Ns} = lists:keyfind(<<"networks">>, 1, Dataset),
    {<<"type">>, Type} = lists:keyfind(<<"type">>, 1, Dataset),

    NicTags = lists:foldl(fun (N, Acc) ->
                                  {<<"nic_tag">>, Tag} = lists:keyfind(<<"nic_tag">>, 1, N),
                                  [Tag | Acc]
                          end, [], Ns),
    case libsnarl:user_cache(Owner) of
        {ok, Permissions} ->
            Conditions = [{must, 'allowed', Permission, Permissions},
                          {must, 'subset', <<"networks">>, NicTags},
                          {must, 'element', <<"virtualisation">>, Type},
                          {must, '>=', <<"resouroces.free-memory">>, Ram}],
            CondB = list_to_binary(io_lib:format("~p", [Conditions])),
            sniffle_vm:log(UUID, <<"Finding hypervisor ", CondB/binary>>),


            {ok, [{HypervisorID, _} | _]} = sniffle_hypervisor:list(Conditions),
            sniffle_vm:log(UUID, <<"Deploying on hypervisor ", HypervisorID/binary>>),

            {ok, H} = sniffle_hypervisor:get(HypervisorID),
            {ok, Port} = jsxd:get(<<"port">>, H),
            {ok, Host} = jsxd:get(<<"host">>, H),
            {next_state, create_permissions, State#state{hypervisor = {binary_to_list(Host), Port}}, 0};
        _ ->
            {next_state, get_server, State, 10000}
    end.

create_permissions(_Event, State = #state{
                             uuid = _UUID,
                             config = Config}) ->
    {<<"owner">>, _Owner} = lists:keyfind(<<"owner">>, 1, Config),
%%% TODO: give permissions!
    {next_state, create, State, 0}.

create(_Event, State = #state{
                 dataset = Dataset,
                 package = Package,
                 uuid = UUID,
                 config = Config,
                 hypervisor = {Host, Port}}) ->
    sniffle_vm:log(UUID, <<"Handing off to hypervisor.">>),
    sniffle_vm:set_attribute(UUID, <<"state">>, <<"creating">>),
    {<<"owner">>, Owner} = lists:keyfind(<<"owner">>, 1, Config),
    libsnarl:user_grant(Owner, [<<"vms">>, UUID, '...']),
    libchunter:create_machine(Host, Port, UUID, Package, Dataset, Config),
    {stop, normal, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever a gen_fsm receives an event sent using
%% gen_fsm:send_all_state_event/2, this function is called to handle
%% the event.
%%
%% @spec handle_event(Event, StateName, State) ->
%%                   {next_state, NextStateName, NextState} |
%%                   {next_state, NextStateName, NextState, Timeout} |
%%                   {stop, Reason, NewState}
%% @end
%%--------------------------------------------------------------------
handle_event(_Event, StateName, State) ->
    {next_state, StateName, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever a gen_fsm receives an event sent using
%% gen_fsm:sync_send_all_state_event/[2,3], this function is called
%% to handle the event.
%%
%% @spec handle_sync_event(Event, From, StateName, State) ->
%%                   {next_state, NextStateName, NextState} |
%%                   {next_state, NextStateName, NextState, Timeout} |
%%                   {reply, Reply, NextStateName, NextState} |
%%                   {reply, Reply, NextStateName, NextState, Timeout} |
%%                   {stop, Reason, NewState} |
%%                   {stop, Reason, Reply, NewState}
%% @end
%%--------------------------------------------------------------------
handle_sync_event(_Event, _From, StateName, State) ->
    Reply = ok,
    {reply, Reply, StateName, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_fsm when it receives any
%% message other than a synchronous or asynchronous event
%% (or a system message).
%%
%% @spec handle_info(Info,StateName,State)->
%%                   {next_state, NextStateName, NextState} |
%%                   {next_state, NextStateName, NextState, Timeout} |
%%                   {stop, Reason, NewState}
%% @end
%%--------------------------------------------------------------------
handle_info(_Info, StateName, State) ->
    {next_state, StateName, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_fsm when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_fsm terminates with
%% Reason. The return value is ignored.
%%
%% @spec terminate(Reason, StateName, State) -> void()
%% @end
%%--------------------------------------------------------------------

terminate(shutdown, create, _StateData) ->
    ok;

terminate(shutdown, _StateName, _StateData) ->
    ok;

terminate(_Reason, StateName, _State = #state{uuid=UUID}) ->
    StateBin = list_to_binary(atom_to_list(StateName)),
    sniffle_vm:set_attribute(UUID, <<"state">>, <<"failed-", StateBin/binary>>),
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, StateName, State, Extra) ->
%%                   {ok, StateName, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
