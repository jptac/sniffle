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
           config = jsxd:from_list(Config)
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
    sniffle_vm:set(UUID, <<"state">>, <<"fetching_package">>),
    {ok, Package} = sniffle_package:get(PackageName),
    sniffle_vm:set(UUID, <<"package">>, PackageName),
    {next_state, get_dataset, State#state{package = Package}, 0}.

get_dataset(_Event, State = #state{
                      uuid = UUID,
                      dataset_name = DatasetName}) ->
    sniffle_vm:set(UUID, <<"state">>, <<"fetching_dataset">>),
    sniffle_vm:log(UUID, <<"Fetching dataset ", DatasetName/binary>>),
    {ok, Dataset} = sniffle_dataset:get(DatasetName),
    sniffle_vm:set(UUID, <<"dataset">>, DatasetName),
    {next_state, get_ips, State#state{dataset = Dataset}, 0}.

get_ips(_Event, State = #state{config = Config,
                               uuid = UUID,
                               dataset = Dataset}) ->
    Dataset1 = jsxd:update(<<"networks">>,
                           fun(Nics) ->
                                   {Nics1, _} =
                                       jsxd:fold(
                                         fun(K, Nic, {NicsF, Mappings}) ->
                                                 {ok, Name} = jsxd:get(<<"name">>, Nic),
                                                 {ok, NicTag} = jsxd:get([<<"networks">>, Name], Config),
                                                 sniffle_vm:log(UUID, <<"Fetching network ", NicTag/binary, " for NIC ", Name/binary>>),
                                                 {ok, {Tag, IP, Net, Gw}} = sniffle_iprange:claim_ip(NicTag),
                                                 {ok, Range} = sniffle_iprange:get(NicTag),
                                                 IPb = sniffle_iprange_state:to_bin(IP),
                                                 Netb = sniffle_iprange_state:to_bin(Net),
                                                 GWb = sniffle_iprange_state:to_bin(Gw),
                                                 sniffle_vm:log(UUID, <<"Assigning IP ", IPb/binary,
                                                                        " netmask ", Netb/binary,
                                                                        " gateway ", GWb/binary,
                                                                        " tag ", Tag/binary>>),
                                                 Mappings1 = jsxd:append(
                                                               [],
                                                               jsxd:from_list([{<<"network">>, NicTag},
                                                                               {<<"ip">>, IP}]),
                                                               Mappings),
                                                 sniffle_vm:set(UUID,
                                                                <<"network_mappings">>,
                                                                Mappings1),
                                                 Res = jsxd:from_list([{<<"nic_tag">>, Tag},
                                                                       {<<"ip">>, IPb},
                                                                       {<<"netmask">>, Netb},
                                                                       {<<"gateway">>, GWb}]),
                                                 Res1 = case jsxd:get(<<"vlan">>, 0, Range) of
                                                            0 ->
                                                                Res;
                                                            VLAN ->
                                                                jsxd:set(<<"vlan_di">>, VLAN, Res)
                                                            end,
                                                 NicsF1 = jsxd:set(K, Res1, NicsF),
                                                 {NicsF1, Mappings1}
                                         end, {[], []}, Nics),
                                   Nics1
                           end, Dataset),
    {next_state, get_server, State#state{dataset = Dataset1}, 0}.

get_server(_Event, State = #state{
                     dataset = Dataset,
                     uuid = UUID,
                     config = Config,
                     package = Package}) ->
    {ok, Owner} = jsxd:get(<<"owner">>, Config),
    sniffle_vm:log(UUID, <<"Assigning owner ", Owner/binary>>),
    {ok, Ram} = jsxd:get(<<"ram">>, Package),
    RamB = list_to_binary(integer_to_list(Ram)),
    sniffle_vm:log(UUID, <<"Assigning memory ", RamB/binary>>),
    sniffle_vm:set(UUID, <<"state">>, <<"fetching_server">>),
    Permission = [<<"hypervisor">>, {<<"res">>, <<"name">>}, <<"create">>],
    {ok, Ns} = jsxd:get(<<"networks">>, Dataset),
    {ok, Type} = jsxd:get(<<"type">>, Dataset),

    NicTags = jsxd:map(fun (_N, E) ->
                               jsxd:get(<<"nic_tag">>, <<"undefined">>, E)
                       end, Ns),
    case libsnarl:user_cache(Owner) of
        {ok, Permissions} ->
            Conditions0 = jsxd:get(<<"requirements">>, [], Package),
            Conditions1 = Conditions0 ++ jsxd:get(<<"requirements">>, [], Dataset),
            Conditions = [{must, 'allowed', Permission, Permissions},
                          {must, 'subset', <<"networks">>, NicTags},
                          {must, 'element', <<"virtualisation">>, Type},
                          {must, '>=', <<"resources.free-memory">>, Ram}] ++
                lists:map(fun(C) -> make_condition(C, Permissions) end, Conditions1),
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
                             uuid = UUID,
                             config = Config}) ->
    {<<"owner">>, Owner} = lists:keyfind(<<"owner">>, 1, Config),
    libsnarl:user_grant(Owner, [<<"vms">>, UUID, '...']),
    {next_state, create, State, 0}.

create(_Event, State = #state{
                 dataset = Dataset,
                 package = Package,
                 uuid = UUID,
                 config = Config,
                 hypervisor = {Host, Port}}) ->
    sniffle_vm:log(UUID, <<"Handing off to hypervisor.">>),
    sniffle_vm:set(UUID, <<"state">>, <<"creating">>),
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
    sniffle_vm:set(UUID, <<"state">>, <<"failed-", StateBin/binary>>),
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

make_condition(C, Permissions) ->
    Weight = case jsxd:get(<<"weight">>, <<"must">>, C) of
                 <<"must">> ->
                     must;
                 <<"cant">> ->
                     cant;
                 I when is_integer(I) ->
                     I
             end,
    Condition = case jsxd:get(<<"condition">>, <<"=:=">>, C) of
                    <<">=">> -> '>=';
                    <<">">> -> '>';
                    <<"=<">> -> '=<';
                    <<"<">> -> '<';
                    <<"=:=">> -> '=:=';
                    <<"=/=">> -> '=/=';
                    <<"subset">> -> 'subset';
                    <<"superset">> -> 'superset';
                    <<"disjoint">> -> 'disjoint';
                    <<"element">> -> 'element';
                    <<"allowed">> -> 'allowed'
                end,
    {ok, Attribute} = jsxd:get(<<"attribute">>, C),
    case Condition of
        'allowed' ->
            {Weight, Condition, Attribute, Permissions};
        _ ->
            {ok, Value} = jsxd:get(<<"value">>, C),
            {Weight, Condition, Attribute, Value}
    end.

