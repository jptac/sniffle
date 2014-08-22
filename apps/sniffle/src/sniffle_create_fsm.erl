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
-export([create/5,
         create/4,
         start_link/5]).

%% gen_fsm callbacks
-export([
         init/1,
         handle_event/3,
         handle_sync_event/4,
         handle_info/3,
         terminate/3,
         get_networks/2,
         code_change/4
        ]).

-export([
         generate_grouping_rules/2,
         get_package/2,
         get_dataset/2,
         callbacks/2,
         create/2,
         get_server/2,
         create_permissions/2,
         get_ips/2,
         build_key/2
        ]).

-define(SERVER, ?MODULE).

-ignore_xref([
              create/5,
              create/2,
              generate_grouping_rules/2,
              callbacks/2,
              get_dataset/2,
              get_package/2,
              start_link/5,
              get_server/2,
              create_permissions/2,
              get_networks/2,
              get_ips/2,
              build_key/2
             ]).

-record(state, {
          test_pid,
          uuid,
          package,
          package_name,
          dataset,
          dataset_name,
          config,
          resulting_networks = [],
          owner,
          creator,
          type,
          nets,
          hypervisor,
          mapping = [],
          delay = 5000,
          retry = 0,
          max_retries = 0,
          grouping_rules = [],
          log_cache = [],
          last_error
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

start_link(UUID, Package, Dataset, Config, Pid) ->
    gen_fsm:start_link(?MODULE, [UUID, Package, Dataset, Config, Pid], []).

create(UUID, Package, Dataset, Config) ->
    create(UUID, Package, Dataset, Config, undefined).

create(UUID, Package, Dataset, Config, Pid) ->
    supervisor:start_child(sniffle_create_fsm_sup, [UUID, Package, Dataset, Config, Pid]).


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

init([UUID, Package, Dataset, Config, Pid]) ->
    lager:info("[create] Starting FSM for ~s", [UUID]),
    process_flag(trap_exit, true),
    Config1 = jsxd:from_list(Config),
    %% We're transforming the networks map {nic -> networkid} into
    %% an array that is close to what it will look after the VM was
    %% created, that way the structure stays consistant.
    Delay = case application:get_env(create_retry_delay) of
                {ok, D} ->
                    D;
                _ ->
                    5000
            end,
    MaxRetries = case application:get_env(create_max_retries) of
                     {ok, D1} ->
                         D1;
                     _ ->
                         5
                 end,
    {ok, generate_grouping_rules, #state{
                                     uuid = UUID,
                                     package_name = Package,
                                     dataset_name = Dataset,
                                     config = Config1,
                                     delay = Delay,
                                     max_retries = MaxRetries,
                                     test_pid = Pid
                                    }, 0}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% When a grouping is used during creation certain additional rules
%% need to be applied to guarantee the propper constraints. Here we get
%% them.
%% @spec state_name(Event, State) ->
%%                   {next_state, NextStateName, NextState} |
%%                   {next_state, NextStateName, NextState, Timeout} |
%%                   {stop, Reason, NewState}
%% @end
%%--------------------------------------------------------------------

generate_grouping_rules(_Event, State = #state{test_pid = {_,_}, config = Config}) ->
    case jsxd:get([<<"grouping">>], Config) of
        {ok, Grouping} ->
            Rules = sniffle_grouping:create_rules(Grouping),
            {next_state, create_permissions, State#state{grouping_rules = Rules}, 0};
        _ ->
            {next_state, create_permissions, State, 0}
    end;

generate_grouping_rules(_Event, State = #state{
                                           uuid = UUID,
                                           config = Config
                                          }) ->
    case jsxd:get([<<"grouping">>], Config) of
        {ok, Grouping} ->
            Rules = sniffle_grouping:create_rules(Grouping),
            case sniffle_grouping:add_element(Grouping, UUID) of
                ok ->
                    sniffle_vm:add_grouping(UUID, Grouping),
                    {next_state, create_permissions,
                     State#state{grouping_rules = Rules}, 0};
                E ->
                    vm_log(State, error, "Failed to create routing rule."),
                    lager:error("[create] Creation Faild since grouing could not be "
                                "joined: ~p", [E]),
                    {stop, E, State}
            end;
        _ ->
            {next_state, create_permissions, State, 0}
    end.

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
create_permissions(_Event, State = #state{test_pid = {_,_}, config = Config}) ->
    {ok, Creator} = jsxd:get([<<"owner">>], Config),
    Owner = case ls_user:active_org(Creator) of
                {ok, <<"">>} ->
                    lager:warning("[create] User ~p has no active org.",
                                  [Creator]),
                    <<"">>;
                {ok, Org} ->
                    lager:info("[create] User ~p has active org: ~p.",
                               [Creator, Org]),
                    Org
            end,
    Config1 = jsxd:set([<<"owner">>], Owner, Config),
    {next_state, get_package,
     State#state{
       config = Config1,
       creator = Creator,
       owner = Owner
      }, 0};

create_permissions(_Event, State = #state{
                                      uuid = UUID,
                                      config = Config}) ->
    {ok, Creator} = jsxd:get([<<"owner">>], Config),
    ls_user:grant(Creator, [<<"vms">>, UUID, <<"...">>]),
    ls_user:grant(Creator, [<<"channels">>, UUID, <<"join">>]),
    eplugin:call('create:permissions', UUID, Config, Creator),
    Owner = case ls_user:active_org(Creator) of
                {ok, <<>>} ->
                    lager:warning("[create] User ~p has no active org.",
                                  [Creator]),
                    <<"">>;
                {ok, Org} ->
                    lager:info("[create] User ~p has active org: ~p.",
                               [Creator, Org]),
                    sniffle_vm:owner(UUID, Org),
                    ls_org:execute_trigger(Org, vm_create, UUID),
                    Org
            end,
    libhowl:send(UUID, [{<<"event">>, <<"update">>},
                        {<<"data">>, [{<<"owner">>, Owner}]}]),
    Config1 = jsxd:set([<<"owner">>], Owner, Config),
    {next_state, get_package,
     State#state{
       config = Config1,
       creator = Creator,
       owner = Owner
      }, 0}.

get_package(_Event, State = #state{
                               uuid = UUID,
                               package_name = PackageName
                              }) ->
    lager:info("[create] Fetching package: ~p", [PackageName]),
    vm_log(State, info, <<"Fetching package ", PackageName/binary>>),
    sniffle_vm:state(UUID, <<"fetching_package">>),
    {ok, Package} = sniffle_package:get(PackageName),
    sniffle_vm:package(UUID, PackageName),
    {next_state, get_dataset, State#state{package = Package}, 0}.

get_dataset(_Event, State = #state{
                               uuid = UUID,
                               dataset_name = DatasetName
                              }) ->
    lager:info("[create] Fetching Dataset: ~p", [DatasetName]),
    vm_log(State, info, <<"Fetching dataset ", DatasetName/binary>>),
    sniffle_vm:state(UUID, <<"fetching_dataset">>),
    {ok, Dataset} = sniffle_dataset:get(DatasetName),
    sniffle_vm:dataset(UUID, DatasetName),
    {next_state, callbacks, State#state{dataset = Dataset}, 0}.


callbacks(_Event, State = #state{
                             uuid = UUID,
                             dataset = Dataset,
                             package = Package,
                             config = Config}) ->
    {UUID, Package1, Dataset1, Config1} =
        eplugin:fold('create:update', {UUID, Package, Dataset, Config}),
    {next_state, get_networks, State#state{
                                 dataset = Dataset1,
                                 package = Package1,
                                 config = Config1}, 0}.

get_networks(_event, State = #state{retry = R, max_retries = Max})
  when R > Max ->
    lager:error("[create] Failed after too many retries: ~p > ~p.",
                [R, Max]),
    BR = integer_to_binary(R),
    BMax= integer_to_binary(Max),
    vm_log(State, error, <<"Failed after too many retries: ", BR/binary, " > ",
                           BMax/binary>>),
    {stop, failed, State};

get_networks(_Event, State = #state{config = Config, retry = Try}) ->
    Nets = jsxd:get([<<"networks">>], [], Config),
    Nets1 = lists:map(fun({Name, Network}) ->
                              {ok, N} = sniffle_network:get(Network),
                              Rs = ft_network:ipranges(N),
                              Rs1 = [{R, sniffle_iprange:get(R)} || R <- Rs],
                              Rs2 = [{R, D} || {R, {ok, D}} <- Rs1],
                              Rs3 = lists:map(fun({ID, R}) ->
                                                      {ID, ft_iprange:tag(R)}
                                              end, Rs2),
                              {Name, Rs3}
                      end, Nets),
    {next_state, get_server,
     State#state{nets = Nets1, retry = Try + 1, log_cache = []}, 0}.

get_server(_Event, State = #state{
                              dataset = Dataset,
                              uuid = UUID,
                              creator = Creator,
                              config = Config,
                              nets = Nets,
                              package = Package,
                              grouping_rules = GroupingRules}) ->
    lager:debug("[create] get_server: ~p", [Nets]),
    Ram = ft_package:ram(Package),
    sniffle_vm:state(UUID, <<"fetching_server">>),
    Permission = [<<"hypervisors">>, {<<"res">>, <<"uuid">>}, <<"create">>],
    Type = case ft_dataset:type(Dataset) of
               kvm -> <<"kvm">>;
               zone -> <<"zone">>
           end,
    case ls_user:cache(Creator) of
        {ok, Permissions} ->
            Conditions0 = ft_package:requirements(Package)
                ++ ft_dataset:requirements(Dataset)
                ++ jsxd:get(<<"requirements">>, [], Config),
            Conditions1 = [{must, 'allowed', Permission, Permissions},
                           {must, 'element', <<"virtualisation">>, Type},
                           {must, '>=', <<"resources.free-memory">>, Ram}] ++
                lists:map(fun(C) -> make_condition(C, Permissions) end, Conditions0),
            Conditions2 = Conditions1 ++ GroupingRules,
            {UUID, Config, Conditions} = eplugin:fold('create:conditions', {UUID, Config, Conditions2}),
            lager:debug("[create] Finding hypervisor: ~p", [Conditions]),
            {ok, Hypervisors} = sniffle_hypervisor:list(Conditions, false),
            Hypervisors1 = eplugin:fold('create:hypervisor_select', Hypervisors),
            Hypervisors2 = lists:reverse(lists:sort(Hypervisors1)),
            lager:debug("[create] Hypervisors found: ~p", [Hypervisors2]),
            case {Hypervisors2, test_hypervisors(UUID, Hypervisors2, Nets)} of
                {_, {ok, HypervisorID, H, Nets1}} ->
                    RamB = list_to_binary(integer_to_list(Ram)),
                    S1 = add_log(State, info, <<"Assigning memory ", RamB/binary>>),
                    S2 = add_log(S1, info, <<"Deploying on hypervisor ", HypervisorID/binary>>),
                    eplugin:call('create:handoff', UUID, HypervisorID),
                    {next_state, get_ips,
                     S2#state{hypervisor = H, nets = Nets1}, 0};
                {[], _} ->
                    S1 = warn(State,
                              "Could not find Hypervisors matching rules.",
                              "[create] Cound not find hypervisor for "
                              "rules: ~p.", [Conditions]),
                    do_retry(S1);
                {Hvs, EH} ->
                    S1 = warn(State,
                              "loud not lock hypervisor.",
                              "[create] Cound claim a lock on any of "
                              "the provided hypervisors: ~p ->",
                              [Hvs, EH]),
                    do_retry(S1)
            end;
        EC ->
            S1 = warn(State,
                      "Cound not cache user.",
                      "[create] Cound lot cache user: ~p -> ~p",
                      [Creator, EC]),
            do_retry(S1)
    end.

get_ips(_Event, State = #state{nets = Nets,
                               uuid = UUID,
                               config = Config,
                               dataset = Dataset}) ->
    lager:debug("[create] get_ips: ~p", [Nets]),
    Nics0 = ft_dataset:networks(Dataset),
    case update_nics(UUID, Nics0, Config, Nets, State) of
        {error, E, Mapping} ->
            S1 = warn(State,
                      "Could not get IP's.",
                      "[create] Failed to get ips: ~p with mapping: ~p",
                      [E, Mapping]),
            [sniffle_iprange:release_ip(Range, IP) || {Range, IP} <- Mapping],
            do_retry(S1);
        {Nics1, Mapping} ->
            [sniffle_vm:add_network_map(UUID, IP, Range)
             || {Range, IP} <- Mapping],
            {next_state, build_key,
             State#state{mapping=Mapping, resulting_networks=Nics1},
             0}
    end.

build_key(_Event, State = #state{
                             creator = Creator,
                             config = Config}) ->
    case ls_user:keys(Creator) of
        {ok, []} ->
            {next_state, create, State, 0};
        {ok, Keys} ->
            KeysB = iolist_to_binary(merge_keys(Keys)),
            Config1 = jsxd:update([<<"ssh_keys">>],
                                  fun (Ks) ->
                                          <<KeysB/binary, Ks/binary>>
                                  end, KeysB, Config),
            {next_state, create, State#state{config = Config1}, 0}
    end.


create(_Event, State = #state{
                          mapping = Mapping,
                          uuid = UUID,
                          hypervisor = {Host, Port},
                          test_pid = {Pid, Ref}
                         }) ->
    [sniffle_iprange:release_ip(Range, IP) ||{Range, IP} <- Mapping],
    libchunter:release(Host, Port, UUID),
    Pid ! {Ref, success},
    {stop, normal, State};

create(_Event, State = #state{
                          dataset = Dataset,
                          package = Package,
                          uuid = UUID,
                          config = Config,
                          resulting_networks = Nics,
                          hypervisor = {Host, Port},
                          mapping = Mapping}) ->
    vm_log(State, <<"Handing off to hypervisor.">>),
    sniffle_vm:state(UUID, <<"creating">>),
    Package1 = ft_package:to_json(Package),
    Dataset1 = ft_dataset:to_json(Dataset),
    Dataset2 = jsxd:set(<<"networks">>, Nics, Dataset1),
    case libchunter:create_machine(Host, Port, UUID, Package1, Dataset2, Config) of
        {error, lock} ->
            [sniffle_vm:add_network_map(UUID, IP, Range)
             || {Range, IP} <- Mapping],

            [begin
                 sniffle_iprange:release_ip(Range, IP),
                 sniffle_vm:remove_network_map(UUID, IP)
             end || {Range, IP} <- Mapping],
            lager:warning("[create] Could not get log."),
            do_retry(State);
        ok ->
            {stop, normal, State}
    end.

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

terminate(_, create, _StateData) ->
    ok;

terminate(shutdown, _StateName, _StateData) ->
    ok;

terminate(_Reason, StateName, #state{hypervisor = {Host, Port},
                                     uuid = UUID,
                                     test_pid={Pid, Ref}}) ->
    libchunter:release(Host, Port, UUID),
    Pid ! {Ref, {failed, StateName}},
    ok;

terminate(_Reason, StateName, #state{test_pid={Pid, Ref}}) ->
    Pid ! {Ref, {failed, StateName}},
    ok;

terminate(Reason, StateName, State = #state{uuid = UUID, log_cache = C}) ->
    eplugin:call('create:fail', UUID, StateName),
    warn(State,
         "Creation failed.",
         "Hypervisor creation failed in state ~p for reason ~p",
         [Reason, StateName]),
    [vm_log(State, Type, Msg) || {Type, Msg} <- lists:reverse(C)],
    sniffle_vm:state(UUID, <<"failed">>),
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

merge_keys(Keys) ->
    [[Key, "\n"] || {_ID, Key} <- Keys].

test_net(Have, [{ID, Tag} | R]) ->
    lager:debug("[create] test_net: ~p ~p", [Have, [{ID, Tag} | R]]),
    case lists:member(Tag, Have) of
        true ->
            case sniffle_iprange:full(ID) of
                true ->
                    test_net(Have, R);
                false ->
                    ID
            end;
        _ ->
            test_net(Have, R)
    end;

test_net(_Have, []) ->
    lager:debug("[create] test_net: false"),
    false.

test_hypervisors(UUID, [{_, HypervisorID} | R], Nets) ->
    lager:debug("[create] test_hypervisors: ~p ~p",
                [HypervisorID, Nets]),
    {ok, H} = sniffle_hypervisor:get(HypervisorID),
    case test_hypervisor(UUID, ft_hypervisor:networks(H), Nets, []) of
        {ok, Nets1} ->
            {Host, Port} = ft_hypervisor:endpoint(H),
            case libchunter:lock(Host, Port, UUID) of
                ok ->
                    {ok, HypervisorID, {Host, Port}, Nets1};
                _ ->
                    test_hypervisors(UUID, R, Nets)
            end;
        _ ->
            test_hypervisors(UUID, R, Nets)
    end;

test_hypervisors(_, [], _) ->
    {error, no_hypervisors}.


test_hypervisor(UUID, H, [{NetName, Posibilities} | Nets], Acc) ->
    lager:debug("[create] test_hypervisor: ~p ~p ~p",
                [H, [{NetName, Posibilities} | Nets], Acc]),
    case test_net(H, Posibilities) of
        false ->
            false;
        ID ->
            test_hypervisor(UUID, H, Nets, [{NetName, ID} | Acc])
    end;

test_hypervisor(_UUID, _, [], Acc) ->
    {ok, Acc}.

make_condition(C, Permissions) ->
    case jsxd:get(<<"weight">>, <<"must">>, C) of
        <<"must">> ->
            make_rule(must, C, Permissions);
        <<"cant">> ->
            make_rule(cant, C, Permissions);
        <<"scale">> ->
            make_scale(scale, C);
        <<"random">> ->
            make_random(random, C);
        I when is_integer(I) ->
            make_rule(I, C, Permissions)
    end.

make_rule(Weight, C, Permissions) ->
    Condition = case jsxd:get(<<"condition">>, C) of
                    {ok, <<">=">>} -> '>=';
                    {ok, <<">">>} -> '>';
                    {ok, <<"=<">>} -> '=<';
                    {ok, <<"<">>} -> '<';
                    {ok, <<"=:=">>} -> '=:=';
                    {ok, <<"=/=">>} -> '=/=';
                    {ok, <<"subset">>} -> 'subset';
                    {ok, <<"superset">>} -> 'superset';
                    {ok, <<"disjoint">>} -> 'disjoint';
                    {ok, <<"element">>} -> 'element';
                    {ok, <<"allowed">>} -> 'allowed'
                end,
    {ok, Attribute} = jsxd:get(<<"attribute">>, C),
    case Condition of
        'allowed' ->
            {Weight, Condition, Attribute, Permissions};
        _ ->
            {ok, Value} = jsxd:get(<<"value">>, C),
            {Weight, Condition, Attribute, Value}
    end.

make_scale(Weight, C) ->
    {ok, Attribute} = jsxd:get(<<"attribute">>, C),
    {ok, Low} = jsxd:get(<<"low">>, C),
    {ok, High} = jsxd:get(<<"high">>, C),
    {Weight, Attribute, Low, High}.

make_random(Weight, C) ->
    {ok, Low} = jsxd:get(<<"low">>, C),
    {ok, High} = jsxd:get(<<"high">>, C),
    {Weight, Low, High}.


update_nics(UUID, Nics, Config, Nets, State) ->
    jsxd:fold(
      fun (_, _, {error, E, Mps}) ->
              {error, E, Mps};
          (K, Nic, {NicsF, Mappings}) ->
              {ok, Name} = jsxd:get(<<"name">>, Nic),
              {ok, NicTag} = jsxd:get(Name, Nets),
              vm_log(State, info, <<"Fetching network ", NicTag/binary, " for NIC ", Name/binary>>),
              case sniffle_iprange:claim_ip(NicTag) of
                  {ok, {Tag, IP, Net, Gw, VLAN}} ->
                      IPb = ft_iprange:to_bin(IP),
                      Netb = ft_iprange:to_bin(Net),
                      GWb = ft_iprange:to_bin(Gw),
                      vm_log(State, info,
                             <<"Assigning IP ", IPb/binary,
                               " netmask ", Netb/binary,
                               " gateway ", GWb/binary,
                               " tag ", Tag/binary>>),
                      Res = jsxd:from_list([{<<"nic_tag">>, Tag},
                                            {<<"ip">>, IPb},
                                            {<<"netmask">>, Netb},
                                            {<<"gateway">>, GWb}]),
                      Res1 = case VLAN of
                                 0 ->
                                     eplugin:apply(
                                       'vm:ip_assigned',
                                       [UUID, Config, Name, Tag, IPb, Netb, GWb, none]),
                                     Res;
                                 VLAN ->
                                     eplugin:apply(
                                       'vm:ip_assigned',
                                       [UUID, Config, Name, Tag, IPb, Netb, GWb, VLAN]),
                                     jsxd:set(<<"vlan_id">>, VLAN, Res)
                             end,
                      NicsF1 = jsxd:set(K, Res1, NicsF),
                      Mappings1 = [{NicTag, IP} | Mappings],
                      {NicsF1, Mappings1};
                  E ->
                      {error, E, Mappings}
              end
      end, {[], []}, Nics).

do_retry(State = #state{test_pid = undefined,
                        delay = Delay}) ->
    {next_state, get_networks, State, Delay};

do_retry(State) ->
    {stop, error, State}.

vm_log(#state{test_pid = {_,_}}, _)  ->
    ok;

vm_log(#state{uuid = UUID}, M)  ->
    sniffle_vm:log(UUID, M).

vm_log(#state{test_pid = {_,_}}, _, _)  ->
    ok;

vm_log(S, T, M) when is_list(M) ->
    vm_log(S, T, list_to_binary(M));

vm_log(State, info, M)  ->
    vm_log(State, <<"[info] ", M/binary>>);

vm_log(State, warning, M)  ->
    vm_log(State, <<"[warning] ", M/binary>>);

vm_log(State, error, M)  ->
    vm_log(State, <<"[error] ", M/binary>>);

vm_log(State, _, M)  ->
    vm_log(State, M).

add_log(State = #state{log_cache = C}, Type, Msg) ->
    State#state{log_cache = [{Type, Msg} | C]}.

add_log(State = #state{log_cache = C}, Type, Msg, EID) ->
    Msg1 = io_lib:format("~s Please see thw warning log for further details"
                         "the error id ~p will identify the entry.",
                         [Msg, EID]),
    State#state{log_cache = [{Type, Msg1} | C]}.

warn(State, Log, S, Fmt) ->
    EID = uuid:uuid4s(),
    lager:warning("[~s] " ++ S, [EID] ++ Fmt),
    add_log(State, error, Log, EID).
