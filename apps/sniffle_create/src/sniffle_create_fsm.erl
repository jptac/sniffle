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

%% API
-export([create/5,
         create/4,
         restore/5,
         start_link/6]).

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
         test_hypervisors/2,
         get_backup_package/2,
         finish_rules/2,
         prepare_create/2,
         prepare_backup/2,
         retry/2,
         generate_grouping_rules/2,
         get_package/2,
         check_org_resources/2,
         claim_org_resources/2,
         get_dataset/2,
         create/2,
         set_hostname/2,
         get_server/2,
         get_owner/2,
         create_permissions/2,
         write_accounting/2,
         get_ips/2,
         build_key/2,
         restore/2
        ]).

-define(SERVER, ?MODULE).

-ignore_xref([
              create/5,
              check_org_resources/2,
              claim_org_resources/2,
              get_backup_package/2,
              create/2,
              generate_grouping_rules/2,
              get_dataset/2,
              get_package/2,
              write_accounting/2,
              start_link/5,
              get_server/2,
              create_permissions/2,
              get_networks/2,
              get_ips/2,
              build_key/2,
              finish_rules/2,
              test_hypervisors/2
             ]).

-record(state, {
          test_pid                :: {pid(), reference()},
          uuid                    :: binary(),
          package                 :: ft_package:package(),
          package_uuid            :: binary(),
          dataset                 :: ft_dataset:dataset() | {docker, binary()},
          dataset_uuid            :: binary() | {docker, binary()},
          config                  :: term() | undefined,
          resulting_networks = [] :: [binary()],
          owner                   :: binary() | undefined,
          creator                 :: binary() | undefined,
          creator_obj             :: ft_org:org(),
          nets = []               :: [{binary(), {binary(), [binary()]}}],
          hypervisor              :: {string(), pos_integer()},
          hypervisor_id           :: binary(),
          mapping = []            :: [{binary(), binary(),
                                       binary(), integer()}],
          delay = 5000            :: pos_integer(),
          retry = 0               :: non_neg_integer(),
          max_retries = 1         :: pos_integer(),
          rules = []              :: [librankmatcher:rule()],
          log_cache = []          :: [{error | warning | info, binary()}],
          last_error              :: atom(),
          backup                  :: binary(),
          type = create           :: create | restore,
          grouping                :: binary() | undefined,
          backup_vm               :: ft_vm:vm() | undefined,
          permissions             :: [[binary()]],
          resources = []          :: [{binary(), integer()}],
          grouping_rules = []     :: [librankmatcher:rule()],
          hypervisors             :: [{integer(), binary()}]
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

start_link(create, UUID, Package, Dataset, Config, Pid) ->
    gen_fsm:start_link(
      ?MODULE, [create, UUID, Package, Dataset, Config, Pid], []);

start_link(restore, UUID, BackupID, Requirements, Package, Creator) ->
    gen_fsm:start_link(
      ?MODULE, [restore, UUID, BackupID, Requirements, Package, Creator], []).

create(UUID, Package, Dataset, Config) ->
    create(UUID, Package, Dataset, Config, undefined).

create(UUID, Package, Dataset, Config, Pid) ->
    supervisor:start_child(sniffle_create_fsm_sup,
                           [create, UUID, Package, Dataset, Config, Pid]).

restore(UUID, BackupID, Requirements, Package, Creator) ->
    supervisor:start_child(
      sniffle_create_fsm_sup,
      [restore, UUID, BackupID, Requirements, Package, Creator]).


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

%% We are restoring a backup
init([restore, UUID, BackupID, Rules, Package, Creator]) ->
    lager:debug("[create] initialiing restore ~s ~s ~p ~s ~s",
                [UUID, BackupID, Rules, Package, Creator]),
    sniffle_vm:state(UUID, <<"restoring">>),
    random:seed(erlang:phash2([node()]),
                erlang:monotonic_time(),
                erlang:unique_integer()),
    lager:info("[create] Starting FSM for ~s", [UUID]),
    process_flag(trap_exit, true),
    {ok, Permissions} = ls_user:cache(Creator),
    Rules1 = lists:map(
               fun(Rule) -> make_condition(Rule, Permissions) end,
               Rules),
    Delay = case application:get_env(sniffle, create_retry_delay) of
                {ok, D} ->
                    D;
                _ ->
                    5000
            end,
    MaxRetries = case application:get_env(sniffle, create_max_retries) of
                     {ok, D1} ->
                         D1;
                     _ ->
                         5
                 end,

    next(),
    {ok, prepare_backup, #state{
                            package_uuid = Package,
                            creator = Creator,
                            permissions = Permissions,
                            uuid = UUID,
                            rules = Rules1,
                            backup = BackupID,
                            type = restore,
                            delay = Delay,
                            max_retries = MaxRetries
                           }};


init([create, UUID, Package, Dataset, Config, Pid]) ->
    lager:debug("[create] initialiing create ~s ~s ~s ~p ~p",
                [UUID, Package, Dataset, Config, Pid]),
    sniffle_vm:state(UUID, <<"placing">>),
    sniffle_vm:creating(UUID, {creating, erlang:system_time(seconds)}),
    random:seed(erlang:phash2([node()]),
                erlang:monotonic_time(),
                erlang:unique_integer()),
    lager:info("[create] Starting FSM for ~s", [UUID]),
    process_flag(trap_exit, true),
    Config1 = jsxd:from_list(Config),
    Delay = case application:get_env(sniffle, create_retry_delay) of
                {ok, D} ->
                    D;
                _ ->
                    5000
            end,
    MaxRetries = case application:get_env(sniffle, create_max_retries) of
                     {ok, D1} ->
                         D1;
                     _ ->
                         5
                 end,
    next(),
    {ok, prepare_create, #state{
                            uuid = UUID,
                            package_uuid = Package,
                            dataset_uuid = Dataset,
                            config = Config1,
                            delay = Delay,
                            max_retries = MaxRetries,
                            test_pid = Pid
                           }}.

prepare_create(_Event, State = #state{config = Config}) ->
    G = case jsxd:get([<<"grouping">>], Config) of
            {ok, V} -> V;
            _ -> undefined
        end,
    {ok, Creator} = jsxd:get([<<"owner">>], Config),
    {ok, Permissions} = ls_user:cache(Creator),

    Rules = lists:map(fun(Rule) -> make_condition(Rule, Permissions) end,
                      jsxd:get(<<"requirements">>, [], Config)),
    next(),
    State1 = State#state{
               permissions = Permissions,
               creator = Creator,
               rules = Rules,
               grouping = G
              },
    {next_state, get_owner, State1}.

prepare_backup(_Event, State = #state{uuid = UUID}) ->
    lager:debug("[create] prepare backup"),
    {ok, V} = sniffle_vm:get(UUID),
    Dataset = ft_vm:dataset(V),
    Dataset = ft_vm:dataset(V),
    Owner = ft_vm:owner(V),
    State1 = State#state{
               backup_vm = V,
               owner = Owner,
               dataset_uuid = Dataset},
    next(),
    {next_state, get_backup_package, State1}.

get_backup_package(_Event, State = #state{package_uuid = undefined,
                                          backup_vm = V}) ->
    lager:debug("[create] get backup package from VM"),
    Package = ft_vm:package(V),
    State1 = State#state{package_uuid = Package},
    next(),
    {next_state, get_package, State1};

get_backup_package(_Event, State) ->
    lager:debug("[create] get backup package was passed"),
    next(),
    {next_state, get_package, State}.

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

get_owner(_Event, State = #state{config = Config, owner = undefined,
                                 creator = Creator}) ->
    lager:debug("[create] get owner"),
    {ok, C} = ls_user:get(Creator),
    Owner = ft_user:active_org(C),
    case Owner of
        <<"">> ->
            vm_log(State, warning, <<"No owner">>),
            lager:warning("[create] User ~p has no active org.",
                          [Creator]);
        _ ->
            lager:info("[create] User ~p has active org: ~p.",
                       [Creator, Owner])
    end,
    Config1 = jsxd:set([<<"owner">>], Owner, Config),
    next(),
    {next_state, get_package,
     State#state{
       config = Config1,
       creator = Creator,
       creator_obj = C,
       owner = Owner
      }};

get_owner(_, State) ->
    lager:debug("[create] get owner (was already set)"),
    next(),
    {next_state, get_package, State}.


%% We delay setting the package so we can be sure that only when
%% package resources were claimed a package is actually set.
%% The reason for that is that when deleting a we want to ensure
%% that the resoruces were actually claimed when releasing them agian
%% the easiest way of doing this is to only set the package when claiming
%% the resources
get_package(_Event, State = #state{
                               uuid = UUID,
                               package_uuid = PackageUUID,
                               rules = Rules
                              }) ->
    lager:debug("[create] Fetching Package: ~p", [PackageUUID]),
    vm_log(State, info, <<"Fetching package ", PackageUUID/binary>>),
    sniffle_vm:state(UUID, <<"fetching_package">>),
    {ok, Package} = sniffle_package:get(PackageUUID),
    Rules1 = ft_package:requirements(Package),
    next(),
    {next_state, check_org_resources,
     State#state{package = Package, rules = Rules ++ Rules1}}.

check_org_resources(_Event, State = #state{owner = <<>>, package = P}) ->
    lager:debug("[create] Checking resources (no owner)"),
    case maps:size(ft_package:org_resources(P)) of
        0 ->
            lager:debug("[create] No resources required by package"),
            next(),
            {next_state, claim_org_resources, State};
        _ ->
            vm_log(State, error,
                   <<"No org selected but package requiresresource!">>),
            {stop, failed, State}
    end;

check_org_resources(_Event, State = #state{owner = OrgID, package = P}) ->
    Res  = ft_package:org_resources(P),
    lager:debug("[create] Checking resources: ~p", [Res]),
    {ok, Org} = ls_org:get(OrgID),
    Ok = maps:fold(fun(R, V, true) ->
                           case ft_org:resource(Org, R) of
                               {ok, V1} when V1 >= V -> true;
                               _ -> {R, V}
                           end;
                      (_, _, R) -> R
                   end, true, Res),
    case Ok of
        true ->
            next(),
            {next_state, claim_org_resources, State};
        {R, V} ->
            lager:debug("[create] Resource '~p' insuficient with ~p.",
                        [R, V]),
            vm_log(State, error,
                   <<"Org cant provide resource : ", R/binary, "!">>),
            {stop, failed, State}
    end.

%% We don't claim resources if there is no owner
claim_org_resources(_Event, State = #state{uuid = UUID, owner = <<>>,
                                           package_uuid = PackageUUID}) ->
    lager:debug("[create] claim resources (no owner)"),
    sniffle_vm:package(UUID, PackageUUID),
    next(),
    {next_state, create_permissions, State};

%% We don't claim resources if this is a test run,
%% we can also skip create_permissions.
claim_org_resources(_Event, State = #state{uuid = UUID, test_pid = {_, _},
                                           package_uuid = PackageUUID}) ->
    lager:debug("[create] claim resources (test)"),
    sniffle_vm:package(UUID, PackageUUID),
    next(),
    {next_state, get_dataset, State};

%% Now we calim resoruces
claim_org_resources(_Event, State = #state{uuid = UUID,
                                           owner = OrgID, package = P,
                                           package_uuid = PackageUUID}) ->
    lager:debug("[create] claim resources"),
    Res = ft_package:org_resources(P),
    [ls_org:resource_dec(OrgID, R, V)  || {R, V} <- maps:to_list(Res)],
    sniffle_vm:package(UUID, PackageUUID),
    next(),
    {next_state, create_permissions, State}.

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

%% We can skip this for backups
create_permissions(_Event, State = #state{type = restore}) ->
    lager:debug("[create] create permissions (backup)"),
    next(),
    {next_state, get_dataset, State};

create_permissions(_Event, State = #state{
                                      uuid = UUID,
                                      creator = Creator,
                                      owner = Owner
                                     }) ->
    lager:debug("[create] create permissions"),
    ls_user:grant(Creator, [<<"vms">>, UUID, <<"...">>]),
    ls_user:grant(Creator, [<<"channels">>, UUID, <<"join">>]),
    case Owner of
        <<>> ->
            ok;
        _ ->
            sniffle_vm:owner(UUID, Owner),
            ls_org:execute_trigger(Owner, vm_create, UUID)
    end,
    libhowl:send(UUID, #{<<"event">> => <<"update">>,
                         <<"data">> => #{<<"owner">> => Owner}}),
    next(),
    {next_state, write_accounting, State}.

%% If there is no owner we don't need to add triggers
write_accounting(_Event, State = #state{owner = <<>>}) ->
    lager:debug("[create] write accounting (no owner)"),
    next(),
    {next_state, get_dataset, State};

write_accounting(_Event, State = #state{
                                    uuid = UUID,
                                    package_uuid = Package,
                                    dataset_uuid = Dataset,
                                    creator = Creator,
                                    owner = Org
                                   }) ->
    lager:debug("[create] write accounting"),
    ls_acc:create(Org, UUID, sniffle_vm:timestamp(),
                  [{user, Creator},
                   {package, Package},
                   {dataset, encode_dataset(Dataset)}]),
    next(),
    {next_state, get_dataset, State}.

get_dataset(_Event, State = #state{
                               dataset_uuid = {docker, DockerImage}
                              }) ->
    lager:debug("[create] Oh My this is a docker image: ~p", [DockerImage]),
    next(),
    {next_state, finish_rules, State#state{dataset = {docker, DockerImage}}};

get_dataset(_Event, State = #state{
                               uuid = UUID,
                               dataset_uuid = DatasetName,
                               rules = Rules
                              }) ->
    lager:debug("[create] Fetching Dataset: ~p", [DatasetName]),
    vm_log(State, info, <<"Fetching dataset ", DatasetName/binary>>),
    sniffle_vm:state(UUID, <<"fetching_dataset">>),
    {ok, Dataset} = sniffle_dataset:get(DatasetName),
    sniffle_vm:dataset(UUID, DatasetName),
    Rules1 = ft_dataset:requirements(Dataset),
    next(),
    {next_state, finish_rules,
     State#state{dataset = Dataset, rules = Rules ++ Rules1 }}.

finish_rules(_Event, State = #state{
                                dataset = Dataset,
                                uuid = UUID,
                              package = Package,
                                permissions = Permissions,
                                rules = Rules}) ->
    lager:debug("[create] finish srules"),
    Ram = ft_package:ram(Package),
    sniffle_vm:state(UUID, <<"fetching_server">>),
    Permission = [<<"hypervisors">>, {<<"res">>, <<"uuid">>}, <<"create">>],
    Type
        = case Dataset of
              {docker, _} ->
                  lager:warning("[TODO] We need some dataset reqs for lx"),
                  <<"zone">>;
              _ ->
                  TZT = {ft_dataset:type(Dataset),
                         ft_dataset:zone_type(Dataset)},
                  case TZT of
                      {kvm, _}      -> <<"kvm">>;
                      {zone, lipkg} -> <<"ipkg">>;
                      {zone, ipkg}  -> <<"ipkg">>;
                      {zone, _}     -> <<"zone">>
                  end
          end,
    Rules1 = [{must, 'allowed', Permission, Permissions},
              {must, 'element', <<"virtualisation">>, Type},
              {must, '>=', <<"resources.free-memory">>, Ram}
              | Rules],
    Rules2 = case State#state.backup_vm of
                 undefined ->
                     Rules1;
                 VM ->
                     Mappings = ft_vm:iprange_map(VM),
                     Networks = [sniffle_iprange:get(N)
                                 || N <- maps:values(Mappings)],
                     Networks1 = [ft_iprange:tag(N) || {ok, N} <- Networks],
                     Networks2 = ordsets:from_list(Networks1),
                     [{must, 'subset', <<"networks">>, Networks2}
                      | Rules1]
             end,
    next(),
    {next_state, generate_grouping_rules,
     State#state{rules = Rules2}}.


retry(_event, State = #state{retry = R, max_retries = Max})
  when R >= Max ->
    lager:error("[create] Failed after too many retries: ~p > ~p",
                [R, Max]),
    BR = integer_to_binary(R),
    BMax= integer_to_binary(Max),
    vm_log(State, error, <<"Failed after too many retries: ", BR/binary, " > ",
                           BMax/binary,
                           ".">>),
    {stop, failed, State};

retry(_Event, State = #state{retry = Try}) ->
    lager:debug("[create] retry #~p", [Try + 1]),
    next(),
    {next_state, generate_grouping_rules,
     State#state{retry = Try + 1, log_cache = []}}.


generate_grouping_rules(_Event, State = #state{grouping = undefined}) ->
    lager:debug("[create] generate grouping rules (no grouping)"),
    next(),
    {next_state, get_networks, State};

generate_grouping_rules(_Event, State = #state{test_pid = {_, _},
                                               grouping = Grouping}) ->
    lager:debug("[create] generate grouping rules (test)"),
    Rules = sniffle_grouping:create_rules(Grouping),
    next(),
    {next_state, get_networks, State#state{grouping_rules = Rules}};

generate_grouping_rules(_Event, State = #state{
                                           uuid = UUID,
                                           grouping = Grouping
                                          }) ->
    lager:debug("[create] generate grouping rules"),
    Rules = sniffle_grouping:create_rules(Grouping),
    case sniffle_grouping:add_element(Grouping, UUID) of
        ok ->
            sniffle_vm:add_grouping(UUID, Grouping),
            next(),
            State1 = State#state{grouping_rules = Rules},
            {next_state, get_networks, State1};
        E ->
            vm_log(State, error, "Failed to create routing rule."),
            lager:error("[create] Creation Faild since grouing could "
                        "not be joined: ~p", [E]),
            {stop, E, State}
    end.

%% We are restoring so we do not need thos whole shabang.
get_networks(_Event, State = #state{type = restore}) ->
    lager:debug("[create] get_networks (backups)"),
    next(),
    {next_state, get_server,
     State#state{nets = []}};

get_networks(_Event, State = #state{config = Config}) ->
    lager:debug("[create] get_networks"),
    Nets = jsxd:get([<<"networks">>], [], Config),
    Nets1 = lists:map(fun({Name, Network}) ->
                              {ok, N} = sniffle_network:get(Network),
                              Rs = ft_network:ipranges(N),
                              Rs1 = [{R, sniffle_iprange:get(R)} || R <- Rs],
                              Rs2 = [{R, D} || {R, {ok, D}} <- Rs1],
                              Rs3 = [{IPRange, ft_iprange:tag(R)}
                                     || {IPRange, R} <- Rs2],
                              Free = [length(ft_iprange:free(R)) ||
                                         {_, {ok, R}} <- Rs1],
                              Sum = lists:sum(Free),
                              {Name, {Network, Rs3, Sum}}
                      end, Nets),
    Nets2 = [{Name, {Network, Rs}} ||
                {Name, {Network, Rs, _Sum}}  <- Nets1],
    State1 = lists:foldl(
               fun ({Name, {Network, Rs, Sum}}, SAcc) ->
                       Count = length(Rs),
                       fmt_log(SAcc, info, "[net:~s/~s] Fund ~p free ip "
                               "addresses in ~p ipranges.",
                               [Name, Network, Sum, Count])
               end, State, Nets1),
    next(),
    {next_state, get_server,
     State1#state{nets = Nets2}}.

get_server(_Event, State = #state{
                              uuid = UUID,
                              rules = Rules,
                              grouping_rules = GRules}) ->
    lager:debug("[create] get_server"),
    FinalRules = Rules ++ GRules,
    sniffle_vm:state(UUID, <<"fetching_server">>),
    lager:debug("[create] Finding hypervisor: ~p", [FinalRules]),
    {ok, Hypervisors} = sniffle_hypervisor:list(FinalRules, false),
    Hypervisors1 = lists:reverse(lists:sort(Hypervisors)),
    lager:debug("[create] Hypervisors found: ~p", [Hypervisors1]),
    State1 = fmt_log(State, info, "[hypervisors] Found a total of ~p "
                     "hypervisors matching the required rules.",
                     [length(Hypervisors1)]),
    next(),
    {next_state, test_hypervisors,
     State1#state{hypervisors = Hypervisors1}}.

test_hypervisors(_Event, State = #state{
                                    hypervisors = Hypervisors,
                                    uuid = UUID,
                                    nets = Nets,
                                    package = Package,
                                    rules = Rules,
                                    grouping_rules = GRules}) ->
    lager:debug("[create] test_hypervisors: ~p", [Nets]),
    FinalRules = Rules ++ GRules,
    Ram = ft_package:ram(Package),
    case {Hypervisors, test_hypervisors(UUID, Hypervisors, Nets, State)} of
        {_, {ok, HypervisorID, H, Nets1, State1}} ->
            RamB = list_to_binary(integer_to_list(Ram)),
            S1 = add_log(State1, info,
                         <<"Assigning memory ", RamB/binary>>),
            S2 = add_log(S1, info, <<"Deploying on hypervisor ",
                                     HypervisorID/binary>>),
            next(),
            {next_state, get_ips,
             S2#state{hypervisor_id = HypervisorID,
                      hypervisor = H, nets = Nets1}};
        {[], _} ->
            S1 = warn(State,
                      "Could not find Hypervisors matching rules.",
                      "[create] Could not find hypervisor for "
                      "rules: ~p.", [FinalRules]),
            do_retry(S1);
        {Hvs, {error, EH, State1}} ->
            S1 = warn(State1,
                      "could not lock hypervisor.",
                      "[create] Could not claim a lock on any of "
                      "the provided hypervisors: ~p -> ~p",
                      [Hvs, EH]),
            do_retry(S1)
    end.


get_ips(_Event, State = #state{type = restore}) ->
    lager:debug("[create] get ips (restore)"),
    next(),
    {next_state, restore, State#state{}};

get_ips(_Event, State = #state{nets = Nets,
                               uuid = UUID,
                               dataset = Dataset}) ->
    lager:debug("[create] get ips: ~p", [Nets]),
    Nics0 = case Dataset of
                {docker, _} ->
                    lager:warning("[TODO] Need to figure out docker nics"),
                    [{K, <<"docker">>} || {K, _} <- Nets];
                _ ->
                    ft_dataset:networks(Dataset)
            end,
    case update_nics(Nics0, Nets, State) of
        {error, E, Mapping} ->
            S1 = warn(State,
                      "Could not get IP's.",
                      "[create] Failed to get ips: ~p with mapping: ~p",
                      [E, Mapping]),
            [sniffle_iprange:release_ip(Range, IP)
             || {Range, _, IP} <- Mapping],
            do_retry(S1);
        {Nics1, Mapping} ->
            [begin
                 sniffle_vm:add_iprange_map(UUID, IP, Range),
                 sniffle_vm:add_network_map(UUID, IP, Network)
             end
             || {_Name, Range, Network, IP} <- Mapping],
            next(),
            {next_state, build_key,
             State#state{mapping=Mapping, resulting_networks=Nics1}}
    end.

build_key(_Event, State = #state{
                             config = Config,
                             creator_obj = User}) ->
    lager:debug("[create] build keys"),
    Keys = ft_user:keys(User),
    KeysB = iolist_to_binary(merge_keys(Keys)),
    Config1 = jsxd:update([<<"ssh_keys">>],
                          fun (Ks) ->
                                  <<KeysB/binary, Ks/binary>>
                          end, KeysB, Config),
    next(),
    {next_state, set_hostname, State#state{config = Config1}}.

restore(_Event, State = #state{
                           uuid = UUID,
                           hypervisor_id = HypervisorID,
                           hypervisor = {Host, Port},
                           backup = BID
                          }) ->
    lager:debug("[create] restore"),
    {ok, {S3Host, S3Port, AKey, SKey, Bucket}} =
        sniffle_s3:config(snapshot),
    sniffle_vm:hypervisor(UUID, HypervisorID),
    libchunter:restore_backup(Host, Port, UUID, BID, S3Host,
                              S3Port, Bucket, AKey, SKey),
    {stop, normal, State}.

%% We don't set hostnames when we only do a dry run
set_hostname(_Event, State = #state{
                                test_pid = {_Pid, _Ref}
                               }) ->
    next(),
    {next_state, create, State};

set_hostname(_Event, State = #state{
                                mapping = Mapping,
                                config = Config,
                                uuid = UUID
                               }) ->
    [case jsxd:get([<<"hostnames">>, Name], <<>>, Config) of
         <<>> -> ok;
         Hostname ->
             sniffle_vm:add_hostname_map(UUID, IP, Hostname)
     end
     || {Name, _Range, _Network, IP} <- Mapping],
    next(),
    {next_state, create, State}.


create(_Event, State = #state{
                          mapping = Mapping,
                          uuid = UUID,
                          hypervisor = {Host, Port},
                          test_pid = {Pid, Ref}
                         }) ->
    [sniffle_iprange:release_ip(Range, IP)
     || {_Name, Range, _Network, IP} <- Mapping],
    libchunter:release(Host, Port, UUID),
    Pid ! {Ref, success},
    sniffle_vm:creating(UUID, false),
    {stop, normal, State};

create(_Event, State = #state{
                          dataset = Dataset,
                          package = Package,
                          uuid = UUID,
                          config = Config,
                          resulting_networks = Nics,
                          hypervisor_id = HID,
                          hypervisor = {Host, Port},
                          mapping = Mapping}) ->
    vm_log(State, <<"Handing off to hypervisor ", HID/binary, ".">>),
    Config1 = jsxd:set(<<"nics">>, Nics, Config),
    case
        libchunter:create_machine(Host, Port, UUID, Package, Dataset, Config1)
    of
        {error, _} ->
            %% TODO is it a good idea to handle all errors like this?
            %% How can we assure no creation was started?
            [begin
                 sniffle_iprange:release_ip(Range, IP),
                 sniffle_vm:remove_iprange_map(UUID, IP),
                 sniffle_vm:remove_network_map(UUID, IP)
             end || {_Name, Range, _Network, IP} <- Mapping],
            lager:warning("[create] Could not get lock."),
            do_retry(State);
        ok ->
            sniffle_vm:hypervisor(UUID, HID),
            sniffle_vm:creating(UUID,
                                {hypervisor, erlang:system_time(seconds)}),
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

terminate(normal, create, _StateData) ->
    ok;

terminate(shutdown, _StateName, _StateData) ->
    ok;

terminate(_Reason, StateName, #state{hypervisor = {Host, Port},
                                     uuid = UUID,
                                     test_pid={Pid, Ref}}) ->
    libchunter:release(Host, Port, UUID),
    Pid ! {Ref, {failed, StateName}},
    sniffle_vm:creating(UUID, false),
    ok;

terminate(_Reason, StateName, #state{test_pid={Pid, Ref}, uuid = UUID}) ->
    Pid ! {Ref, {failed, StateName}},
    sniffle_vm:creating(UUID, false),
    ok;

terminate(Reason, StateName, State = #state{uuid = UUID, log_cache = C}) ->
    warn(State,
         "Creation failed.",
         "Hypervisor creation failed in state ~p for reason ~p",
         [Reason, StateName]),
    [vm_log(State, Type, Msg) || {Type, Msg} <- lists:reverse(C)],
    sniffle_vm:state(UUID, <<"failed">>),
    sniffle_vm:creating(UUID, false),
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

test_net(Have, [{ID, Tag } | R]) ->
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

test_hypervisors(UUID, [{_, HypervisorID} | R], Nets, State) ->
    lager:debug("[create] test_hypervisors: ~p ~p",
                [HypervisorID, Nets]),
    {ok, H} = sniffle_hypervisor:get(HypervisorID),
    case test_hypervisor(UUID, ft_hypervisor:networks(H), Nets, []) of
        {ok, Nets1} ->
            State1 = fmt_log(State, info, "[hypervisor:~s] Found ~p valid "
                             "networks.", [HypervisorID, length(Nets1)]),
            {Host, Port} = ft_hypervisor:endpoint(H),
            case libchunter:lock(Host, Port, UUID) of
                ok ->
                    State2 = fmt_log(State1, info,
                                     "[hypervisor:~s] locked.",
                                     [HypervisorID]),
                    {ok, HypervisorID, {Host, Port}, Nets1, State2};
                _ ->
                    State2 = fmt_log(State1, warning,
                                     "[hypervisor:~s] could not be lock.",
                                     [HypervisorID]),
                    test_hypervisors(UUID, R, Nets, State2)
            end;
        _ ->
            State1 = fmt_log(State, warning, "[hypervisor:~p] Found no valid "
                             "networks.", [HypervisorID]),
            test_hypervisors(UUID, R, Nets, State1)
    end;

test_hypervisors(_, [], _, State) ->
    {error, no_hypervisors, State}.


test_hypervisor(UUID, H, [{NetName, {Network, Posibilities}} | Nets], Acc) ->
    lager:debug("[create] test_hypervisor: ~p ~p ~p",
                [H, [{NetName, Posibilities} | Nets], Acc]),
    case test_net(H, Posibilities) of
        false ->
            false;
        ID ->
            test_hypervisor(UUID, H, Nets, [{NetName, {Network, ID}} | Acc])
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

-spec update_nics(term(), term(), term()) ->
                         {error, term(), [{binary(), binary(), integer()}]} |
                         {[[{binary(), term()}]],
                          [{binary(), binary(), binary(), integer()}]}.
update_nics(Nics, Nets, State) ->
    lists:foldl(
      fun (_, {error, E, Mps}) ->
              {error, E, Mps};
          ({Name, _Desc}, {NicsF, Mappings}) ->
              {Name, {Network, NicTag}} = lists:keyfind(Name, 1, Nets),
              vm_log(State, info, <<"Fetching network ", NicTag/binary,
                                    " for NIC ", Name/binary>>),
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
                                            {<<"network_uuid">>, NicTag},
                                            {<<"netmask">>, Netb},
                                            {<<"gateway">>, GWb}]),
                      Res1 = add_vlan(VLAN, Res),
                      NicsF1 = [Res1 | NicsF],
                      Mappings1 = [{Name, NicTag, Network, IP} | Mappings],
                      {NicsF1, Mappings1};
                  E ->
                      {error, E, Mappings}
              end
      end, {[], []}, Nics).

add_vlan(0, Res) ->
    Res;
add_vlan(VLAN, Res) ->
    jsxd:set(<<"vlan_id">>, VLAN, Res).

do_retry(State = #state{test_pid = undefined,
                        delay = Delay}) ->
    timer:sleep(Delay),
    next(),
    {next_state, retry, State};

do_retry(State) ->
    {stop, error, State}.

vm_log(#state{test_pid = {_, _}}, _)  ->
    ok;

vm_log(#state{uuid = UUID}, M) when is_binary(M) ->
    sniffle_vm:log(UUID, M).

vm_log(#state{test_pid = {_, _}}, _, _)  ->
    ok;

vm_log(S, T, M) when is_list(M) ->
    vm_log(S, T, list_to_binary(M));

vm_log(State, info, M)  ->
    vm_log(State, <<"[info] ", M/binary>>);

vm_log(State, warning, M)  ->
    vm_log(State, <<"[warning] ", M/binary>>);

vm_log(State, error, M)  ->
    vm_log(State, <<"[error] ", M/binary>>).

fmt_log(State, Type, Msg, Args) ->
    add_log(State, Type, list_to_binary(io_lib:format(Msg, Args))).

add_log(State = #state{log_cache = C}, Type, Msg)
  when is_binary(Msg),
       (Type =:= info orelse
        Type =:= warning orelse
        Type =:= error) ->
    State#state{log_cache = [{Type, Msg} | C]}.

add_log(State = #state{log_cache = C}, Type, Msg, EID)
  when
      is_binary(EID),
      (is_binary(Msg) orelse is_list(Msg)),
      (Type =:= info orelse
       Type =:= warning orelse
       Type =:= error) ->
    Msg1 = io_lib:format("~s | Please see the warning log for further details "
                         "the error id is ~s which will identify the entry.",
                         [Msg, EID]),
    State#state{log_cache = [{Type, list_to_binary(Msg1)} | C]}.

warn(State, Log, S, Fmt) when
      is_list(Log), is_list(S), is_list(Fmt) ->
    EID = fifo_utils:uuid(),
    lager:warning("[~s] " ++ S, [EID] ++ Fmt),
    add_log(State, warning, Log, EID).

encode_dataset({docker, D}) ->
    <<"docker:", D/binary>>;

encode_dataset(D) when is_binary(D) ->
    D.

next() ->
    gen_fsm:send_event(self(), next).
