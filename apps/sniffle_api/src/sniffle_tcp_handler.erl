-module(sniffle_tcp_handler).

-export([init/2, message/2, raw/2]).

-ignore_xref([init/2, message/2, raw/2]).

-define(HM(M),
        message({hypervisor, M, Hypervisor, V}, State) when
      is_binary(Hypervisor) ->
               {reply,
                sniffle_hypervisor:M(Hypervisor, V),
                State}).

-define(VM(M),
        message({vm, M, VM, V}, State) when
      is_binary(VM) ->
               {reply,
                sniffle_vm:M(VM, V),
                State}).

-define(DSM(M),
        message({dataset, M, VM, V}, State) when
      is_binary(VM) ->
               {reply,
                sniffle_dataset:M(VM, V),
                State}).
-define(DTM(M),
        message({dtrace, M, VM, V}, State) when
      is_binary(VM) ->
               {reply,
                sniffle_dtrace:M(VM, V),
                State}).


-define(IPM(M),
        message({iprange, M, VM, V}, State) when
      is_binary(VM) ->
               {reply,
                sniffle_iprange:M(VM, V),
                State}).

-define(NM(M),
        message({network, M, VM, V}, State) when
      is_binary(VM) ->
               {reply,
                sniffle_network:M(VM, V),
                State}).

-define(PM(M),
        message({package, M, VM, V}, State) when
      is_binary(VM) ->
               {reply,
                sniffle_package:M(VM, V),
                State}).

-record(state, {port}).

init(Prot, []) ->
    {ok, #state{port = Prot}}.

%%%===================================================================
%%%  RAW Functions
%%%===================================================================


raw({dtrace, _} = R, State) ->
    State#state.port ! {data, R},
    {ok, State};

raw(_Data, State) ->
    {ok, State}.


%%%===================================================================
%%%  General Functions
%%%===================================================================

-type message() ::
        fifo:sniffle_message() |
        fifo:sniffle_dataset_message() |
        fifo:sniffle_dtrace_message() |
        fifo:sniffle_grouping_message() |
        fifo:sniffle_hypervisor_message() |
        fifo:sniffle_iprange_message() |
        fifo:sniffle_network_message() |
        fifo:sniffle_package_message() |
        fifo:sniffle_vm_message().

-spec message(message(), any()) -> any().

message(version, State) ->
    {reply, {ok, sniffle_version:v()}, State};

message({s3, Type}, State) ->
    %% {ok,{S3Host, S3Port, AKey, SKey, Bucket}}
    {reply, sniffle_s3:config(Type), State};

%%%===================================================================
%%%  Grouping Functions
%%%===================================================================

message({grouping, add, Name, cluster}, State) ->
    {reply,
     sniffle_grouping:create(Name, cluster),
     State};

message({grouping, add, Name, stack}, State) ->
    {reply,
     sniffle_grouping:create(Name, stack),
     State};

message({grouping, add, Name, none}, State) ->
    {reply,
     sniffle_grouping:create(Name, none),
     State};

message({grouping, add, _, _}, State) ->
    {reply,
     {error, unsupported_type},
     State};

message({grouping, delete, ID}, State) ->
    {reply,
     sniffle_grouping:delete(ID),
     State};

message({grouping, get, ID}, State) ->
    {reply,
     sniffle_grouping:get(ID),
     State};

message({grouping, list}, State) ->
    {reply,
     sniffle_grouping:list(),
     State};

message({grouping, list, Requreiments}, State) ->
    message({grouping, list, Requreiments, false}, State);

message({grouping, list, Requreiments, Full}, State) ->
    {reply,
     sniffle_grouping:list(Requreiments, Full),
     State};

message({grouping, stream, Requirements}, State) when
      is_list(Requirements) ->
    Fn = fun(Send) ->
                 Fold = fun(Es, _) ->
                                Send(Es)
                        end,
                 sniffle_grouping:list(Requirements, Fold, ok)
         end,
    {stream, Fn, State};

message({grouping, element, add, ID, Element}, State) when
      is_binary(ID) ->
    {reply,
     sniffle_grouping:add_element(ID, Element),
     State};

message({grouping, element, remove, ID, Element}, State) when
      is_binary(ID) ->
    {reply,
     sniffle_grouping:remove_element(ID, Element),
     State};

message({grouping, grouping, add, ID, Grouping}, State) when
      is_binary(ID) ->
    {reply,
     sniffle_grouping:add_grouping(ID, Grouping),
     State};

message({grouping, grouping, remove, ID, Grouping}, State) when
      is_binary(ID) ->
    {reply,
     sniffle_grouping:remove_grouping(ID, Grouping),
     State};

message({grouping, metadata, set, ID, Attributes}, State) when
      is_binary(ID),
      is_list(Attributes) ->
    {reply,
     sniffle_grouping:set_metadata(ID, Attributes),
     State};

message({grouping, config, set, ID, Attributes}, State) when
      is_binary(ID),
      is_list(Attributes) ->
    {reply,
     sniffle_grouping:set_config(ID, Attributes),
     State};

%%%===================================================================
%%%  DTrace Functions
%%%===================================================================

message({dtrace, add, Name, Script}, State) ->
    {reply,
     sniffle_dtrace:add(Name, Script),
     State};

message({dtrace, delete, ID}, State) ->
    {reply,
     sniffle_dtrace:delete(ID),
     State};

message({dtrace, get, ID}, State) ->
    {reply,
     sniffle_dtrace:get(ID),
     State};

message({dtrace, list}, State) ->
    {reply,
     sniffle_dtrace:list(),
     State};

message({dtrace, list, Requreiments}, State) ->
    message({dtrace, list, Requreiments, false}, State);

message({dtrace, list, Requreiments, Full}, State) ->
    {reply,
     sniffle_dtrace:list(Requreiments, Full),
     State};

message({dtrace, stream, Requirements}, State) when
      is_list(Requirements) ->
    Fn = fun(Send) ->
                 Fold = fun(Es, _) ->
                                Send(Es)
                        end,
                 sniffle_dtrace:list(Requirements, Fold, ok)
         end,
    {stream, Fn, State};


message({dtrace, run, ID, Servers}, State) ->
    {ok, _Pid} = sniffle_dtrace_server:run(ID, Servers, self()),
    {claim, State};

?DTM(name);
?DTM(uuid);
?DTM(script);
?DTM(set_metadata);
?DTM(set_config);

%%%===================================================================
%%%  VM Functions
%%%===================================================================

message(({vm, fw, add, Vm, Rule}), State) when
      is_binary(Vm),
      is_tuple(Rule) ->
    {reply,
     sniffle_vm:add_fw_rule(Vm, Rule),
     State};

message(({vm, fw, remove, Vm, Rule}), State) when
      is_binary(Vm),
      is_tuple(Rule) ->
    {reply,
     sniffle_vm:remove_fw_rule(Vm, Rule),
     State};

message({vm, nic, add, Vm, Network}, State) when
      is_binary(Vm),
      is_binary(Network) ->
    {reply,
     sniffle_vm:add_nic(Vm, Network),
     State};

message({vm, nic, remove, Vm, Nic}, State) when
      is_binary(Vm),
      is_binary(Nic) ->
    {reply,
     sniffle_vm:remove_nic(Vm, Nic),
     State};

message({vm, nic, primary, Vm, Nic}, State) when
      is_binary(Vm),
      is_binary(Nic) ->
    {reply,
     sniffle_vm:primary_nic(Vm, Nic),
     State};

message({vm, log, Vm, Log}, State) when
      is_binary(Vm) ->
    {reply,
     sniffle_vm:log(Vm, Log),
     State};

message({vm, register, Vm, Hypervisor}, State) when
      is_binary(Vm),
      is_binary(Hypervisor) ->
    {reply,
     sniffle_vm:register(Vm, Hypervisor),
     State};


message({vm, service, enable, Vm, Service}, State) when
      is_binary(Vm),
      is_binary(Service) ->
    {reply,
     sniffle_vm:service_enable(Vm, Service),
     State};

message({vm, service, disable, Vm, Service}, State) when
      is_binary(Vm),
      is_binary(Service) ->
    {reply,
     sniffle_vm:service_disable(Vm, Service),
     State};

message({vm, service, clear, Vm, Service}, State) when
      is_binary(Vm),
      is_binary(Service) ->
    {reply,
     sniffle_vm:service_clear(Vm, Service),
     State};

message({vm, service, restart, Vm, Service}, State) when
      is_binary(Vm),
      is_binary(Service) ->
    {reply,
     sniffle_vm:service_restart(Vm, Service),
     State};

message({vm, service, refresh, Vm, Service}, State) when
      is_binary(Vm),
      is_binary(Service) ->
    {reply,
     sniffle_vm:service_refresh(Vm, Service),
     State};

message({vm, snapshot, Vm, Comment}, State) when
      is_binary(Vm),
      is_binary(Comment) ->
    {reply,
     sniffle_vm:snapshot(Vm, Comment),
     State};

message({vm, snapshot, delete, Vm, Snap}, State) when
      is_binary(Vm),
      is_binary(Snap) ->
    {reply,
     sniffle_vm:delete_snapshot(Vm, Snap),
     State};


message({vm, backup, full, Vm, Comment, Opts}, State) when
      is_binary(Vm),
      is_binary(Comment) ->
    {reply,
     sniffle_vm:create_backup(Vm, full, Comment, Opts),
     State};

message({vm, backup, incremental, Vm, Parent, Comment, Opts}, State) when
      is_binary(Vm),
      is_binary(Comment),
      is_binary(Parent) ->
    {reply,
     sniffle_vm:create_backup(Vm, incremental, Comment,
                              [{parent, Parent} | Opts]),
     State};

message({vm, backup, restore, Vm, Backup}, State) when
      is_binary(Vm),
      is_binary(Backup) ->
    {reply,
     sniffle_vm:restore_backup(Vm, Backup),
     State};

message({vm, backup, restore, User, Vm, Backup, Rules}, State) when
      is_binary(Vm),
      is_binary(Backup) ->
    {reply,
     sniffle_vm:restore(User, Vm, Backup, Rules),
     State};

message({vm, backup, restore, User, Vm, Backup, Package, Rules}, State) when
      is_binary(Vm),
      is_binary(Backup) ->
    {reply,
     sniffle_vm:restore(User, Vm, Backup, Package, Rules),
     State};

message({vm, backup, delete, Vm, Backup}, State) when
      is_binary(Vm),
      is_binary(Backup) ->
    {reply,
     sniffle_vm:delete_backup(Vm, Backup),
     State};

message({vm, backup, delete, Vm, UUID, cloud}, State) when
      is_binary(Vm),
      is_binary(UUID) ->
    {reply,
     sniffle_vm:delete_backup(Vm, UUID),
     State};

message({vm, backup, delete, Vm, UUID, hypervisor}, State) when
      is_binary(Vm),
      is_binary(UUID) ->
    {reply,
     sniffle_vm:remove_backup(Vm, UUID),
     State};

message({vm, snapshot, rollback, Vm, UUID}, State) when
      is_binary(Vm),
      is_binary(UUID) ->
    {reply,
     sniffle_vm:rollback_snapshot(Vm, UUID),
     State};

message({vm, snapshot, commit_rollback, Vm, UUID}, State) when
      is_binary(Vm),
      is_binary(UUID) ->
    {reply,
     sniffle_vm:commit_snapshot_rollback(Vm, UUID),
     State};

message({vm, snapshot, promote, Vm, UUID, Config}, State) when
      is_binary(Vm),
      is_binary(UUID) ->
    {reply,
     sniffle_vm:promote_to_image(Vm, UUID, Config),
     State};

message({vm, create, Package, Dataset, Config}, State) when
      is_binary(Package),
      is_list(Config) ->
    {reply,
     sniffle_vm:create(Package, Dataset, Config),
     State};

message({vm, dry_run, Package, Dataset, Config}, State) when
      is_binary(Package),
      is_list(Config),
      is_binary(Dataset) ->
    {reply,
     sniffle_vm:dry_run(Package, Dataset, Config),
     State};

message({vm, update, User, Vm, Package, Config}, State) when
      is_binary(Vm),
      is_list(Config) ->
    {reply,
     sniffle_vm:update(User, Vm, Package, Config),
     State};

message({vm, unregister, Vm}, State) when
      is_binary(Vm) ->
    {reply,
     sniffle_vm:unregister(Vm),
     State};

message({vm, get, docker, Vm}, State) when
      is_binary(Vm) ->
    {reply,
     sniffle_vm:get_docker(Vm),
     State};

message({vm, get, Vm}, State) when
      is_binary(Vm) ->
    {reply,
     sniffle_vm:get(Vm),
     State};

message({vm, start, Vm}, State) when
      is_binary(Vm) ->
    {reply,
     sniffle_vm:start(Vm),
     State};

message({vm, delete, User, Vm}, State) when
      is_binary(Vm) ->
    {reply,
     sniffle_vm:delete(User, Vm),
     State};

message({vm, store, User, Vm}, State) when
      is_binary(Vm) ->
    {reply,
     sniffle_vm:store(User, Vm),
     State};

message({vm, stop, Vm}, State) when
      is_binary(Vm) ->
    {reply,
     sniffle_vm:stop(Vm),
     State};

message({vm, stop, force, Vm}, State) when
      is_binary(Vm) ->
    {reply,
     sniffle_vm:stop(Vm, [force]),
     State};

message({vm, reboot, Vm}, State) when
      is_binary(Vm) ->
    {reply,
     sniffle_vm:reboot(Vm),
     State};

message({vm, reboot, force, Vm}, State) when
      is_binary(Vm) ->
    {reply,
     sniffle_vm:reboot(Vm, [force]),
     State};

message({vm, owner, User, Vm, Owner}, State) when
      is_binary(Vm) ->
    {reply,
     sniffle_vm:set_owner(User, Vm, Owner),
     State};

message({vm, hostname, Vm, Interface, Hostname}, State) when
      is_binary(Vm),
      is_binary(Interface),
      is_binary(Hostname) ->
    {reply,
     sniffle_vm:set_hostname(Vm, Interface, Hostname),
     State};

message({vm, get, hostname, Hostname, Org}, State) when
      is_binary(Hostname),
      is_binary(Org) ->
    {reply,
     sniffle_hostname:get(Hostname, Org),
     State};


message({vm, set_service, _VM, _V}, State) when
      is_binary(_VM) ->
    {reply, ok, State};

?VM(state);
?VM(creating);
?VM(set_info);
?VM(set_backup);
?VM(set_snapshot);
?VM(set_config);
?VM(set_metadata);

message({vm, list}, State) ->
    {reply,
     sniffle_vm:list(),
     State};

message({vm, list, Requirements}, State) ->
    message({vm, list, Requirements, false}, State);

message({vm, list, Requirements, Full}, State) when
      is_list(Requirements) ->
    {reply,
     sniffle_vm:list(Requirements, Full),
     State};


message({vm, stream, Requirements}, State) when
      is_list(Requirements) ->
    Fn = fun(Send) ->
                 Fold = fun(Es, _) ->
                                Send(Es)
                        end,
                 sniffle_vm:list(Requirements, Fold, ok)
         end,
    {stream, Fn, State};

%%%===================================================================
%%%  Hypervisor Functions
%%%===================================================================

?HM(set_resource);
?HM(set_characteristic);
?HM(set_metadata);
?HM(set_pool);

message({hypervisor, set_service, _VM, _V}, State) when
      is_binary(_VM) ->
    {reply, ok, State};


?HM(alias);
?HM(last_seen);
?HM(etherstubs);
?HM(host);
?HM(networks);
?HM(path);
?HM(port);
?HM(sysinfo);
?HM(uuid);
?HM(version);
?HM(virtualisation);


message({hypervisor, register, Hypervisor, Host, Port}, State) when
      is_binary(Hypervisor),
      is_integer(Port) ->
    {reply,
     sniffle_hypervisor:register(Hypervisor, Host, Port),
     State};

message({hypervisor, unregister, Hypervisor}, State) when
      is_binary(Hypervisor) ->
    {reply,
     sniffle_hypervisor:unregister(Hypervisor),
     State};

message({hypervisor, service, Hypervisor, Action, Service}, State) when
      is_binary(Hypervisor),
      (Action =:= enable orelse Action =:= disable orelse Action =:= clear
       orelse Action =:= refresh orelse Action =:= restart) ->
    {reply,
     sniffle_hypervisor:service(Hypervisor, Action, Service),
     State};

message({hypervisor, get, Hypervisor}, State) when
      is_binary(Hypervisor) ->
    {reply,
     sniffle_hypervisor:get(Hypervisor),
     State};

message({hypervisor, update, Hypervisor}, State) when
      is_binary(Hypervisor) ->
    {reply,
     sniffle_hypervisor:update(Hypervisor),
     State};

message({hypervisor, update}, State) ->
    {reply,
     sniffle_hypervisor:update(),
     State};

message({hypervisor, list}, State) ->
    {reply,
     sniffle_hypervisor:list(),
     State};

message({hypervisor, list, Requirements}, State) ->
    message({hypervisor, list, Requirements, false}, State);

message({hypervisor, list, Requirements, Full}, State) when
      is_list(Requirements) ->
    {reply,
     sniffle_hypervisor:list(Requirements, Full),
     State};

message({hypervisor, stream, Requirements}, State) when
      is_list(Requirements) ->
    Fn = fun(Send) ->
                 Fold = fun(Es, _) ->
                                Send(Es)
                        end,
                 sniffle_hypervisor:list(Requirements, Fold, ok)
         end,
    {stream, Fn, State};


%%%===================================================================
%%%  Dataset Functions
%%%===================================================================

message({dataset, create, Dataset}, State) when
      is_binary(Dataset) ->
    {reply,
     sniffle_dataset:create(Dataset),
     State};

message({dataset, delete, Dataset}, State) when
      is_binary(Dataset) ->
    {reply,
     sniffle_dataset:delete(Dataset),
     State};

message({dataset, get, Dataset}, State) when
      is_binary(Dataset) ->
    {reply,
     sniffle_dataset:get(Dataset),
     State};

message({dataset, list}, State) ->
    {reply,
     sniffle_dataset:list(),
     State};

message({dataset, list, Requirements}, State) ->
    message({dataset, list, Requirements, false}, State);

message({dataset, list, Requirements, Full}, State) when
      is_list(Requirements) ->
    {reply,
     sniffle_dataset:list(Requirements, Full),
     State};

message({dataset, stream, Requirements}, State) when
      is_list(Requirements) ->
    Fn = fun(Send) ->
                 Fold = fun(Es, _) ->
                                Send(Es)
                        end,
                 sniffle_dataset:list(Requirements, Fold, ok)
         end,
    {stream, Fn, State};

message({dataset, import, URL}, State) ->
    {reply,
     sniffle_dataset:import(URL),
     State};

?DSM(imported);
?DSM(status);
?DSM(sha1);
?DSM(description);
?DSM(disk_driver);
?DSM(homepage);
?DSM(image_size);
?DSM(name);
?DSM(nic_driver);
?DSM(os);
?DSM(type);
?DSM(zone_type);
?DSM(users);
?DSM(version);
?DSM(kernel_version);
?DSM(set_metadata);
?DSM(remove_requirement);
?DSM(add_requirement);
?DSM(remove_network);
?DSM(add_network);


%%%===================================================================
%%%  Network Functions
%%%===================================================================

message({network, create, Name}, State) when
      is_binary(Name) ->
    {reply,
     sniffle_network:create(Name),
     State};

message({network, delete, Network}, State) when
      is_binary(Network) ->
    {reply,
     sniffle_network:delete(Network),
     State};

message({network, get, Network}, State) when
      is_binary(Network) ->
    {reply,
     sniffle_network:get(Network),
     State};

message({network, add_iprange, Network, IPRange}, State) when
      is_binary(Network) ->
    {reply,
     sniffle_network:add_iprange(Network, IPRange),
     State};

message({network, remove_iprange, Network, IPRange}, State) when
      is_binary(Network) ->
    {reply,
     sniffle_network:remove_iprange(Network, IPRange),
     State};

message({network, list}, State) ->
    {reply,
     sniffle_network:list(),
     State};

message({network, list, Requirements}, State) ->
    message({network, list, Requirements, false}, State);

message({network, list, Requirements, Full}, State) ->
    {reply,
     sniffle_network:list(Requirements, Full),
     State};

message({network, stream, Requirements}, State) when
      is_list(Requirements) ->
    Fn = fun(Send) ->
                 Fold = fun(Es, _) ->
                                Send(Es)
                        end,
                 sniffle_network:list(Requirements, Fold, ok)
         end,
    {stream, Fn, State};


?NM(uuid);
?NM(name);
?NM(set_metadata);

%%%===================================================================
%%%  IPRange Functions
%%%===================================================================

message({iprange, create, Iprange, Network, Gateway, Netmask, First, Last, Tag,
         Vlan}, State) when
      is_binary(Iprange), is_binary(Tag),
      is_integer(Network), is_integer(Gateway), is_integer(Netmask),
      is_integer(First), is_integer(Last),
      is_integer(Vlan)->
    {reply,
     sniffle_iprange:create(Iprange, Network, Gateway, Netmask, First, Last,
                            Tag, Vlan),
     State};

message({iprange, delete, Iprange}, State) when
      is_binary(Iprange) ->
    {reply,
     sniffle_iprange:delete(Iprange),
     State};

message({iprange, get, Iprange}, State) when
      is_binary(Iprange) ->
    {reply,
     sniffle_iprange:get(Iprange),
     State};

message({iprange, release, Iprange, Ip}, State) when
      is_binary(Iprange),
      is_integer(Ip) ->
    {reply,
     sniffle_iprange:release_ip(Iprange, Ip),
     State};

message({iprange, claim, Iprange}, State) when
      is_binary(Iprange) ->
    {reply,
     sniffle_iprange:claim_ip(Iprange),
     State};

message({iprange, list}, State) ->
    {reply,
     sniffle_iprange:list(),
     State};

message({iprange, list, Requirements}, State) ->
    message({iprange, list, Requirements, false}, State);

message({iprange, list, Requirements, Full}, State) when
      is_list(Requirements)->
    {reply,
     sniffle_iprange:list(Requirements, Full),
     State};

message({iprange, stream, Requirements}, State) when
      is_list(Requirements) ->
    Fn = fun(Send) ->
                 Fold = fun(Es, _) ->
                                Send(Es)
                        end,
                 sniffle_iprange:list(Requirements, Fold, ok)
         end,
    {stream, Fn, State};

?IPM(name);
?IPM(uuid);
?IPM(network);
?IPM(netmask);
?IPM(gateway);
?IPM(set_metadata);
?IPM(tag);
?IPM(vlan);

%%%===================================================================
%%%  PACKAGE Functions
%%%===================================================================

message({package, create, Package}, State) when
      is_binary(Package) ->
    {reply,
     sniffle_package:create(Package),
     State};

message({package, delete, Package}, State) when
      is_binary(Package) ->
    {reply,
     sniffle_package:delete(Package),
     State};

message({package, get, Package}, State) when
      is_binary(Package) ->
    {reply,
     sniffle_package:get(Package),
     State};

message({package, list}, State) ->
    {reply,
     sniffle_package:list(),
     State};

message({package, list, Requirements}, State) ->
    message({package, list, Requirements, false}, State);

message({package, list, Requirements, Full}, State) when
      is_list(Requirements) ->
    {reply,
     sniffle_package:list(Requirements, Full),
     State};

message({package, stream, Requirements}, State) when
      is_list(Requirements) ->
    Fn = fun(Send) ->
                 Fold = fun(Es, _) ->
                                Send(Es)
                        end,
                 sniffle_package:list(Requirements, Fold, ok)
         end,
    {stream, Fn, State};

message({package, resources, org, inc, Package, Resource, V}, State) when
      is_binary(Package), is_binary(Resource), is_integer(V) ->
    {reply,
     sniffle_package:org_resource_inc(Package, Resource, V),
     State};

message({package, resources, org, dec, Package, Resource, V}, State) when
      is_binary(Package), is_binary(Resource), is_integer(V) ->
    {reply,
     sniffle_package:org_resource_dec(Package, Resource, V),
     State};

?PM(set_metadata);
?PM(blocksize);
?PM(compression);
?PM(cpu_cap);
?PM(cpu_shares);
?PM(max_swap);
?PM(name);
?PM(quota);
?PM(ram);
?PM(uuid);
?PM(zfs_io_priority);
?PM(remove_requirement);
?PM(add_requirement);

%%%===================================================================
%%%  Cloud Functions
%%%===================================================================

message({cloud, status}, State) ->
    {reply,
     sniffle_hypervisor:status(),
     State};

message(Message, State) ->
    io:format("Unsuppored TCP message: ~p", [Message]),
    {noreply, State}.

%%%===================================================================
%%%  Internal Functions
%%%===================================================================
