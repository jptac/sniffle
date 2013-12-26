-module(sniffle_tcp_handler).

-export([init/2, message/2, raw/2]).

-include("sniffle_version.hrl").

-ignore_xref([init/2, message/2, raw/2]).


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

-spec message(fifo:sniffle_message(), any()) -> any().

message(version, State) ->
    {reply, {ok, ?VERSION}, State};

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
    {reply,
     sniffle_dtrace:list(Requreiments),
     State};

message({dtrace, set, ID, Attribute, Value}, State) when
      is_binary(ID) ->
    {reply,
     sniffle_dtrace:set(ID, Attribute, Value),
     State};

message({dtrace, set, ID, Attributes}, State) when
      is_binary(ID),
      is_list(Attributes) ->
    {reply,
     sniffle_dtrace:set(ID, Attributes),
     State};

message({dtrace, run, ID, Servers}, State) ->
    {ok, _Pid} = sniffle_dtrace_server:run(ID, Servers, self()),
    {claim, State};

%%%===================================================================
%%%  VM Functions
%%%===================================================================
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

message({vm, snapshot, Vm, Comment}, State) when
      is_binary(Vm),
      is_binary(Comment) ->
    {reply,
     sniffle_vm:snapshot(Vm, Comment),
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

message({vm, backup, restore, Vm, Backup, _Hypervisor}, State) when
      is_binary(Vm),
      is_binary(Backup) ->
    {reply,
     sniffle_vm:restore_backup(Vm, Backup),
     State};

message({vm, backup, delete, Vm, Backup}, State) when
      is_binary(Vm),
      is_binary(Backup) ->
    {reply,
     sniffle_vm:delete_backup(Vm, Backup),
     State};

message({vm, snapshot, delete, Vm, UUID, _Where}, State) when
      is_binary(Vm),
      is_binary(UUID) ->
    {reply,
     sniffle_vm:delete_snapshot(Vm, UUID),
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
      is_list(Config),
      is_binary(Dataset) ->
    {reply,
     sniffle_vm:create(Package, Dataset, Config),
     State};

message({vm, update, Vm, Package, Config}, State) when
      is_binary(Vm),
      is_list(Config) ->
    {reply,
     sniffle_vm:update(Vm, Package, Config),
     State};

message({vm, unregister, Vm}, State) when
      is_binary(Vm) ->
    {reply,
     sniffle_vm:unregister(Vm),
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

message({vm, delete, Vm}, State) when
      is_binary(Vm) ->
    {reply,
     sniffle_vm:delete(Vm),
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

message({vm, set, Vm, Attribute, Value}, State) when
      is_binary(Vm) ->
    {reply,
     sniffle_vm:set(Vm, Attribute, Value),
     State};

message({vm, owner, Vm, Owner}, State) when
      is_binary(Vm),
      is_binary(Owner) ->
    {reply,
     sniffle_vm:set_owner(Vm, Owner),
     State};

message({vm, set, Vm, Attributes}, State) when
      is_binary(Vm),
      is_list(Attributes) ->
    {reply,
     sniffle_vm:set(Vm, Attributes),
     State};

message({vm, list}, State) ->
    {reply,
     sniffle_vm:list(),
     State};

message({vm, list, Requirements}, State) when
      is_list(Requirements) ->
    {reply,
     sniffle_vm:list(Requirements),
     State};

%%%===================================================================
%%%  Hypervisor Functions
%%%===================================================================

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

message({hypervisor, get, Hypervisor}, State) when
      is_binary(Hypervisor) ->
    {reply,
     sniffle_hypervisor:get(Hypervisor),
     State};

message({hypervisor, set, Hypervisor, Resource, Value}, State) when
      is_binary(Hypervisor) ->
    {reply,
     sniffle_hypervisor:set(Hypervisor, Resource, Value),
     State};

message({hypervisor, set, Hypervisor, Resources}, State) when
      is_binary(Hypervisor),
      is_list(Resources) ->
    {reply,
     sniffle_hypervisor:set(Hypervisor, Resources),
     State};

message({hypervisor, list}, State) ->
    {reply,
     sniffle_hypervisor:list(),
     State};

message({hypervisor, list, Requirements}, State) when
      is_list(Requirements) ->
    {reply,
     sniffle_hypervisor:list(Requirements),
     State};

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

message({dataset, set, Dataset, Attribute, Value}, State) when
      is_binary(Dataset) ->
    {reply,
     sniffle_dataset:set(Dataset, Attribute, Value),
     State};

message({dataset, set, Dataset, Attributes}, State) when
      is_binary(Dataset),
      is_list(Attributes) ->
    {reply,
     sniffle_dataset:set(Dataset, Attributes),
     State};

message({dataset, list}, State) ->
    {reply,
     sniffle_dataset:list(),
     State};

message({dataset, list, Requirements}, State) when
      is_list(Requirements) ->
    {reply,
     sniffle_dataset:list(Requirements),
     State};

message({dataset, import, URL}, State) ->
    {reply,
     sniffle_dataset:import(URL),
     State};


%%%===================================================================
%%%  Img Functions
%%%===================================================================

message({img, create, Img, Idx, Data}, State) ->
    message({img, create, Img, Idx, Data, undefined}, State);

message({img, create, Img, Idx, Data, Ref}, State) when
      is_binary(Img) ->
    {reply,
     sniffle_img:create(Img, Idx, Data, Ref),
     State};

message({img, delete, Img, Idx}, State) when
      is_binary(Img) ->
    {reply,
     sniffle_img:delete(Img, Idx),
     State};

message({img, delete, Img}, State) when
      is_binary(Img) ->
    {reply,
     sniffle_img:delete(Img),
     State};

message({img, get, Img, Idx}, State) when
      is_binary(Img) ->
    {reply,
     sniffle_img:get(Img, Idx),
     State};

message({img, list}, State) ->
    {reply,
     sniffle_img:list(),
     State};

message({img, list, Img}, State) when
      is_binary(Img) ->
    {reply,
     sniffle_img:list(Img),
     State};

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

message({network, set, Network, Attribute, Value}, State) when
      is_binary(Network) ->
    {reply,
     sniffle_network:set(Network, Attribute, Value),
     State};

message({network, set, Network, Attributes}, State) when
      is_binary(Network),
      is_list(Attributes) ->
    {reply,
     sniffle_network:set(Network, Attributes),
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
    {reply,
     sniffle_network:list(Requirements),
     State};

%%%===================================================================
%%%  IPRange Functions
%%%===================================================================

message({iprange, create, Iprange, Network, Gateway, Netmask, First, Last, Tag, Vlan}, State) when
      is_binary(Iprange), is_binary(Tag),
      is_integer(Network), is_integer(Gateway), is_integer(Netmask),
      is_integer(First), is_integer(Last),
      is_integer(Vlan)->
    {reply,
     sniffle_iprange:create(Iprange, Network, Gateway, Netmask, First, Last, Tag, Vlan),
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

message({iprange, list, Requirements}, State) when
      is_list(Requirements)->
    {reply,
     sniffle_iprange:list(Requirements),
     State};

message({iprange, set, Iprange, Attribute, Value}, State) when
      is_binary(Iprange) ->
    {reply,
     sniffle_iprange:set(Iprange, Attribute, Value),
     State};

message({iprange, set, Iprange, Attributes}, State) when
      is_binary(Iprange),
      is_list(Attributes) ->
    {reply,
     sniffle_iprange:set(Iprange, Attributes),
     State};


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

message({package, set, Package, Attribute, Value}, State) when
      is_binary(Package) ->
    {reply,
     sniffle_package:set(Package, Attribute, Value),
     State};

message({package, set, Package, Attributes}, State) when
      is_binary(Package),
      is_list(Attributes) ->
    {reply,
     sniffle_package:set(Package, Attributes),
     State};

message({package, list}, State) ->
    {reply,
     sniffle_package:list(),
     State};

message({package, list, Requirements}, State) when
      is_list(Requirements) ->
    {reply,
     sniffle_package:list(Requirements),
     State};

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
