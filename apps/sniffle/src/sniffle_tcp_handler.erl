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
    {stop, normal, ?VERSION, State};

%%%===================================================================
%%%  DTrace Functions
%%%===================================================================

message({dtrace, add, Name, Script}, State) ->
    {stop, normal,
     sniffle_dtrace:add(Name, Script),
     State};

message({dtrace, delete, ID}, State) ->
    {stop, normal,
     sniffle_dtrace:delete(ID),
     State};

message({dtrace, get, ID}, State) ->
    {stop, normal,
     sniffle_dtrace:get(ID),
     State};

message({dtrace, list}, State) ->
    {stop, normal,
     sniffle_dtrace:list(),
     State};

message({dtrace, list, Requreiments}, State) ->
    {stop, normal,
     sniffle_dtrace:list(Requreiments),
     State};

message({dtrace, run, ID, Servers}, State) ->
    {ok, _Pid} = sniffle_dtrace_server:run(ID, Servers, self()),
    {claim, State};

%%%===================================================================
%%%  VM Functions
%%%===================================================================

message({vm, log, Vm, Log}, State) when
      is_binary(Vm) ->
    {stop, normal,
     sniffle_vm:log(Vm, Log),
     State};

message({vm, register, Vm, Hypervisor}, State) when
      is_binary(Vm),
      is_binary(Hypervisor) ->
    {stop, normal,
     sniffle_vm:register(Vm, Hypervisor),
     State};

message({vm, snapshot, Vm, Comment}, State) when
      is_binary(Vm),
      is_binary(Comment) ->
    {stop, normal,
     sniffle_vm:snapshot(Vm, Comment),
     State};

message({vm, snapshot, delete, Vm, UUID}, State) when
      is_binary(Vm),
      is_binary(UUID) ->
    {stop, normal,
     sniffle_vm:delete_snapshot(Vm, UUID),
     State};

message({vm, snapshot, rollback, Vm, UUID}, State) when
      is_binary(Vm),
      is_binary(UUID) ->
    {stop, normal,
     sniffle_vm:rollback_snapshot(Vm, UUID),
     State};

message({vm, create, Package, Dataset, Config}, State) when
      is_binary(Package),
      is_list(Config),
      is_binary(Dataset) ->
    {stop, normal,
     sniffle_vm:create(Package, Dataset, Config),
     State};

message({vm, update, Vm, Package, Config}, State) when
      is_binary(Vm),
      is_list(Config) ->
    {stop, normal,
     sniffle_vm:update(Vm, Package, Config),
     State};

message({vm, unregister, Vm}, State) when
      is_binary(Vm) ->
    {stop, normal,
     sniffle_vm:unregister(Vm),
     State};

message({vm, get, Vm}, State) when
      is_binary(Vm) ->
    {stop, normal,
     sniffle_vm:get(Vm),
     State};

message({vm, start, Vm}, State) when
      is_binary(Vm) ->
    {stop, normal,
     sniffle_vm:start(Vm),
     State};

message({vm, delete, Vm}, State) when
      is_binary(Vm) ->
    {stop, normal,
     sniffle_vm:delete(Vm),
     State};

message({vm, stop, Vm}, State) when
      is_binary(Vm) ->
    {stop, normal,
     sniffle_vm:stop(Vm),
     State};

message({vm, reboot, Vm}, State) when
      is_binary(Vm) ->
    {stop, normal,
     sniffle_vm:reboot(Vm),
     State};

message({vm, set, Vm, Attribute, Value}, State) when
      is_binary(Vm) ->
    {stop, normal,
     sniffle_vm:set(Vm, Attribute, Value),
     State};

message({vm, set, Vm, Attributes}, State) when
      is_binary(Vm),
      is_list(Attributes) ->
    {stop, normal,
     sniffle_vm:set(Vm, Attributes),
     State};

message({vm, list}, State) ->
    {stop, normal,
     sniffle_vm:list(),
     State};

message({vm, list, Requirements}, State) when
      is_list(Requirements) ->
    {stop, normal,
     sniffle_vm:list(Requirements),
     State};



%%%===================================================================
%%%  Hypervisor Functions
%%%===================================================================

message({hypervisor, register, Hypervisor, Host, Port}, State) when
      is_binary(Hypervisor),
      is_integer(Port) ->
    {stop, normal,
     sniffle_hypervisor:register(Hypervisor, Host, Port),
     State};

message({hypervisor, unregister, Hypervisor}, State) when
      is_binary(Hypervisor) ->
    {stop, normal,
     sniffle_hypervisor:unregister(Hypervisor),
     State};

message({hypervisor, get, Hypervisor}, State) when
      is_binary(Hypervisor) ->
    {stop, normal,
     sniffle_hypervisor:get(Hypervisor),
     State};

message({hypervisor, set, Hypervisor, Resource, Value}, State) when
      is_binary(Hypervisor) ->
    {stop, normal,
     sniffle_hypervisor:set(Hypervisor, Resource, Value),
     State};

message({hypervisor, set, Hypervisor, Resources}, State) when
      is_binary(Hypervisor),
      is_list(Resources) ->
    {stop, normal,
     sniffle_hypervisor:set(Hypervisor, Resources),
     State};

message({hypervisor, list}, State) ->
    {stop, normal,
     sniffle_hypervisor:list(),
     State};

message({hypervisor, list, Requirements}, State) when
      is_list(Requirements) ->
    {stop, normal,
     sniffle_hypervisor:list(Requirements),
     State};

%%%===================================================================
%%%  Dataset Functions
%%%===================================================================

message({dataset, create, Dataset}, State) when
      is_binary(Dataset) ->
    {stop, normal,
     sniffle_dataset:create(Dataset),
     State};

message({dataset, delete, Dataset}, State) when
      is_binary(Dataset) ->
    {stop, normal,
     sniffle_dataset:delete(Dataset),
     State};

message({dataset, get, Dataset}, State) when
      is_binary(Dataset) ->
    {stop, normal,
     sniffle_dataset:get(Dataset),
     State};

message({dataset, set, Dataset, Attribute, Value}, State) when
      is_binary(Dataset) ->
    {stop, normal,
     sniffle_dataset:set(Dataset, Attribute, Value),
     State};

message({dataset, set, Dataset, Attributes}, State) when
      is_binary(Dataset),
      is_list(Attributes) ->
    {stop, normal,
     sniffle_dataset:set(Dataset, Attributes),
     State};

message({dataset, list}, State) ->
    {stop, normal,
     sniffle_dataset:list(),
     State};

message({dataset, list, Requirements}, State) when
      is_list(Requirements) ->
    {stop, normal,
     sniffle_dataset:list(Requirements),
     State};

%%%===================================================================
%%%  IPRange Functions
%%%===================================================================

message({iprange, create, Iprange, Network, Gateway, Netmask, First, Last, Tag, Vlan}, State) when
      is_binary(Iprange), is_binary(Tag),
      is_integer(Network), is_integer(Gateway), is_integer(Netmask),
      is_integer(First), is_integer(Last),
      is_integer(Vlan)->
    {stop, normal,
     sniffle_iprange:create(Iprange, Network, Gateway, Netmask, First, Last, Tag, Vlan),
     State};

message({iprange, delete, Iprange}, State) when
      is_binary(Iprange) ->
    {stop, normal,
     sniffle_iprange:delete(Iprange),
     State};

message({iprange, get, Iprange}, State) when
      is_binary(Iprange) ->
    {stop, normal,
     sniffle_iprange:get(Iprange),
     State};

message({iprange, release, Iprange, Ip}, State) when
      is_binary(Iprange),
      is_integer(Ip) ->
    {stop, normal,
     sniffle_iprange:release_ip(Iprange, Ip),
     State};

message({iprange, claim, Iprange}, State) when
      is_binary(Iprange) ->
    {stop, normal,
     sniffle_iprange:claim_ip(Iprange),
     State};

message({iprange, list}, State) ->
    {stop, normal,
     sniffle_iprange:list(),
     State};

message({iprange, list, Requirements}, State) when
      is_list(Requirements)->
    {stop, normal,
     sniffle_iprange:list(Requirements),
     State};

message({iprange, set, Iprange, Attribute, Value}, State) when
      is_binary(Iprange) ->
    {stop, normal,
     sniffle_iprange:set(Iprange, Attribute, Value),
     State};

message({iprange, set, Iprange, Attributes}, State) when
      is_binary(Iprange),
      is_list(Attributes) ->
    {stop, normal,
     sniffle_iprange:set(Iprange, Attributes),
     State};


%%%===================================================================
%%%  PACKAGE Functions
%%%===================================================================

message({package, create, Package}, State) when
      is_binary(Package) ->
    {stop, normal,
     sniffle_package:create(Package),
     State};

message({package, delete, Package}, State) when
      is_binary(Package) ->
    {stop, normal,
     sniffle_package:delete(Package),
     State};

message({package, get, Package}, State) when
      is_binary(Package) ->
    {stop, normal,
     sniffle_package:get(Package),
     State};

message({package, set, Package, Attribute, Value}, State) when
      is_binary(Package) ->
    {stop, normal,
     sniffle_package:set(Package, Attribute, Value),
     State};

message({package, set, Package, Attributes}, State) when
      is_binary(Package),
      is_list(Attributes) ->
    {stop, normal,
     sniffle_package:set(Package, Attributes),
     State};

message({package, list}, State) ->
    {stop, normal,
     sniffle_package:list(),
     State};

message({package, list, Requirements}, State) when
      is_list(Requirements) ->
    {stop, normal,
     sniffle_package:list(Requirements),
     State};

%%%===================================================================
%%%  Cloud Functions
%%%===================================================================

message({cloud, status}, State) ->
    {stop, normal,
     sniffle_hypervisor:status(),
     State}.

%%%===================================================================
%%%  Internal Functions
%%%===================================================================
