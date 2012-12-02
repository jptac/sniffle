-module(sniffle_zmq_handler).

-export([init/1, message/2]).

-ignore_xref([init/1, message/2]).

init([]) ->
    {ok, stateless}.

%%%===================================================================
%%%  VM Functions
%%%===================================================================

-spec message(fifo:sniffle_message(), any()) -> any().

message({vm, register, Vm, Hypervisor}, State) when
      is_binary(Vm),
      is_binary(Hypervisor) ->
    {reply,
     sniffle_vm:register(Vm, Hypervisor),
     State};

message({vm, create, Package, Dataset, Config}, State) when
      is_binary(Package),
      is_list(Config) ,
      is_binary(Dataset) ->
    {reply,
     sniffle_vm:create(Package, Dataset, Config),
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

message({vm, attribute, get, Vm}, State) when
      is_binary(Vm) ->
    {reply,
     sniffle_vm:get_attribute(Vm),
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

message({vm, reboot, Vm}, State) when
      is_binary(Vm) ->
    {reply,
     sniffle_vm:reboot(Vm),
     State};

message({vm, attribute, get, Vm, Attribute}, State) when
      is_binary(Vm),
      is_binary(Attribute) ->
    {reply,
     sniffle_vm:get_attribute(Vm, Attribute),
     State};

message({vm, attribute, set, Vm, Attribute, Value}, State) when
      is_binary(Vm),
      is_binary(Attribute) ->
    {reply,
     sniffle_vm:set_attribute(Vm, Attribute, Value),
     State};

message({vm, attribute, set, Vm, Attributes}, State) when
      is_binary(Vm),
      is_list(Attributes) ->
    {reply,
     sniffle_vm:set_attribute(Vm, Attributes),
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

message({hypervisor, resource, get, Hypervisor, Resource}, State) when
      is_binary(Hypervisor),
      is_binary(Resource) ->
    {reply,
     sniffle_hypervisor:get_resource(Hypervisor, Resource),
     State};

message({hypervisor, resource, get, Hypervisor}, State) when
      is_binary(Hypervisor) ->
    {reply,
     sniffle_hypervisor:get_resource(Hypervisor),
     State};

message({hypervisor, resource, set, Hypervisor, Resource, Value}, State) when
      is_binary(Hypervisor),
      is_binary(Resource) ->
    {reply,
     sniffle_hypervisor:set_resource(Hypervisor, Resource, Value),
     State};

message({hypervisor, resource, set, Hypervisor, Resources}, State) when
      is_binary(Hypervisor),
      is_list(Resources) ->
    {reply,
     sniffle_hypervisor:set_resource(Hypervisor, Resources),
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
%%%  DATASET Functions
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

message({dataset, attribute, get, Dataset}, State) when
      is_binary(Dataset) ->
    {reply,
     sniffle_dataset:get_attribute(Dataset),
     State};

message({dataset, attribute, get, Dataset, Attribute}, State) when
      is_binary(Dataset),
      is_binary(Attribute) ->
    {reply,
     sniffle_dataset:get_attribute(Dataset, Attribute),
     State};

message({dataset, attribute, set, Dataset, Attribute, Value}, State) when
      is_binary(Dataset),
      is_binary(Attribute) ->
    {reply,
     sniffle_dataset:set_attribute(Dataset, Attribute, Value),
     State};

message({dataset, attribute, set, Dataset, Attributes}, State) when
      is_binary(Dataset),
      is_list(Attributes) ->
    {reply,
     sniffle_dataset:set_attribute(Dataset, Attributes),
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

%%%===================================================================
%%%  IPRange Functions
%%%===================================================================

message({iprange, create, Iprange, Network, Gateway, Netmask, First, Last, Tag}, State) when
      is_binary(Iprange), is_binary(Tag),
      is_integer(Network), is_integer(Gateway), is_integer(Netmask),
      is_integer(First), is_integer(Last) ->
    {reply,
     sniffle_iprange:create(Iprange, Network, Gateway, Netmask, First, Last, Tag),
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

message({package, attribute, get, Package}, State) when
      is_binary(Package) ->
    {reply,
     sniffle_package:get_attribute(Package),
     State};

message({package, attribute, get, Package, Attribute}, State) when
      is_binary(Package),
      is_binary(Attribute) ->
    {reply,
     sniffle_package:get_attribute(Package, Attribute),
     State};

message({package, attribute, set, Package, Attribute, Value}, State) when
      is_binary(Package),
      is_binary(Attribute) ->
    {reply,
     sniffle_package:set_attribute(Package, Attribute, Value),
     State};

message({package, attribute, set, Package, Attributes}, State) when
      is_binary(Package),
      is_list(Attributes) ->
    {reply,
     sniffle_package:set_attribute(Package, Attributes),
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
     State}.

%%%===================================================================
%%%  Internal Functions
%%%===================================================================
