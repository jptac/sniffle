-module(sniffle_zmq_handler).

-export([init/1, message/2]).

-ignore_xref([init/1, message/2]).

init([]) ->
    {ok, stateless}.

%%%===================================================================
%%%  VM Functions
%%%===================================================================

message({vm, register, Vm, Hypervisor}, State) when
      is_binary(Vm) 
      andalso is_binary(Hypervisor) ->
    {reply, 
     sniffle_vm:register(Vm, Hypervisor),
     State};

message({vm, create, Package, Dataset, Owner}, State) when 
      is_binary(Package) 
      andalso is_binary(Dataset) 
      andalso is_binary(Owner) ->
    {reply, 
     sniffle_vm:create(Package, Dataset, Owner),
     State};

message({vm, unregister, Vm}, State) when
      is_binary(Vm) ->
    {reply, 
     sniffle_vm:unregister(Vm),
     State};

message({vm, attribute, get, Vm}, State) when
      is_binary(Vm) ->
    {reply,
     sniffle_vm:get_attribute(Vm),
     State};

message({vm, attribute, get, Vm, Attribute}, State) when
      is_binary(Vm) ->
    {reply,
     sniffle_vm:get_attribute(Vm, Attribute),
     State};

message({vm, attribute, set, Vm, Attribute, Value}, State) when
      is_binary(Vm) ->
    {reply,
     sniffle_vm:set_attribute(Vm, Attribute, Value),
     State};

message({vm, attribute, set, Vm, Attributes}, State) when
      is_binary(Vm) ->
    {reply, 
     sniffle_vm:set_attribute(Vm, Attributes),
     State};

message({vm, list}, State) ->
    {reply,
     sniffle_vm:list(),
     State};

message({vm, list, User}, State) when 
      is_binary(User) ->
    {reply,
     sniffle_vm:list(User),
     State};

%%%===================================================================
%%%  Hypervisor Functions
%%%===================================================================

message({hypervisor, register, Hypervisor, Host, Port}, State) ->
    {reply, 
     sniffle_hypervisor:register(ensure_binary(Hypervisor), Host, Port),
     State};

message({hypervisor, unregister, Hypervisor}, State) ->
    {reply, 
     sniffle_hypervisor:unregister(ensure_binary(Hypervisor)),
     State};

message({hypervisor, resource, get, Hypervisor, Resource}, State) ->
    {reply, 
     sniffle_hypervisor:get_resource(ensure_binary(Hypervisor), Resource),
     State};

message({hypervisor, resource, set, Hypervisor, Resource, Value}, State) ->
    {reply, 
     sniffle_hypervisor:set_resource(ensure_binary(Hypervisor), Resource, Value),
     State};

message({hypervisor, list}, State) ->
    {reply, 
     sniffle_hypervisor:list(),
     State};

message({hypervisor, list, User}, State) ->
    {reply, 
     sniffle_hypervisor:list(ensure_binary(User)),
     State};

%%%===================================================================
%%%  DATASET Functions
%%%===================================================================

message({dataset, create, Dataset}, State) ->
    {reply, 
     sniffle_dataset:create(ensure_binary(Dataset)),
     State};

message({dataset, delete, Dataset}, State) ->
    {reply, 
     sniffle_dataset:delete(ensure_binary(Dataset)),
     State};

message({dataset, attribute, get, Dataset}, State) ->
    {reply,
     sniffle_dataset:get_attribute(ensure_binary(Dataset)),
     State};

message({dataset, attribute, get, Dataset, Attribute}, State) ->
    {reply,
     sniffle_dataset:get_attribute(ensure_binary(Dataset), Attribute),
     State};

message({dataset, attribute, set, Dataset, Attribute, Value}, State) ->
    {reply,
     sniffle_dataset:set_attribute(ensure_binary(Dataset), Attribute, Value),
     State};

message({dataset, attribute, set, Dataset, Attributes}, State) ->
    {reply, 
     sniffle_dataset:set_attribute(ensure_binary(Dataset), Attributes),
     State};

message({dataset, list}, State) ->
    {reply,
     sniffle_dataset:list(),
     State};

message({dataset, list, User}, State) ->
    {reply,
     sniffle_dataset:list(ensure_binary(User)),
     State};

%%%===================================================================
%%%  IPRange Functions
%%%===================================================================

message({iprange, create, Iprange, Network, Gateway, Netmask, First, Last, Tag}, State) ->
    {reply, 
     sniffle_iprange:create(ensure_binary(Iprange), Network, Gateway, Netmask, First, Last, Tag),
     State};

message({iprange, delete, Iprange}, State) ->
    {reply, 
     sniffle_iprange:delete(ensure_binary(Iprange)),
     State};

message({iprange, get, Iprange}, State) ->
    {reply, 
     sniffle_iprange:get(ensure_binary(Iprange)),
     State};

message({iprange, release, Iprange, Ip}, State) ->
    {reply,
     sniffle_iprange:release_ip(ensure_binary(Iprange), Ip),
     State};

message({iprange, claim, Iprange}, State) ->
    {reply,
     sniffle_iprange:claim_ip(ensure_binary(Iprange)),
     State};

message({iprange, list}, State) ->
    {reply,
     sniffle_iprange:list(),
     State};

message({iprange, list, User}, State) ->
    {reply,
     sniffle_iprange:list(ensure_binary(User)),
     State};

%%%===================================================================
%%%  PACKAGE Functions
%%%===================================================================

message({package, create, Package}, State) when is_binary(Package) ->
    {reply, 
     sniffle_package:create(Package),
     State};

message({package, delete, Package}, State) when is_binary(Package) ->
    {reply, 
     sniffle_package:delete(Package),
     State};

message({package, get, Package}, State) when is_binary(Package) ->
    {reply, 
     sniffle_package:get(Package),
     State};

message({package, attribute, get, Package}, State) when is_binary(Package) ->
    {reply,
     sniffle_package:get_attribute(Package),
     State};

message({package, attribute, get, Package, Attribute}, State) when is_binary(Package) ->
    {reply,
     sniffle_package:get_attribute(Package, Attribute),
     State};

message({package, attribute, set, Package, Attribute, Value}, State) when is_binary(Package) ->
    {reply,
     sniffle_package:set_attribute(Package, Attribute, Value),
     State};

message({package, attribute, set, Package, Attributes}, State) when is_binary(Package) ->
    {reply, 
     sniffle_package:set_attribute(Package, Attributes),
     State};

message({package, list}, State) ->
    {reply,
     sniffle_package:list(),
     State};

message({package, list, User}, State) when is_binary(User) ->
    {reply,
     sniffle_package:list(User),
     State};

%%%===================================================================
%%%  Internal Functions
%%%===================================================================

message(Message, State) ->
    io:format("Unsuppored message: ~p", [Message]),
    {noreply, State}.

ensure_binary(A) when is_atom(A) ->
    list_to_binary(atom_to_list(A));
ensure_binary(L) when is_list(L) ->
    list_to_binary(L);
ensure_binary(B) when is_binary(B)->
    B;
ensure_binary(I) when is_integer(I) ->
    list_to_binary(integer_to_list(I));
ensure_binary(F) when is_float(F) ->
    list_to_binary(float_to_list(F));
ensure_binary(T) ->
    term_to_binary(T).

