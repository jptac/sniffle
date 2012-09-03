-module(sniffle_zmq_handler).

-export([init/1, message/2]).

init([]) ->
    {ok, stateless}.

%%%===================================================================
%%%  VM Functions
%%%===================================================================

message({vm, register, Vm, Hypervisor}, State) ->
    {reply, 
     sniffle_vm:register(ensure_binary(Vm), ensure_binary(Hypervisor)),
     State};

message({vm, unregister, Vm}, State) ->
    {reply, 
     sniffle_vm:unregister(ensure_binary(Vm)),
     State};

message({vm, attribute, get, Vm, Attribute}, State) ->
    {reply, 
     sniffle_vm:get_attribute(ensure_binary(Vm), Attribute),
     State};

message({vm, attribute, set, Vm, Attribute, Value}, State) ->
    {reply, 
     sniffle_vm:get_attribute(ensure_binary(Vm), Attribute, Value),
     State};

message({vm, list}, State) ->
    {reply,
     sniffle_vm:list(),
     State};

message({vm, list, User}, State) ->
    {reply,
     sniffle_vm:list(ensure_binary(User)),
     State};

%%%===================================================================
%%%  Hypervisor Functions
%%%===================================================================

message({hyperisor, register, Hypervisor, Host, Port}, State) ->
    {reply, 
     sniffle_hypervisor:register(ensure_binary(Hypervisor), Host, Port),
     State};

message({hyperisor, unregister, Hypervisor}, State) ->
    {reply, 
     sniffle_hypervisor:unregister(ensure_binary(Hypervisor)),
     State};

message({hyperisor, resource, get, Hypervisor, Resource}, State) ->
    {reply, 
     sniffle_hypervisor:get_resource(ensure_binary(Hypervisor), Resource),
     State};

message({hyperisor, resource, set, Hypervisor, Resource, Value}, State) ->
    {reply, 
     sniffle_hypervisor:get_resource(ensure_binary(Hypervisor), Resource, Value),
     State};

message({hyperisor, list}, State) ->
    {reply, 
     sniffle_hypervisor:list(),
     State};

message({hyperisor, list, User}, State) ->
    {reply, 
     sniffle_hypervisor:list(ensure_binary(User)),
     State};

%%%===================================================================
%%%  Internal Functions
%%%===================================================================

message(Message, State) ->
    io:format("Unsuppored 0MQ message: ~p", [Message]),
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
