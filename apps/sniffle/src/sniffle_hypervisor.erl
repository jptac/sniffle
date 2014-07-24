-module(sniffle_hypervisor).
-include("sniffle.hrl").
-include_lib("fifo_dt/include/ft.hrl").

-define(MASTER, sniffle_hypervisor_vnode_master).
-define(VNODE, sniffle_hypervisor_vnode).
-define(SERVICE, sniffle_hypervisor).

-export(
   [
    register/3,
    unregister/1,
    get/1,
    list/0,
    list/2,
    set/3,
    set/2,
    status/0,
    service/3,
    update/1,
    update/0,
    wipe/1,
    sync_repair/2,
    list_/0
   ]).

-export([
         set_resource/2,
         set_characteristic/2,
         set_metadata/2,
         set_pool/2,
         set_service/2,
         alias/2,
         etherstubs/2,
         host/2,
         networks/2,
         path/2,
         port/2,
         sysinfo/2,
         uuid/2,
         version/2,
         virtualisation/2
        ]).

-ignore_xref([
              sync_repair/2,
              list_/0,
              wipe/1
             ]).

wipe(UUID) ->
    sniffle_coverage:start(?MASTER, ?SERVICE, {wipe, UUID}).

sync_repair(UUID, Obj) ->
    do_write(UUID, sync_repair, Obj).

list_() ->
    {ok, Res} = sniffle_full_coverage:start(
                  ?MASTER, ?SERVICE, {list, [], true, true}),
    Res1 = [R || {_, R} <- Res],
    {ok,  Res1}.

-spec register(Hypervisor::fifo:hypervisor_id(),
               IP::inet:ip_address() | inet:hostname(),
               Port::inet:port_number()) ->
                      duplicate | {error, timeout} | ok.

register(Hypervisor, IP, Port) ->
    case sniffle_hypervisor:get(Hypervisor) of
        not_found ->
            do_write(Hypervisor, register, [IP, Port]);
        {ok, _UserObj} ->
            set(Hypervisor, [{<<"host">>, IP}, {<<"port">>, Port}]),
            duplicate
    end.

-spec unregister(Hypervisor::fifo:hypervisor_id()) ->
                        not_found | {error, timeout} | ok.
unregister(Hypervisor) ->
    {ok, VMs} = sniffle_vm:list([{must, '=:=', <<"hypervisor">>, Hypervisor}], false),
    Values = [{<<"state">>, <<"limbo">>}, {<<"hypervisor">>, delete}],
    [sniffle_vm:set(VM, Values) || {_, VM} <- VMs],
    do_write(Hypervisor, unregister).

-spec get(Hypervisor::fifo:hypervisor_id()) ->
                 not_found | {ok, HV::#?HYPERVISOR{}} | {error, timeout}.
get(Hypervisor) ->
    sniffle_entity_read_fsm:start({?VNODE, ?SERVICE}, get, Hypervisor).

-spec status() -> {error, timeout} |
                  {ok, {Resources::fifo:object(),
                        Warnings::fifo:object()}}.
status() ->
    Storage = case backend() of
                  internal ->
                      <<"internal">>;
                  s3 ->
                      <<"s3">>
              end,
    case sniffle_cloud_status:start() of
        {ok, {Resources0, Warnings}} ->
            Resources = [{<<"storage">>, Storage} | Resources0],
            Warnings1 = case riak_core_status:transfers() of
                            {[], []} ->
                                Warnings;
                            {[], L} ->
                                W = jsxd:from_list(
                                      [{<<"category">>, <<"sniffle">>},
                                       {<<"element">>, <<"handoff">>},
                                       {<<"type">>, <<"info">>},
                                       {<<"message">>, bin_fmt("~b handofs pending.",
                                                               [length(L)])}]),
                                [W | Warnings];
                            {S, []} ->
                                Warnings ++ server_errors(S);
                            {S, L} ->
                                W = jsxd:from_list(
                                      [{<<"category">>, <<"sniffle">>},
                                       {<<"element">>, <<"handoff">>},
                                       {<<"type">>, <<"info">>},
                                       {<<"message">>, bin_fmt("~b handofs pending.",
                                                               [length(L)])}]),
                                [W | Warnings ++ server_errors(S)]
                        end,
            {ok, {ordsets:from_list(Resources), ordsets:from_list(Warnings1)}};
        E ->
            {ok, {[{<<"storage">>, Storage}],
                  [[{<<"category">>, <<"sniffle">>},
                    {<<"element">>, <<"general">>},
                    {<<"type">>, <<"error">>},
                    {<<"message">>, bin_fmt("Failed with ~b.", [E])}]]}}
    end.

-spec list() ->
                  {ok, [IPR::fifo:hypervisor_id()]} | {error, timeout}.
list() ->
    sniffle_coverage:start(?MASTER, ?SERVICE, list).

service(UUID, Action, Service) ->
    case sniffle_hypervisor:get(UUID) of
        {ok, HypervisorObj} ->
            {Host, Port} = ft_hypervisor:endpoint(HypervisorObj),
            service(Host, Port, Action, Service);
        E ->
            E
    end.

service(Host, Port, enable, Service) ->
    libchunter:service_enable(Host, Port, Service);
service(Host, Port, disable, Service) ->
    libchunter:service_disable(Host, Port, Service);
service(Host, Port, clear, Service) ->
    libchunter:service_clear(Host, Port, Service).

update(UUID) when is_binary(UUID) ->
    {ok, HypervisorObj} = sniffle_hypervisor:get(UUID),
    update(HypervisorObj);

update(HypervisorObj) ->
    {Host, Port} = ft_hypervisor:endpoint(HypervisorObj),
    libchunter:update(Host, Port).

update() ->
    {ok, L} = list([], true),
    [update(O) || {_, O} <- L],
    ok.

-define(S(T),
        T(UUID, V) ->
               do_write(UUID, T, V)).
?S(set_resource).
?S(set_characteristic).
?S(set_metadata).
?S(set_pool).
?S(set_service).
?S(alias).
?S(etherstubs).
?S(host).
?S(networks).
?S(path).
?S(port).
?S(sysinfo).
?S(uuid).
?S(version).
?S(virtualisation).


%%--------------------------------------------------------------------
%% @doc Lists all vm's and fiters by a given matcher set.
%% @end
%%--------------------------------------------------------------------
-spec list([fifo:matcher()], boolean()) -> {error, timeout} | {ok, [fifo:uuid()]}.

list(Requirements, true) ->
    {ok, Res} = sniffle_full_coverage:start(
                  ?MASTER, ?SERVICE, {list, Requirements, true}),
    Res1 = lists:sort(rankmatcher:apply_scales(Res)),
    {ok,  Res1};

list(Requirements, false) ->
    {ok, Res} = sniffle_coverage:start(
                  ?MASTER, ?SERVICE, {list, Requirements}),
    Res1 = rankmatcher:apply_scales(Res),
    {ok,  lists:sort(Res1)}.

-spec set(Hypervisor::fifo:hypervisor_id(),
          Attribute::fifo:keys(),
          Value::fifo:value()) ->
                 ok | {error, timeout}.
set(Hypervisor, Attribute, Value) ->
    set(Hypervisor, [{Attribute, Value}]).

-spec set(Hypervisor::fifo:hypervisor_id(),
          Attributes::fifo:attr_list()) ->
                 ok | {error, timeout}.
set(Hypervisor, Attributes) ->
    do_write(Hypervisor, set, Attributes).

%%%===================================================================
%%% Internal Functions
%%%===================================================================

do_write(User, Op) ->
    sniffle_entity_write_fsm:write({?VNODE, ?SERVICE}, User, Op).

do_write(User, Op, Val) ->
    sniffle_entity_write_fsm:write({?VNODE, ?SERVICE}, User, Op, Val).

bin_fmt(F, L) ->
    list_to_binary(io_lib:format(F, L)).

server_errors(Servers) ->
    lists:map(fun (Server) ->
                      jsxd:from_list(
                        [{<<"category">>, <<"sniffle">>},
                         {<<"element">>, list_to_binary(atom_to_list(Server))},
                         {<<"type">>, <<"critical">>},
                         {<<"message">>, bin_fmt("Sniffle server ~s down.", [Server])}])
              end, Servers).

backend() ->
    sniffle_opt:get(storage, general, backend, large_data_backend, internal).
