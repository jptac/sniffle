-module(sniffle_hypervisor).
-include("sniffle.hrl").

-define(MASTER, sniffle_hypervisor_vnode_master).
-define(VNODE, sniffle_hypervisor_vnode).
-define(SERVICE, sniffle_hypervisor).

-define(FM(Met, Mod, Fun, Args),
        folsom_metrics:histogram_timed_update(
          {sniffle, hypervisor, Met},
          Mod, Fun, Args)).

-export(
   [
    register/3,
    unregister/1,
    get/1,
    list/0,
    list/2,
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
    ?FM(wipe, sniffle_coverage, start, [?MASTER, ?SERVICE, {wipe, UUID}]).

sync_repair(UUID, Obj) ->
    do_write(UUID, sync_repair, Obj).

list_() ->
    {ok, Res} = ?FM(list_all, sniffle_full_coverage, raw,
                    [?MASTER, ?SERVICE, []]),
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
            host(Hypervisor, IP),
            port(Hypervisor, Port),
            duplicate
    end.

-spec unregister(Hypervisor::fifo:hypervisor_id()) ->
                        not_found | {error, timeout} | ok.
unregister(Hypervisor) ->
    {ok, VMs} = sniffle_vm:list([{must, '=:=', <<"hypervisor">>, Hypervisor}],
                                false),
    [begin
         sniffle_vm:hypervisor(VM, <<>>),
         sniffle_vm:state(VM, <<"limbo">>)
     end || {_, VM} <- VMs],
    do_write(Hypervisor, unregister).

-spec get(Hypervisor::fifo:hypervisor_id()) ->
                 not_found | {ok, HV::fifo:hypervisor()} | {error, timeout}.
get(Hypervisor) ->
    ?FM(get, sniffle_entity_read_fsm, start,
        [{?VNODE, ?SERVICE}, get, Hypervisor]).

-spec status() -> {error, timeout} |
                  {ok, {Resources::fifo:object(),
                        Warnings::fifo:object()}}.
status() ->
    %% TODO: We need to remove the storage part here!
    {ok, {Warnings, Resources}} = sniffle_watchdog:status(),
    Resources1 = [{<<"storage">>, <<"s3">>} | Resources],
    Warnings1 = [to_msg(W) || W <- Warnings],
    {ok,  {lists:sort(Resources1), lists:sort(Warnings1)}}.

to_msg({handoff, Node}) ->
    [
     {<<"category">>, <<"sniffle">>},
     {<<"element">>, <<"handoff">>},
     {<<"message">>, bin_fmt("Handoff pending on node ~s.", [Node])},
     {<<"type">>, <<"info">>}
    ];

to_msg({stopped, Node}) ->
    [
     {<<"category">>, <<"sniffle">>},
     {<<"element">>, <<"handoff">>},
     {<<"message">>, bin_fmt("Node ~s stopped.", [Node])},
     {<<"type">>, <<"warning">>}
    ];

to_msg({down, Node}) ->
    [{<<"category">>, <<"sniffle">>},
     {<<"element">>, <<"handoff">>},
     {<<"type">>, <<"error">>},
     {<<"message">>, bin_fmt("Node ~s DOWN.", [Node])}];

to_msg({chunter_down, UUID, Alias}) ->
    [
     {<<"category">>, <<"chunter">>},
     {<<"element">>, UUID},
     {<<"message">>, bin_fmt("Chunter node ~s(~s) down.", [Alias, UUID])},
     {<<"type">>, <<"critical">>}
    ];

to_msg({pool_error, UUID, Alias, Name, State}) ->
    [
     {<<"category">>, <<"chunter">>},
     {<<"element">>, UUID},
     {<<"message">>, bin_fmt("Zpool ~s on node ~s(~s) in state ~s.",
                             [Name, Alias, UUID, State])},
     {<<"type">>, <<"critical">>}
    ].


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

service(Host, Port, refresh, Service) ->
    libchunter:service_refresh(Host, Port, Service);
service(Host, Port, restart, Service) ->
    libchunter:service_restart(Host, Port, Service);

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

?SET(set_resource).
?SET(set_characteristic).
?SET(set_metadata).
?SET(set_pool).
?SET(set_service).
?SET(alias).
?SET(etherstubs).
?SET(host).
?SET(networks).
?SET(path).
?SET(port).
?SET(sysinfo).
?SET(uuid).
?SET(version).
?SET(virtualisation).

%%--------------------------------------------------------------------
%% @doc Lists all vm's and fiters by a given matcher set.
%% @end
%%--------------------------------------------------------------------
-spec list([fifo:matcher()], boolean()) ->
                  {error, timeout} | {ok, [fifo:uuid()]}.

list(Requirements, true) ->
    {ok, Res} = sniffle_full_coverage:list(?MASTER, ?SERVICE, Requirements),
    Res1 = lists:sort(rankmatcher:apply_scales(Res)),
    {ok,  Res1};

list(Requirements, false) ->
    {ok, Res} = sniffle_coverage:start(
                  ?MASTER, ?SERVICE, {list, Requirements}),
    Res1 = rankmatcher:apply_scales(Res),
    {ok,  lists:sort(Res1)}.

%%%===================================================================
%%% Internal Functions
%%%===================================================================

do_write(User, Op) ->
    ?FM(Op, sniffle_entity_write_fsm, write, [{?VNODE, ?SERVICE}, User, Op]).

do_write(User, Op, Val) ->
    ?FM(Op, sniffle_entity_write_fsm, write,
        [{?VNODE, ?SERVICE}, User, Op, Val]).

bin_fmt(F, L) ->
    list_to_binary(io_lib:format(F, L)).
