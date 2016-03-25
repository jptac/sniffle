-module(sniffle_hypervisor_cmd).
-define(BUCKET, <<"hypervisor">>).
-include("sniffle.hrl").

-export([repair/4,
         get/3,
         register/4,
         unregister/3,
         set_resource/4,
         set_characteristic/4,
         set_metadata/4,
         set_pool/4,
         set_service/4,
         alias/4,
         etherstubs/4,
         host/4,
         networks/4,
         path/4,
         port/4,
         sysinfo/4,
         uuid/4,
         version/4,
         virtualisation/4,
         sync_repair/4,
         last_seen/4
        ]).

%%%===================================================================
%%% API
%%%===================================================================

repair(Preflist, UUID, VClock, Obj) ->
    riak_core_vnode_master:command(Preflist,
                                   ?REQ({repair, UUID, VClock, Obj}),
                                   ignore,
                                   ?MASTER).

%%%===================================================================
%%% API - reads
%%%===================================================================

get(Preflist, ReqID, UUID) ->
    ?REQUEST(Preflist, ReqID, {get, UUID}).

%%%===================================================================
%%% API - writes
%%%===================================================================

sync_repair(Preflist, ReqID, UUID, Obj) ->
    ?REQUEST(Preflist, ReqID, {sync_repair, UUID, Obj}).

register_fn({_, Coordinator} = ReqID, not_found, [UUID, IP, Port]) ->
    H0 = ft_hypervisor:new(ReqID),
    H1 = ft_hypervisor:port(ReqID, Port, H0),
    H2 = ft_hypervisor:host(ReqID, IP, H1),
    H3 = ft_hypervisor:uuid(ReqID, UUID, H2),
    H4 = ft_hypervisor:path(ReqID, [{UUID, 1}], H3),
    {write, ok, ft_obj:new(H4, Coordinator)};

register_fn({_, Coordinator} = ReqID, {ok, O}, [UUID, IP, Port]) ->
    HV = ft_obj:val(O),
    H0 = ft_hypervisor:load(ReqID, HV),
    H1 = ft_hypervisor:port(ReqID, Port, H0),
    H2 = ft_hypervisor:host(ReqID, IP, H1),
    H3 = ft_hypervisor:uuid(ReqID, UUID, H2),
    {write, ok, ft_obj:update(H3, Coordinator, O)}.

register(Preflist, ReqID, UUID, Args) ->
    ?REQUEST(Preflist, ReqID, {apply, UUID, fun register_fn/3, [UUID | Args]}).

unregister(Preflist, ReqID, UUID) ->
    ?REQUEST(Preflist, ReqID, {delete, UUID}).

?VSET(last_seen).
?VSET(set_resource).
?VSET(set_characteristic).
?VSET(set_metadata).
?VSET(set_pool).
?VSET(set_service).
?VSET(alias).
?VSET(etherstubs).
?VSET(host).
?VSET(networks).
?VSET(path).
?VSET(port).
?VSET(sysinfo).
?VSET(uuid).
?VSET(version).
?VSET(virtualisation).
