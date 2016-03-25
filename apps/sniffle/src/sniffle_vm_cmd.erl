-module(sniffle_vm_cmd).
-define(BUCKET, <<"vm">>).
-include("sniffle.hrl").

-export([repair/4,
         get/3,
         log/4,
         register/4,
         unregister/3
        ]).

-export([
         sync_repair/4,
         set_iprange_map/4,
         set_network_map/4,
         set_hostname_map/4,
         set_backup/4,
         set_snapshot/4,
         set_service/4,
         set_config/4,
         set_info/4,
         set_metadata/4,
         set_docker/4,
         vm_type/4,
         created_at/4,
         created_by/4,
         add_grouping/4,
         remove_grouping/4,
         add_fw_rule/4,
         remove_fw_rule/4,
         state/4,
         deleting/4,
         creating/4,
         alias/4,
         owner/4,
         dataset/4,
         package/4,
         hypervisor/4
        ]).

%% those functions do not get called directly.
-ignore_xref([
              set_iprange_map/4,
              set_network_map/4,
              set_hostname_map/4,
              set_backup/4,
              set_config/4,
              set_snapshot/4,
              set_service/4,
              set_metadata/4,
              add_grouping/4,
              set_docker/4,
              vm_type/4,
              created_at/4,
              created_by/4,
              set_info/4,
              remove_grouping/4,
              add_fw_rule/4,
              remove_fw_rule/4,
              state/4,
              deleting/4,
              creating/4,
              alias/4,
              owner/4,
              dataset/4,
              package/4,
              hypervisor/4,

              get/3,
              log/4,
              register/4,
              repair/4,
              start_vnode/1,
              unregister/3,
              handle_info/2,
              sync_repair/4
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

-spec get(any(), any(), Vm::fifo:uuid()) -> ok.

get(Preflist, ReqID, UUID) ->
    ?REQUEST(Preflist, ReqID, {get, UUID}).

%%%===================================================================
%%% API - writes
%%%===================================================================

sync_repair(Preflist, ReqID, UUID, Obj) ->
    ?REQUEST(Preflist, ReqID, {sync_repair, UUID, Obj}).

register_fn({_, Coordinator} = ReqID, not_found, [UUID, Hypervisor]) ->
    H0 = ft_vm:new(ReqID),
    H1 = ft_vm:uuid(ReqID, UUID, H0),
    H2 = ft_vm:hypervisor(ReqID, Hypervisor, H1),
    {write, ok, ft_obj:new(H2, Coordinator)};
register_fn({_, Coordinator} = ReqID, {ok, O}, [UUID, Hypervisor]) ->
    H0 = ft_obj:val(O),
    H1 = ft_vm:load(ReqID, H0),
    H2 = ft_vm:uuid(ReqID, UUID, H1),
    H3 = ft_vm:hypervisor(ReqID, Hypervisor, H2),
    {write, ok, ft_obj:update(H3, Coordinator, O)}.

-spec register(any(), any(), fifo:uuid(), binary()) -> ok.

register(Preflist, ReqID, UUID, Hypervisor) ->
    ?REQUEST(Preflist, ReqID, {apply, UUID, fun register_fn/3,
                               [UUID, Hypervisor]}).

log(Preflist, ReqID, UUID, [Time, Log]) ->
    ?REQUEST(Preflist, ReqID, {change, log, UUID, [Time, Log]}).

-spec unregister(any(), any(), fifo:uuid()) -> ok.

unregister(Preflist, ReqID, UUDI) ->
    ?REQUEST(Preflist, ReqID, {delete, UUDI}).

?VSET(set_iprange_map).
?VSET(set_network_map).
?VSET(set_hostname_map).
?VSET(set_service).
?VSET(set_backup).
?VSET(set_snapshot).
?VSET(set_config).
?VSET(set_info).
?VSET(set_docker).
?VSET(set_metadata).
?VSET(add_fw_rule).
?VSET(remove_fw_rule).
?VSET(add_grouping).
?VSET(remove_grouping).
?VSET(state).
?VSET(deleting).
?VSET(creating).
?VSET(alias).
?VSET(owner).
?VSET(dataset).
?VSET(package).
?VSET(created_at).
?VSET(created_by).
?VSET(vm_type).
?VSET(hypervisor).

%%%===================================================================
%%% VNode
%%%===================================================================
