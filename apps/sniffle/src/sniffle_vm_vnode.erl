-module(sniffle_vm_vnode).
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

-export([
         master/0,
         aae_repair/2,
         hash_object/2
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

-define(SERVICE, sniffle).

-define(MASTER, sniffle_vnode_master).

-define(REQ(R), #req{request = R, bucket = <<"vm">>}).
-define(REQ(ID, R), #req{id = ID, request = R, bucket = <<"vm">>}).

%%%===================================================================
%%% AAE
%%%===================================================================

master() ->
    ?MASTER.

hash_object(BKey, RObj) ->
    sniffle_vnode:hash_object(BKey, RObj).

aae_repair(_, Key) ->
    lager:debug("AAE Repair: ~p", [Key]),
    sniffle_vm:get(Key).

%%%===================================================================
%%% API
%%%===================================================================

repair(Preflist, Vm, VClock, Obj) ->
    riak_core_vnode_master:command(Preflist,
                                   ?REQ({repair, Vm, VClock, Obj}),
                                   ignore,
                                   ?MASTER).

request(Preflist, ReqID, Request) ->
    riak_core_vnode_master:command(Preflist,
                                   ?REQ(ReqID, Request),
                                   {fsm, undefined, self()},
                                   ?MASTER).

%%%===================================================================
%%% API - reads
%%%===================================================================

-spec get(any(), any(), Vm::fifo:uuid()) -> ok.

get(Preflist, ReqID, Vm) ->
    request(Preflist, ReqID, {get, Vm}).

%%%===================================================================
%%% API - writes
%%%===================================================================

sync_repair(Preflist, ReqID, UUID, Obj) ->
    request(Preflist, ReqID, {sync_repair, UUID, Obj}).

-spec register(any(), any(), fifo:uuid(), binary()) -> ok.

register(Preflist, {_, Coordinator} = ReqID, VM, Hypervisor) ->
    RegFn = fun(not_found) ->
                    H0 = ft_vm:new(ReqID),
                    H1 = ft_vm:uuid(ReqID, VM, H0),
                    H2 = ft_vm:hypervisor(ReqID, Hypervisor, H1),
                    ft_obj:new(H2, Coordinator);
               ({ok, O}) ->
                    H0 = ft_obj:val(O),
                    H1 = ft_vm:load(ReqID, H0),
                    H2 = ft_vm:uuid(ReqID, VM, H1),
                    H3 = ft_vm:hypervisor(ReqID, Hypervisor, H2),
                    ft_obj:update(H3, Coordinator, O)
            end,
    request(Preflist, ReqID, {add, VM, RegFn}).

log(Preflist, ReqID, UUID, [Time, Log]) ->
    request(Preflist, ReqID, {change, log, UUID, [Time, Log]}).

-spec unregister(any(), any(), fifo:uuid()) -> ok.

unregister(Preflist, ReqID, UUDI) ->
    request(Preflist, ReqID, {delete, UUDI}).

set_iprange_map(Preflist, ReqID, UUID, [IP, Net]) ->
    request(Preflist, ReqID, {change, set_iprange_map, UUID, [IP, Net]}).

set_network_map(Preflist, ReqID, UUID, [IP, Net]) ->
    request(Preflist, ReqID, {change, set_network_map, UUID, [IP, Net]}).

set_hostname_map(Preflist, ReqID, UUID, [IP, Net]) ->
    request(Preflist, ReqID, {change, set_hostname_map, UUID, [IP, Net]}).

-define(S(Field),
        Field(Preflist, ReqID, UUID, Val) ->
               request(Preflist, ReqID, {change, Field, UUID, [Val]})).

?S(set_service).
?S(set_backup).
?S(set_snapshot).
?S(set_config).
?S(set_info).
?S(set_docker).
?S(set_metadata).
?S(add_fw_rule).
?S(remove_fw_rule).
?S(add_grouping).
?S(remove_grouping).
?S(state).
?S(deleting).
?S(creating).
?S(alias).
?S(owner).
?S(dataset).
?S(package).
?S(created_at).
?S(created_by).
?S(vm_type).
?S(hypervisor).

%%%===================================================================
%%% VNode
%%%===================================================================
