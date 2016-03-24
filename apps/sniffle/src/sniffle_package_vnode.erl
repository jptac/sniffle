-module(sniffle_package_vnode).
-behaviour(riak_core_vnode).
-behaviour(riak_core_aae_vnode).
-include("sniffle.hrl").
-include_lib("riak_core/include/riak_core_vnode.hrl").

-export([repair/4,
         get/3,
         create/4,
         delete/3,
         set/4]).

-export([start_vnode/1,
         init/1,
         terminate/2,
         handle_command/3,
         is_empty/1,
         delete/1,
         handle_handoff_command/3,
         handoff_starting/2,
         handoff_cancelled/1,
         handoff_finished/2,
         handle_handoff_data/2,
         encode_handoff_item/2,
         handle_coverage/4,
         handle_exit/3,
         handle_info/2,
         sync_repair/4]).

-export([master/0,
         aae_repair/2,
         hash_object/2]).

-ignore_xref([create/4,
              delete/3,
              get/3,
              repair/4,
              set/4,
              start_vnode/1,
              handle_info/2,
              sync_repair/4]).

-export([
         org_resource_inc/4, org_resource_dec/4, org_resource_remove/4,
         set_metadata/4,
         blocksize/4,
         compression/4,
         cpu_cap/4,
         cpu_shares/4,
         max_swap/4,
         name/4,
         quota/4,
         ram/4,
         uuid/4,
         zfs_io_priority/4,
         remove_requirement/4,
         add_requirement/4
        ]).

-ignore_xref([
              org_resource_inc/4, org_resource_dec/4, org_resource_remove/4,
              set_metadata/4,
              blocksize/4,
              compression/4,
              cpu_cap/4,
              cpu_shares/4,
              max_swap/4,
              name/4,
              quota/4,
              ram/4,
              uuid/4,
              zfs_io_priority/4,
              remove_requirement/4,
              add_requirement/4
             ]).

-define(SERVICE, sniffle_package).

-define(MASTER, sniffle_package_vnode_master).

%%%===================================================================
%%% AAE
%%%===================================================================

master() ->
    ?MASTER.

hash_object(BKey, RObj) ->
    sniffle_vnode:hash_object(BKey, RObj).

aae_repair(_, Key) ->
    lager:debug("AAE Repair: ~p", [Key]),
    sniffle_package:get(Key).


%%%===================================================================
%%% API
%%%===================================================================

start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

repair(IdxNode, Package, VClock, Obj) ->
    riak_core_vnode_master:command(IdxNode,
                                   {repair, Package, VClock, Obj},
                                   ignore,
                                   ?MASTER).

%%%===================================================================
%%% API - reads
%%%===================================================================

get(Preflist, ReqID, Package) ->
    riak_core_vnode_master:command(Preflist,
                                   {get, ReqID, Package},
                                   {fsm, undefined, self()},
                                   ?MASTER).

%%%===================================================================
%%% API - writes
%%%===================================================================

sync_repair(Preflist, ReqID, UUID, Obj) ->
    riak_core_vnode_master:command(Preflist,
                                   {sync_repair, ReqID, UUID, Obj},
                                   {fsm, undefined, self()},
                                   ?MASTER).

create(Preflist, ReqID, UUID, Data) ->
    riak_core_vnode_master:command(Preflist,
                                   {create, ReqID, UUID, Data},
                                   {fsm, undefined, self()},
                                   ?MASTER).

delete(Preflist, ReqID, Package) ->
    riak_core_vnode_master:command(Preflist,
                                   {delete, ReqID, Package},
                                   {fsm, undefined, self()},
                                   ?MASTER).

set(Preflist, ReqID, Vm, Data) ->
    riak_core_vnode_master:command(Preflist,
                                   {set, ReqID, Vm, Data},
                                   {fsm, undefined, self()},
                                   ?MASTER).

org_resource_inc(Preflist, ReqID, UUID, {Resource, Value}) ->
    riak_core_vnode_master:command(Preflist,
                                   {org_resource_inc, ReqID, UUID, Resource,
                                    Value},
                                   {fsm, undefined, self()},
                                   ?MASTER).

org_resource_dec(Preflist, ReqID, UUID, {Resource, Value}) ->
    riak_core_vnode_master:command(Preflist,
                                   {org_resource_dec, ReqID, UUID, Resource,
                                    Value},
                                   {fsm, undefined, self()},
                                   ?MASTER).

org_resource_remove(Preflist, ReqID, UUID, Resource) ->
    riak_core_vnode_master:command(Preflist,
                                   {org_resource_remove, ReqID, UUID, Resource},
                                   {fsm, undefined, self()},
                                   ?MASTER).



?VSET(set_metadata).
?VSET(blocksize).
?VSET(compression).
?VSET(cpu_cap).
?VSET(cpu_shares).
?VSET(max_swap).
?VSET(name).
?VSET(quota).
?VSET(ram).
?VSET(uuid).
?VSET(zfs_io_priority).
?VSET(remove_requirement).
?VSET(add_requirement).

%%%===================================================================
%%% VNode
%%%===================================================================

init([Part]) ->
    sniffle_vnode:init(Part, <<"package">>, ?SERVICE, ?MODULE,
                       ft_package).

%%%===================================================================
%%% General
%%%===================================================================

handle_command({create, {ReqID, Coordinator}=ID, UUID, [Package]},
               _Sender, State) ->
    I0 = ft_package:new(ID),
    I1 = ft_package:uuid(ID, UUID, I0),
    I2 = ft_package:name(ID, Package, I1),
    Obj = ft_obj:new(I2, Coordinator),
    sniffle_vnode:put(UUID, Obj, State),
    {reply, {ok, ReqID}, State};

handle_command(Message, Sender, State) ->
    sniffle_vnode:handle_command(Message, Sender, State).

handle_handoff_command(?FOLD_REQ{foldfun=Fun, acc0=Acc0}, _Sender, State) ->
    Acc = fifo_db:fold(State#vstate.db, <<"package">>, Fun, Acc0),
    {reply, Acc, State};

handle_handoff_command({get, _ReqID, _Vm} = Req, Sender, State) ->
    handle_command(Req, Sender, State);

handle_handoff_command(Req, Sender, State) ->
    S1 = case handle_command(Req, Sender, State) of
             {noreply, NewState} ->
                 NewState;
             {reply, _, NewState} ->
                 NewState
         end,
    {forward, S1}.

handoff_starting(_TargetNode, State) ->
    {true, State}.

handoff_cancelled(State) ->
    {ok, State}.

handoff_finished(_TargetNode, State) ->
    {ok, State}.

handle_handoff_data(Data, State) ->
    sniffle_vnode:repair(Data, State).

encode_handoff_item(Package, Data) ->
    term_to_binary({Package, Data}).

is_empty(State) ->
    sniffle_vnode:is_empty(State).

delete(State) ->
    sniffle_vnode:delete(State).

handle_coverage(Req, KeySpaces, Sender, State) ->
    sniffle_vnode:handle_coverage(Req, KeySpaces, Sender, State).

handle_exit(_Pid, _Reason, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

%%%===================================================================
%%% AAE
%%%===================================================================

handle_info(Msg, State) ->
    sniffle_vnode:handle_info(Msg, State).
