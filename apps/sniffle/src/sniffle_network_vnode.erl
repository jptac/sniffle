-module(sniffle_network_vnode).
-behaviour(riak_core_vnode).
-behaviour(riak_core_aae_vnode).
-include("sniffle.hrl").
-include_lib("riak_core/include/riak_core_vnode.hrl").
-include_lib("fifo_dt/include/ft.hrl").

-export([
         repair/4,
         get/3,
         create/4,
         delete/3,
         add_iprange/4,
         remove_iprange/4,
         set/4
        ]).

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

-export([
         master/0,
         aae_repair/2,
         hash_object/2
        ]).

-ignore_xref([
              release_ip/4,
              create/4,
              delete/3,
              get/3,
              set/4,
              claim_ip/4,
              repair/4,
              add_iprange/4,
              remove_iprange/4,
              start_vnode/1,
              handle_info/2,
              sync_repair/4
             ]).

-define(SERVICE, sniffle_network).

-define(MASTER, sniffle_network_vnode_master).

%%%===================================================================
%%% AAE
%%%===================================================================

master() ->
    ?MASTER.

hash_object(BKey, RObj) ->
    lager:debug("Hashing Key: ~p", [BKey]),
    list_to_binary(integer_to_list(erlang:phash2({BKey, RObj}))).

aae_repair(_, Key) ->
    lager:debug("AAE Repair: ~p", [Key]),
    sniffle_network:get(Key).

%%%===================================================================
%%% API
%%%===================================================================

start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

repair(IdxNode, Network, VClock, Obj) ->
    riak_core_vnode_master:command(IdxNode,
                                   {repair, Network, VClock, Obj},
                                   ignore,
                                   ?MASTER).

%%%===================================================================
%%% API - reads
%%%===================================================================

get(Preflist, ReqID, Network) ->
    riak_core_vnode_master:command(Preflist,
                                   {get, ReqID, Network},
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

delete(Preflist, ReqID, Network) ->
    riak_core_vnode_master:command(Preflist,
                                   {delete, ReqID, Network},
                                   {fsm, undefined, self()},
                                   ?MASTER).

add_iprange(Preflist, ReqID, Network, IPRange) ->
    riak_core_vnode_master:command(Preflist,
                                   {add_iprange, ReqID, Network, IPRange},
                                   {fsm, undefined, self()},
                                   ?MASTER).
remove_iprange(Preflist, ReqID, Network, IPRange) ->
    riak_core_vnode_master:command(Preflist,
                                   {remove_iprange, ReqID, Network, IPRange},
                                   {fsm, undefined, self()},
                                   ?MASTER).

set(Preflist, ReqID, Hypervisor, Data) ->
    riak_core_vnode_master:command(Preflist,
                                   {set, ReqID, Hypervisor, Data},
                                   {fsm, undefined, self()},
                                   ?MASTER).

%%%===================================================================
%%% VNode
%%%===================================================================

init([Part]) ->
    sniffle_vnode:init(Part, <<"network">>, ?SERVICE, ?MODULE, ft_network).

%%%===================================================================
%%% General
%%%===================================================================

handle_command({create, {ReqID, Coordinator}=TID, UUID,
                [Name]},
               _Sender, State) ->
    V0 = ft_network:new(TID),
    V1 = ft_network:uuid(TID, UUID, V0),
    V2 = ft_network:name(TID, Name, V1),
    Obj = ft_obj:new(V2, Coordinator),
    sniffle_vnode:put(UUID, Obj, State),
    {reply, {ok, ReqID}, State};

handle_command({add_iprange,
                {ReqID, Coordinator} = TID, Network,
                IPRange}, _Sender, State) ->
    case fifo_db:get(State#vstate.db, <<"network">>, Network) of
        {ok, O} ->
            H0 = ft_obj:val(O),
            H1 = ft_network:load(TID, H0),
            H2 = ft_network:add_iprange(TID, IPRange, H1),
            Obj = ft_obj:update(H2, Coordinator, O),
            sniffle_vnode:put(Network, Obj, State),
            {reply, {ok, ReqID}, State};
        R ->
            lager:error("[hypervisors] tried to write to a non existing hypervisor: ~p", [R]),
            {reply, {ok, ReqID, not_found}, State}
    end;

handle_command({remove_iprange,
                {ReqID, Coordinator} = TID, Network,
                IPRange}, _Sender, State) ->
    case fifo_db:get(State#vstate.db, <<"network">>, Network) of
        {ok, O} ->
            H0 = ft_obj:val(O),
            H1 = ft_network:load(TID, H0),
            H2 = ft_network:remove_iprange(TID, IPRange, H1),
            Obj = ft_obj:update(H2, Coordinator, O),
            sniffle_vnode:put(Network, Obj, State),
            {reply, {ok, ReqID}, State};
        R ->
            lager:error("[hypervisors] tried to write to a non existing hypervisor: ~p", [R]),
            {reply, {ok, ReqID, not_found}, State}
    end;

handle_command(Message, Sender, State) ->
    sniffle_vnode:handle_command(Message, Sender, State).

handle_handoff_command(?FOLD_REQ{foldfun=Fun, acc0=Acc0}, _Sender, State) ->
    Acc = fifo_db:fold(State#vstate.db, <<"network">>, Fun, Acc0),
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
    {Network, HObject} = binary_to_term(Data),
    sniffle_vnode:put(Network, HObject, State),
    {reply, ok, State}.

encode_handoff_item(Network, Data) ->
    term_to_binary({Network, Data}).

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
