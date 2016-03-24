-module(sniffle_iprange_vnode).
-behaviour(riak_core_vnode).
-behaviour(riak_core_aae_vnode).
-include("sniffle.hrl").
-include_lib("riak_core/include/riak_core_vnode.hrl").

-export([
         repair/4,
         get/3,
         create/4,
         delete/3,
         claim_ip/4,
         release_ip/4,
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
              release_ip/4,
              start_vnode/1,
              handle_info/2,
              sync_repair/4
             ]).

-export([
         name/4,
         uuid/4,
         network/4,
         netmask/4,
         gateway/4,
         set_metadata/4,
         tag/4,
         vlan/4
        ]).

-ignore_xref([
              name/4,
              uuid/4,
              network/4,
              netmask/4,
              gateway/4,
              set_metadata/4,
              tag/4,
              vlan/4
             ]).

-define(SERVICE, sniffle_iprange).

-define(MASTER, sniffle_iprange_vnode_master).

%%%===================================================================
%%% AAE
%%%===================================================================

master() ->
    ?MASTER.

hash_object(BKey, RObj) ->
    sniffle_vnode:hash_object(BKey, RObj).

aae_repair(_, Key) ->
    lager:debug("AAE Repair: ~p", [Key]),
    sniffle_iprange:get(Key).

%%%===================================================================
%%% API
%%%===================================================================

start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

repair(IdxNode, Iprange, VClock, Obj) ->
    riak_core_vnode_master:command(IdxNode,
                                   {repair, Iprange, VClock, Obj},
                                   ignore,
                                   ?MASTER).

%%%===================================================================
%%% API - reads
%%%===================================================================

get(Preflist, ReqID, Iprange) ->
    riak_core_vnode_master:command(Preflist,
                                   {get, ReqID, Iprange},
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

delete(Preflist, ReqID, Iprange) ->
    riak_core_vnode_master:command(Preflist,
                                   {delete, ReqID, Iprange},
                                   {fsm, undefined, self()},
                                   ?MASTER).

claim_ip(Preflist, ReqID, Iprange, Ip) ->
    riak_core_vnode_master:command(Preflist,
                                   {ip, claim, ReqID, Iprange, Ip},
                                   {fsm, undefined, self()},
                                   ?MASTER).

release_ip(Preflist, ReqID, Iprange, IP) ->
    riak_core_vnode_master:command(Preflist,
                                   {ip, release, ReqID, Iprange, IP},
                                   {fsm, undefined, self()},
                                   ?MASTER).

set(Preflist, ReqID, Hypervisor, Data) ->
    riak_core_vnode_master:command(Preflist,
                                   {set, ReqID, Hypervisor, Data},
                                   {fsm, undefined, self()},
                                   ?MASTER).

?VSET(name).
?VSET(uuid).
?VSET(network).
?VSET(netmask).
?VSET(gateway).
?VSET(set_metadata).
?VSET(tag).
?VSET(vlan).

%%%===================================================================
%%% VNode
%%%===================================================================

init([Part]) ->
    sniffle_vnode:init(Part, <<"iprange">>, ?SERVICE, ?MODULE, ft_iprange).

handle_command({create, {ReqID, Coordinator} = ID, UUID,
                [Iprange, Network, Gateway, Netmask, First, Last, Tag, Vlan]},
               _Sender, State) ->
    I0 = ft_iprange:new(ID, First, Last),
    I1 = lists:foldl(
           fun ({F, A}, IPR) ->
                   erlang:apply(F, [ID | A] ++ [IPR])
           end, I0, [{fun ft_iprange:uuid/3, [UUID]},
                     {fun ft_iprange:name/3, [Iprange]},
                     {fun ft_iprange:network/3, [Network]},
                     {fun ft_iprange:gateway/3, [Gateway]},
                     {fun ft_iprange:netmask/3, [Netmask]},
                     {fun ft_iprange:tag/3, [Tag]},
                     {fun ft_iprange:vlan/3, [Vlan]}]),
    Obj = ft_obj:new(I1, Coordinator),
    sniffle_vnode:put(UUID, Obj, State),
    {reply, {ok, ReqID}, State};

handle_command({ip, claim,
                {ReqID, Coordinator}=ID, Iprange, IP}, _Sender, State) ->
    case fifo_db:get(State#vstate.db, <<"iprange">>, Iprange) of
        {ok, O} ->
            H0 = ft_obj:val(O),
            H1 = ft_iprange:load(ID, H0),
            case ft_iprange:claim_ip(ID, IP, H1) of
                {ok, H2} ->
                    Obj = ft_obj:update(H2, Coordinator, O),
                    sniffle_vnode:put(Iprange, Obj, State),
                    {reply, {ok, ReqID,
                             {ft_iprange:tag(H2),
                              IP,
                              ft_iprange:netmask(H2),
                              ft_iprange:gateway(H2),
                              ft_iprange:vlan(H2)}
                            }, State};
                _ ->
                    {reply, {error, ReqID, duplicate}, State}
            end;
        _ ->
            {reply, {ok, ReqID, not_found}, State}
    end;

handle_command({ip, release,
                {ReqID, Coordinator}=ID, Iprange, IP}, _Sender, State) ->
    case fifo_db:get(State#vstate.db, <<"iprange">>, Iprange) of
        {ok, O} ->
            H0 = ft_obj:val(O),
            H1 = ft_iprange:load(ID, H0),
            {ok, H2} = ft_iprange:release_ip(ID, IP, H1),
            Obj =  ft_obj:update(H2, Coordinator, O),
            sniffle_vnode:put(Iprange, Obj, State),
            {reply, {ok, ReqID}, State};

        _ ->
            {reply, {ok, ReqID, not_found}, State}
    end;

handle_command(Message, Sender, State) ->
    sniffle_vnode:handle_command(Message, Sender, State).

handle_handoff_command(?FOLD_REQ{foldfun=Fun, acc0=Acc0}, _Sender, State) ->
    Acc = fifo_db:fold(State#vstate.db, <<"iprange">>, Fun, Acc0),
    {reply, Acc, State};

handle_handoff_command({get, _ReqID, _Iprange} = Req, Sender, State) ->
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

encode_handoff_item(Iprange, Data) ->
    term_to_binary({Iprange, Data}).

is_empty(State) ->
    sniffle_vnode:is_empty(State).

delete(State) ->
    sniffle_vnode:delete(State).

handle_coverage({overlap, ReqID, _Start, _Stop}, _KeySpaces, _Sender, State) ->
    {reply,
     {ok, ReqID, {State#vstate.partition, State#vstate.node}},
     State};

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
