-module(sniffle_grouping_vnode).

-behaviour(riak_core_vnode).
-behaviour(riak_core_aae_vnode).
-include("sniffle.hrl").
-include_lib("riak_core/include/riak_core_vnode.hrl").

-export([
         repair/4,
         get/3,
         create/4,
         delete/3,
         set/4
        ]).

-export([
         start_vnode/1,
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
         add_element/4,
         remove_element/4,
         add_grouping/4,
         remove_grouping/4,
         sync_repair/4
        ]).

-export([
         master/0,
         aae_repair/2,
         hash_object/2
        ]).

-ignore_xref([
              create/4,
              add_element/4,
              remove_element/4,
              add_grouping/4,
              remove_grouping/4,
              delete/3,
              get/3,
              repair/4,
              set/4,
              start_vnode/1,
              handle_info/2,
              sync_repair/4
             ]).

-define(SERVICE, sniffle_grouping).

-define(MASTER, sniffle_grouping_vnode_master).

%%%===================================================================
%%% AAE
%%%===================================================================

master() ->
    ?MASTER.

hash_object(BKey, RObj) ->
    sniffle_vnode:hash_object(BKey, RObj).

aae_repair(_, Key) ->
    sniffle_grouping:get(Key).

%%%===================================================================
%%% API
%%%===================================================================

start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

repair(IdxNode, Grouping, VClock, Obj) ->
    riak_core_vnode_master:command([IdxNode],
                                   {repair, Grouping, VClock, Obj},
                                   ?MASTER).

%%%===================================================================
%%% API - reads
%%%===================================================================

get(Preflist, ReqID, Grouping) ->
    riak_core_vnode_master:command(Preflist,
                                   {get, ReqID, Grouping},
                                   {fsm, undefined, self()},
                                   ?MASTER).

%%%===================================================================
%%% API - writes
%%%===================================================================

add_element(Preflist, ReqID, UUID, Obj) ->
    riak_core_vnode_master:command(Preflist,
                                   {add_element, ReqID, UUID, Obj},
                                   {fsm, undefined, self()},
                                   ?MASTER).

remove_element(Preflist, ReqID, UUID, Obj) ->
    riak_core_vnode_master:command(Preflist,
                                   {remove_element, ReqID, UUID, Obj},
                                   {fsm, undefined, self()},
                                   ?MASTER).

add_grouping(Preflist, ReqID, UUID, Obj) ->
    riak_core_vnode_master:command(Preflist,
                                   {add_grouping, ReqID, UUID, Obj},
                                   {fsm, undefined, self()},
                                   ?MASTER).

remove_grouping(Preflist, ReqID, UUID, Obj) ->
    riak_core_vnode_master:command(Preflist,
                                   {remove_grouping, ReqID, UUID, Obj},
                                   {fsm, undefined, self()},
                                   ?MASTER).

sync_repair(Preflist, ReqID, UUID, Obj) ->
    riak_core_vnode_master:command(Preflist,
                                   {sync_repair, ReqID, UUID, Obj},
                                   {fsm, undefined, self()},
                                   ?MASTER).

create(Preflist, ReqID, Grouping, Data) ->
    riak_core_vnode_master:command(Preflist,
                                   {create, ReqID, Grouping, Data},
                                   {fsm, undefined, self()},
                                   ?MASTER).

delete(Preflist, ReqID, Grouping) ->
    riak_core_vnode_master:command(Preflist,
                                   {delete, ReqID, Grouping},
                                   {fsm, undefined, self()},
                                   ?MASTER).
set(Preflist, ReqID, Grouping, Data) ->
    riak_core_vnode_master:command(Preflist,
                                   {set, ReqID, Grouping, Data},
                                   {fsm, undefined, self()},
                                   ?MASTER).

%%%===================================================================
%%% VNode
%%%===================================================================

init([Part]) ->
    sniffle_vnode:init(Part, <<"grouping">>, ?SERVICE, ?MODULE,
                       ft_grouping).

handle_command({create, {ReqID, Coordinator} = ID, UUID, [Name, Type]},
               _Sender, State) ->
    G0 = ft_grouping:new(ID),
    G1 = ft_grouping:uuid(ID, UUID, G0),
    G2 = ft_grouping:name(ID, Name, G1),
    G3 = ft_grouping:type(ID, Type, G2),
    VC0 = vclock:fresh(),
    VC = vclock:increment(Coordinator, VC0),
    Obj = #sniffle_obj{val=G3, vclock=VC},
    sniffle_vnode:put(UUID, Obj, State),
    {reply, {ok, ReqID}, State};

handle_command(Message, Sender, State) ->
    sniffle_vnode:handle_command(Message, Sender, State).

handle_handoff_command(?FOLD_REQ{foldfun=Fun, acc0=Acc0}, _Sender, State) ->
    Acc = fifo_db:fold(State#vstate.db, <<"grouping">>, Fun, Acc0),
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
    {Grouping, Obj} = binary_to_term(Data),
    sniffle_vnode:put(Grouping, Obj, State),
    {reply, ok, State}.

encode_handoff_item(Grouping, Data) ->
    term_to_binary({Grouping, Data}).

is_empty(State) ->
    sniffle_vnode:is_empty(State).

delete(State) ->
    sniffle_vnode:delete(State).

handle_coverage(Req, KeySpaces, Sender, State) ->
    sniffle_vnode:handle_coverage(Req, KeySpaces, Sender, State).

handle_exit(_Pid, _Reason, State) ->
    {noreply, State}.

terminate(_Reason,  _State) ->
    ok.

%%%===================================================================
%%% AAE
%%%===================================================================

handle_info(Msg, State) ->
    sniffle_vnode:handle_info(Msg, State).
