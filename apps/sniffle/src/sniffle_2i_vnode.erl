-module(sniffle_2i_vnode).
-behaviour(riak_core_vnode).
-behaviour(riak_core_aae_vnode).
-include("sniffle.hrl").
-include_lib("riak_core/include/riak_core_vnode.hrl").

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
         handle_info/2]).

-export([
         master/0,
         aae_repair/2,
         hash_object/2
        ]).

%% Reads
-export([get/3]).

%% Writes
-export([add/4,
         delete/3,
         repair/4, sync_repair/4]).

-ignore_xref([
              start_vnode/1,
              get/3,
              add/4,
              delete/3,
              repair/4, sync_repair/4,
              handle_info/2
             ]).

-define(SERVICE, sniffle_2i).

-define(MASTER, sniffle_2i_vnode_master).

%%%===================================================================
%%% AAE
%%%===================================================================

master() ->
    ?MASTER.

hash_object(Key, Obj) ->
    sniffle_vnode:hash_object(Key, Obj).

aae_repair(_, Key) ->
    lager:debug("AAE Repair: ~p", [Key]),
    sniffle_2i:get(Key).

%%%===================================================================
%%% API
%%%===================================================================

start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).


repair(IdxNode, TK, VClock, Obj) ->
    riak_core_vnode_master:command(IdxNode,
                                   {repair, TK, VClock, Obj},
                                   ignore,
                                   ?MASTER).

%%%===================================================================
%%% API - reads
%%%===================================================================

get(Preflist, ReqID, Key) ->
    riak_core_vnode_master:command(Preflist,
                                   {get, ReqID, Key},
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

add(Preflist, ReqID, Key, Target) ->
    riak_core_vnode_master:command(Preflist,
                                   {add, ReqID, Key, Target},
                                   {fsm, undefined, self()},
                                   ?MASTER).

delete(Preflist, ReqID, Key) ->
    riak_core_vnode_master:command(Preflist,
                                   {delete, ReqID, Key},
                                   {fsm, undefined, self()},
                                   ?MASTER).

%%%===================================================================
%%% VNode
%%%===================================================================
init([Part]) ->
    sniffle_vnode:init(Part, <<"2i">>, ?SERVICE, ?MODULE, sniffle_2i_state).

%%%===================================================================
%%% General
%%%===================================================================

handle_command({add, {ReqID, Coordinator} = ID, Key, Target},
               _Sender, State) ->
    S2i = sniffle_2i_state:new(ID),
    S2i1 = sniffle_2i_state:target(ID, Target, S2i),
    S2i1Obj = ft_obj:new(S2i1, Coordinator),
    sniffle_vnode:put(Key, S2i1Obj, State),
    {reply, {ok, ReqID}, State};

%% why did we even have this?
%% handle_command({delete, {ReqID, _} = ID, Key},
%%                _Sender, State) ->
%%     sniffle_vnode:change(Key, target, [not_found], ID, State),
%%     {reply, {ok, ReqID}, State};

handle_command(Message, Sender, State) ->
    sniffle_vnode:handle_command(Message, Sender, State).

handle_handoff_command(?FOLD_REQ{foldfun=Fun, acc0=Acc0}, _Sender, State) ->
    Acc = fifo_db:fold(State#vstate.db, <<"2i">>, Fun, Acc0),
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

handoff_starting(TargetNode, State) ->
    lager:warning("Starting handof to: ~p", [TargetNode]),
    {true, State}.

handoff_cancelled(State) ->
    {ok, State}.

handoff_finished(_TargetNode, State) ->
    {ok, State}.

handle_handoff_data(Data, State) ->
    sniffle_vnode:repair(Data, State).


encode_handoff_item(Role, Data) ->
    term_to_binary({Role, Data}).

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

handle_info(Msg, State) ->
    sniffle_vnode:handle_info(Msg, State).
