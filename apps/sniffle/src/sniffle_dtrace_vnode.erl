-module(sniffle_dtrace_vnode).
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
         sync_repair/4
        ]).

-export([
         master/0,
         aae_repair/2,
         hash_object/2
        ]).

-ignore_xref([
              create/4,
              delete/3,
              get/3,
              repair/4,
              set/4,
              start_vnode/1,
              handle_info/2,
              sync_repair/4
             ]).

-export([
         name/4,
         uuid/4,
         script/4,
         set_metadata/4,
         set_config/4
        ]).


-ignore_xref([
         name/4,
         uuid/4,
         script/4,
         set_metadata/4,
         set_config/4
        ]).


-define(SERVICE, sniffle_dtrace).

-define(MASTER, sniffle_dtrace_vnode_master).

%%%===================================================================
%%% AAE
%%%===================================================================

master() ->
    ?MASTER.

hash_object(BKey, RObj) ->
    sniffle_vnode:hash_object(BKey, RObj).

aae_repair(_, Key) ->
    lager:debug("AAE Repair: ~p", [Key]),
    sniffle_dtrace:get(Key).

%%%===================================================================
%%% API
%%%===================================================================

start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

repair(IdxNode, Dtrace, VClock, Obj) ->
    riak_core_vnode_master:command([IdxNode],
                                   {repair, Dtrace, VClock, Obj},
                                   ?MASTER).

%%%===================================================================
%%% API - reads
%%%===================================================================

get(Preflist, ReqID, Dtrace) ->
    riak_core_vnode_master:command(Preflist,
                                   {get, ReqID, Dtrace},
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

create(Preflist, ReqID, Dtrace, Data) ->
    riak_core_vnode_master:command(Preflist,
                                   {create, ReqID, Dtrace, Data},
                                   {fsm, undefined, self()},
                                   ?MASTER).

delete(Preflist, ReqID, Dtrace) ->
    riak_core_vnode_master:command(Preflist,
                                   {delete, ReqID, Dtrace},
                                   {fsm, undefined, self()},
                                   ?MASTER).
set(Preflist, ReqID, Dtrace, Data) ->
    riak_core_vnode_master:command(Preflist,
                                   {set, ReqID, Dtrace, Data},
                                   {fsm, undefined, self()},
                                   ?MASTER).

?VSET(name).
?VSET(uuid).
?VSET(script).
?VSET(set_metadata).
?VSET(set_config).

%%%===================================================================
%%% VNode
%%%===================================================================

init([Part]) ->
    sniffle_vnode:init(Part, <<"dtrace">>, ?SERVICE, ?MODULE,
                       ft_dtrace).

%%%===================================================================
%%% General
%%%===================================================================

handle_command({create, {ReqID, Coordinator} = ID, Dtrace, [Name, Script]},
               _Sender, State) ->
    I0 = ft_dtrace:new(ID),
    I1 = ft_dtrace:uuid(ID, Dtrace, I0),
    I2 = ft_dtrace:name(ID, Name, I1),
    I3 = ft_dtrace:script(ID, Script, I2),
    Obj = ft_obj:new(I3, Coordinator),
    sniffle_vnode:put(Dtrace, Obj, State),
    {reply, {ok, ReqID}, State};

handle_command(Message, Sender, State) ->
    sniffle_vnode:handle_command(Message, Sender, State).

handle_handoff_command(?FOLD_REQ{foldfun=Fun, acc0=Acc0}, _Sender, State) ->
    Acc = fifo_db:fold(State#vstate.db, <<"dtrace">>, Fun, Acc0),
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

encode_handoff_item(Dtrace, Data) ->
    term_to_binary({Dtrace, Data}).

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
