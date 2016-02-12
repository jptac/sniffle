-module(sniffle_hostname_vnode).
-behaviour(riak_core_vnode).
-behaviour(riak_core_aae_vnode).
-include("sniffle.hrl").
-include_lib("riak_core/include/riak_core_vnode.hrl").

-export([repair/4,
         get/3,
         delete/3,
         add_a/4, remove_a/4
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

-export([master/0,
         aae_repair/2,
         hash_object/2]).

-ignore_xref([repair/4,
              get/3,
              delete/3,
              add_a/4, remove_a/4
             ]).

-define(SERVICE, sniffle_hostname).

-define(MASTER, sniffle_hostname_vnode_master).

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
    sniffle_hostname:get(Key).

%%%===================================================================
%%% API
%%%===================================================================

start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

repair(IdxNode, Hostname, VClock, Obj) ->
    riak_core_vnode_master:command(IdxNode,
                                   {repair, Hostname, VClock, Obj},
                                   ignore,
                                   ?MASTER).

%%%===================================================================
%%% API - reads
%%%===================================================================

get(Preflist, ReqID, Hostname) ->
    riak_core_vnode_master:command(Preflist,
                                   {get, ReqID, Hostname},
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

delete(Preflist, ReqID, Hostname) ->
    riak_core_vnode_master:command(Preflist,
                                   {delete, ReqID, Hostname},
                                   {fsm, undefined, self()},
                                   ?MASTER).

add_a(Preflist, ReqID, Hostname, Data) ->
    riak_core_vnode_master:command(Preflist,
                                   {add_a, ReqID, Hostname, Data},
                                   {fsm, undefined, self()},
                                   ?MASTER).

remove_a(Preflist, ReqID, Hostname, Data) ->
    riak_core_vnode_master:command(Preflist,
                                   {remove_a, ReqID, Hostname, Data},
                                   {fsm, undefined, self()},
                                   ?MASTER).


%%%===================================================================
%%% VNode
%%%===================================================================

init([Part]) ->
    sniffle_vnode:init(Part, <<"hostname">>, ?SERVICE, ?MODULE,
                       ft_hostname).

%%%===================================================================
%%% Node Specific
%%%===================================================================

handle_command(Message, Sender, State) ->
    sniffle_vnode:handle_command(Message, Sender, State).

handle_handoff_command(?FOLD_REQ{foldfun=Fun, acc0=Acc0}, _Sender, State) ->
    Acc = fifo_db:fold(State#vstate.db, <<"hostname">>, Fun, Acc0),
    {reply, Acc, State};

handle_handoff_command({get, _ReqID, _Hostname} = Req, Sender, State) ->
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

encode_handoff_item(Hostname, Data) ->
    term_to_binary({Hostname, Data}).

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

%%%===================================================================
%%% General
%%%===================================================================
