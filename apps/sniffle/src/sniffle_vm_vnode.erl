-module(sniffle_vm_vnode).
-behaviour(riak_core_vnode).
-behaviour(riak_core_aae_vnode).
-include("sniffle.hrl").
-include_lib("riak_core/include/riak_core_vnode.hrl").

-export([repair/4,
         get/3,
         log/4,
         register/4,
         unregister/3,
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
         handle_info/2]).

-export([
         master/0,
         aae_repair/2,
         hashtree_pid/1,
         rehash/3,
         hash_object/2,
         request_hashtree_pid/1
        ]).

%% those functions do not get called directly.
-ignore_xref([
              get/3,
              log/4,
              register/4,
              repair/4,
              set/4,
              start_vnode/1,
              unregister/3,
              handle_info/2
             ]).

-record(state, {db, partition, node, hashtrees}).

-define(SERVICE, sniffle_vm).

-define(MASTER, sniffle_vm_vnode_master).

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
    sniffle_vm:get(Key).

hashtree_pid(Partition) ->
    riak_core_vnode_master:sync_command({Partition, node()},
                                        {hashtree_pid, node()},
                                        ?MASTER,
                                        infinity).

%% Asynchronous version of {@link hashtree_pid/1} that sends a message back to
%% the calling process. Used by the {@link riak_core_entropy_manager}.
request_hashtree_pid(Partition) ->
    ReqId = {hashtree_pid, Partition},
    riak_core_vnode_master:command({Partition, node()},
                                   {hashtree_pid, node()},
                                   {raw, ReqId, self()},
                                   ?MASTER).

%% Used by {@link riak_core_exchange_fsm} to force a vnode to update the hashtree
%% for repaired keys. Typically, repairing keys will trigger read repair that
%% will update the AAE hash in the write path. However, if the AAE tree is
%% divergent from the KV data, it is possible that AAE will try to repair keys
%% that do not have divergent KV replicas. In that case, read repair is never
%% triggered. Always rehashing keys after any attempt at repair ensures that
%% AAE does not try to repair the same non-divergent keys over and over.
rehash(Preflist, _, Key) ->
    riak_core_vnode_master:command(Preflist,
                                   {rehash, Key},
                                   ignore,
                                   ?MASTER).

%%%===================================================================
%%% API
%%%===================================================================

start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

repair(IdxNode, Vm, VClock, Obj) ->
    riak_core_vnode_master:command(IdxNode,
                                   {repair, Vm, VClock, Obj},
                                   ignore,
                                   ?MASTER).

%%%===================================================================
%%% API - reads
%%%===================================================================

-spec get(any(), any(), Vm::fifo:uuid()) -> ok.

get(Preflist, ReqID, Vm) ->
    riak_core_vnode_master:command(Preflist,
                                   {get, ReqID, Vm},
                                   {fsm, undefined, self()},
                                   ?MASTER).

%%%===================================================================
%%% API - writes
%%%===================================================================

-spec register(any(), any(), fifo:uuid(), binary()) -> ok.

register(Preflist, ReqID, Vm, Hypervisor) ->
    riak_core_vnode_master:command(Preflist,
                                   {register, ReqID, Vm, Hypervisor},
                                   {fsm, undefined, self()},
                                   ?MASTER).

log(Preflist, ReqID, Vm, Log) ->
    riak_core_vnode_master:command(Preflist,
                                   {log, ReqID, Vm, Log},
                                   {fsm, undefined, self()},
                                   ?MASTER).

-spec unregister(any(), any(), fifo:uuid()) -> ok.

unregister(Preflist, ReqID, Vm) ->
    riak_core_vnode_master:command(Preflist,
                                   {unregister, ReqID, Vm},
                                   {fsm, undefined, self()},
                                   ?MASTER).


-spec set(any(), any(), fifo:uuid(), [{Key::binary(), V::fifo:value()}]) -> ok.

set(Preflist, ReqID, Vm, Data) ->
    riak_core_vnode_master:command(Preflist,
                                   {set, ReqID, Vm, Data},
                                   {fsm, undefined, self()},
                                   ?MASTER).

%%%===================================================================
%%% VNode
%%%===================================================================

init([Partition]) ->
    DB = list_to_atom(integer_to_list(Partition)),
    fifo_db:start(DB),
    HT = riak_core_aae_vnode:maybe_create_hashtrees(?SERVICE,
                                                    Partition,
                                                    undefined),
    {ok, #state{db = DB, hashtrees = HT, partition = Partition, node = node()}}.

-type vm_command() ::
        ping |
        {repair, Vm::fifo:uuid(), Obj::any()} |
        {get, ReqID::any(), Vm::fifo:uuid()} |
        {get, ReqID::any(), Vm::fifo:uuid()} |
        {log, ReqID::any(), Vm::fifo:uuid(), Log::fifo:log()} |
        {register, {ReqID::any(), Coordinator::any()}, Vm::fifo:uuid(), Hypervisor::binary()} |
        {unregister, {ReqID::any(), _Coordinator::any()}, Vm::fifo:uuid()} |
        {set,
         {ReqID::any(), Coordinator::any()}, Vm::fifo:uuid(),
         Resources::[{Key::binary(), Value::fifo:value()}]}.

-spec handle_command(vm_command(), any(), any()) ->
                            {reply, any(), any()} |
                            {noreply, any()}.

handle_command(ping, _Sender, State) ->
    {reply, {pong, State#state.partition}, State};

handle_command({repair, Vm, VClock, Obj}, _Sender, State) ->
    case fifo_db:get(State#state.db, <<"vm">>, Vm) of
        {ok, #sniffle_obj{vclock = VC1}} when VC1 =:= VClock ->
            do_put(Vm, Obj, State);
        not_found ->
            do_put(Vm, Obj, State);
        _ ->
            lager:error("[vms] Read repair failed, data was updated too recent.")
    end,
    {noreply, State};

%%%===================================================================
%%% AAE
%%%===================================================================

handle_command({hashtree_pid, Node}, _, State=#state{hashtrees=HT}) ->
    %% Handle riak_core request forwarding during ownership handoff.
    %% Following is necessary in cases where anti-entropy was enabled
    %% after the vnode was already running
    case {node(), HT} of
        {Node, undefined} ->
            HT1 =  riak_core_aae_vnode:maybe_create_hashtrees(
                     ?SERVICE,
                     State#state.partition,
                     HT),
            {reply, {ok, HT1}, State#state{hashtrees = HT1}};
        {Node, _} ->
            {reply, {ok, HT}, State};
        _ ->
            {reply, {error, wrong_node}, State}
    end;

handle_command({rehash, Key}, _, State=#state{db=DB}) ->
    case fifo_db:get(DB, <<"vm">>, Key) of
        {ok, Term} ->
            riak_core_aae_vnode:update_hashtree(<<"vm">>, Key,
                                                term_to_binary(Term),
                                                State#state.hashtrees);
        _ ->
            %% Make sure hashtree isn't tracking deleted data
            riak_core_index_hashtree:delete({<<"vm">>, Key}, State#state.hashtrees)
    end,
    {noreply, State};

%%%===================================================================
%%% General
%%%===================================================================

handle_command({get, ReqID, Vm}, _Sender, State) ->
    Res = case fifo_db:get(State#state.db, <<"vm">>, Vm) of
              {ok, R} ->
                  R;
              not_found ->
                  not_found
          end,
    NodeIdx = {State#state.partition, State#state.node},
    {reply, {ok, ReqID, NodeIdx, Res}, State};

handle_command({register, {ReqID, Coordinator}, Vm, Hypervisor}, _Sender, State) ->
    HObject = case fifo_db:get(State#state.db, <<"vm">>, Vm) of
                  not_found ->
                      H0 = statebox:new(fun sniffle_vm_state:new/0),
                      H1 = statebox:modify({fun sniffle_vm_state:uuid/2, [Vm]}, H0),
                      H2 = statebox:modify({fun sniffle_vm_state:hypervisor/2, [Hypervisor]}, H1),
                      VC0 = vclock:fresh(),
                      VC = vclock:increment(Coordinator, VC0),
                      #sniffle_obj{val=H2, vclock=VC};
                  {ok, #sniffle_obj{val=H0} = O} ->
                      H1 = statebox:modify({fun sniffle_vm_state:load/1,[]}, H0),
                      H2 = statebox:modify({fun sniffle_vm_state:hypervisor/2, [Hypervisor]}, H1),
                      H3 = statebox:expire(?STATEBOX_EXPIRE, H2),
                      sniffle_obj:update(H3, Coordinator, O)
              end,
    do_put(Vm, HObject, State),
    {reply, {ok, ReqID}, State};

handle_command({unregister, {ReqID, _Coordinator}, Vm}, _Sender, State) ->
    fifo_db:delete(State#state.db, <<"vm">>, Vm),
    riak_core_index_hashtree:delete({<<"vm">>, Vm}, State#state.hashtrees),
    {reply, {ok, ReqID}, State};

handle_command({set,
                {ReqID, Coordinator}, Vm,
                Resources}, _Sender, State) ->
    case fifo_db:get(State#state.db, <<"vm">>, Vm) of
        {ok, #sniffle_obj{val=H0} = O} ->
            H1 = statebox:modify({fun sniffle_vm_state:load/1,[]}, H0),
            H2 = lists:foldr(
                   fun ({Resource, Value}, H) ->
                           statebox:modify(
                             {fun sniffle_vm_state:set/3,
                              [Resource, Value]}, H)
                   end, H1, Resources),
            H3 = statebox:expire(?STATEBOX_EXPIRE, H2),
            Obj = sniffle_obj:update(H3, Coordinator, O),
            do_put(Vm, Obj, State),
            {reply, {ok, ReqID}, State};
        R ->
            lager:error("[vms] tried to write to a non existing vm: ~p", [R]),
            {reply, {ok, ReqID, not_found}, State}
    end;

handle_command({log,
                {ReqID, Coordinator}, Vm,
                {Time, Log}}, _Sender, State) ->
    case fifo_db:get(State#state.db, <<"vm">>, Vm) of
        {ok, #sniffle_obj{val=H0} = O} ->
            H1 = statebox:modify({fun sniffle_vm_state:load/1,[]}, H0),
            H2 = statebox:modify({fun sniffle_vm_state:log/3, [Time, Log]}, H1),
            H3 = statebox:expire(?STATEBOX_EXPIRE, H2),
            Obj = sniffle_obj:update(H3, Coordinator, O),
            do_put(Vm, Obj, State),
            {reply, {ok, ReqID}, State};
        R ->
            lager:error("[vms] tried to write to a non existing vm: ~p", [R]),
            {reply, {ok, ReqID, not_found}, State}
    end;

%%%===================================================================
%%% AAE
%%%===================================================================

handle_command(?FOLD_REQ{foldfun=Fun, acc0=Acc0}, _Sender, State) ->
    lager:debug("Fold on ~p", [State#state.partition]),
    Acc = fifo_db:fold(State#state.db, <<"vm">>,
                       fun(K, V, O) ->
                               Fun({<<"vm">>, K}, V, O)
                       end, Acc0),
    {reply, Acc, State};

handle_command(Message, _Sender, State) ->
    lager:error("[vms] Unknown command: ~p", [Message]),
    {noreply, State}.

handle_handoff_command(?FOLD_REQ{foldfun=Fun, acc0=Acc0}, _Sender, State) ->
    Acc = fifo_db:fold(State#state.db, <<"vm">>, Fun, Acc0),
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
    {Vm, HObject} = binary_to_term(Data),
    do_put(Vm, HObject, State),
    {reply, ok, State}.

encode_handoff_item(Vm, Data) ->
    term_to_binary({Vm, Data}).

is_empty(State) ->
    fifo_db:fold_keys(State#state.db,
                      <<"vm">>,
                      fun (_, _) ->
                              {false, State}
                      end, {true, State}).

delete(State) ->
    Trans = fifo_db:fold_keys(State#state.db,
                              <<"vm">>,
                              fun (K, A) ->
                                      [{delete, <<"vm", K/binary>>} | A]
                              end, []),
    fifo_db:transact(State#state.db, Trans),
    {ok, State}.

handle_coverage(list, _KeySpaces, {_, ReqID, _}, State) ->
    List = fifo_db:fold_keys(
             State#state.db,
             <<"vm">>,
             fun (K, L) ->
                     [K|L]
             end, []),

    {reply,
     {ok, ReqID, {State#state.partition,State#state.node}, List},
     State};

handle_coverage({list, Requirements}, _KeySpaces, {_, ReqID, _}, State) ->
    Getter = fun(#sniffle_obj{val=S0}, Resource) ->
                     jsxd:get(Resource, 0, statebox:value(S0))
             end,
    List = fifo_db:fold(State#state.db,
                        <<"vm">>,
                        fun (Key, E, C) ->
                                case rankmatcher:match(E, Getter, Requirements) of
                                    false ->
                                        C;
                                    Pts ->
                                        [{Pts, Key} | C]
                                end
                        end, []),
    {reply,
     {ok, ReqID, {State#state.partition, State#state.node}, List},
     State};

handle_coverage(_Req, _KeySpaces, _Sender, State) ->
    {stop, not_implemented, State}.

handle_exit(_Pid, _Reason, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

%%%===================================================================
%%% AAE
%%%===================================================================

handle_info(retry_create_hashtree, State=#state{hashtrees=undefined}) ->
    HT = riak_core_aae_vnode:maybe_create_hashtrees(?SERVICE, State#state.partition,
                                                    undefined),
    {ok, State#state{hashtrees = HT}};
handle_info(retry_create_hashtree, State) ->
    {ok, State};
handle_info({'DOWN', _, _, Pid, _}, State=#state{hashtrees=Pid}) ->
    HT = riak_core_aae_vnode:maybe_create_hashtrees(?SERVICE, State#state.partition,
                                                    Pid),
    {ok, State#state{hashtrees = HT}};
handle_info({'DOWN', _, _, _, _}, State) ->
    {ok, State};
handle_info(_, State) ->
    {ok, State}.

do_put(Key, Obj, State) ->
    fifo_db:put(State#state.db, <<"vm">>, Key, Obj),
    riak_core_aae_vnode:update_hashtree(<<"vm">>, Key,
                                        term_to_binary(Obj),
                                        State#state.hashtrees).
