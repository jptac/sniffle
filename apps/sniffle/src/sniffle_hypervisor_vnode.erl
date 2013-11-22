-module(sniffle_hypervisor_vnode).
-behaviour(riak_core_vnode).
-behaviour(riak_core_aae_vnode).
-include("sniffle.hrl").
-include_lib("riak_core/include/riak_core_vnode.hrl").

-export([
         repair/4,
         get/3,
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

-ignore_xref([
              get/3,
              register/4,
              repair/4,
              set/4,
              start_vnode/1,
              unregister/3,
              handle_info/2
             ]).

-record(state, {db, partition, node, hashtrees}).

-define(SERVICE, sniffle_hypervisor).

-define(MASTER, sniffle_hypervisor_vnode_master).

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
    sniffle_hypervisor:get(Key).

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

repair(IdxNode, Hypervisor, VClock, Obj) ->
    riak_core_vnode_master:command(IdxNode,
                                   {repair, Hypervisor, VClock, Obj},
                                   ignore,
                                   ?MASTER).

%%%===================================================================
%%% API - reads
%%%===================================================================

get(Preflist, ReqID, Hypervisor) ->
    riak_core_vnode_master:command(Preflist,
                                   {get, ReqID, Hypervisor},
                                   {fsm, undefined, self()},
                                   ?MASTER).

%%%===================================================================
%%% API - writes
%%%===================================================================

register(Preflist, ReqID, Hypervisor, Data) ->
    riak_core_vnode_master:command(Preflist,
                                   {register, ReqID, Hypervisor, Data},
                                   {fsm, undefined, self()},
                                   ?MASTER).

unregister(Preflist, ReqID, Hypervisor) ->
    riak_core_vnode_master:command(Preflist,
                                   {unregister, ReqID, Hypervisor},
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

init([Partition]) ->
    DB = list_to_atom(integer_to_list(Partition)),
    fifo_db:start(DB),
    HT = riak_core_aae_vnode:maybe_create_hashtrees(?SERVICE,
                                                    Partition,
                                                    undefined),
    {ok, #state{db = DB, hashtrees = HT, partition = Partition, node = node()}}.

handle_command(ping, _Sender, State) ->
    {reply, {pong, State#state.partition}, State};

handle_command({repair, Hypervisor, VClock, Obj}, _Sender, State) ->
    case fifo_db:get(State#state.db, <<"hypervisor">>, Hypervisor) of
        {ok, #sniffle_obj{vclock = VC1}} when VC1 =:= VClock ->
            do_put(Hypervisor, Obj, State);
        not_found ->
            do_put(Hypervisor, Obj, State);
        _ ->
            lager:error("[hypervisors] Read repair failed, data was updated too recent.")
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
    case fifo_db:get(DB, <<"hypervisor">>, Key) of
        {ok, Term} ->
            riak_core_aae_vnode:update_hashtree(<<"hypervisor">>, Key,
                                                term_to_binary(Term),
                                                State#state.hashtrees);
        _ ->
            %% Make sure hashtree isn't tracking deleted data
            riak_core_index_hashtree:delete({<<"hypervisor">>, Key}, State#state.hashtrees)
    end,
    {noreply, State};

handle_command(?FOLD_REQ{foldfun=Fun, acc0=Acc0}, _Sender, State) ->
    Acc = fifo_db:fold(State#state.db, <<"hypervisor">>,
                       fun(K, V, O) ->
                               Fun({<<"hypervisor">>, K}, V, O)
                       end, Acc0),
    {reply, Acc, State};

%%%===================================================================
%%% General
%%%===================================================================

handle_command({register, {ReqID, Coordinator}, Hypervisor, [Ip, Port]}, _Sender, State) ->
    H0 = statebox:new(fun sniffle_hypervisor_state:new/0),
    H1 = statebox:modify({fun sniffle_hypervisor_state:uuid/2, [Hypervisor]}, H0),
    H2 = statebox:modify({fun sniffle_hypervisor_state:host/2, [Ip]}, H1),
    H3 = statebox:modify({fun sniffle_hypervisor_state:port/2, [Port]}, H2),

    VC0 = vclock:fresh(),
    VC = vclock:increment(Coordinator, VC0),
    HObject = #sniffle_obj{val=H3, vclock=VC},

    do_put(Hypervisor, HObject, State),
    {reply, {ok, ReqID}, State};

handle_command({get, ReqID, Hypervisor}, _Sender, State) ->
    Res = case fifo_db:get(State#state.db, <<"hypervisor">>, Hypervisor) of
              {ok, R} ->
                  R;
              not_found ->
                  not_found
          end,
    NodeIdx = {State#state.partition, State#state.node},
    {reply, {ok, ReqID, NodeIdx, Res}, State};

handle_command({unregister, {ReqID, _Coordinator}, Hypervisor}, _Sender, State) ->
    fifo_db:delete(State#state.db, <<"hypervisor">>, Hypervisor),
    riak_core_index_hashtree:delete({<<"hypervisor">>, Hypervisor}, State#state.hashtrees),
    {reply, {ok, ReqID}, State};

handle_command({set,
                {ReqID, Coordinator}, Hypervisor,
                Resources}, _Sender, State) ->
    case fifo_db:get(State#state.db, <<"hypervisor">>, Hypervisor) of
        {ok, #sniffle_obj{val=H0} = O} ->
            H1 = statebox:modify({fun sniffle_hypervisor_state:load/1,[]}, H0),
            H2 = lists:foldr(
                   fun ({Resource, Value}, H) ->
                           statebox:modify(
                             {fun sniffle_hypervisor_state:set/3,
                              [Resource, Value]}, H)
                   end, H1, Resources),
            H3 = statebox:truncate(?STATEBOX_TRUNCATE, statebox:expire(?STATEBOX_EXPIRE, H2)),
            Obj = sniffle_obj:update(H3, Coordinator, O),
            do_put(Hypervisor, Obj, State),
            {reply, {ok, ReqID}, State};
        R ->
            lager:error("[hypervisors] tried to write to a non existing hypervisor ~p, causing read repair", [R]),
            spawn(sniffle_hypervisor, get, [Hypervisor]),
            {reply, {ok, ReqID, not_found}, State}
    end;

handle_command(Message, _Sender, State) ->
    lager:error("[hypervisors] Unknown command: ~p", [Message]),
    {noreply, State}.

handle_handoff_command(?FOLD_REQ{foldfun=Fun, acc0=Acc0}, _Sender, State) ->
    Acc = fifo_db:fold(State#state.db,
                       <<"hypervisor">>, Fun, Acc0),
    {reply, Acc, State};

handle_handoff_command({get, _ReqID, _Hypervisor} = Req, Sender, State) ->
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
    {Hypervisor, Obj} = binary_to_term(Data),
    do_put(Hypervisor, Obj, State),
    {reply, ok, State}.

encode_handoff_item(Hypervisor, Data) ->
    term_to_binary({Hypervisor, Data}).

is_empty(State) ->
    fifo_db:fold_keys(State#state.db,
                      <<"hypervisor">>,
                      fun (_, _) ->
                              {false, State}
                      end, {true, State}).

delete(State) ->
    Trans = fifo_db:fold_keys(State#state.db,
                              <<"hypervisor">>,
                              fun (K, A) ->
                                      [{delete, <<"hypervisor", K/binary>>} | A]
                              end, []),
    fifo_db:transact(State#state.db, Trans),
    {ok, State}.

handle_coverage({list, Requirements}, _KeySpaces, {_, ReqID, _}, State) ->
    Getter = fun(#sniffle_obj{val=S0}, Resource) ->
                     jsxd:get(Resource, 0, statebox:value(S0))
             end,
    List = fifo_db:fold(State#state.db,
                        <<"hypervisor">>,
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

handle_coverage(list, _KeySpaces, {_, ReqID, _}, State) ->
    List = fifo_db:fold_keys(State#state.db,
                             <<"hypervisor">>,
                             fun (K, L) ->
                                     [K|L]
                             end, []),
    {reply,
     {ok, ReqID, {State#state.partition, State#state.node}, List},
     State};



handle_coverage(status, _KeySpaces, {_, ReqID, _}, State) ->
    R = fifo_db:fold(
          State#state.db, <<"hypervisor">>,
          fun(K, #sniffle_obj{val=S0}, {Res, Warnings}) ->
                  H=statebox:value(S0),
                  {ok, Host} = jsxd:get(<<"host">>, H),
                  {ok, Port} = jsxd:get(<<"port">>, H),
                  Warnings1 =
                      case libchunter:ping(binary_to_list(Host), Port) of
                          {error,connection_failed} ->
                              [jsxd:from_list(
                                 [{<<"category">>, <<"chunter">>},
                                  {<<"element">>, K},
                                  {<<"type">>, <<"critical">>},
                                  {<<"message">>,
                                   bin_fmt("Chunter server ~s down.", [K])}]) |
                               Warnings];
                          pong ->
                              Warnings
                      end,
                  {Res1, W2} =
                      case jsxd:get(<<"pools">>, H) of
                          undefined ->
                              {jsxd:get(<<"resources">>, [], H), Warnings1};
                          {ok, Pools} ->
                              jsxd:fold(
                                fun (Name, Pool, {ResAcc, WarningsAcc}) ->
                                        Size = jsxd:get(<<"size">>, 0, Pool),
                                        Used = jsxd:get(<<"used">>, 0, Pool),
                                        ResAcc1 =
                                            jsxd:thread([{update, <<"size">>,
                                                          fun(C) ->
                                                                  C + Size
                                                          end, Size},
                                                         {update, <<"used">>,
                                                          fun(C) ->
                                                                  C + Used
                                                          end, Used}],
                                                        ResAcc),
                                        case jsxd:get(<<"health">>, <<"ONLINE">>, Pool) of
                                            <<"ONLINE">> ->
                                                {ResAcc1, WarningsAcc};
                                            PoolState ->
                                                {ResAcc1,
                                                 [jsxd:from_list(
                                                    [{<<"category">>, <<"chunter">>},
                                                     {<<"element">>, Name},
                                                     {<<"type">>, <<"critical">>},
                                                     {<<"message">>,
                                                      bin_fmt("Zpool ~s in state ~s.", [Name, PoolState])}])|
                                                  WarningsAcc]}
                                        end
                                end,{jsxd:get(<<"resources">>, [], H), Warnings1}, Pools)
                      end,
                  {[{K, Res1} | Res], W2}
          end, {[], []}),
    {reply,
     {ok, ReqID, {State#state.partition, State#state.node}, R},
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
    fifo_db:put(State#state.db, <<"hypervisor">>, Key, Obj),
    riak_core_aae_vnode:update_hashtree(<<"hypervisor">>, Key, term_to_binary(Obj),
                                        State#state.hashtrees).

%%%===================================================================
%%% General
%%%===================================================================

bin_fmt(F, L) ->
    list_to_binary(io_lib:format(F, L)).
