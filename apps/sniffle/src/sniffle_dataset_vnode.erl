-module(sniffle_dataset_vnode).
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
         handle_info/2
        ]).

-export([
         master/0,
         aae_repair/2,
         hashtree_pid/1,
         rehash/3,
         hash_object/2,
         request_hashtree_pid/1
        ]).

-ignore_xref([
              create/4,
              delete/3,
              get/3,
              repair/4,
              set/4,
              start_vnode/1,
              handle_info/2
             ]).

-record(state, {db, partition, node, hashtrees}).

-define(SERVICE, sniffle_dataset).

-define(MASTER, sniffle_dataset_vnode_master).

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
    sniffle_dataset:get(Key).

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

repair(IdxNode, Dataset, VClock, Obj) ->
    riak_core_vnode_master:command([IdxNode],
                                   {repair, Dataset, VClock, Obj},
                                   ?MASTER).

%%%===================================================================
%%% API - reads
%%%===================================================================

get(Preflist, ReqID, Dataset) ->
    riak_core_vnode_master:command(Preflist,
                                   {get, ReqID, Dataset},
                                   {fsm, undefined, self()},
                                   ?MASTER).

%%%===================================================================
%%% API - writes
%%%===================================================================

create(Preflist, ReqID, Dataset, Data) ->
    riak_core_vnode_master:command(Preflist,
                                   {create, ReqID, Dataset, Data},
                                   {fsm, undefined, self()},
                                   ?MASTER).

delete(Preflist, ReqID, Dataset) ->
    riak_core_vnode_master:command(Preflist,
                                   {delete, ReqID, Dataset},
                                   {fsm, undefined, self()},
                                   ?MASTER).
set(Preflist, ReqID, Dataset, Data) ->
    riak_core_vnode_master:command(Preflist,
                                   {set, ReqID, Dataset, Data},
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

handle_command({repair, Dataset, VClock, Obj}, _Sender, State) ->
    case fifo_db:get(State#state.db, <<"dataset">>, Dataset) of
        {ok, #sniffle_obj{vclock = VC1}} when VC1 =:= VClock ->
            do_put(Dataset, Obj, State);
        not_found ->
            do_put(Dataset, Obj, State);
        _ ->
            lager:error("[datasets] Read repair failed, data was updated too recent.")
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
    case fifo_db:get(DB, <<"dataset">>, Key) of
        {ok, Term} ->
            riak_core_aae_vnode:update_hashtree(<<"dataset">>, Key,
                                                term_to_binary(Term),
                                                State#state.hashtrees);
        _ ->
            %% Make sure hashtree isn't tracking deleted data
            riak_core_index_hashtree:delete({<<"dataset">>, Key},
                                            State#state.hashtrees)
    end,
    {noreply, State};

handle_command(?FOLD_REQ{foldfun=Fun, acc0=Acc0}, _Sender, State) ->
    lager:debug("Fold on ~p", [State#state.partition]),
    Acc = fifo_db:fold(State#state.db, <<"dataset">>,
                       fun(K, V, O) ->
                               Fun({<<"dataset">>, K}, V, O)
                       end, Acc0),
    {reply, Acc, State};


%%%===================================================================
%%% General
%%%===================================================================

handle_command({get, ReqID, Dataset}, _Sender, State) ->
    Res = case fifo_db:get(State#state.db, <<"dataset">>, Dataset) of
              {ok, R} ->
                  R;
              not_found ->
                  not_found
          end,
    NodeIdx = {State#state.partition, State#state.node},
    {reply, {ok, ReqID, NodeIdx, Res}, State};

handle_command({create, {ReqID, Coordinator}, Dataset, []},
               _Sender, State) ->
    I0 = statebox:new(fun sniffle_dataset_state:new/0),
    I1 = statebox:modify({fun sniffle_dataset_state:name/2, [Dataset]}, I0),
    VC0 = vclock:fresh(),
    VC = vclock:increment(Coordinator, VC0),
    Obj = #sniffle_obj{val=I1, vclock=VC},
    do_put(Dataset, Obj, State),
    {reply, {ok, ReqID}, State};

handle_command({delete, {ReqID, _Coordinator}, Dataset}, _Sender, State) ->
    fifo_db:delete(State#state.db, <<"dataset">>, Dataset),
    riak_core_index_hashtree:delete({<<"dataset">>, Dataset}, State#state.hashtrees),
    {reply, {ok, ReqID}, State};

handle_command({set,
                {ReqID, Coordinator}, Dataset,
                Resources}, _Sender, State) ->
    case fifo_db:get(State#state.db, <<"dataset">>, Dataset) of
        {ok, #sniffle_obj{val=H0} = O} ->
            H1 = statebox:modify({fun sniffle_dataset_state:load/1,[]}, H0),
            H2 = lists:foldr(
                   fun ({Resource, Value}, H) ->
                           statebox:modify(
                             {fun sniffle_dataset_state:set/3,
                              [Resource, Value]}, H)
                   end, H1, Resources),
            H3 = statebox:expire(?STATEBOX_EXPIRE, H2),
            Obj = sniffle_obj:update(H3, Coordinator, O),
            do_put(Dataset, Obj, State),
            {reply, {ok, ReqID}, State};
        R ->
            lager:error("[datasets] tried to write to a non existing dataset: ~p", [R]),
            {reply, {ok, ReqID, not_found}, State}
    end;

handle_command(_Message, _Sender, State) ->
    {noreply, State}.

handle_handoff_command(?FOLD_REQ{foldfun=Fun, acc0=Acc0}, _Sender, State) ->
    Acc = fifo_db:fold(State#state.db, <<"dataset">>, Fun, Acc0),
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
    {Dataset, Obj} = binary_to_term(Data),
    fifo_db:put(State#state.db, <<"dataset">>, Dataset, Obj),
    do_put(Dataset, Obj, State),
    {reply, ok, State}.

encode_handoff_item(Dataset, Data) ->
    term_to_binary({Dataset, Data}).

is_empty(State) ->
    fifo_db:fold_keys(State#state.db,
                      <<"dataset">>,
                      fun (_, _) ->
                              {false, State}
                      end, {true, State}).

delete(State) ->
    Trans = fifo_db:fold_keys(State#state.db,
                              <<"dataset">>,
                              fun (K, A) ->
                                      [{delete, <<"dataset", K/binary>>} | A]
                              end, []),
    fifo_db:transact(State#state.db, Trans),
    {ok, State}.

handle_coverage(list, _KeySpaces, {_, ReqID, _}, State) ->
    List = fifo_db:fold_keys(State#state.db,
                             <<"dataset">>,
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
                        <<"dataset">>,
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

terminate(_Reason,  _State) ->
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
    fifo_db:put(State#state.db, <<"dataset">>, Key, Obj),
    riak_core_aae_vnode:update_hashtree(<<"dataset">>, Key, term_to_binary(Obj),
                                        State#state.hashtrees).
