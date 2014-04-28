-module(sniffle_vnode).

-include("sniffle.hrl").
-include_lib("riak_core/include/riak_core_vnode.hrl").

-export([init/5,
         is_empty/1,
         delete/1,
         put/3,
         fold/4,
         handle_command/3,
         handle_coverage/4,
         handle_info/2,
         mkid/0,
         mkid/1]).

-ignore_xref([mkid/0, mkid/1]).

mkid() ->
    mkid(node()).

mkid(Actor) ->
    {mk_reqid(), Actor}.

mk_reqid() ->
    {MegaSecs,Secs,MicroSecs} = erlang:now(),
    (MegaSecs*1000000 + Secs)*1000000 + MicroSecs.

init(Partition, Bucket, Service, VNode, StateMod) ->
    DB = list_to_atom(integer_to_list(Partition)),
    fifo_db:start(DB),
    HT = riak_core_aae_vnode:maybe_create_hashtrees(Service, Partition, VNode,
                                                    undefined),
    WorkerPoolSize = application:get_env(sniffle, async_workers, 5),
    FoldWorkerPool = {pool, sniffle_worker, WorkerPoolSize, []},
    {ok,
     #vstate{db=DB, hashtrees=HT, partition=Partition, node=node(),
             service=Service, bucket=Bucket, state=StateMod, vnode=VNode},
     [FoldWorkerPool]}.

list(Getter, Requirements, Sender, State=#vstate{state=StateMod}) ->
    FoldFn = fun (Key, E, C) ->
                     E1 = E#sniffle_obj{val=StateMod:load(E#sniffle_obj.val)},
                     case rankmatcher:match(E1, Getter, Requirements) of
                         false ->
                             C;
                         Pts ->
                             [{Pts, {Key, E1}} | C]
                     end
             end,
    fold(FoldFn, [], Sender, State).

list_keys(Sender, State=#vstate{db=DB, bucket=Bucket}) ->
    FoldFn = fun (K, L) ->
                     [K|L]
             end,
    AsyncWork = fun() ->
                        fifo_db:fold_keys(DB, Bucket, FoldFn, [])
                end,
    FinishFun = fun(Data) ->
                        reply(Data, Sender, State)
                end,
    {async, {fold, AsyncWork, FinishFun}, Sender, State}.

list_keys(Getter, Requirements, Sender, State) ->
    FoldFn = fun (Key, E, C) ->
                     case rankmatcher:match(E, Getter, Requirements) of
                         false ->
                             C;
                         Pts ->
                             [{Pts, Key} | C]
                     end
             end,
    fold(FoldFn, [], Sender, State).

fold_with_bucket(Fun, Acc0, Sender, State) ->
    FoldFn = fun(K, V, O) ->
                     Fun({State#vstate.bucket, K}, V, O)
             end,
    fold(FoldFn, Acc0, Sender, State).

fold(Fun, Acc0, Sender, State=#vstate{db=DB, bucket=Bucket}) ->
    AsyncWork = fun() ->
                        fifo_db:fold(DB, Bucket, Fun, Acc0)
                end,
    FinishFun = fun(Data) ->
                        reply(Data, Sender, State)
                end,
    {async, {fold, AsyncWork, FinishFun}, Sender, State}.

put(Key, Obj, State) ->
    fifo_db:put(State#vstate.db, State#vstate.bucket, Key, Obj),
    riak_core_aae_vnode:update_hashtree(
      State#vstate.bucket, Key, Obj#sniffle_obj.vclock, State#vstate.hashtrees).

%%%===================================================================
%%% Callbacks
%%%===================================================================

is_empty(State=#vstate{db=DB, bucket=Bucket}) ->
    FoldFn = fun (_, _) -> {false, State} end,
    fifo_db:fold_keys(DB, Bucket, FoldFn, {true, State}).

delete(State=#vstate{db=DB, bucket=Bucket}) ->
    FoldFn = fun (K, A) -> [{delete, <<Bucket/binary, K/binary>>} | A] end,
    Trans = fifo_db:fold_keys(DB, Bucket, FoldFn, []),
    fifo_db:transact(State#vstate.db, Trans),
    {ok, State}.

load_obj(Mod, Obj = #sniffle_obj{val = V}) ->
    Obj#sniffle_obj{val = Mod:load(V)}.

handle_coverage({lookup, Name}, _KeySpaces, Sender, State=#vstate{state=Mod}) ->
    FoldFn = fun (U, #sniffle_obj{val=V}, [not_found]) ->
                     V1 = statebox:value(Mod:load(V)),
                     case jsxd:get(<<"name">>, V1) of
                         {ok, AName} when AName =:= Name ->
                             [U];
                         _ ->
                             [not_found]
                     end;
                 (_, O, Res) ->
                     lager:info("Oops: ~p", [O]),
                     Res
             end,
    fold(FoldFn, [not_found], Sender, State);

handle_coverage(list, _KeySpaces, Sender, State) ->
    list_keys(Sender, State);

handle_coverage({list, Requirements}, _KeySpaces, Sender, State) ->
    handle_coverage({list, Requirements, false}, _KeySpaces, Sender, State);

handle_coverage({list, Requirements, Full}, _KeySpaces, Sender,
                State = #vstate{state=Mod}) ->
    case Full of
        true ->
            list(fun Mod:getter/2, Requirements, Sender, State);
        false ->
            list_keys(fun Mod:getter/2, Requirements, Sender, State)
    end;
handle_coverage(Req, _KeySpaces, _Sender, State) ->
    lager:warning("Unknown coverage request: ~p", [Req]),
    {stop, not_implemented, State}.


handle_command(ping, _Sender, State) ->
    {reply, {pong, State#vstate.partition}, State};

handle_command({repair, UUID, _VClock, Obj}, _Sender,
               State=#vstate{state=Mod}) ->
    case get(UUID, State) of
        {ok, Old} ->
            Old1 = load_obj(Mod, Old),
            Merged = sniffle_obj:merge(sniffle_entity_read_fsm, [Old1, Obj]),
            sniffle_vnode:put(UUID, Merged, State);
        not_found ->
            sniffle_vnode:put(UUID, Obj, State);
        _ ->
            lager:error("[~s] Read repair failed, data was updated too recent.",
                        [State#vstate.bucket])
    end,
    {noreply, State};

handle_command({sync_repair, {ReqID, _}, UUID, Obj = #sniffle_obj{}}, _Sender,
               State=#vstate{state=Mod}) ->
    case get(UUID, State) of
        {ok, Old} ->
            ID = sniffle_vnode:mkid(),
            Old1 = load_obj(ID, Mod, Old),
            lager:info("[sync-repair:~s] Merging with old object", [UUID]),
            Merged = sniffle_obj:merge(sniffle_entity_read_fsm, [Old1, Obj]),
            sniffle_vnode:put(UUID, Merged, State);
        not_found ->
            lager:info("[sync-repair:~s] Writing new object", [UUID]),
            sniffle_vnode:put(UUID, Obj, State);
        _ ->
            lager:error("[~s] Read repair failed, data was updated too recent.",
                        [State#vstate.bucket])
    end,
    {reply, {ok, ReqID}, State};

handle_command({get, ReqID, UUID}, _Sender, State) ->
    Res = case fifo_db:get(State#vstate.db, State#vstate.bucket, UUID) of
              {ok, R} ->
                  R;
              not_found ->
                  not_found
          end,
    NodeIdx = {State#vstate.partition, State#vstate.node},
    {reply, {ok, ReqID, NodeIdx, Res}, State};

handle_command({delete, {ReqID, _Coordinator}, UUID}, _Sender, State) ->
    fifo_db:delete(State#vstate.db, State#vstate.bucket, UUID),
    riak_core_index_hashtree:delete(
      {State#vstate.bucket, UUID}, State#vstate.hashtrees),
    {reply, {ok, ReqID}, State};


handle_command({set,
                {ReqID, Coordinator}, Dataset,
                Resources}, _Sender,
               State = #vstate{db=DB, state=Mod, bucket=Bucket}) ->
    case fifo_db:get(DB, Bucket, Dataset) of
        {ok, #sniffle_obj{val=H0} = O} ->
            H1 = statebox:modify({fun Mod:load/1,[]}, H0),
            H2 = lists:foldr(
                   fun ({Resource, Value}, H) ->
                           statebox:modify(
                             {fun Mod:set/3,
                              [Resource, Value]}, H)
                   end, H1, Resources),
            H3 = statebox:expire(?STATEBOX_EXPIRE, H2),
            Obj = sniffle_obj:update(H3, Coordinator, O),
            sniffle_vnode:put(Dataset, Obj, State),
            {reply, {ok, ReqID}, State};
        R ->
            lager:error("[~s] tried to write to a non existing dataset: ~p",
                        [Bucket, R]),
            {reply, {ok, ReqID, not_found}, State}
    end;

%%%===================================================================
%%% AAE
%%%===================================================================

handle_command({hashtree_pid, Node}, _, State) ->
    %% Handle riak_core request forwarding during ownership handoff.
    %% Following is necessary in cases where anti-entropy was enabled
    %% after the vnode was already running
    case {node(), State#vstate.hashtrees} of
        {Node, undefined} ->
            HT1 =  riak_core_aae_vnode:maybe_create_hashtrees(
                     State#vstate.service,
                     State#vstate.partition,
                     State#vstate.vnode,
                     undefined),
            {reply, {ok, HT1}, State#vstate{hashtrees = HT1}};
        {Node, HT} ->
            {reply, {ok, HT}, State};
        _ ->
            {reply, {error, wrong_node}, State}
    end;

handle_command({rehash, {_, UUID}}, _,
               State=#vstate{bucket=Bucket, hashtrees=HT}) ->
    case get(UUID, State) of
        {ok, Obj} ->
            riak_core_aae_vnode:update_hashtree(
              Bucket, UUID, Obj#sniffle_obj.vclock, HT);
        _ ->
            %% Make sure hashtree isn't tracking deleted data
            riak_core_index_hashtree:delete({State#vstate.bucket, UUID}, HT)
    end,
    {noreply, State};

handle_command(?FOLD_REQ{foldfun=Fun, acc0=Acc0}, Sender, State) ->
    fold_with_bucket(Fun, Acc0, Sender, State);

handle_command(Message, _Sender, State) ->
    lager:error("[vms] Unknown command: ~p", [Message]),
    {noreply, State}.

reply(Reply, {_, ReqID, _} = Sender, #vstate{node=N, partition=P}) ->
    riak_core_vnode:reply(Sender, {ok, ReqID, {P, N}, Reply}).

get(UUID, State) ->
    fifo_db:get(State#vstate.db, State#vstate.bucket, UUID).

handle_info(retry_create_hashtree,
            State=#vstate{service=Srv, hashtrees=undefined, partition=Idx,
                          vnode=VNode}) ->
    lager:debug("~p/~p retrying to create a hash tree.", [Srv, Idx]),
    HT = riak_core_aae_vnode:maybe_create_hashtrees(Srv, Idx, VNode, undefined),
    {ok, State#vstate{hashtrees = HT}};
handle_info(retry_create_hashtree, State) ->
    {ok, State};
handle_info({'DOWN', _, _, Pid, _},
            State=#vstate{service=Service, hashtrees=Pid, partition=Idx}) ->
    lager:debug("~p/~p hashtree ~p went down.", [Service, Idx, Pid]),
    erlang:send_after(1000, self(), retry_create_hashtree),
    {ok, State#vstate{hashtrees = undefined}};
handle_info({'DOWN', _, _, _, _}, State) ->
    {ok, State};
handle_info(_, State) ->
    {ok, State}.

load_obj(_ID, _Mod, Old) ->
    Old.
