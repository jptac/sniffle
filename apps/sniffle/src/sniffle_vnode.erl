-module(sniffle_vnode).

-include("sniffle.hrl").
-include_lib("riak_core/include/riak_core_vnode.hrl").

-export([init/3,
         list_keys/2,
         list_keys/4,
         is_empty/1,
         delete/1,
         %%fold_with_bucket/4,
         lookup/3,
         put/3,
         fold/4,
         handle_command/3,
         handle_info/2]).

init(Partition, Bucket, Service) ->
    DB = list_to_atom(integer_to_list(Partition)),
    fifo_db:start(DB),
    HT = riak_core_aae_vnode:maybe_create_hashtrees(Service, Partition,
                                                    undefined),
    WorkerPoolSize = application:get_env(sniffle, async_workers, 5),
    FoldWorkerPool = {pool, sniffle_worker, WorkerPoolSize, []},
    {ok,
     #vstate{db = DB, hashtrees = HT, partition = Partition, node = node(),
             service = Service, bucket = Bucket},
     [FoldWorkerPool]}.

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
      State#vstate.bucket, Key, term_to_binary(Obj), State#vstate.hashtrees).

%%%===================================================================
%%% Callbacks
%%%===================================================================
lookup(Name, Sender, State) ->
    FoldFn = fun (_U, #sniffle_obj{val=SB}, [not_found]) ->
                     V = statebox:value(SB),
                     case jsxd:get(<<"name">>, V) of
                         {ok, Name} ->
                             [V];
                         _ ->
                             [not_found]
                     end;
                 (_, _, Res) ->
                     Res
             end,
    sniffle_vnode:fold(FoldFn, [not_found], Sender, State).

is_empty(State=#vstate{db=DB, bucket=Bucket}) ->
    FoldFn = fun (_, _) -> {false, State} end,
    fifo_db:fold_keys(DB, Bucket, FoldFn, {true, State}).

delete(State=#vstate{db=DB, bucket=Bucket}) ->
    FoldFn = fun (K, A) -> [{delete, <<Bucket/binary, K/binary>>} | A] end,
    Trans = fifo_db:fold_keys(DB, Bucket, FoldFn, []),
    fifo_db:transact(State#vstate.db, Trans),
    {ok, State}.

handle_info(retry_create_hashtree,
            State=#vstate{service=Srv, hashtrees=undefined, partition=Idx}) ->
    lager:debug("~p/~p retrying to create a hash tree.", [Srv, Idx]),
    HT = riak_core_aae_vnode:maybe_create_hashtrees(Srv, Idx, undefined),
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

handle_command(ping, _Sender, State) ->
    {reply, {pong, State#vstate.partition}, State};

handle_command({repair, UUID, VClock, Obj}, _Sender, State) ->
    case get(UUID, State) of
        {ok, #sniffle_obj{vclock = VC1}} when VC1 =:= VClock ->
            sniffle_vnode:put(UUID, Obj, State);
        not_found ->
            sniffle_vnode:put(UUID, Obj, State);
        _ ->
            lager:error("[~s] Read repair failed, data was updated too recent.",
                        [State#vstate.bucket])
    end,
    {noreply, State};

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
        {ok, Term} ->
            Bin = term_to_binary(Term),
            riak_core_aae_vnode:update_hashtree(Bucket, UUID, Bin, HT);
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
