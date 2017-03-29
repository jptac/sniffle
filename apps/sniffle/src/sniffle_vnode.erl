-module(sniffle_vnode).
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
         handle_overload_command/3,
         handle_overload_info/2,
         handle_info/2
        ]).

-export([
         object_info/1,
         request_hash/1,
         nval_map/1,
         master/0,
         aae_repair/2,
         hash_object/2
        ]).

-export([mk_reqid/0]).

-define(FM(Mod, Fun, Args),
        folsom_metrics:histogram_timed_update(
          {Mod, Fun},
          Mod, Fun, Args)).

-record(state, {
          db,
          partition,
          node,
          hashtrees,
          partial_size = 10
         }).

%%%===================================================================
%%% AAE
%%%===================================================================

master() ->
    ?MASTER.

hash_object(Key, Obj) ->
    term_to_binary(erlang:phash2({Key, Obj})).

aae_repair(_, Key) ->
    lager:debug("AAE Repair: ~p", [Key]),
    %% TODO!
    sniffle_vm:get(Key).

%%%===================================================================
%%% API
%%%===================================================================

start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

%%%===================================================================
%%% VNode
%%%===================================================================

init([Partition]) ->
    DB = list_to_atom(integer_to_list(Partition)),
    fifo_db:start(DB),
    HT = riak_core_aae_vnode:maybe_create_hashtrees(
           sniffle, Partition, ?MODULE, undefined),
    WorkerPoolSize = application:get_env(sniffle, async_workers, 5),
    FoldWorkerPool = {pool, sniffle_worker, WorkerPoolSize, []},
    Partial = application:get_env(sniffle, partial_size, 10),
    {ok,
     #state{db=DB, hashtrees=HT, partition=Partition, node=node(),
            partial_size = Partial},
     [FoldWorkerPool]}.

handle_overload_command(_Req, Sender, Idx) ->
    riak_core_vnode:reply(Sender, {fail, Idx, overload}).

handle_overload_info(_, _Idx) ->
    ok.

%%%===================================================================
%%% Commands
%%%===================================================================

handle_command(#req{id = ID = {ReqID, _}, request = {apply, Key, Fun, Args},
                    bucket = Bucket}, _Sender, State) ->
    R = case Fun(ID, fifo_db:get(State#state.db, Bucket, Key), Args) of
            {write, R0, Obj} ->
                put(Bucket, Key, Obj, State),
                R0;
            {reply, R0} ->
                R0
        end,
    case R of
        ok ->
            {reply, {ok, ReqID}, State};
        {ok, V} ->
            {reply, {ok, ReqID, V}, State};
        {error, V} ->
            {reply, {error, ReqID, V}, State};
        {raw, R1} ->
            {reply, R1, State};
        R1 ->
            {reply, R1, State}
    end;

handle_command(ping, _Sender, State) ->
    {reply, {pong, State#state.partition}, State};

handle_command(#req{request = {repair, UUID, _VClock, Obj}, bucket = Bucket},
               _Sender, State) ->
    Mod = bkt_to_mod(Bucket),
    ID = mkid(),
    Obj1 = load_obj(UUID, ID, Mod, Obj),
    case get(Bucket, UUID, State) of
        {ok, Old} ->
            Old1 = load_obj(UUID, ID, Mod, Old),
            Merged = ft_obj:merge(sniffle_entity_read_fsm, [Old1, Obj1]),
            put(Bucket, UUID, Merged, State);
        not_found ->
            put(Bucket, UUID, Obj1, State);
        E ->
            lager:error("[~s] Read repair failed: ~p.", [Bucket, E])
    end,
    {noreply, State};

handle_command(
  #req{id = {ReqID, _}, request = {sync_repair, UUID, Obj}, bucket = Bucket},
  _Sender, State) ->
    Mod = bkt_to_mod(Bucket),
    case get(Bucket, UUID, State) of
        {ok, Old} ->
            ID = mkid(),
            Old1 = load_obj(UUID, ID, Mod, Old),
            lager:info("[sync-repair:~s] Merging with old object", [UUID]),
            Merged = ft_obj:merge(sniffle_entity_read_fsm, [Old1, Obj]),
            put(Bucket, UUID, Merged, State);
        not_found ->
            lager:info("[sync-repair:~s] Writing new object", [UUID]),
            put(Bucket, UUID, Obj, State);
        _ ->
            lager:error("[~s] Read repair failed, data was updated too recent.",
                        [Bucket])
    end,
    {reply, {ok, ReqID}, State};

handle_command(#req{id = ReqID, request = {get, UUID}, bucket = Bucket},
               _Sender, State) ->
    Mod = bkt_to_mod(Bucket),
    Res = case get(Bucket, UUID, State) of
              {ok, R} ->
                  ID = {ReqID, load},
                  %% We want to write a loaded object back to storage
                  %% if a change happend.
                  case  load_obj(UUID, ID, Mod, R) of
                      R1 when R =/= R1 ->
                          put(Bucket, UUID, R1, State),
                          R1;
                      R1 ->
                          R1
                  end;
              not_found ->
                  not_found
          end,
    NodeIdx = {State#state.partition, State#state.node},
    {reply, {ok, ReqID, NodeIdx, Res}, State};

handle_command(#req{id = {ReqID, _Coordinator}, request = {delete, UUID},
                    bucket = Bucket}, _Sender, State) ->
    ?FM(fifo_db, delete, [State#state.db, Bucket, UUID]),
    riak_core_index_hashtree:delete(
      [{object, {Bucket, UUID}}], State#state.hashtrees),
    {reply, {ok, ReqID}, State};


handle_command(#req{
                  id = {ReqID, Coordinator} = ID,
                  request = {set, UUID, Resources},
                  bucket = Bucket
                 } = Req, _Sender, State) ->
    lager:error("DEPRECATED set command: ~p", [Req]),
    Mod = bkt_to_mod(Bucket),
    case get(Bucket, UUID, State) of
        {ok, O} ->
            O1 = load_obj(UUID, ID, Mod, O),
            H1 = ft_obj:val(O1),
            H2 = lists:foldr(
                   fun ({Resource, Value}, H) ->
                           Mod:set(ID, Resource, Value, H)
                   end, H1, Resources),
            Obj = ft_obj:update(H2, Coordinator, O),
            put(Bucket, UUID, Obj, State),
            {reply, {ok, ReqID}, State};
        R ->
            lager:error("[~s/~p] tried to write to non existing element: "
                        "~s -> ~p",
                        [Bucket, State#state.partition, UUID, R]),
            {reply, {ok, ReqID, not_found}, State}
    end;

%%%===================================================================
%%% AAE
%%%===================================================================

handle_command({hashtree_pid, Node}, _, State) ->
    %% Handle riak_core request forwarding during ownership handoff.
    %% Following is necessary in cases where anti-entropy was enabled
    %% after the vnode was already running
    case {node(), State#state.hashtrees} of
        {Node, undefined} ->
            HT1 =  riak_core_aae_vnode:maybe_create_hashtrees(
                     sniffle,
                     State#state.partition,
                     ?MODULE,
                     undefined),
            {reply, {ok, HT1}, State#state{hashtrees = HT1}};
        {Node, HT} ->
            {reply, {ok, HT}, State};
        _ ->
            {reply, {error, wrong_node}, State}
    end;

handle_command({rehash, {Bucket, UUID}}, _,
               State=#state{hashtrees=HT}) ->
    case get(Bucket, UUID, State) of
        {ok, Obj} ->
            riak_core_aae_vnode:update_hashtree(
              Bucket, UUID, vc_bin(ft_obj:vclock(Obj)), HT);
        _ ->
            %% Make sure hashtree isn't tracking deleted data
            riak_core_index_hashtree:delete([{object, {Bucket, UUID}}], HT)
    end,
    {noreply, State};

handle_command(?FOLD_REQ{foldfun=Fun, acc0=Acc0}, Sender, State) ->
    fold_with_bucket(Fun, Acc0, Sender, State);

handle_command(#req{id = ID, request={change, Action, UUID, Args},
                    bucket = Bucket}, _Sender, State) ->
    change(Bucket, UUID, Action, Args, ID, State);

handle_command(Message, _Sender, State) ->
    lager:error("[vnode] Unknown command: ~p", [Message]),
    {noreply, State}.

handle_handoff_command(?FOLD_REQ{foldfun=Fun, acc0=Acc0}, _Sender, State) ->
    Fun1 = fun(Key, V, Acc) -> Fun(split(Key), V, Acc) end,
    Acc = fifo_db:fold(State#state.db, <<>>, Fun1, Acc0),
    {reply, Acc, State};

handle_handoff_command(Req = #req{request = {get, _Key}},
                       Sender, State) ->
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
    repair(Data, State).

encode_handoff_item(Key, Data) ->
    term_to_binary({Key, Data}).

is_empty(State=#state{db=DB}) ->
    FoldFn = fun (_, _) -> {false, State} end,
    fold_keys(DB, <<>>, FoldFn, {true, State}).

delete(State) ->
    fifo_db:destroy(State#state.db),
    case State#state.hashtrees of
        undefined ->
            ok;
        HT ->
            riak_core_index_hashtree:destroy(HT)
    end,
    {ok, State#state{hashtrees=undefined}}.

handle_coverage(#req{request = {wipe, UUID}, bucket = Bucket}, _KeySpaces,
                {_, ReqID, _}, State) ->
    ?FM(fifo_db, delete, [State#state.db, Bucket, UUID]),
    {reply, {ok, ReqID}, State};

handle_coverage(#req{request = {lookup, Name}, bucket = Bucket},
                _KeySpaces, Sender, State) ->
    Mod = bkt_to_mod(Bucket),
    ID = mkid(),
    FoldFn = fun (U, O, [not_found]) ->
                     O1 = load_obj(U, ID, Mod, O),
                     V = ft_obj:val(O1),
                     case Mod:name(V) of
                         AName when AName =:= Name ->
                             [U];
                         _ ->
                             [not_found]
                     end;
                 (_, O, Res) ->
                     lager:info("Oops: ~p", [O]),
                     Res
             end,
    fold(Bucket, FoldFn, [not_found], Sender, State);

handle_coverage(#req{request = list, bucket = Bucket},
                _KeySpaces, Sender, State) ->
    list_keys(Bucket, Sender, State);

handle_coverage(R = #req{request = {list, Requirements}},
                KeySpaces, Sender, State) ->
    R1 = R#req{request = {list, Requirements, false}},
    handle_coverage(R1, KeySpaces, Sender, State);

handle_coverage(#req{request = {list, Requirements, Full},
                     bucket = Bucket},
                _KeySpaces, Sender, State) ->
    Mod = bkt_to_mod(Bucket),
    case Full of
        true ->
            list(Bucket, Mod, fun Mod:getter/2, Requirements, Sender, State);
        false ->
            list_keys(
              Bucket, Mod, fun Mod:getter/2, Requirements, Sender, State)

    end;

handle_coverage(Req, _KeySpaces, _Sender, State) ->
    lager:warning("[vnode] Unknown coverage request: ~p", [Req]),
    {stop, not_implemented, State}.


handle_exit(_Pid, _Reason, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

%%%===================================================================
%%% AAE
%%%===================================================================

handle_info(retry_create_hashtree,
            State=#state{hashtrees=undefined, partition=Idx}) ->
    lager:debug("[vnode:~~p] retrying to create a hash tree.", [Idx]),
    HT = riak_core_aae_vnode:maybe_create_hashtrees(
           sniffle, Idx, ?MODULE, undefined),
    {ok, State#state{hashtrees = HT}};
handle_info(retry_create_hashtree, State) ->
    {ok, State};
handle_info({'DOWN', _, _, Pid, Reason},
            State=#state{hashtrees=Pid, partition=Idx}) ->
    lager:info("[vnode:~p] hashtree ~p went down: ~p.", [Idx, Pid, Reason]),
    erlang:send_after(1000, self(), retry_create_hashtree),
    {ok, State#state{hashtrees = undefined}};
handle_info({'DOWN', _, _, _, _}, State) ->
    {ok, State};
handle_info(_, State) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions - Access
%%%===================================================================

get(Bucket, UUID, State) ->
    try
        ?FM(fifo_db, get, [State#state.db, Bucket, UUID])
    catch
        E1:E2 ->
            lager:error("[vnode] Failed to get object ~s/~s ~p:~p ~w",
                        [Bucket, UUID, E1, E2,
                         erlang:get_stacktrace()]),
            not_found
    end.

put(Bucket, Key, Obj, State) ->
    ?FM(fifo_db, put, [State#state.db, Bucket, Key, Obj]),
    riak_core_aae_vnode:update_hashtree(
      Bucket, Key, vc_bin(ft_obj:vclock(Obj)), State#state.hashtrees).



load_obj(UUID, {T, ID}, Mod, Obj) ->
    V = ft_obj:val(Obj),
    try
        case Mod:load({T-1, ID}, V) of
            V1 when V1 /= V ->
                ft_obj:update(V1, ID, Obj);
            _ ->
                ft_obj:update(Obj)
        end
    catch
        E1:E2 ->
            lager:error("[~p:~p] Failed to load ~s : ~p", [E1, E2, Mod, Obj]),
            O = Mod:new({1, ID}),
            O1 = Mod:uuid({2, ID}, UUID, O),
            ft_obj:new(O1, node())
    end.
%%%===================================================================
%%% Internal functions - Folding
%%%===================================================================

fold_with_bucket(Fun, Acc0, Sender, State) ->
    ID = mkid(),
    FoldFn = fun(K, E, O) ->
                     {Bucket, Key} = split(K),
                     Mod = bkt_to_mod(Bucket),
                     E1 = load_obj(K, ID, Mod, E),
                     Fun({Bucket, Key}, E1, O)
             end,
    fold(<<>>, FoldFn, Acc0, Sender, State).


fold_keys(DB, Bucket, FoldFn, Acc0) ->
    ?FM(fifo_db, fold_keys, [DB, Bucket, FoldFn, Acc0]).

fold(Bucket, Fun, Acc0, Sender, State=#state{db=DB}) ->
    AsyncWork = fun() ->
                        ?FM(fifo_db, fold, [DB, Bucket, Fun, Acc0])
                end,
    FinishFun = fun(Data) ->
                        reply(Data, Sender, State)
                end,
    {async, {fold, AsyncWork, FinishFun}, Sender, State}.


send_partial(Sender, L, State = #state{partial_size = Size})
  when length(L) >= Size ->
    partial(L, Sender, State),
    [];
send_partial(_Sender, L, _State) ->
    L.

list_keys(Bucket, Sender, State=#state{db=DB}) ->
    FoldFn = fun (K, L) ->
                     send_partial(Sender, [K|L], State)
             end,
    AsyncWork = fun() ->
                        fold_keys(DB, Bucket, FoldFn, [])
                end,
    FinishFun = fun(Data) ->
                        reply(Data, Sender, State)
                end,
    {async, {fold, AsyncWork, FinishFun}, Sender, State}.

list_keys(Bucket, Mod, Getter, Requirements, Sender,
          State) ->
    ID = mkid(),
    FoldFn = fun (Key, E, C) ->
                     E1 = load_obj(Key, ID, Mod, E),
                     C1 = case rankmatcher:match(E1, Getter, Requirements) of
                              false ->
                                  C;
                              Pts ->
                                  [{Pts, Key} | C]
                          end,
                     send_partial(Sender, C1, State)
             end,
    fold(Bucket, FoldFn, [], Sender, State).

list(Bucket, Mod, Getter, Requirements, Sender, State) ->
    ID = mkid(),
    FoldFn = fun (Key, E, C) ->
                     E1 = load_obj(Key, ID, Mod, E),
                     C1 = case rankmatcher:match(E1, Getter, Requirements) of
                              false ->
                                  C;
                              Pts ->
                                  [{Pts, {Key, E1}} | C]
                          end,
                     send_partial(Sender, C1, State)
             end,
    fold(Bucket, FoldFn, [], Sender, State).

change(Bucket, UUID, Action, Vals, {ReqID, Coordinator} = ID,
       State) ->
    Mod= bkt_to_mod(Bucket),
    OR = case get(Bucket, UUID, State) of
             {ok, O} ->
                 load_obj(UUID, ID, Mod, O);
             _R ->
                 O = Mod:new({1, Coordinator}),
                 O1 = Mod:uuid({2, ID}, UUID, O),
                 ft_obj:new(O1, Coordinator)
         end,
    H1 = ft_obj:val(OR),
    H2 = erlang:apply(Mod, Action, [ID] ++ Vals ++ [H1]),
    Obj = ft_obj:update(H2, Coordinator, OR),
    put(Bucket, UUID, Obj, State),
    {reply, {ok, ReqID}, State}.


%%%===================================================================
%%% Resize functions
%%%===================================================================

nval_map(Ring) ->
    riak_core_bucket:bucket_nval_map(Ring).

%% callback used by dynamic ring sizing to determine where requests should be
%% forwarded.
%% Puts/deletes are forwarded during the operation, all other requests are not

%% We do use the sniffle_prefix to define bucket as the bucket in read/write
%% does equal the system.
request_hash(#req{ request = {apply, UUID, _, _}, bucket = Bucket}) ->
    riak_core_util:chash_key({<<"sniffle_", Bucket/binary>>, UUID});
request_hash(#req{ request = {delete, UUID}, bucket = Bucket}) ->
    riak_core_util:chash_key({<<"sniffle_", Bucket/binary>>, UUID});
request_hash(_) ->
    undefined.

object_info({Bucket, UUID}=BKey) ->
    Hash = riak_core_util:chash_key({<<"sniffle_", Bucket/binary>>, UUID}),
    R = {Bucket, Hash},
    lager:info("object_info(~p) -> ~p.", [BKey, R]),
    R.

%%%===================================================================
%%% Internal functions - Replies
%%%===================================================================

%% If we don't have a reqid it's some riak_core specific thingything.
%% OMG why am I doing this o.O it can't end well.
reply(Reply, {_, undefined, _} = Sender, _) ->
    riak_core_vnode:reply(Sender, Reply);

reply(Reply, {_, ReqID, _} = Sender, #state{node=N, partition=P}) ->
    riak_core_vnode:reply(Sender, {ok, ReqID, {P, N}, Reply}).

partial(Reply, {_, ReqID, _} = Sender, #state{node=N, partition=P}) ->
    riak_core_vnode:reply(Sender, {partial, ReqID, {P, N}, Reply}).

%%%===================================================================
%%% Internal functions - Translations
%%%===================================================================

bkt_to_mod(<<"2i">>)         -> sniffle_2i_state;
bkt_to_mod(<<"dataset">>)    -> ft_dataset;
bkt_to_mod(<<"dtrace">>)     -> ft_dtrace;
bkt_to_mod(<<"grouping">>)   -> ft_grouping;
bkt_to_mod(<<"hostname">>)   -> ft_hostname;
bkt_to_mod(<<"hypervisor">>) -> ft_hypervisor;
bkt_to_mod(<<"iprange">>)    -> ft_iprange;
bkt_to_mod(<<"network">>)    -> ft_network;
bkt_to_mod(<<"package">>)    -> ft_package;
bkt_to_mod(<<"vm">>)         -> ft_vm.

split(<<"2i", UUID/binary>>)         -> {<<"2i">>, UUID};
split(<<"dataset", UUID/binary>>)    -> {<<"dataset">>, UUID};
split(<<"dtrace", UUID/binary>>)     -> {<<"dtrace">>, UUID};
split(<<"grouping", UUID/binary>>)   -> {<<"grouping">>, UUID};
split(<<"hostname", UUID/binary>>)   -> {<<"hostname">>, UUID};
split(<<"hypervisor", UUID/binary>>) -> {<<"hypervisor">>, UUID};
split(<<"iprange", UUID/binary>>)    -> {<<"iprange">>, UUID};
split(<<"network", UUID/binary>>)    -> {<<"network">>, UUID};
split(<<"package", UUID/binary>>)    -> {<<"package">>, UUID};
split(<<"vm", UUID/binary>>)         -> {<<"vm">>, UUID}.

%%%===================================================================
%%% Internal functions - Helper
%%%===================================================================

mkid() ->
    mkid(node()).

mkid(Actor) ->
    {mk_reqid(), Actor}.

mk_reqid() ->
    erlang:system_time(nano_seconds).

repair(Data, State) ->
    {{Bkt, UUID}, Obj} = binary_to_term(Data),
    Req = #req{request = {repair, UUID, undefined, Obj}, bucket = Bkt},
    {noreply, State1} = handle_command(Req, undefined, State),
    {reply, ok, State1}.

vc_bin(VClock) ->
    term_to_binary(lists:sort(VClock)).

