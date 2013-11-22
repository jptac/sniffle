-module(sniffle_iprange_vnode).
-behaviour(riak_core_vnode).
-behaviour(riak_core_aae_vnode).
-include("sniffle.hrl").
-include_lib("riak_core/include/riak_core_vnode.hrl").

-export([
         repair/4,
         get/3,
         create/4,
         delete/3,
         claim_ip/4,
         release_ip/4,
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
              release_ip/4,
              create/4,
              delete/3,
              get/3,
              set/4,
              claim_ip/4,
              repair/4,
              release_ip/4,
              start_vnode/1,
              handle_info/2
             ]).

-record(state, {db, partition, node, hashtrees}).

-define(SERVICE, sniffle_iprange).

-define(MASTER, sniffle_iprange_vnode_master).

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
    sniffle_iprange:get(Key).

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

repair(IdxNode, Iprange, VClock, Obj) ->
    riak_core_vnode_master:command(IdxNode,
                                   {repair, Iprange, VClock, Obj},
                                   ignore,
                                   ?MASTER).

%%%===================================================================
%%% API - reads
%%%===================================================================

get(Preflist, ReqID, Iprange) ->
    riak_core_vnode_master:command(Preflist,
                                   {get, ReqID, Iprange},
                                   {fsm, undefined, self()},
                                   ?MASTER).

%%%===================================================================
%%% API - writes
%%%===================================================================

create(Preflist, ReqID, UUID, Data) ->
    riak_core_vnode_master:command(Preflist,
                                   {create, ReqID, UUID, Data},
                                   {fsm, undefined, self()},
                                   ?MASTER).

delete(Preflist, ReqID, Iprange) ->
    riak_core_vnode_master:command(Preflist,
                                   {delete, ReqID, Iprange},
                                   {fsm, undefined, self()},
                                   ?MASTER).

claim_ip(Preflist, ReqID, Iprange, Ip) ->
    riak_core_vnode_master:command(Preflist,
                                   {ip, claim, ReqID, Iprange, Ip},
                                   {fsm, undefined, self()},
                                   ?MASTER).

release_ip(Preflist, ReqID, Iprange, IP) ->
    riak_core_vnode_master:command(Preflist,
                                   {ip, release, ReqID, Iprange, IP},
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

handle_command({repair, Iprange, VClock, Obj}, _Sender, State) ->
    case fifo_db:get(State#state.db, <<"iprange">>, Iprange) of
        {ok, #sniffle_obj{vclock = VC1}} when VC1 =:= VClock ->
            do_put(Iprange, Obj, State);
        not_found ->
            do_put(Iprange, Obj, State);
        _ ->
            lager:error("[uprange] Read repair failed, data was updated too recent.")
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
    case fifo_db:get(DB, <<"iprange">>, Key) of
        {ok, Term} ->
            riak_core_aae_vnode:update_hashtree(<<"iprange">>, Key,
                                                term_to_binary(Term),
                                                State#state.hashtrees);
        _ ->
            %% Make sure hashtree isn't tracking deleted data
            riak_core_index_hashtree:delete({<<"iprange">>, Key},
                                            State#state.hashtrees)
    end,
    {noreply, State};

handle_command(?FOLD_REQ{foldfun=Fun, acc0=Acc0}, _Sender, State) ->
    lager:debug("Fold on ~p", [State#state.partition]),
    Acc = fifo_db:fold(State#state.db, <<"iprange">>,
                       fun(K, V, O) ->
                               Fun({<<"iprange">>, K}, V, O)
                       end, Acc0),
    {reply, Acc, State};

%%%===================================================================
%%% General
%%%===================================================================

handle_command({get, ReqID, Iprange}, _Sender, State) ->
    Res = case fifo_db:get(State#state.db, <<"iprange">>, Iprange) of
              {ok, R} ->
                  R;
              not_found ->
                  not_found
          end,
    NodeIdx = {State#state.partition, State#state.node},
    {reply, {ok, ReqID, NodeIdx, Res}, State};

handle_command({create, {ReqID, Coordinator}, UUID,
                [Iprange, Network, Gateway, Netmask, First, Last, Tag, Vlan]},
               _Sender, State) ->
    I0 = statebox:new(fun sniffle_iprange_state:new/0),
    I1 = lists:foldl(
           fun (OP, SB) ->
                   statebox:modify(OP, SB)
           end, I0, [{fun sniffle_iprange_state:uuid/2, [UUID]},
                     {fun sniffle_iprange_state:name/2, [Iprange]},
                     {fun sniffle_iprange_state:network/2, [Network]},
                     {fun sniffle_iprange_state:gateway/2, [Gateway]},
                     {fun sniffle_iprange_state:netmask/2, [Netmask]},
                     {fun sniffle_iprange_state:first/2, [First]},
                     {fun sniffle_iprange_state:current/2, [First]},
                     {fun sniffle_iprange_state:last/2, [Last]},
                     {fun sniffle_iprange_state:tag/2, [Tag]},
                     {fun sniffle_iprange_state:vlan/2, [Vlan]}]),
    VC0 = vclock:fresh(),
    VC = vclock:increment(Coordinator, VC0),
    Obj = #sniffle_obj{val=I1, vclock=VC},
    do_put(UUID, Obj, State),
    {reply, {ok, ReqID}, State};

handle_command({delete, {ReqID, _Coordinator}, Iprange}, _Sender, State) ->
    fifo_db:delete(State#state.db, <<"iprange">>, Iprange),
    riak_core_index_hashtree:delete({<<"iprange">>, Iprange}, State#state.hashtrees),
    {reply, {ok, ReqID}, State};

handle_command({ip, claim,
                {ReqID, Coordinator}, Iprange, IP}, _Sender, State) ->
    case fifo_db:get(State#state.db, <<"iprange">>, Iprange) of
        {ok, #sniffle_obj{val=H0} = O} ->
            case sniffle_iprange_state:is_free(IP, statebox:value(H0)) of
                true ->
                    H1 = statebox:modify({fun sniffle_iprange_state:load/1,[]}, H0),
                    H2 = statebox:modify({fun sniffle_iprange_state:claim_ip/2,[IP]}, H1),
                    H3 = statebox:expire(?STATEBOX_EXPIRE, H2),
                    Obj =  sniffle_obj:update(H3, Coordinator, O),
                    do_put(Iprange, Obj, State),
                    V1 = statebox:value(H3),
                    {reply, {ok, ReqID,
                             {jsxd:get(<<"tag">>, <<"">>, V1),
                              IP,
                              jsxd:get(<<"netmask">>, 0, V1),
                              jsxd:get(<<"gateway">>, 0, V1)}}, State};
                false ->
                    {reply, {error, ReqID, duplicate}, State}
            end;
        _ ->
            {reply, {ok, ReqID, not_found}, State}
    end;

handle_command({set,
                {ReqID, Coordinator}, Iprange,
                Resources}, _Sender, State) ->
    case fifo_db:get(State#state.db, <<"iprange">>, Iprange) of
        {ok, #sniffle_obj{val=H0} = O} ->
            H1 = statebox:modify({fun sniffle_iprange_state:load/1,[]}, H0),
            H2 = lists:foldr(
                   fun ({Resource, Value}, H) ->
                           statebox:modify(
                             {fun sniffle_iprange_state:set/3,
                              [Resource, Value]}, H)
                   end, H1, Resources),
            H3 = statebox:expire(?STATEBOX_EXPIRE, H2),
            Obj =  sniffle_obj:update(H3, Coordinator, O),
            do_put(Iprange, Obj, State),
            {reply, {ok, ReqID}, State};
        R ->
            lager:error("[hypervisors] tried to write to a non existing hypervisor: ~p", [R]),
            {reply, {ok, ReqID, not_found}, State}
    end;

handle_command({ip, release,
                {ReqID, Coordinator}, Iprange, IP}, _Sender, State) ->
    case fifo_db:get(State#state.db, <<"iprange">>, Iprange) of
        {ok, #sniffle_obj{val=H0} = O} ->

            H1 = statebox:modify({fun sniffle_iprange_state:load/1,[]}, H0),
            H2 = statebox:modify({fun sniffle_iprange_state:release_ip/2,[IP]}, H1),
            H3 = statebox:expire(?STATEBOX_EXPIRE, H2),
            Obj =  sniffle_obj:update(H3, Coordinator, O),
            do_put(Iprange, Obj, State),
            {reply, {ok, ReqID}, State};

        _ ->
            {reply, {ok, ReqID, not_found}, State}
    end;

handle_command(Message, _Sender, State) ->
    lager:error("[ipranges] Unknown command: ~p", [Message]),
    {noreply, State}.

handle_handoff_command(?FOLD_REQ{foldfun=Fun, acc0=Acc0}, _Sender, State) ->
    Acc = fifo_db:fold(State#state.db,
                       <<"iprange">>, Fun, Acc0),
    {reply, Acc, State};

handle_handoff_command({get, _ReqID, _Iprange} = Req, Sender, State) ->
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
    {Iprange, Obj} = binary_to_term(Data),
    do_put(Iprange, Obj, State),
    {reply, ok, State}.

encode_handoff_item(Iprange, Data) ->
    term_to_binary({Iprange, Data}).

is_empty(State) ->
    fifo_db:fold_keys(State#state.db,
                      <<"iprange">>,
                      fun (_, _) ->
                              {false, State}
                      end, {true, State}).

delete(State) ->
    Trans = fifo_db:fold_keys(State#state.db,
                              <<"iprange">>,
                              fun (K, A) ->
                                      [{delete, <<"iprange", K/binary>>} | A]
                              end, []),
    fifo_db:transact(State#state.db, Trans),
    {ok, State}.

handle_coverage({lookup, Name}, _KeySpaces, {_, ReqID, _}, State) ->
    Res = fifo_db:fold(State#state.db,
                       <<"iprange">>,
                       fun (_U, #sniffle_obj{val=SB}, Res) ->
                               V = statebox:value(SB),
                               case jsxd:get(<<"name">>, V) of
                                   {ok, Name} ->
                                       V;
                                   _ ->
                                       Res
                               end
                       end, not_found),
    {reply,
     {ok, ReqID, {State#state.partition, State#state.node}, [Res]},
     State};

handle_coverage({list, Requirements}, _KeySpaces, {_, ReqID, _}, State) ->
    Getter = fun(#sniffle_obj{val=S0}, V) ->
                     jsxd:get(V, 0, statebox:value(S0))
             end,
    List = fifo_db:fold(State#state.db,
                        <<"iprange">>,
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
                             <<"iprange">>,
                             fun (K, L) ->
                                     [K|L]
                             end, []),

    {reply,
     {ok, ReqID, {State#state.partition,State#state.node}, List},
     State};

handle_coverage({overlap, ReqID, _Start, _Stop}, _KeySpaces, _Sender, State) ->
    {reply,
     {ok, ReqID, {State#state.partition, State#state.node}},
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
handle_info(retry_create_hashtree, State=#state{
                                            hashtrees=undefined,
                                            partition=Idx
                                           }) ->
    lager:debug("~p/~p retrying to create a hash tree.", [?SERVICE, Idx]),
    HT = riak_core_aae_vnode:maybe_create_hashtrees(?SERVICE, State#state.partition,
                                                    undefined),
    {ok, State#state{hashtrees = HT}};
handle_info(retry_create_hashtree, State) ->
    {ok, State};
handle_info({'DOWN', _, _, Pid, _}, State=#state{
                                             hashtrees=Pid,
                                             partition=Idx
                                            }) ->
    lager:debug("~p/~p hashtree ~p went down.", [?SERVICE, Idx, Pid]),
    erlang:send_after(1000, self(), retry_create_hashtree),
    {ok, State#state{hashtrees = undefined}};
handle_info({'DOWN', _, _, _, _}, State) ->
    {ok, State};
handle_info(_, State) ->
    {ok, State}.

do_put(Key, Obj, State) ->
    fifo_db:put(State#state.db, <<"iprange">>, Key, Obj),
    riak_core_aae_vnode:update_hashtree(<<"iprange">>, Key, term_to_binary(Obj),
                                        State#state.hashtrees).
