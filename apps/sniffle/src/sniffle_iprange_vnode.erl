-module(sniffle_iprange_vnode).
-behaviour(riak_core_vnode).
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
         handle_exit/3]).

-record(state, {
          db,
          partition,
          node
         }).

-ignore_xref([
              release_ip/4,
              create/4,
              delete/3,
              get/3,
              set/4,
              claim_ip/4,
              repair/4,
              release_ip/4,
              start_vnode/1
             ]).

-define(MASTER, sniffle_iprange_vnode_master).

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
    {ok, #state{
            db = DB,
            partition = Partition,
            node = node()
           }}.

handle_command(ping, _Sender, State) ->
    {reply, {pong, State#state.partition}, State};

handle_command({repair, Iprange, VClock, Obj}, _Sender, State) ->
    case fifo_db:get(State#state.db, <<"iprange">>, Iprange) of
        {ok, #sniffle_obj{vclock = VC1}} when VC1 =:= VClock ->
            fifo_db:put(State#state.db, <<"iprange">>, Iprange, Obj);
        not_found ->
            fifo_db:put(State#state.db, <<"iprange">>, Iprange, Obj);
        _ ->
            lager:error("[uprange] Read repair failed, data was updated too recent.")
    end,
    {noreply, State};

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
    HObject = #sniffle_obj{val=I1, vclock=VC},
    fifo_db:put(State#state.db, <<"iprange">>, UUID, HObject),
    {reply, {ok, ReqID}, State};

handle_command({delete, {ReqID, _Coordinator}, Iprange}, _Sender, State) ->
    fifo_db:delete(State#state.db, <<"iprange">>, Iprange),
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
                    fifo_db:put(State#state.db, <<"iprange">>, Iprange,
                                sniffle_obj:update(H3, Coordinator, O)),
                    V1 = statebox:value(H3),
                    {reply, {ok, ReqID, {jsxd:get(<<"tag">>, <<"">>, V1),
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
            fifo_db:put(State#state.db, <<"iprange">>, Iprange,
                        sniffle_obj:update(H3, Coordinator, O)),
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
            fifo_db:put(State#state.db, <<"iprange">>, Iprange,
                        sniffle_obj:update(H3, Coordinator, O)),
            {reply, {ok, ReqID}, State};

        _ ->
            {reply, {ok, ReqID, not_found}, State}
    end;

handle_command(?FOLD_REQ{foldfun=Fun, acc0=Acc0}, _Sender, State) ->
    Acc = fifo_db:fold(State#state.db,
                       <<"iprange">>, Fun, Acc0),
    {reply, Acc, State};

handle_command(Message, _Sender, State) ->
    lager:error("[ipranges] Unknown command: ~p", [Message]),
    {noreply, State}.

handle_handoff_command(?FOLD_REQ{foldfun=Fun, acc0=Acc0}, _Sender, State) ->
    Acc = fifo_db:fold(State#state.db,
                       <<"iprange">>, Fun, Acc0),
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
    {Iprange, HObject} = binary_to_term(Data),
    fifo_db:put(State#state.db, <<"iprange">>, Iprange, HObject),
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
