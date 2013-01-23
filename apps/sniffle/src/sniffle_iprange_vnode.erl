-module(sniffle_iprange_vnode).
-behaviour(riak_core_vnode).
-include("sniffle.hrl").
-include_lib("riak_core/include/riak_core_vnode.hrl").

-export([
         repair/4,
         get/3,
         list/2,
         list/3,
         create/4,
         delete/3,
         lookup/3,
         claim_ip/4,
         release_ip/4
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
              lookup/3,
              delete/3,
              get/3,
              claim_ip/4,
              list/2,
              list/3,
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
    ?PRINT({get, Preflist, ReqID, Iprange}),
    riak_core_vnode_master:command(Preflist,
                                   {get, ReqID, Iprange},
                                   {fsm, undefined, self()},
                                   ?MASTER).

lookup(Preflist, ReqID, Name) ->
    riak_core_vnode_master:coverage(
      {lookup, ReqID, Name},
      Preflist,
      all,
      {fsm, undefined, self()},
      ?MASTER).

%%%===================================================================
%%% API - coverage
%%%===================================================================

list(Preflist, ReqID) ->
    riak_core_vnode_master:coverage(
      {list, ReqID},
      Preflist,
      all,
      {fsm, undefined, self()},
      ?MASTER).

list(Preflist, ReqID, Requirements) ->
    riak_core_vnode_master:coverage(
      {list, ReqID, Requirements},
      Preflist,
      all,
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

%%%===================================================================
%%% VNode
%%%===================================================================

init([Partition]) ->
    DB = list_to_atom(integer_to_list(Partition)),
    sniffle_db:start(DB),
    {ok, #state{
       db = DB,
       partition = Partition,
       node = node()
      }}.

handle_command(ping, _Sender, State) ->
    {reply, {pong, State#state.partition}, State};

handle_command({repair, Iprange, VClock, Obj}, _Sender, State) ->
    case sniffle_db:get(State#state.db, <<"iprange">>, Iprange) of
        {ok, #sniffle_obj{vclock = VC1}} when VC1 =:= VClock ->
            estatsd:increment("sniffle.ipranges.readrepair.success"),
            sniffle_db:put(State#state.db, <<"iprange">>, Iprange, Obj);
        not_found ->
            estatsd:increment("sniffle.ipranges.readrepair.success"),
            sniffle_db:put(State#state.db, <<"iprange">>, Iprange, Obj);
        _ ->
            estatsd:increment("sniffle.ipranges.readrepair.failed"),
            lager:error("[uprange] Read repair failed, data was updated too recent.")
    end,
    {noreply, State};

handle_command({get, ReqID, Iprange}, _Sender, State) ->
    Res = case sniffle_db:get(State#state.db, <<"iprange">>, Iprange) of
              {ok, R} ->
                  estatsd:increment("sniffle.ipranges.read.success"),
                  R;
              not_found ->
                  estatsd:increment("sniffle.ipranges.read.failed"),
                  not_found
          end,
    NodeIdx = {State#state.partition, State#state.node},
    {reply, {ok, ReqID, NodeIdx, Res}, State};

handle_command({create, {ReqID, Coordinator}, UUID,
                [Iprange, Network, Gateway, Netmask, First, Last, Tag, Vlan]},
               _Sender, State) ->
    estatsd:increment("sniffle.ipranges.create"),
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
    sniffle_db:put(State#state.db, <<"iprange">>, UUID, HObject),
    {reply, {ok, ReqID}, State};

handle_command({delete, {ReqID, _Coordinator}, Iprange}, _Sender, State) ->
    estatsd:increment("sniffle.ipranges.delete"),
    sniffle_db:delete(State#state.db, <<"iprange">>, Iprange),
    {reply, {ok, ReqID}, State};

handle_command({ip, claim,
                {ReqID, Coordinator}, Iprange, IP}, _Sender, State) ->
    case sniffle_db:get(State#state.db, <<"iprange">>, Iprange) of
        {ok, #sniffle_obj{val=H0} = O} ->
            estatsd:increment("sniffle.ipranges.claim.success"),
            H1 = statebox:modify({fun sniffle_iprange_state:load/1,[]}, H0),
            H2 = statebox:modify({fun sniffle_iprange_state:claim_ip/2,[IP]}, H1),
            H3 = statebox:expire(?STATEBOX_EXPIRE, H2),
            sniffle_db:put(State#state.db, <<"iprange">>, Iprange,
                           sniffle_obj:update(H3, Coordinator, O)),
            V1 = statebox:value(H3),
            {reply, {ok, ReqID, {jsxd:get(<<"tag">>, <<"">>, V1),
                                 IP,
                                 jsxd:get(<<"netmask">>, 0, V1),
                                 jsxd:get(<<"gateway">>, 0, V1)}}, State};
        _ ->
            estatsd:increment("sniffle.ipranges.claim.failed"),
            {reply, {ok, ReqID, failed}, State}
    end;

handle_command({ip, release,
                {ReqID, Coordinator}, Iprange, IP}, _Sender, State) ->
    case sniffle_db:get(State#state.db, <<"iprange">>, Iprange) of
        {ok, #sniffle_obj{val=H0} = O} ->
            estatsd:increment("sniffle.ipranges.release.success"),

            H1 = statebox:modify({fun sniffle_iprange_state:load/1,[]}, H0),
            H2 = statebox:modify({fun sniffle_iprange_state:release_ip/2,[IP]}, H1),
            H3 = statebox:expire(?STATEBOX_EXPIRE, H2),
            sniffle_db:put(State#state.db, <<"iprange">>, Iprange,
                           sniffle_obj:update(H3, Coordinator, O));
        _ ->
            estatsd:increment("sniffle.ipranges.release.failed"),
            ok
    end,
    {reply, {ok, ReqID}, State};

handle_command(Message, _Sender, State) ->
    estatsd:increment("sniffle.ipranges.unknown_command"),
    lager:error("[ipranges] Unknown command: ~p", [Message]),
    {noreply, State}.

handle_handoff_command(?FOLD_REQ{foldfun=Fun, acc0=Acc0}, _Sender, State) ->
    Acc = sniffle_db:fold(State#state.db,
                          <<"iprange">>, Fun, Acc0),
    {reply, Acc, State}.

handoff_starting(_TargetNode, State) ->
    estatsd:increment("sniffle.ipranges.handoff.started"),
    {true, State}.

handoff_cancelled(State) ->
    estatsd:increment("sniffle.ipranges.handoff.started"),
    {ok, State}.

handoff_finished(_TargetNode, State) ->
    estatsd:increment("sniffle.ipranges.handoff.started"),
    {ok, State}.

handle_handoff_data(Data, State) ->
    {Iprange, HObject} = binary_to_term(Data),
    sniffle_db:put(State#state.db, <<"iprange">>, Iprange, HObject),
    {reply, ok, State}.

encode_handoff_item(Iprange, Data) ->
    term_to_binary({Iprange, Data}).

is_empty(State) ->
    sniffle_db:fold(State#state.db,
                    <<"iprange">>,
                    fun (_, _, _) ->
                            {false, State}
                    end, {true, State}).

delete(State) ->
    Trans = sniffle_db:fold(State#state.db,
                            <<"iprange">>,
                            fun (K,_, A) ->
                                    [{delete, K} | A]
                            end, []),
    sniffle_db:transact(State#state.db, Trans),
    {ok, State}.

handle_coverage({lookup, ReqID, Name}, _KeySpaces, _Sender, State) ->
    estatsd:increment("sniffle.ipranges.lookup"),
    Res = sniffle_db:fold(State#state.db,
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

handle_coverage({list, ReqID, Requirements}, _KeySpaces, _Sender, State) ->
    estatsd:increment("sniffle.ipranges.list"),
    Getter = fun(#sniffle_obj{val=S0}, V) ->
                     jsxd:get(V, 0, statebox:value(S0))
             end,
    List = sniffle_db:fold(State#state.db,
                           <<"iprange">>,
                           fun (Key, E, C) ->
                                   case sniffle_matcher:match(E, Getter, Requirements) of
                                       false ->
                                           C;
                                       Pts ->
                                           [{Key, Pts} | C]
                                   end
                           end, []),
    {reply,
     {ok, ReqID, {State#state.partition, State#state.node}, List},
     State};


handle_coverage({list, ReqID}, _KeySpaces, _Sender, State) ->
    estatsd:increment("sniffle.ipranges.list"),
    List = sniffle_db:fold(State#state.db,
                           <<"iprange">>,
                           fun (K, _, L) ->
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
