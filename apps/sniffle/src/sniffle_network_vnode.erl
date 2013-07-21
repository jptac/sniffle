-module(sniffle_network_vnode).
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
         add_iprange/4,
         remove_iprange/4,
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
              lookup/3,
              delete/3,
              get/3,
              set/4,
              claim_ip/4,
              list/2,
              list/3,
              repair/4,
              add_iprange/4,
              remove_iprange/4,
              start_vnode/1
             ]).

-define(MASTER, sniffle_network_vnode_master).

%%%===================================================================
%%% API
%%%===================================================================

start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

repair(IdxNode, Network, VClock, Obj) ->
    riak_core_vnode_master:command(IdxNode,
                                   {repair, Network, VClock, Obj},
                                   ignore,
                                   ?MASTER).

%%%===================================================================
%%% API - reads
%%%===================================================================

get(Preflist, ReqID, Network) ->
    riak_core_vnode_master:command(Preflist,
                                   {get, ReqID, Network},
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

delete(Preflist, ReqID, Network) ->
    riak_core_vnode_master:command(Preflist,
                                   {delete, ReqID, Network},
                                   {fsm, undefined, self()},
                                   ?MASTER).

add_iprange(Preflist, ReqID, Network, IPRange) ->
    riak_core_vnode_master:command(Preflist,
                                   {add_iprange, ReqID, Network, IPRange},
                                   {fsm, undefined, self()},
                                   ?MASTER).
remove_iprange(Preflist, ReqID, Network, IPRange) ->
    riak_core_vnode_master:command(Preflist,
                                   {remove_iprange, ReqID, Network, IPRange},
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
    sniffle_db:start(DB),
    {ok, #state{
            db = DB,
            partition = Partition,
            node = node()
           }}.

handle_command(ping, _Sender, State) ->
    {reply, {pong, State#state.partition}, State};

handle_command({repair, Network, VClock, Obj}, _Sender, State) ->
    case sniffle_db:get(State#state.db, <<"network">>, Network) of
        {ok, #sniffle_obj{vclock = VC1}} when VC1 =:= VClock ->
            sniffle_db:put(State#state.db, <<"network">>, Network, Obj);
        not_found ->
            sniffle_db:put(State#state.db, <<"network">>, Network, Obj);
        _ ->
            lager:error("[uprange] Read repair failed, data was updated too recent.")
    end,
    {noreply, State};

handle_command({get, ReqID, Network}, _Sender, State) ->
    Res = case sniffle_db:get(State#state.db, <<"network">>, Network) of
              {ok, R} ->
                  R;
              not_found ->
                  not_found
          end,
    NodeIdx = {State#state.partition, State#state.node},
    {reply, {ok, ReqID, NodeIdx, Res}, State};

handle_command({create, {ReqID, Coordinator}, UUID,
                [Name]},
               _Sender, State) ->
    I0 = statebox:new(fun sniffle_network_state:new/0),
    I1 = lists:foldl(
           fun (OP, SB) ->
                   statebox:modify(OP, SB)
           end, I0, [{fun sniffle_network_state:uuid/2, [UUID]},
                     {fun sniffle_network_state:name/2, [Name]}]),
    VC0 = vclock:fresh(),
    VC = vclock:increment(Coordinator, VC0),
    HObject = #sniffle_obj{val=I1, vclock=VC},
    sniffle_db:put(State#state.db, <<"network">>, UUID, HObject),
    {reply, {ok, ReqID}, State};

handle_command({delete, {ReqID, _Coordinator}, Network}, _Sender, State) ->
    sniffle_db:delete(State#state.db, <<"network">>, Network),
    {reply, {ok, ReqID}, State};

handle_command({set,
                {ReqID, Coordinator}, Network,
                Resources}, _Sender, State) ->
    case sniffle_db:get(State#state.db, <<"network">>, Network) of
        {ok, #sniffle_obj{val=H0} = O} ->
            H1 = statebox:modify({fun sniffle_network_state:load/1,[]}, H0),
            H2 = lists:foldr(
                   fun ({Resource, Value}, H) ->
                           statebox:modify(
                             {fun sniffle_network_state:set_metadata/3,
                              [Resource, Value]}, H)
                   end, H1, Resources),
            H3 = statebox:expire(?STATEBOX_EXPIRE, H2),
            sniffle_db:put(State#state.db, <<"network">>, Network,
                           sniffle_obj:update(H3, Coordinator, O)),
            {reply, {ok, ReqID}, State};
        R ->
            lager:error("[hypervisors] tried to write to a non existing hypervisor: ~p", [R]),
            {reply, {ok, ReqID, not_found}, State}
    end;

handle_command({add_iprange,
                {ReqID, Coordinator}, Network,
                IPRange}, _Sender, State) ->
    case sniffle_db:get(State#state.db, <<"network">>, Network) of
        {ok, #sniffle_obj{val=H0} = O} ->
            H1 = statebox:modify({fun sniffle_network_state:load/1,[]}, H0),
            H2 = statebox:modify(
                   {fun sniffle_network_state:add_iprange/2,
                    [IPRange]}, H1),
            H3 = statebox:expire(?STATEBOX_EXPIRE, H2),
            sniffle_db:put(State#state.db, <<"network">>, Network,
                           sniffle_obj:update(H3, Coordinator, O)),
            {reply, {ok, ReqID}, State};
        R ->
            lager:error("[hypervisors] tried to write to a non existing hypervisor: ~p", [R]),
            {reply, {ok, ReqID, not_found}, State}
    end;

handle_command({remove_iprange,
                {ReqID, Coordinator}, Network,
                IPRange}, _Sender, State) ->
    case sniffle_db:get(State#state.db, <<"network">>, Network) of
        {ok, #sniffle_obj{val=H0} = O} ->
            H1 = statebox:modify({fun sniffle_network_state:load/1,[]}, H0),
            H2 = statebox:modify(
                   {fun sniffle_network_state:remove_iprange/2,
                    [IPRange]}, H1),
            H3 = statebox:expire(?STATEBOX_EXPIRE, H2),
            sniffle_db:put(State#state.db, <<"network">>, Network,
                           sniffle_obj:update(H3, Coordinator, O)),
            {reply, {ok, ReqID}, State};
        R ->
            lager:error("[hypervisors] tried to write to a non existing hypervisor: ~p", [R]),
            {reply, {ok, ReqID, not_found}, State}
    end;

handle_command(Message, _Sender, State) ->
    lager:error("[networks] Unknown command: ~p", [Message]),
    {noreply, State}.

handle_handoff_command(?FOLD_REQ{foldfun=Fun, acc0=Acc0}, _Sender, State) ->
    Acc = sniffle_db:fold(State#state.db,
                          <<"network">>, Fun, Acc0),
    {reply, Acc, State}.

handoff_starting(_TargetNode, State) ->
    {true, State}.

handoff_cancelled(State) ->
    {ok, State}.

handoff_finished(_TargetNode, State) ->
    {ok, State}.

handle_handoff_data(Data, State) ->
    {Network, HObject} = binary_to_term(Data),
    sniffle_db:put(State#state.db, <<"network">>, Network, HObject),
    {reply, ok, State}.

encode_handoff_item(Network, Data) ->
    term_to_binary({Network, Data}).

is_empty(State) ->
    sniffle_db:fold(State#state.db,
                    <<"network">>,
                    fun (_, _, _) ->
                            {false, State}
                    end, {true, State}).

delete(State) ->
    Trans = sniffle_db:fold(State#state.db,
                            <<"network">>,
                            fun (K,_, A) ->
                                    [{delete, <<"network", K/binary>>} | A]
                            end, []),
    sniffle_db:transact(State#state.db, Trans),
    {ok, State}.

handle_coverage({lookup, ReqID, Name}, _KeySpaces, _Sender, State) ->
    Res = sniffle_db:fold(State#state.db,
                          <<"network">>,
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
    Getter = fun(#sniffle_obj{val=S0}, V) ->
                     jsxd:get(V, 0, statebox:value(S0))
             end,
    List = sniffle_db:fold(State#state.db,
                           <<"network">>,
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


handle_coverage({list, ReqID}, _KeySpaces, _Sender, State) ->
    List = sniffle_db:fold(State#state.db,
                           <<"network">>,
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
