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
         hash_object/2
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

-define(SERVICE, sniffle_hypervisor).

-define(MASTER, sniffle_hypervisor_vnode_master).

%%%===================================================================
%%% AAE
%%%===================================================================

master() ->
    ?MASTER.

hash_object(BKey, #sniffle_obj{vclock = RObj}) ->
    lager:debug("Hashing Key: ~p", [BKey]),
    list_to_binary(integer_to_list(erlang:phash2({BKey, RObj})));
hash_object(BKey, RObj) ->
    lager:debug("Hashing Key: ~p", [BKey]),
    list_to_binary(integer_to_list(erlang:phash2({BKey, RObj}))).

aae_repair(_, Key) ->
    lager:debug("AAE Repair: ~p", [Key]),
    sniffle_hypervisor:get(Key).

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
                                   {delete, ReqID, Hypervisor},
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

init([Part]) ->
    sniffle_vnode:init(Part, <<"hypervisor">>, ?SERVICE, ?MODULE,
                       sniffle_hypervisor_state).

%%%===================================================================
%%% Node Specific
%%%===================================================================

handle_command({register, {ReqID, Coordinator}, Hypervisor, [Ip, Port]}, _Sender, State) ->
    H0 = statebox:new(fun sniffle_hypervisor_state:new/0),
    H1 = statebox:modify({fun sniffle_hypervisor_state:uuid/2, [Hypervisor]}, H0),
    H2 = statebox:modify({fun sniffle_hypervisor_state:host/2, [Ip]}, H1),
    H3 = statebox:modify({fun sniffle_hypervisor_state:port/2, [Port]}, H2),

    VC0 = vclock:fresh(),
    VC = vclock:increment(Coordinator, VC0),
    HObject = #sniffle_obj{val=H3, vclock=VC},

    sniffle_vnode:put(Hypervisor, HObject, State),
    {reply, {ok, ReqID}, State};

handle_command(Message, Sender, State) ->
    sniffle_vnode:handle_command(Message, Sender, State).

handle_handoff_command(?FOLD_REQ{foldfun=Fun, acc0=Acc0}, _Sender, State) ->
    Acc = fifo_db:fold(State#vstate.db, <<"hypervisor">>, Fun, Acc0),
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
    sniffle_vnode:put(Hypervisor, Obj, State),
    {reply, ok, State}.

encode_handoff_item(Hypervisor, Data) ->
    term_to_binary({Hypervisor, Data}).

is_empty(State) ->
    sniffle_vnode:is_empty(State).

delete(State) ->
    sniffle_vnode:delete(State).

handle_coverage(status, _KeySpaces, Sender, State) ->
    FoldFn = fun(K, #sniffle_obj{val=S0}, {Res, Warnings}) ->
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
             end,
    sniffle_vnode:fold(FoldFn, {[], []}, Sender, State);

handle_coverage(Req, KeySpaces, Sender, State) ->
    sniffle_vnode:handle_coverage(Req, KeySpaces, Sender, State).

handle_exit(_Pid, _Reason, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

%%%===================================================================
%%% AAE
%%%===================================================================

handle_info(Msg, State) ->
    sniffle_vnode:handle_info(Msg, State).

%%%===================================================================
%%% General
%%%===================================================================

bin_fmt(F, L) ->
    list_to_binary(io_lib:format(F, L)).
