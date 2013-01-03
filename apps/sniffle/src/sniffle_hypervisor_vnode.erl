-module(sniffle_hypervisor_vnode).
-behaviour(riak_core_vnode).
-include("sniffle.hrl").
-include_lib("riak_core/include/riak_core_vnode.hrl").

-export([
         repair/4,
         get/3,
         status/2,
         list/2,
         list/3,
         register/4,
         unregister/3,
         set_resource/4,
         mset_resource/4
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
          hypervisors,
          partition,
          node
         }).

-ignore_xref([
              get/3,
              list/2,
              list/3,
              status/2,
              register/4,
              repair/4,
              set_resource/4,
              mset_resource/4,
              start_vnode/1,
              unregister/3
             ]).

-define(MASTER, sniffle_hypervisor_vnode_master).

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
    ?PRINT({get, Preflist, ReqID, Hypervisor}),
    riak_core_vnode_master:command(Preflist,
                                   {get, ReqID, Hypervisor},
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

status(Preflist, ReqID) ->
    riak_core_vnode_master:coverage(
      {status, ReqID},
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

set_resource(Preflist, ReqID, Hypervisor, Data) ->
    riak_core_vnode_master:command(Preflist,
                                   {resource, set, ReqID, Hypervisor, Data},
                                   {fsm, undefined, self()},
                                   ?MASTER).

mset_resource(Preflist, ReqID, Hypervisor, Data) ->
    riak_core_vnode_master:command(Preflist,
                                   {resource, mset, ReqID, Hypervisor, Data},
                                   {fsm, undefined, self()},
                                   ?MASTER).

%%%===================================================================
%%% VNode
%%%===================================================================

init([Partition]) ->
    {ok, #state{
       hypervisors = dict:new(),
       partition = Partition,
       node = node()
      }}.

handle_command(ping, _Sender, State) ->
    {reply, {pong, State#state.partition}, State};

handle_command({repair, Hypervisor, VClock, Obj}, _Sender, State) ->
    Hs0 = dict:update(Hypervisor,
                      fun(Obj1) ->
                              case Obj1#sniffle_obj.vclock of
                                  VClock ->
                                      Obj;
                                  _ ->
                                      lager:error("[hypervisor] Read repair failed, data was updated too recent."),
                                      Obj1
                              end
                      end, Obj, State#state.hypervisors),
    {noreply, State#state{hypervisors=Hs0}};

handle_command({get, ReqID, Hypervisor}, _Sender, State) ->
    ?PRINT({handle_command, get, ReqID, Hypervisor}),
    NodeIdx = {State#state.partition, State#state.node},
    Res = case dict:find(Hypervisor, State#state.hypervisors) of
              error ->
                  {ok, ReqID, NodeIdx, not_found};
              {ok, V} ->
                  {ok, ReqID, NodeIdx, V}
          end,
    {reply,
     Res,
     State};

handle_command({register, {ReqID, Coordinator}, Hypervisor, [Ip, Port]}, _Sender, State) ->
    H0 = statebox:new(fun sniffle_hypervisor_state:new/0),
    H1 = statebox:modify({fun sniffle_hypervisor_state:name/2, [Hypervisor]}, H0),
    H2 = statebox:modify({fun sniffle_hypervisor_state:host/2, [Ip]}, H1),
    H3 = statebox:modify({fun sniffle_hypervisor_state:port/2, [Port]}, H2),

    VC0 = vclock:fresh(),
    VC = vclock:increment(Coordinator, VC0),
    HObject = #sniffle_obj{val=H3, vclock=VC},

    Hs0 = dict:store(Hypervisor, HObject, State#state.hypervisors),
    {reply, {ok, ReqID}, State#state{hypervisors = Hs0}};

handle_command({unregister, {ReqID, _Coordinator}, Hypervisor}, _Sender, State) ->
    Hs0 = dict:erase(Hypervisor, State#state.hypervisors),
    {reply, {ok, ReqID}, State#state{hypervisors = Hs0}};

handle_command({resource, set,
                {ReqID, Coordinator}, Hypervisor,
                [Resource, Value]}, _Sender, State) ->
    Hs0 = dict:update(Hypervisor,
                      fun(#sniffle_obj{val=H0} = O) ->
                              H1 = statebox:modify(
                                     {fun sniffle_hypervisor_state:resource/3,
                                      [Resource, Value]}, H0),
                              H2 = statebox:expire(?STATEBOX_EXPIRE, H1),
                              sniffle_obj:update(H2, Coordinator, O)
                      end, State#state.hypervisors),
    {reply, {ok, ReqID}, State#state{hypervisors = Hs0}};

handle_command({resource, mset,
                {ReqID, Coordinator}, Vm,
                Resources}, _Sender, State) ->
    Hs0 = dict:update(Vm,
                      fun(#sniffle_obj{val=H0} = O) ->
                              H1 = lists:foldr(
                                     fun ({Resource, Value}, H) ->
                                             statebox:modify(
                                               {fun sniffle_hypervisor_state:resource/3,
                                                [Resource, Value]}, H)
                                     end, H0, Resources),
                              H2 = statebox:expire(?STATEBOX_EXPIRE, H1),
                              sniffle_obj:update(H2, Coordinator, O)
                      end, State#state.hypervisors),
    {reply, {ok, ReqID}, State#state{hypervisors = Hs0}};

handle_command(Message, _Sender, State) ->
    ?PRINT({unhandled_command, Message}),
    {noreply, State}.

handle_handoff_command(?FOLD_REQ{foldfun=Fun, acc0=Acc0}, _Sender, State) ->
    Acc = dict:fold(Fun, Acc0, State#state.hypervisors),
    {reply, Acc, State}.

handoff_starting(_TargetNode, State) ->
    {true, State}.

handoff_cancelled(State) ->
    {ok, State}.

handoff_finished(_TargetNode, State) ->
    {ok, State}.

handle_handoff_data(Data, State) ->
    {Hypervisor, HObject} = binary_to_term(Data),
    Hs0 = dict:store(Hypervisor, HObject, State#state.hypervisors),
    {reply, ok, State#state{hypervisors = Hs0}}.

encode_handoff_item(Hypervisor, Data) ->
    term_to_binary({Hypervisor, Data}).

is_empty(State) ->
    case dict:size(State#state.hypervisors) of
        0 ->
            {true, State};
        _ ->
            {true, State}
    end.

delete(State) ->
    {ok, State#state{hypervisors = dict:new()}}.

handle_coverage({list, ReqID, Requirements}, _KeySpaces, _Sender, State) ->
    Getter = fun(#sniffle_obj{val=S0}, Resource) ->
                     jsxd:get(re:split(Resource, <<"\\.">>), 0, statebox:value(S0))
             end,
    Server = sniffle_matcher:match_dict(State#state.hypervisors, Getter, Requirements),
    {reply,
     {ok, ReqID, {State#state.partition, State#state.node}, Server},
     State};

handle_coverage({list, ReqID}, _KeySpaces, _Sender, State) ->
    {reply,
     {ok, ReqID, {State#state.partition, State#state.node}, dict:fetch_keys(State#state.hypervisors)},
     State};



handle_coverage({status, ReqID}, _KeySpaces, _Sender, State) ->
    Res = dict:fold(
            fun(K, #sniffle_obj{val=S0}, {Res, Warnings}) ->
                    H=statebox:value(S0),
                    {ok, Host} = jsxd:get(<<"host">>, H),
                    {ok, Port} = jsxd:get(<<"port">>, H),
                    Res1 = jsxd:reduce(fun (Resource, Value, Acc) ->
                                               jsxd:update(Resource,
                                                           fun(Current)->
                                                                   Current + Value
                                                           end, Value, Acc)
                                       end, Res, jsxd:get(<<"resources">>, [], H)),
                    Res2 = jsxd:update(<<"hypervisors">>, fun(Current)-> [K|Current] end, [K], Res1),
                    Warnings1 = case libchunter:ping(Host, Port) of
                                    {error,connection_failed} ->
                                        [jsxd:from_list(
                                           [{<<"category">>, <<"chunter">>},
                                            {<<"elementy">>, K},
                                            {<<"typey">>, <<"critical">>},
                                            {<<"messagey">>,
                                             bin_fmt("Chunter server ~s down.", [K])}]) |
                                         Warnings];
                                    pong ->
                                        Warnings
                                end,
                    case jxsd:get(<<"pools">>, H) of
                        not_found ->
                            {Res2, Warnings1};
                        {ok, Pools} ->
                            jxsd:reduce(
                              fun (Name, Pool, {ResAcc, WarningsAcc}) ->
                                      Size = jsxd:get(<<"size">>, 0, Pool),
                                      Used = jsxd:get(<<"used">>, 0, Pool),
                                      ResAcc1 = jsxd:thread([{update, <<"size">>, fun(C) -> C + Size end, Size},
                                                             {update, <<"used">>, fun(C) -> C + Used end, Used}],
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
                              end,{Res2, Warnings1}, Pools)
                    end
            end, {[], []}, State#state.hypervisors),
    {reply,
     {ok, ReqID, {State#state.partition, State#state.node}, [Res]},
     State};

handle_coverage(_Req, _KeySpaces, _Sender, State) ->
    {stop, not_implemented, State}.

handle_exit(_Pid, _Reason, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

bin_fmt(F, L) ->
    list_to_binary(io_lib:format(F, L)).
