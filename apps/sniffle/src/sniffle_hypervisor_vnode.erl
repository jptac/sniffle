-module(sniffle_hypervisor_vnode).
-behaviour(riak_core_vnode).
-behaviour(riak_core_aae_vnode).
-include("sniffle.hrl").
-include_lib("riak_core/include/riak_core_vnode.hrl").

-export([repair/4,
         get/3,
         register/4,
         unregister/3,
         set/4]).

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
         handle_info/2,
         sync_repair/4]).

-export([
         set_resource/4,
         set_characteristic/4,
         set_metadata/4,
         set_pool/4,
         set_service/4,
         alias/4,
         etherstubs/4,
         host/4,
         networks/4,
         path/4,
         port/4,
         sysinfo/4,
         uuid/4,
         version/4,
         virtualisation/4
        ]).

-ignore_xref([
              set_resource/4,
              set_characteristic/4,
              set_metadata/4,
              set_pool/4,
              set_service/4,
              alias/4,
              etherstubs/4,
              host/4,
              networks/4,
              path/4,
              port/4,
              sysinfo/4,
              uuid/4,
              version/4,
              virtualisation/4
             ]).

-export([master/0,
         aae_repair/2,
         hash_object/2]).

-ignore_xref([get/3,
              register/4,
              repair/4,
              set/4,
              start_vnode/1,
              unregister/3,
              handle_info/2,
              sync_repair/4]).

-define(SERVICE, sniffle_hypervisor).

-define(MASTER, sniffle_hypervisor_vnode_master).

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

sync_repair(Preflist, ReqID, UUID, Obj) ->
    riak_core_vnode_master:command(Preflist,
                                   {sync_repair, ReqID, UUID, Obj},
                                   {fsm, undefined, self()},
                                   ?MASTER).

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

?VSET(set_resource).
?VSET(set_characteristic).
?VSET(set_metadata).
?VSET(set_pool).
?VSET(set_service).
?VSET(alias).
?VSET(etherstubs).
?VSET(host).
?VSET(networks).
?VSET(path).
?VSET(port).
?VSET(sysinfo).
?VSET(uuid).
?VSET(version).
?VSET(virtualisation).

%%%===================================================================
%%% VNode
%%%===================================================================

init([Part]) ->
    sniffle_vnode:init(Part, <<"hypervisor">>, ?SERVICE, ?MODULE,
                       ft_hypervisor).

%%%===================================================================
%%% Node Specific
%%%===================================================================

handle_command({register, {ReqID, Coordinator} = ID, Hypervisor, [IP, Port]}, _Sender, State) ->
    H0 = ft_hypervisor:new(ID),
    H1 = ft_hypervisor:port(ID, Port, H0),
    H2 = ft_hypervisor:host(ID, IP, H1),
    H3 = ft_hypervisor:uuid(ID, Hypervisor, H2),
    H4 = ft_hypervisor:path(ID, [{Hypervisor, 1}], H3),
    HObject = ft_obj:new(H4, Coordinator),
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
    ID = sniffle_vnode:mkid(list),
    FoldFn = fun(K, O, {Res, W, Hs}) ->
                     S0 = ft_obj:val(O),
                     S1 = ft_hypervisor:load(ID, S0),
                     {Host, Port} = ft_hypervisor:endpoint(S1),
                     Hs1 = [{K, Host, Port, ft_hypervisor:alias(S0)} | Hs],
                     %% W1 =
                     {Res1, W2} =
                         case ft_hypervisor:pools(S1) of
                             [] ->
                                 {ft_hypervisor:resources(S1), W};
                             Pools ->
                                 jsxd:fold(
                                   fun status_fold/3,
                                   {ft_hypervisor:resources(S1), W},
                                   Pools)
                         end,
                     {[{K, Res1, Hs} | Res], W2, Hs1}
             end,
    sniffle_vnode:fold(FoldFn, {[], [], []}, Sender, State);

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

status_fold(Name, Pool, {ResAcc, WarningsAcc}) ->
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
    end.

%% %% Taken from (and slightly modified):
%% %% from https://www.cs.umd.edu/class/fall2010/cmsc433/examples/erlang/pmap.erl
%% %% applies F to each element of L in a separate process.  The results
%% %% returned may not be in the same order as they appear in L.
%% pmap(F, L) ->
%%     S = self(),
%%     Ref = erlang:make_ref(),
%%     [spawn(fun() -> do_f1(S, Ref, F, I) end) || I <- L],
%%     %% gather the results
%%     gather1(length(L), Ref, []).

%% do_f1(Parent, Ref, F, I) ->
%%     Parent ! {Ref, (catch F(I))}.

%% gather1(0, _, L) -> L;
%% gather1(N, Ref, L) ->
%%     receive
%%         {Ref, Ret} -> gather1(N-1, Ref, [Ret|L])
%%     end.
