-module(sniffle_dataset_vnode).
-behaviour(riak_core_vnode).
-behaviour(riak_core_aae_vnode).
-include("sniffle.hrl").
-include_lib("riak_core/include/riak_core_vnode.hrl").

-export([
         repair/4,
         get/3,
         create/4,
         delete/3,
         set/4
        ]).

-export([
         start_vnode/1,
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
         handle_info/2
        ]).

-export([
         master/0,
         aae_repair/2,
         hash_object/2
        ]).

-ignore_xref([
              create/4,
              delete/3,
              get/3,
              repair/4,
              set/4,
              start_vnode/1,
              handle_info/2
             ]).

-define(SERVICE, sniffle_dataset).

-define(MASTER, sniffle_dataset_vnode_master).

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
    sniffle_dataset:get(Key).

%%%===================================================================
%%% API
%%%===================================================================

start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

repair(IdxNode, Dataset, VClock, Obj) ->
    riak_core_vnode_master:command([IdxNode],
                                   {repair, Dataset, VClock, Obj},
                                   ?MASTER).

%%%===================================================================
%%% API - reads
%%%===================================================================

get(Preflist, ReqID, Dataset) ->
    riak_core_vnode_master:command(Preflist,
                                   {get, ReqID, Dataset},
                                   {fsm, undefined, self()},
                                   ?MASTER).

%%%===================================================================
%%% API - writes
%%%===================================================================

create(Preflist, ReqID, Dataset, Data) ->
    riak_core_vnode_master:command(Preflist,
                                   {create, ReqID, Dataset, Data},
                                   {fsm, undefined, self()},
                                   ?MASTER).

delete(Preflist, ReqID, Dataset) ->
    riak_core_vnode_master:command(Preflist,
                                   {delete, ReqID, Dataset},
                                   {fsm, undefined, self()},
                                   ?MASTER).
set(Preflist, ReqID, Dataset, Data) ->
    riak_core_vnode_master:command(Preflist,
                                   {set, ReqID, Dataset, Data},
                                   {fsm, undefined, self()},
                                   ?MASTER).

%%%===================================================================
%%% VNode
%%%===================================================================

init([Partition]) ->
    sniffle_vnode:init(Partition, <<"dataset">>, ?SERVICE).

handle_command({create, {ReqID, Coordinator}, Dataset, []},
               _Sender, State) ->
    I0 = statebox:new(fun sniffle_dataset_state:new/0),
    I1 = statebox:modify({fun sniffle_dataset_state:name/2, [Dataset]}, I0),
    VC0 = vclock:fresh(),
    VC = vclock:increment(Coordinator, VC0),
    Obj = #sniffle_obj{val=I1, vclock=VC},
    sniffle_vnode:put(Dataset, Obj, State),
    {reply, {ok, ReqID}, State};

handle_command({delete, {ReqID, _Coordinator}, Dataset}, _Sender, State) ->
    fifo_db:delete(State#vstate.db, <<"dataset">>, Dataset),
    riak_core_index_hashtree:delete({<<"dataset">>, Dataset}, State#vstate.hashtrees),
    {reply, {ok, ReqID}, State};

handle_command({set,
                {ReqID, Coordinator}, Dataset,
                Resources}, _Sender, State) ->
    case fifo_db:get(State#vstate.db, <<"dataset">>, Dataset) of
        {ok, #sniffle_obj{val=H0} = O} ->
            H1 = statebox:modify({fun sniffle_dataset_state:load/1,[]}, H0),
            H2 = lists:foldr(
                   fun ({Resource, Value}, H) ->
                           statebox:modify(
                             {fun sniffle_dataset_state:set/3,
                              [Resource, Value]}, H)
                   end, H1, Resources),
            H3 = statebox:expire(?STATEBOX_EXPIRE, H2),
            Obj = sniffle_obj:update(H3, Coordinator, O),
            sniffle_vnode:put(Dataset, Obj, State),
            {reply, {ok, ReqID}, State};
        R ->
            lager:error("[datasets] tried to write to a non existing dataset: ~p", [R]),
            {reply, {ok, ReqID, not_found}, State}
    end;

handle_command(Message, Sender, State) ->
    sniffle_vnode:handle_command(Message, Sender, State).

handle_handoff_command(?FOLD_REQ{foldfun=Fun, acc0=Acc0}, _Sender, State) ->
    Acc = fifo_db:fold(State#vstate.db, <<"dataset">>, Fun, Acc0),
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
    {Dataset, Obj} = binary_to_term(Data),
    sniffle_vnode:put(Dataset, Obj, State),
    {reply, ok, State}.

encode_handoff_item(Dataset, Data) ->
    term_to_binary({Dataset, Data}).

is_empty(State) ->
    sniffle_vnode:is_empty(State).

delete(State) ->
    sniffle_vnode:delete(State).

handle_coverage(list, _KeySpaces, Sender, State) ->
    sniffle_vnode:list_keys(Sender, State);

handle_coverage({list, Requirements}, _KeySpaces, Sender, State) ->
    Getter = fun(#sniffle_obj{val=S0}, Resource) ->
                     jsxd:get(Resource, 0, statebox:value(S0))
             end,
    sniffle_vnode:list_keys(Getter, Requirements, Sender, State);

handle_coverage(_Req, _KeySpaces, _Sender, State) ->
    {stop, not_implemented, State}.

handle_exit(_Pid, _Reason, State) ->
    {noreply, State}.

terminate(_Reason,  _State) ->
    ok.

%%%===================================================================
%%% AAE
%%%===================================================================

handle_info(Msg, State) ->
    sniffle_vnode:handle_info(Msg, State).
