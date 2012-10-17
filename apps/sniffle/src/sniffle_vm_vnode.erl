-module(sniffle_vm_vnode).
-behaviour(riak_core_vnode).
-include("sniffle.hrl").
-include_lib("riak_core/include/riak_core_vnode.hrl").

-export([repair/3,
	 get/3,
	 list/2,
	 register/4,
	 unregister/3,
	 set_attribute/4,
	 mset_attribute/4
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
	  vms,
	  partition,
	  node
	 }).

% those functions do not get called directly.
-ignore_xref([
	      get/3,
	      list/2,
	      mset_attribute/4,
	      register/4,
	      repair/3,
	      set_attribute/4,
	      start_vnode/1,
	      unregister/3
	      ]).


-define(MASTER, sniffle_vm_vnode_master).

%%%===================================================================
%%% API
%%%===================================================================

start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

repair(IdxNode, Vm, Obj) ->
    riak_core_vnode_master:command(IdxNode,
                                   {repair, Vm, Obj},
                                   ignore,
                                   ?MASTER).

%%%===================================================================
%%% API - reads
%%%===================================================================

get(Preflist, ReqID, Vm) ->
    ?PRINT({get, Preflist, ReqID, Vm}),
    riak_core_vnode_master:command(Preflist,
				   {get, ReqID, Vm},
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

%%%===================================================================
%%% API - writes
%%%===================================================================

register(Preflist, ReqID, Vm, Data) ->
    riak_core_vnode_master:command(Preflist,
				   {register, ReqID, Vm, Data},
				   {fsm, undefined, self()},
				   ?MASTER).

unregister(Preflist, ReqID, Vm) ->
    riak_core_vnode_master:command(Preflist,
                                   {unregister, ReqID, Vm},
				   {fsm, undefined, self()},
                                   ?MASTER).

set_attribute(Preflist, ReqID, Vm, Data) ->
    riak_core_vnode_master:command(Preflist,
                                   {attribute, set, ReqID, Vm, Data},
				   {fsm, undefined, self()},
                                   ?MASTER).

mset_attribute(Preflist, ReqID, Vm, Data) ->
    riak_core_vnode_master:command(Preflist,
                                   {attribute, mset, ReqID, Vm, Data},
				   {fsm, undefined, self()},
                                   ?MASTER).

%%%===================================================================
%%% VNode
%%%===================================================================

init([Partition]) ->
    {ok, #state{
       vms = dict:new(),
       partition = Partition,
       node = node()
      }}.

handle_command(ping, _Sender, State) ->
    {reply, {pong, State#state.partition}, State};

handle_command({repair, Vm, Obj}, _Sender, State) ->
    Hs0 = dict:store(Vm, Obj, State#state.vms),
    {noreply, State#state{vms=Hs0}};

handle_command({get, ReqID, Vm}, _Sender, State) ->
    ?PRINT({handle_command, get, ReqID, Vm}),
    NodeIdx = {State#state.partition, State#state.node},
    Res = case dict:find(Vm, State#state.vms) of
	      error ->
		  {ok, ReqID, NodeIdx, not_found};
	      {ok, V} ->
		  {ok, ReqID, NodeIdx, V}
	  end,
    {reply, 
     Res,
     State};

handle_command({register, {ReqID, Coordinator}, Vm, Hypervisor}, _Sender, State) ->
    H0 = statebox:new(fun sniffle_vm_state:new/0),
    H1 = statebox:modify({fun sniffle_vm_state:uuid/2, [Vm]}, H0),
    H2 = statebox:modify({fun sniffle_vm_state:hypervisor/2, [Hypervisor]}, H1),

    VC0 = vclock:fresh(),
    VC = vclock:increment(Coordinator, VC0),
    HObject = #sniffle_obj{val=H2, vclock=VC},

    Hs0 = dict:store(Vm, HObject, State#state.vms),
    {reply, {ok, ReqID}, State#state{vms = Hs0}};

handle_command({unregister, {ReqID, _Coordinator}, Vm}, _Sender, State) ->
    Hs0 = dict:erase(Vm, State#state.vms),
    {reply, {ok, ReqID}, State#state{vms = Hs0}};

handle_command({attribute, set, 
		{ReqID, Coordinator}, Vm, 
		[Resource, Value]}, _Sender, State) ->
    io:format("1~n"),
    Hs0 = dict:update(Vm, 
		      fun(#sniffle_obj{val=H0} = O) ->
			      io:format("2~n"),			     
			      H1 = statebox:modify(
				     {fun sniffle_vm_state:attribute/3, 
				      [Resource, Value]}, H0),
			      io:format("3~n"),
			      H2 = statebox:expire(?STATEBOX_EXPIRE, H1),
			      io:format("4~n"),
			      sniffle_obj:update(H2, Coordinator, O)
		      end, State#state.vms),
    io:format("5~n"),
    {reply, {ok, ReqID}, State#state{vms = Hs0}};

handle_command({attribute, mset, 
		{ReqID, Coordinator}, Vm, 
		Resources}, _Sender, State) ->
    Hs0 = dict:update(Vm, 
		      fun(#sniffle_obj{val=H0} = O) ->
			      H1 = lists:foldr(
				     fun ({Resource, Value}, H) ->
					     statebox:modify(
					       {fun sniffle_vm_state:attribute/3, 
						[Resource, Value]}, H)
				     end, H0, Resources),
			      H2 = statebox:expire(?STATEBOX_EXPIRE, H1),
			      sniffle_obj:update(H2, Coordinator, O)
		      end, State#state.vms),
    {reply, {ok, ReqID}, State#state{vms = Hs0}};


handle_command(Message, _Sender, State) ->
    ?PRINT({unhandled_command, Message}),
    {noreply, State}.

handle_handoff_command(?FOLD_REQ{foldfun=Fun, acc0=Acc0}, _Sender, State) ->
    Acc = dict:fold(Fun, Acc0, State#state.vms),
    {reply, Acc, State}.

handoff_starting(_TargetNode, State) ->
    {true, State}.

handoff_cancelled(State) ->
    {ok, State}.

handoff_finished(_TargetNode, State) ->
    {ok, State}.

handle_handoff_data(Data, State) ->
    {Vm, HObject} = binary_to_term(Data),
    Hs0 = dict:store(Vm, HObject, State#state.vms),
    {reply, ok, State#state{vms = Hs0}}.

encode_handoff_item(Vm, Data) ->
    term_to_binary({Vm, Data}).

is_empty(State) ->
    case dict:size(State#state.vms) of
	0 ->
	    {true, State};
	_ ->
	    {true, State}
    end.

delete(State) ->
    {ok, State#state{vms = dict:new()}}.

handle_coverage({list, ReqID}, _KeySpaces, _Sender, State) ->
    {reply, 
     {ok, ReqID, {State#state.partition,State#state.node}, dict:fetch_keys(State#state.vms)},
     State};

handle_coverage(_Req, _KeySpaces, _Sender, State) ->
    {stop, not_implemented, State}.

handle_exit(_Pid, _Reason, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.
