-module(sniffle_dataset_vnode).
-behaviour(riak_core_vnode).
-include("sniffle.hrl").
-include_lib("riak_core/include/riak_core_vnode.hrl").

-export([
	 repair/3,
	 get/3,
	 list/2,
	 create/4,
	 delete/3,
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
	  datasets,
	  partition,
	  node,
	  dbref,
	  index
	 }).

-define(MASTER, sniffle_dataset_vnode_master).

%%%===================================================================
%%% API
%%%===================================================================

start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

repair(IdxNode, Dataset, Obj) ->
    riak_core_vnode_master:command(IdxNode,
                                   {repair, undefined, Dataset, Obj},
                                   ignore,
                                   ?MASTER).

%%%===================================================================
%%% API - reads
%%%===================================================================

get(Preflist, ReqID, Dataset) ->
    ?PRINT({get, Preflist, ReqID, Dataset}),
    riak_core_vnode_master:command(Preflist,
				   {get, ReqID, Dataset},
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
set_attribute(Preflist, ReqID, Dataset, Data) ->
    riak_core_vnode_master:command(Preflist,
                                   {attribute, set, ReqID, Dataset, Data},
				   {fsm, undefined, self()},
                                   ?MASTER).

mset_attribute(Preflist, ReqID, Dataset, Data) ->
    riak_core_vnode_master:command(Preflist,
                                   {attribute, mset, ReqID, Dataset, Data},
				   {fsm, undefined, self()},
                                   ?MASTER).

%%%===================================================================
%%% VNode
%%%===================================================================

init([Partition]) ->
    {ok, DBRef} = eleveldb:open("datasets."++integer_to_list(Partition)++".ldb", [{create_if_missing, true}]),
    {Index, Ranges} = read_ranges(DBRef),
    {ok, #state{
       partition = Partition,
       index = Index,
       node = node(),
       datasets = Ranges,
       dbref=DBRef
      }}.

handle_command(ping, _Sender, State) ->
    {reply, {pong, State#state.partition}, State};

handle_command({get, ReqID, Dataset}, _Sender, State) ->
    ?PRINT({handle_command, get, ReqID, Dataset}),
    NodeIdx = {State#state.partition, State#state.node},
    Res = case dict:find(Dataset, State#state.datasets) of
	      error ->
		  {ok, ReqID, NodeIdx, not_found};
	      {ok, V} ->
		  {ok, ReqID, NodeIdx, V}
	  end,
    {reply, 
     Res,
     State};

handle_command({create, {ReqID, Coordinator}, Dataset,
		[]},
	       _Sender, #state{dbref = DBRef} = State) ->
    I0 = statebox:new(fun sniffle_dataset_state:new/0),
    I1 = statebox:modify({fun sniffle_dataset_state:name/2, [Dataset]}, I0),    
    VC0 = vclock:fresh(),
    VC = vclock:increment(Coordinator, VC0),
    HObject = #sniffle_obj{val=I1, vclock=VC},

    Is0 = dict:store(Dataset, HObject, State#state.datasets),

    eleveldb:put(DBRef, <<"#datasets">>, term_to_binary(dict:fetch_keys(Is0)), []),
    eleveldb:put(DBRef, Dataset, term_to_binary(HObject), []),
    
    {reply, {ok, ReqID}, State#state{datasets = Is0}};

handle_command({delete, {ReqID, _Coordinator}, Dataset}, _Sender, #state{dbref = DBRef} = State) ->
    Is0 = dict:erase(Dataset, State#state.datasets),

    eleveldb:put(DBRef, <<"#datasets">>, term_to_binary(dict:fetch_keys(Is0)), []),
    eleveldb:delete(DBRef, Dataset, []),

    {reply, {ok, ReqID}, State#state{datasets = Is0}};

handle_command({attribute, set, 
		{ReqID, Coordinator}, Dataset, 
		[Resource, Value]}, _Sender, State) ->
    io:format("1~n"),
    Hs0 = dict:update(Dataset, 
		      fun(#sniffle_obj{val=H0} = O) ->
			      io:format("2~n"),			     
			      H1 = statebox:modify(
				     {fun sniffle_dataset_state:attribute/3, 
				      [Resource, Value]}, H0),
			      io:format("3~n"),
			      H2 = statebox:expire(?STATEBOX_EXPIRE, H1),
			      io:format("4~n"),
			      sniffle_obj:update(H2, Coordinator, O)
		      end, State#state.datasets),
    io:format("5~n"),
    {reply, {ok, ReqID}, State#state{datasets = Hs0}};

handle_command({attribute, mset, 
		{ReqID, Coordinator}, Dataset, 
		Resources}, _Sender, State) ->
    Hs0 = dict:update(Dataset, 
		      fun(#sniffle_obj{val=H0} = O) ->
			      H1 = lists:foldr(
				     fun ({Resource, Value}, H) ->
					     statebox:modify(
					       {fun sniffle_dataset_state:attribute/3, 
						[Resource, Value]}, H)
				     end, H0, Resources),
			      H2 = statebox:expire(?STATEBOX_EXPIRE, H1),
			      sniffle_obj:update(H2, Coordinator, O)
		      end, State#state.datasets),
    {reply, {ok, ReqID}, State#state{datasets = Hs0}};

handle_command(Message, _Sender, State) ->
    ?PRINT({unhandled_command, Message}),
    {noreply, State}.

handle_handoff_command(?FOLD_REQ{foldfun=Fun, acc0=Acc0}, _Sender, State) ->
    Acc = dict:fold(Fun, Acc0, State#state.datasets),
    {reply, Acc, State}.

handoff_starting(_TargetNode, State) ->
    {true, State}.

handoff_cancelled(State) ->
    {ok, State}.

handoff_finished(_TargetNode, State) ->
    {ok, State}.

handle_handoff_data(Data, #state{dbref = DBRef} = State) ->
    {Dataset, HObject} = binary_to_term(Data),
    Is0 = dict:store(Dataset, HObject, State#state.datasets),
    eleveldb:put(DBRef, <<"#datasets">>, term_to_binary(dict:fetch_keys(Is0)), []),
    eleveldb:put(DBRef, Dataset, term_to_binary(HObject), []),
    {reply, ok, State#state{datasets = Is0}}.

encode_handoff_item(Dataset, Data) ->
    term_to_binary({Dataset, Data}).

is_empty(State) ->
    case dict:size(State#state.datasets) of
	0 ->
	    {true, State};
	_ ->
	    {true, State}
    end.

delete(#state{dbref = DBRef} = State) ->
    eleveldb:close(DBRef),
    eleveldb:destroy("datasets."++integer_to_list(State#state.partition)++".ldb",[]),
    {ok, DBRef1} = eleveldb:open("datasets."++integer_to_list(State#state.partition)++".ldb",
				 [{create_if_missing, true}]),

    {ok, State#state{datasets = dict:new(), dbref = DBRef1}}.

handle_coverage({list, ReqID}, _KeySpaces, _Sender, State) ->
    {reply, 
     {ok, ReqID, {State#state.partition,State#state.node}, dict:fetch_keys(State#state.datasets)},
     State};

handle_coverage(_Req, _KeySpaces, _Sender, State) ->
    {stop, not_implemented, State}.

handle_exit(_Pid, _Reason, State) ->
    {noreply, State}.

terminate(_Reason, #state{dbref=DBRef} = _State) ->
    eleveldb:close(DBRef),
    ok.

read_ranges(DBRef) ->
    case eleveldb:get(DBRef, <<"#datasets">>, []) of
	not_found -> 
	    {[], dict:new()};
	{ok, Bin} ->
	    Index = binary_to_term(Bin),
	    {Index,
	     lists:foldl(fun (Dataset, Datasets0) ->
				 {ok, IpBin} = eleveldb:get(DBRef, Dataset, []),
				 dict:store(Dataset, binary_to_term(IpBin), Datasets0)
			 end, dict:new(), Index)}
    end.
