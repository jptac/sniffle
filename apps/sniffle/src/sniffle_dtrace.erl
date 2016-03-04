-module(sniffle_dtrace).

-include("sniffle.hrl").

-define(MASTER, sniffle_dtrace_vnode_master).
-define(VNODE, sniffle_dtrace_vnode).
-define(SERVICE, sniffle_dtrace).

-define(FM(Met, Mod, Fun, Args),
        folsom_metrics:histogram_timed_update(
          {sniffle, dtrace, Met},
          Mod, Fun, Args)).

-export(
   [
    get/1,
    add/2,
    set/2,
    set/3,
    list/0,
    list/2,
    list/3,
    delete/1,
    wipe/1,
    sync_repair/2,
    list_/0
   ]
  ).

-ignore_xref(
   [
    get/1,
    add/2,
    set/2,
    set/3,
    list/0,
    list/2,
    delete/1,
    sync_repair/2,
    list_/0,
    wipe/1
   ]
  ).

-export([
         name/2,
         uuid/2,
         script/2,
         set_metadata/2,
         set_config/2
        ]).

wipe(UUID) ->
    ?FM(wipe, sniffle_coverage, start, [?MASTER, ?SERVICE, {wipe, UUID}]).

sync_repair(UUID, Obj) ->
    do_write(UUID, sync_repair, Obj).

list_() ->
    {ok, Res} = ?FM(list_all, sniffle_coverage, raw,
                    [?MASTER, ?SERVICE, []]),
    Res1 = [R || {_, R} <- Res],
    {ok,  Res1}.

-spec add(Name::binary(),
          Script::string()) ->
                 {ok, UUID::fifo:dtrace_id()} | {error, timeout}.
add(Name, Script) ->
    UUID = fifo_utils:uuid(dtrace),
    do_write(UUID, create, [Name, Script]),
    {ok, UUID}.

-spec get(UUID::fifo:dtrace_id()) ->
                 not_found | {ok, DTrance::fifo:dtrace()} | {error, timeout}.
get(UUID) ->
    ?FM(get, sniffle_entity_read_fsm, start, [{?VNODE, ?SERVICE}, get, UUID]).

-spec delete(UUID::fifo:dtrace_id()) ->
                    not_found | {error, timeout} | ok.
delete(UUID) ->
    do_write(UUID, delete).

-spec list() ->
                  {ok, [UUID::fifo:dtrace_id()]} | {error, timeout}.
list() ->
    ?FM(list, sniffle_coverage, start, [?MASTER, ?SERVICE, list]).

list(Requirements, FoldFn, Acc0) ->
    ?FM(list_all, sniffle_coverage, list,
                    [?MASTER, ?SERVICE, Requirements, FoldFn, Acc0]).

%%--------------------------------------------------------------------
%% @doc Lists all vm's and fiters by a given matcher set.
%% @end
%%--------------------------------------------------------------------
-spec list([fifo:matcher()], boolean()) ->
                  {error, timeout} |
                  {ok, [{integer(), fifo:uuid() | fifo:dtrace()}]}.

list(Requirements, Full) ->
    {ok, Res} = ?FM(list_all, sniffle_coverage, list,
                    [?MASTER, ?SERVICE, Requirements]),
    Res1 = lists:sort(rankmatcher:apply_scales(Res)),
    Res2 = case Full of
               true ->
                   Res1;
               false ->
                   [{P, ft_dtrace:uuid(O)} || {P, O} <- Res1]
           end,
    {ok, Res2}.

-spec set(UUID::fifo:dtrace_id(),
          Attribute::fifo:keys(),
          Value::fifo:value()) ->
                 ok | {error, timeout}.
set(UUID, Attribute, Value) ->
    do_write(UUID, set, [{Attribute, Value}]).

-spec set(UUID::fifo:dtrace_id(),
          Attributes::fifo:attr_list()) ->
                 ok | {error, timeout}.
set(UUID, Attributes) ->
    do_write(UUID, set, Attributes).

?SET(name).
?SET(uuid).
?SET(script).
?SET(set_metadata).
?SET(set_config).

%%%===================================================================
%%% Internal Functions
%%%===================================================================

-spec do_write(VM::fifo:uuid(), Op::atom()) -> not_found | ok.
do_write(VM, Op) ->
    ?FM(Op, sniffle_entity_write_fsm, write, [{?VNODE, ?SERVICE}, VM, Op]).

-spec do_write(VM::fifo:uuid(), Op::atom(), Val::term()) -> not_found | ok.
do_write(VM, Op, Val) ->
    ?FM(Op, sniffle_entity_write_fsm, write, [{?VNODE, ?SERVICE}, VM, Op, Val]).
