-module(sniffle_dtrace).

-include("sniffle.hrl").

-define(MASTER, sniffle_dtrace_vnode_master).
-define(VNODE, sniffle_dtrace_vnode).
-define(SERVICE, sniffle_dtrace).

-export(
   [
    get/1,
    get_/1,
    add/2,
    set/2,
    set/3,
    list/0,
    list/2,
    delete/1,
    wipe/1,
    sync_repair/2,
    list_/0
   ]
  ).

-ignore_xref(
   [
    get/1,
    get_/1,
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

wipe(UUID) ->
    sniffle_coverage:start(?MASTER, ?SERVICE, {wipe, UUID}).

sync_repair(UUID, Obj) ->
    do_write(UUID, sync_repair, Obj).

list_() ->
    {ok, Res} = sniffle_full_coverage:start(
                  ?MASTER, ?SERVICE, {list, [], true, true}),
    Res1 = [R || {_, R} <- Res],
    {ok,  Res1}.

-spec add(Name::binary(),
          Script::string()) ->
                 {ok, UUID::fifo:dtrace_id()} | {error, timeout}.
add(Name, Script) ->
    UUID = list_to_binary(uuid:to_string(uuid:uuid4())),
    do_write(UUID, create, [Name, Script]),
    {ok, UUID}.

-spec get(UUID::fifo:dtrace_id()) ->
                 not_found | {ok, DTrance::fifo:object()} | {error, timeout}.
get(UUID) ->
    case get_(UUID) of
        {ok, O} ->
            {ok, ft_dtrace:to_json(O)};
        E ->
            E
    end.

-spec get_(UUID::fifo:dtrace_id()) ->
                 not_found | {ok, DTrance::fifo:object()} | {error, timeout}.
get_(UUID) ->
    sniffle_entity_read_fsm:start({?VNODE, ?SERVICE},get, UUID).

-spec delete(UUID::fifo:dtrace_id()) ->
                    not_found | {error, timeout} | ok.
delete(UUID) ->
    do_write(UUID, delete).

-spec list() ->
                  {ok, [UUID::fifo:dtrace_id()]} | {error, timeout}.
list() ->
    sniffle_coverage:start(?MASTER, ?SERVICE,list).

%%--------------------------------------------------------------------
%% @doc Lists all vm's and fiters by a given matcher set.
%% @end
%%--------------------------------------------------------------------
-spec list([fifo:matcher()], boolean()) -> {error, timeout} | {ok, [fifo:uuid()]}.

list(Requirements, true) ->
    {ok, Res} = sniffle_full_coverage:start(
                  ?MASTER, ?SERVICE, {list, Requirements, true}),
    Res1 = rankmatcher:apply_scales(Res),
    {ok,  lists:sort(Res1)};

list(Requirements, false) ->
    {ok, Res} = sniffle_coverage:start(?MASTER, ?SERVICE, {list, Requirements}),
    Res1 = rankmatcher:apply_scales(Res),
    {ok,  lists:sort(Res1)}.


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

%%%===================================================================
%%% Internal Functions
%%%===================================================================

-spec do_write(VM::fifo:uuid(), Op::atom()) -> not_found | ok.
do_write(VM, Op) ->
    sniffle_entity_write_fsm:write({?VNODE, ?SERVICE}, VM, Op).

-spec do_write(VM::fifo:uuid(), Op::atom(), Val::term()) -> not_found | ok.
do_write(VM, Op, Val) ->
    sniffle_entity_write_fsm:write({?VNODE, ?SERVICE}, VM, Op, Val).
