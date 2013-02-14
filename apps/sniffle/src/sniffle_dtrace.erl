-module(sniffle_dtrace).

-include("sniffle.hrl").

-export(
   [
    get/1,
    add/2,
    set/2,
    set/3,
    list/0,
    list/1,
    delete/1
   ]
  ).

-ignore_xref(
   [
    get/1,
    add/2,
    set/2,
    set/3,
    list/0,
    list/1,
    delete/1
   ]
  ).

add(Name, Script) ->
    UUID = list_to_binary(uuid:to_string(uuid:uuid4())),
    do_write(UUID, create, [Name, Script]),
    {ok, UUID}.

get(UUID) ->
    sniffle_entity_read_fsm:start(
      {sniffle_dtrace_vnode, sniffle_dtrace},
      get, UUID
     ).

delete(UUID) ->
    do_write(UUID, delete).

list() ->
    sniffle_entity_coverage_fsm:start(
      {sniffle_dtrace_vnode, sniffle_dtrace},
      list
     ).

list(Requirements) ->
    sniffle_entity_coverage_fsm:start(
      {sniffle_dtrace_vnode, sniffle_dtrace},
      list, Requirements
     ).

set(Script, Attribute, Value) ->
    do_write(Script, set, [{Attribute, Value}]).

set(Script, Attributes) ->
    do_write(Script, set, Attributes).

%%%===================================================================
%%% Internal Functions
%%%===================================================================

-spec do_write(VM::fifo:uuid(), Op::atom()) -> not_found | ok.

do_write(VM, Op) ->
    case sniffle_entity_write_fsm:write({sniffle_dtrace_vnode, sniffle_dtrace}, VM, Op) of
        {ok, not_found} ->
            not_found;
        R ->
            R
    end.

-spec do_write(VM::fifo:uuid(), Op::atom(), Val::term()) -> not_found | ok.

do_write(VM, Op, Val) ->
    case sniffle_entity_write_fsm:write({sniffle_dtrace_vnode, sniffle_dtrace}, VM, Op, Val) of
        {ok, not_found} ->
            not_found;
        R ->
            R
    end.
