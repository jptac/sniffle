-module(sniffle_dtrace).

-include("sniffle.hrl").

-export(
   [
    add/2
   ]
  ).

add(Name, Script) ->
    UUID = list_to_binary(uuid:to_string(uuid:uuid4())),
    do_write(UUID, create, [Name, Script]),
    {ok, UUID}.


%%%===================================================================
%%% Internal Functions
%%%===================================================================

%% -spec do_write(VM::fifo:uuid(), Op::atom()) -> not_found | ok.

%% do_write(VM, Op) ->
%%     case sniffle_entity_write_fsm:write({sniffle_vm_vnode, sniffle_vm}, VM, Op) of
%%         {ok, not_found} ->
%%             not_found;
%%         R ->
%%             R
%%     end.

-spec do_write(VM::fifo:uuid(), Op::atom(), Val::term()) -> not_found | ok.

do_write(VM, Op, Val) ->
    case sniffle_entity_write_fsm:write({sniffle_dtrace_vnode, sniffle_dtrace}, VM, Op, Val) of
        {ok, not_found} ->
            not_found;
        R ->
            R
    end.
