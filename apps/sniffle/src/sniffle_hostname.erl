
-module(sniffle_hostname).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-include("sniffle.hrl").

-define(MASTER, sniffle_hostname_vnode_master).
-define(VNODE, sniffle_hostname_vnode).
-define(SERVICE, sniffle_hostname).
-define(S, ft_hostname).
-define(FM(Met, Mod, Fun, Args),
        folsom_metrics:histogram_timed_update(
          {sniffle, hostname, Met},
          Mod, Fun, Args)).

-export([
         add/3,
         remove/3,
         get/2, get/1,
         sync_repair/2
        ]).


%%--------------------------------------------------------------------
%% @doc Tries to delete a HOSTNAME, either unregistering it if no
%%   Hypervisor was assigned or triggering the delete on hypervisor
%%   site.
%% @end
%%--------------------------------------------------------------------


-spec delete(Hostname::binary(), Org::fifo:uuid()) ->
                    {error, timeout} | not_found | ok.
delete(Hostname, Org) ->
    do_write(to_key(Hostname, Org), delete).


remove(Hostname, Org, Element) ->
    Key = to_key(Hostname, Org),
    R = do_write(Key, remove_a, Element),
    {ok, H} = get(Hostname, Org),
    case ft_hostname:empty(H) of
        true ->
            delete(Hostname, Org);
        false ->
            R
    end.

add(Hostname, Org, Element) ->
    Key = to_key(Hostname, Org),
    do_write(Key, add_a, Element).


-spec get(Hostname::binary(), Org::fifo:uuid()) ->
                 not_found | {error, timeout} | {ok, ft_hostname:hostname()}.
get(Hostname, Org) ->
    Key = to_key(Hostname, Org),
    sniffle_hostname:get(Key).

get(Key) ->
    ?FM(get, sniffle_entity_read_fsm, start, [{?VNODE, ?SERVICE}, get, Key]).

-spec sync_repair(fifo:hostname_id(), ft_obj:obj()) -> ok.
sync_repair(UUID, Obj) ->
    do_write(UUID, sync_repair, Obj).

%%%===================================================================
%%% Internal Functions
%%%===================================================================

-spec do_write(HOSTNAME::fifo:uuid(), Op::atom()) -> fifo:write_fsm_reply().

do_write(HOSTNAME, Op) ->
    ?FM(Op, sniffle_entity_write_fsm, write,
        [{?VNODE, ?SERVICE}, HOSTNAME, Op]).

-spec do_write(HOSTNAME::fifo:uuid(), Op::atom(), Val::term()) ->
                      fifo:write_fsm_reply().

do_write(HOSTNAME, Op, Val) ->
    ?FM(Op, sniffle_entity_write_fsm, write,
        [{?VNODE, ?SERVICE}, HOSTNAME, Op, Val]).


to_key(Hostname, Org) ->
    term_to_binary({Hostname, Org}).

