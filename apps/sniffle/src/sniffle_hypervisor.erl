-module(sniffle_hypervisor).
-include("sniffle.hrl").
%%-include_lib("riak_core/include/riak_core_vnode.hrl").

-export(
   [
    register/3,
    unregister/1,
    get/1,
    list/0,
    list/1,
    set/3,
    set/2,
    status/0
   ]).

-spec register(Hypervisor::fifo:hypervisor_id(),
               IP::inet:ip_address() | inet:hostname(),
               Port::inet:port_number()) ->
                      duplicate | {error, timeout} | ok.
register(Hypervisor, IP, Port) ->
    case sniffle_hypervisor:get(Hypervisor) of
        not_found ->
            do_write(Hypervisor, register, [IP, Port]);
        {ok, _UserObj} ->
            duplicate
    end.

-spec unregister(Hypervisor::fifo:hypervisor_id()) ->
                    not_found | {error, timeout} | ok.
unregister(Hypervisor) ->
    do_write(Hypervisor, unregister).

-spec get(Hypervisor::fifo:hypervisor_id()) ->
                 not_found | {ok, HV::fifo:object()} | {error, timeout}.
get(Hypervisor) ->
    sniffle_entity_read_fsm:start(
      {sniffle_hypervisor_vnode, sniffle_hypervisor},
      get, Hypervisor).

-spec status() -> {error, timeout} |
                  {ok, {Resources::fifo:object(),
                        Warnings::fifo:object()}}.
status() ->
    {ok, Stat} = sniffle_coverage:start(
                   sniffle_hypervisor_vnode_master, sniffle_hypervisor,
                   status),
    Warnings = case riak_core_status:transfers() of
                   {[], []} ->
                       [];
                   {[], L} ->
                       jsxd:from_list(
                         [[{<<"category">>, <<"sniffle">>},
                           {<<"element">>, <<"handoff">>},
                           {<<"type">>, <<"info">>},
                           {<<"message">>, bin_fmt("~b handofs pending.", [length(L)])}]]);
                   {S, []} ->
                       server_errors(S);
                   {S, L} ->
                       [jsxd:from_list(
                          [{<<"category">>, <<"sniffle">>},
                           {<<"element">>, <<"handoff">>},
                           {<<"type">>, <<"info">>},
                           {<<"message">>, bin_fmt("~b handofs pending.", [length(L)])}]) |
                        server_errors(S)]
               end,
    Stat1  = lists:foldl(fun ({R, W}, {R0, W0}) ->
                                 R1 = jsxd:merge(fun(_K, V1, V2) when is_list(V1), is_list(V2) ->
                                                         V1 ++ V2;
                                                    (_K, V1, V2) when is_number(V1), is_number(V2) ->
                                                         V1 + V2
                                                 end, R, R0),
                                 {R1, W ++ W0}
                         end, {[],Warnings}, Stat),
    {ok, Stat1}.

-spec list() ->
                  {ok, [IPR::fifo:hypervisor_id()]} | {error, timeout}.
list() ->
    sniffle_coverage:start(
      sniffle_hypervisor_vnode_master, sniffle_hypervisor,
      list).

-spec list(Reqs::[fifo:matcher()]) ->
                  {ok, [IPR::fifo:hypervisor_id()]} | {error, timeout}.
list(Requirements) ->
    {ok, Res} = sniffle_coverage:start(
                  sniffle_hypervisor_vnode_master, sniffle_hypervisor,
                  {list, Requirements}),
    Res1 = rankmatcher:apply_scales(Res),
    {ok,  lists:sort(Res1)}.

-spec set(Hypervisor::fifo:hypervisor_id(),
          Attribute::fifo:keys(),
          Value::fifo:value()) ->
                 ok | {error, timeout}.
set(Hypervisor, Attribute, Value) ->
    set(Hypervisor, [{Attribute, Value}]).

-spec set(Hypervisor::fifo:hypervisor_id(),
          Attributes::fifo:attr_list()) ->
                 ok | {error, timeout}.
set(Hypervisor, Attributes) ->
    do_write(Hypervisor, set, Attributes).

%%%===================================================================
%%% Internal Functions
%%%===================================================================

do_write(User, Op) ->
    sniffle_entity_write_fsm:write({sniffle_hypervisor_vnode, sniffle_hypervisor}, User, Op).

do_write(User, Op, Val) ->
    sniffle_entity_write_fsm:write({sniffle_hypervisor_vnode, sniffle_hypervisor}, User, Op, Val).

bin_fmt(F, L) ->
    list_to_binary(io_lib:format(F, L)).

server_errors(Servers) ->
    lists:map(fun (Server) ->
                      jsxd:from_list(
                        [{<<"category">>, <<"sniffle">>},
                         {<<"element">>, list_to_binary(atom_to_list(Server))},
                         {<<"type">>, <<"critical">>},
                         {<<"message">>, bin_fmt("Sniffle server ~s down.", [Server])}])
              end, Servers).
