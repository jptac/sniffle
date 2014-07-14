%%% @author Heinz Nikolaus Gies <heinz@licenser.net>
%%% @copyright (C) 2012, Heinz Nikolaus Gies
%%% @doc
%%%
%%% @end
%%% Created : 23 Aug 2012 by Heinz Nikolaus Gies <heinz@licenser.net>

-module(sniffle_iprange_state).

-include("sniffle.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(IS_IP, is_integer(V), V > 0, V < 16#FFFFFFFF).

-export([
         new/3, load/2, merge/2, to_json/1,
         name/1, name/3,
         uuid/1, uuid/3,
         network/1, network/3,
         netmask/1, netmask/3,
         gateway/1, gateway/3,
         metadata/1, set_metadata/4,
         tag/1, tag/3,
         vlan/1, vlan/3,
         free/1, used/1,
         release_ip/3, claim_ip/3,
         set/4,
         set/3,
         to_bin/1,
         parse_bin/1,
         getter/2
        ]).

-ignore_xref([
              name/1, name/3,
              uuid/1, uuid/3,
              network/1, network/3,
              netmask/1, netmask/3,
              gateway/1, gateway/3,
              metadata/1, set_metadata/4,
              tag/1, tag/3,
              vlan/1, vlan/3,
              free/1, used/1,
              release_ip/3, claim_ip/3,
              merge/2
             ]).

-ignore_xref([load/2, name/1, set/4, set/3, getter/2, uuid/1]).

uuid(H) ->
    riak_dt_lwwreg:value(H#?IPRANGE.uuid).

uuid({T, _ID}, V, H) when is_binary(V)  ->
    {ok, V1} = riak_dt_lwwreg:update({assign, V, T}, none, H#?IPRANGE.uuid),
    H#?IPRANGE{uuid = V1}.

name(H) ->
    riak_dt_lwwreg:value(H#?IPRANGE.name).

name({T, _ID}, V, H) when is_binary(V) ->
    {ok, V1} = riak_dt_lwwreg:update({assign, V, T}, none, H#?IPRANGE.name),
    H#?IPRANGE{name = V1}.

network(H) ->
    riak_dt_lwwreg:value(H#?IPRANGE.network).

network({T, _ID}, V, H) when ?IS_IP ->
    {ok, V1} = riak_dt_lwwreg:update({assign, V, T}, none, H#?IPRANGE.network),
    H#?IPRANGE{network = V1}.

netmask(H) ->
    riak_dt_lwwreg:value(H#?IPRANGE.netmask).

netmask({T, _ID}, V, H) when ?IS_IP ->
    {ok, V1} = riak_dt_lwwreg:update({assign, V, T}, none, H#?IPRANGE.netmask),
    H#?IPRANGE{netmask = V1}.

gateway(H) ->
    riak_dt_lwwreg:value(H#?IPRANGE.gateway).

gateway({T, _ID}, V, H) when ?IS_IP ->
    {ok, V1} = riak_dt_lwwreg:update({assign, V, T}, none, H#?IPRANGE.gateway),
    H#?IPRANGE{gateway = V1}.

tag(H) ->
    riak_dt_lwwreg:value(H#?IPRANGE.tag).

tag({T, _ID}, V, H) when is_binary(V) ->
    {ok, V1} = riak_dt_lwwreg:update({assign, V, T}, none, H#?IPRANGE.tag),
    H#?IPRANGE{tag = V1}.

vlan(H) ->
    riak_dt_lwwreg:value(H#?IPRANGE.vlan).

vlan({T, _ID}, V, H) when is_integer(V),
                          V >= 0, V < 4096 ->
    {ok, V1} = riak_dt_lwwreg:update({assign, V, T}, none, H#?IPRANGE.vlan),
    H#?IPRANGE{vlan = V1}.

free(H) ->
    riak_dt_orswot:value(H#?IPRANGE.free).

used(H) ->
    riak_dt_orswot:value(H#?IPRANGE.used).

getter(#sniffle_obj{val=S0}, Resource) ->
    jsxd:get(Resource, 0, statebox:value(S0)).

load(_, #?IPRANGE{} = I) ->
    I;

load({T, ID}, I) ->
    {ok, UUID} = jsxd:get(<<"uuid">>, I),
    {ok, Name} = jsxd:get(<<"name">>, I),
    {ok, Network} = jsxd:get(<<"network">>, I),
    {ok, Gateway} = jsxd:get(<<"gateway">>, I),
    {ok, Netmask} = jsxd:get(<<"netmask">>, I),
    {ok, First} = jsxd:get(<<"first">>, I),
    {ok, Last} = jsxd:get(<<"last">>, I),
    {ok, Current} = jsxd:get(<<"last">>, I),
    {ok, Tag} = jsxd:get(<<"tag">>, I),
    {ok, VLAN} = jsxd:get(<<"vlan">>, I),
    Returned = jsxd:get(<<"returned">>, [], I),
    Metadata = jsxd:get(<<"metadata">>, [], I),
    {ok, Free} = riak_dt_orswot:update({add_all, lists:seq(Current, Last)}, ID,
                                       riak_dt_orswot:new()),
    {ok, Used} = case First == Current of
                     true ->
                         {ok, riak_dt_orswot:new()};
                     false ->
                         riak_dt_orswot:update({add_all, lists:seq(First, Current - 1)}, ID,
                                         riak_dt_orswot:new())
                 end,
    UUID1 = ?NEW_LWW(UUID, T),
    Name1 = ?NEW_LWW(Name, T),
    Network1 = ?NEW_LWW(Network, T),
    Netmask1 = ?NEW_LWW(Netmask, T),
    Gateway1 = ?NEW_LWW(Gateway, T),
    Tag1 = ?NEW_LWW(Tag, T),
    Vlan1 = ?NEW_LWW(VLAN, T),

    {ok, Free1} = riak_dt_orswot:update({add_all, Returned}, ID, Free),
    {ok, Used1} = riak_dt_orswot:update({remove_all, Returned}, ID, Used),
    Metadata1 = fifo_map:from_orddict(Metadata, ID, T),

    #?IPRANGE{
        uuid     = UUID1,
        name     = Name1,

        network  = Network1,
        netmask  = Netmask1,
        gateway  = Gateway1,
        tag      = Tag1,
        vlan     = Vlan1,

        free     = Free1,
        used     = Used1,
        metadata = Metadata1
       }.

new({_T, ID}, S, E) when S < E ->
    {ok, Free} = riak_dt_orswot:update({add_all, lists:seq(S, E)}, ID,
                                       riak_dt_orswot:new()),
    #?IPRANGE{free=Free}.

claim_ip({_T, ID}, IP, I) when
      is_integer(IP) ->
    case riak_dt_orswot:update({remove, IP}, ID, I#?IPRANGE.free) of
        {error,{precondition,{not_present,_}}} ->
            {error, used};
        {ok, Free} ->
            {ok, Used} = riak_dt_orswot:update({add, IP}, ID, I#?IPRANGE.used),
            {ok, I#?IPRANGE{free=Free, used=Used}}
    end.

release_ip({_T, ID}, IP, I) when
      is_integer(IP) ->
    case riak_dt_orswot:update({remove, IP}, ID, I#?IPRANGE.used) of
        {error,{precondition,{not_present,_}}} ->
            I;
        {ok, Used} ->
            {ok, Free} = riak_dt_orswot:update({add, IP}, ID, I#?IPRANGE.free),
            {ok, I#?IPRANGE{free=Free, used=Used}}
    end.

set(_ID, Attribute, Value, D) ->
    statebox:modify({fun set/3, [Attribute, Value]}, D).

set(Resource, delete, Iprange) ->
    jsxd:delete(Resource, Iprange);

set(Resource, Value, Iprange) ->
    jsxd:set(Resource, Value, Iprange).

metadata(I) ->
    fifo_map:value(I#?IPRANGE.metadata).

set_metadata({T, ID}, P, Value, User) when is_binary(P) ->
    set_metadata({T, ID}, fifo_map:split_path(P), Value, User);

set_metadata({_T, ID}, Attribute, delete, I) ->
    {ok, M1} = fifo_map:remove(Attribute, ID, I#?IPRANGE.metadata),
    I#?IPRANGE{metadata = M1};

set_metadata({T, ID}, Attribute, Value, I) ->
    {ok, M1} = fifo_map:set(Attribute, Value, ID, T, I#?IPRANGE.metadata),
    I#?IPRANGE{metadata = M1}.

to_json(I) ->
    [
     {<<"free">>, free(I)},
     {<<"gateway">>, gateway(I)},
     {<<"metadata">>, metadata(I)},
     {<<"name">>, name(I)},
     {<<"netmask">>, netmask(I)},
     {<<"network">>, network(I)},
     {<<"tag">>, tag(I)},
     {<<"used">>, used(I)},
     {<<"uuid">>, uuid(I)},
     {<<"vlan">>, vlan(I)}
    ].

merge(#?IPRANGE{
          uuid     = UUID1,
          name     = Name1,

          network  = Network1,
          netmask  = Netmask1,
          gateway  = Gateway1,
          tag      = Tag1,
          vlan     = Vlan1,

          free     = Free1,
          used     = Used1,
          metadata = Metadata1
         },
      #?IPRANGE{
          uuid     = UUID2,
          name     = Name2,

          network  = Network2,
          netmask  = Netmask2,
          gateway  = Gateway2,
          tag      = Tag2,
          vlan     = Vlan2,

          free     = Free2,
          used     = Used2,
          metadata = Metadata2
         }) ->
    #?IPRANGE{
        uuid     = riak_dt_lwwreg:merge(UUID1, UUID2),
        name     = riak_dt_lwwreg:merge(Name1, Name2),

        network  = riak_dt_lwwreg:merge(Network1, Network2),
        netmask  = riak_dt_lwwreg:merge(Netmask1, Netmask2),
        gateway  = riak_dt_lwwreg:merge(Gateway1, Gateway2),
        tag      = riak_dt_lwwreg:merge(Tag1, Tag2),
        vlan     = riak_dt_lwwreg:merge(Vlan1, Vlan2),

        free     = riak_dt_orswot:merge(Free1, Free2),
        used     = riak_dt_orswot:merge(Used1, Used2),
        metadata = fifo_map:merge(Metadata1, Metadata2)
       }.

to_bin(IP) ->
    <<A, B, C, D>> = <<IP:32>>,
    list_to_binary(io_lib:format("~p.~p.~p.~p", [A, B, C, D])).

parse_bin(Bin) ->
    [A,B,C,D] = [ list_to_integer(binary_to_list(P)) || P <- re:split(Bin, "[.]")],
    <<IP:32>> = <<A:8, B:8, C:8, D:8>>,
    IP.

-ifdef(TESTE).

example_setup() ->
    R = new(),
    R1 = network(100, R),
    R2 = gateway(200, R1),
    R3 = netmask(0, R2),
    R4 = first(110, R3),
    R5 = current(110, R4),
    last(120, R5).

example_setup_test() ->
    R = example_setup(),
    ?assertEqual({ok, 100}, jsxd:get(<<"network">>, R)),
    ?assertEqual({ok, 200}, jsxd:get(<<"gateway">>, R)),
    ?assertEqual({ok, 0}, jsxd:get(<<"netmask">>, R)),
    ?assertEqual({ok, 110}, jsxd:get(<<"first">>, R)),
    ?assertEqual({ok, 110}, jsxd:get(<<"current">>, R)),
    ?assertEqual({ok, 120}, jsxd:get(<<"last">>, R)).

claim1_test() ->
    R = example_setup(),
    R1 = claim_ip(110, R),
    ?assertEqual({ok, 110}, jsxd:get(<<"first">>, R1)),
    ?assertEqual({ok, 111}, jsxd:get(<<"current">>, R1)),
    R2 = claim_ip(111, R1),
    ?assertEqual({ok, 112}, jsxd:get(<<"current">>, R2)).

claim2_test() ->
    R = example_setup(),
    R1 = jsxd:set(<<"free">>, [123, 124], R),
    R2 = jsxd:set(<<"current">>, 125, R1),
    R3 = claim_ip(123, R2),
    ?assertEqual({ok, 125}, jsxd:get(<<"current">>, R3)),
    ?assertEqual({ok, [124]}, jsxd:get(<<"free">>, R3)),
    R4 = claim_ip(125, R2),
    ?assertEqual({ok, 126}, jsxd:get(<<"current">>, R4)),
    ?assertEqual({ok, [123, 124]}, jsxd:get(<<"free">>, R4)),
    R5 = claim_ip(124, R2),
    ?assertEqual({ok, 125}, jsxd:get(<<"current">>, R5)),
    ?assertEqual({ok, [123]}, jsxd:get(<<"free">>, R5)).

return_test() ->
    R = example_setup(),
    R1 = jsxd:thread([{set, <<"free">>, []},
                      {set, <<"current">>, 115}],
                     R),
    R2 = release_ip(114, R1),
    ?assertEqual({ok, 114}, jsxd:get(<<"current">>, R2)),
    ?assertEqual({ok, []}, jsxd:get(<<"free">>, R2)),
    R3 = release_ip(113, R1),
    ?assertEqual({ok, 115}, jsxd:get(<<"current">>, R3)),
    ?assertEqual({ok, [113]}, jsxd:get(<<"free">>, R3)),
    R4 = release_ip(115, R1),
    ?assertEqual({ok, 115}, jsxd:get(<<"current">>, R4)),
    ?assertEqual({ok, []}, jsxd:get(<<"free">>, R4)).

-endif.
