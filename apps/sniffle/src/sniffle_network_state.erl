%%% @author Heinz Nikolaus Gies <heinz@licenser.net>
%%% @copyright (C) 2012, Heinz Nikolaus Gies
%%% @doc
%%%
%%% @end
%%% Created : 23 Aug 2012 by Heinz Nikolaus Gies <heinz@licenser.net>

-module(sniffle_network_state).

-include("sniffle.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([
         new/1,
         load/2,
         to_json/1,
         name/1, name/3,
         uuid/1, uuid/3,
         add_iprange/3, remove_iprange/3, ipranges/1,
         metadata/1, set_metadata/4,
         getter/2,
         merge/2
        ]).

-ignore_xref([load/2, to_json/1, getter/2, merge/2]).

-ignore_xref([
              add_iprange/3, remove_iprange/3, ipranges/1,
              name/1, name/3,
              uuid/1, uuid/3,
              metadata/1, set_metadata/4
             ]).

to_json(N) ->
    [
     {<<"ipranges">>, ipranges(N)},
     {<<"metadata">>, metadata(N)},
     {<<"name">>, name(N)},
     {<<"uuid">>, uuid(N)}
    ].

uuid(H) ->
    riak_dt_lwwreg:value(H#?NETWORK.uuid).

uuid({T, _ID}, V, H) ->
    {ok, V1} = riak_dt_lwwreg:update({assign, V, T}, none, H#?NETWORK.uuid),
    H#?NETWORK{uuid = V1}.

name(H) ->
    riak_dt_lwwreg:value(H#?NETWORK.name).

name({T, _ID}, V, H) ->
    {ok, V1} = riak_dt_lwwreg:update({assign, V, T}, none, H#?NETWORK.name),
    H#?NETWORK{name = V1}.

metadata(H) ->
    fifo_map:value(H#?NETWORK.metadata).

ipranges(H) ->
    riak_dt_orswot:value(H#?NETWORK.ipranges).


add_iprange({_T, ID}, V, H) ->
    {ok, O1} = riak_dt_orswot:update({add, V}, ID, H#?NETWORK.ipranges),
    H#?NETWORK{ipranges = O1}.

remove_iprange({_T, ID}, V, H) ->
    case riak_dt_orswot:update({remove, V}, ID, H#?NETWORK.ipranges) of
        {error,{precondition,{not_present,_}}} ->
            H;
        {ok, O1} ->
            H#?NETWORK{ipranges = O1}
    end.


getter(#sniffle_obj{val=S0}, <<"name">>) ->
    name(S0);
getter(#sniffle_obj{val=S0}, <<"uuid">>) ->
    uuid(S0).

load(_, #?NETWORK{} = N) ->
    N;

load({T, ID},  Sb) ->
    N = statebox:value(Sb),
    {ok, UUID}  =jsxd:get([<<"uuid">>], N),
    {ok, Name}  =jsxd:get([<<"name">>], N),
    {ok, IPRanges}  =jsxd:get([<<"ipranges">>], [], N),
    {ok, Metadata}  =jsxd:get([<<"metadata">>], [], N),
    UUID1 = ?NEW_LWW(UUID, T),
    Name1 = ?NEW_LWW(Name, T),
    IPRanges1 = riak_dt_orswot:update(
                  {add_all, IPRanges}, ID,
                  riak_dt_orswot:new()),
    Metadata1 = fifo_map:from_orddict(Metadata, ID, T),
    #network_0_1_0{
       uuid = UUID1,
       name = Name1,
       ipranges = IPRanges1,
       metadata = Metadata1
      }.

new(_) ->
    #?NETWORK{}.

set_metadata({T, ID}, P, Value, User) when is_binary(P) ->
    set_metadata({T, ID}, fifo_map:split_path(P), Value, User);

set_metadata({_T, ID}, Attribute, delete, G) ->
    {ok, M1} = fifo_map:remove(Attribute, ID, G#?NETWORK.metadata),
    G#?NETWORK{metadata = M1};

set_metadata({T, ID}, Attribute, Value, G) ->
    {ok, M1} = fifo_map:set(Attribute, Value, ID, T, G#?NETWORK.metadata),
    G#?NETWORK{metadata = M1}.

merge(#?NETWORK{
          ipranges = IPRanges1,
          metadata = Metadata1,
          name = Name1,
          uuid = UUID1
         },
      #?NETWORK{
          ipranges = IPRanges2,
          metadata = Metadata2,
          name = Name2,
          uuid = UUID2
         }) ->
    #?NETWORK{
        ipranges = riak_dt_orswot:merge(IPRanges1, IPRanges2),
        metadata = fifo_map:merge(Metadata1, Metadata2),
        name = riak_dt_lwwreg:merge(Name1, Name2),
        uuid = riak_dt_lwwreg:merge(UUID1, UUID2)
       }.
