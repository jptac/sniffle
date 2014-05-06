-module(sniffle_grouping_state).

-include("sniffle.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([
         uuid/3,
         uuid/1,
         getter/2,
         load/2,
         merge/2,
         name/1,
         name/3,
         new/1,
         to_json/1,
         type/1,
         type/3,
         add_element/3,
         remove_element/3,
         add_grouping/3,
         remove_grouping/3,
         elements/1,
         groupings/1,
         set_metadata/4
        ]).

-ignore_xref([
              uuid/1,
              uuid/3,
              getter/2,
              load/2,
              merge/2,
              name/1,
              name/3,
              new/1,
              to_json/1,
              type/1,
              type/3,
              add_element/3,
              remove_element/3,
              add_grouping/3,
              remove_grouping/3,
              elements/1,
              groupings/1,
              set_metadata/4
             ]).

uuid(H) ->
    riak_dt_lwwreg:value(H#?GROUPING.uuid).

uuid({T, _ID}, V, H) ->
    {ok, V1} = riak_dt_lwwreg:update({assign, V, T}, none, H#?GROUPING.uuid),
    H#?GROUPING{uuid = V1}.

name(H) ->
    riak_dt_lwwreg:value(H#?GROUPING.name).

name({T, _ID}, V, H) ->
    {ok, V1} = riak_dt_lwwreg:update({assign, V, T}, none, H#?GROUPING.name),
    H#?GROUPING{name = V1}.

type(H) ->
    riak_dt_lwwreg:value(H#?GROUPING.type).

type({T, _ID}, V, H) ->
    {ok, V1} = riak_dt_lwwreg:update({assign, V, T}, none, H#?GROUPING.type),
    H#?GROUPING{type = V1}.

elements(H) ->
    riak_dt_orswot:value(H#?GROUPING.elements).

add_element({_T, ID}, V, H) ->
    {ok, O1} = riak_dt_orswot:update({add, V}, ID, H#?GROUPING.elements),
    H#?GROUPING{elements = O1}.

remove_element({_T, ID}, V, H) ->
    {ok, O1} = riak_dt_orswot:update({remove, V}, ID, H#?GROUPING.elements),
    H#?GROUPING{elements = O1}.

groupings(H) ->
    riak_dt_orswot:value(H#?GROUPING.groupings).

add_grouping({_T, ID}, V, H) ->
    {ok, O1} = riak_dt_orswot:update({add, V}, ID, H#?GROUPING.groupings),
    H#?GROUPING{groupings = O1}.

remove_grouping({_T, ID}, V, H) ->
    {ok, O1} = riak_dt_orswot:update({remove, V}, ID, H#?GROUPING.groupings),
    H#?GROUPING{groupings = O1}.


getter(#sniffle_obj{val=S0}, <<"name">>) ->
    name(S0);
getter(#sniffle_obj{val=S0}, <<"type">>) ->
    type(S0).

load(_, #?GROUPING{} = G) ->
    G.

new({T, _ID}) ->
    {ok, Name} = ?NEW_LWW(<<>>, T),
    {ok, Type} = ?NEW_LWW(none, T),
    #?GROUPING{
        name = Name,
        type = Type
       }.

set_metadata({T, ID}, P, Value, User) when is_binary(P) ->
    set_metadata({T, ID}, fifo_map:split_path(P), Value, User);

set_metadata({_T, ID}, Attribute, delete, G) ->
    {ok, M1} = fifo_map:remove(Attribute, ID, G#?GROUPING.metadata),
    G#?GROUPING{metadata = M1};

set_metadata({T, ID}, Attribute, Value, G) ->
    {ok, M1} = fifo_map:set(Attribute, Value, ID, T, G#?GROUPING.metadata),
    G#?GROUPING{metadata = M1}.

to_json(#?GROUPING{
            elements = Elements,
            groupings = Groupings,
            metadata = Metadata,
            name = Name,
            type = Type,
            uuid = UUID
           }) ->
    [
     {<<"elements">>, riak_dt_orswot:value(Elements)},
     {<<"groupings">>, riak_dt_orswot:value(Groupings)},
     {<<"metadata">>, fifo_map:value(Metadata)},
     {<<"name">>, riak_dt_lwwreg:value(Name)},
     {<<"type">>, list_to_binary(atom_to_list(riak_dt_lwwreg:value(Type)))},
     {<<"uuid">>, riak_dt_lwwreg:value(UUID)}
    ].

merge(A, _) ->
    A.
