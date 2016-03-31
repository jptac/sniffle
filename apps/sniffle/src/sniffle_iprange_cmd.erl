-module(sniffle_iprange_cmd).
-define(BUCKET, <<"iprange">>).
-include("sniffle.hrl").

-export([
         repair/4,
         sync_repair/4,
         get/3,
         create/4,
         delete/3,
         claim_ip/4,
         release_ip/4
        ]).

-ignore_xref([
              release_ip/4,
              create/4,
              delete/3,
              get/3,
              claim_ip/4,
              repair/4,
              release_ip/4,
              start_vnode/1,
              handle_info/2,
              sync_repair/4
             ]).

-export([
         name/4,
         uuid/4,
         network/4,
         netmask/4,
         gateway/4,
         set_metadata/4,
         tag/4,
         vlan/4
        ]).

-ignore_xref([
              name/4,
              uuid/4,
              network/4,
              netmask/4,
              gateway/4,
              set_metadata/4,
              tag/4,
              vlan/4
             ]).

%%%===================================================================
%%% API
%%%===================================================================

repair(Preflist, UUID, VClock, Obj) ->
    riak_core_vnode_master:command(Preflist,
                                   ?REQ({repair, UUID, VClock, Obj}),
                                   ignore,
                                   ?MASTER).

%%%===================================================================
%%% API - reads
%%%===================================================================

get(Preflist, ReqID, UUID) ->
    ?REQUEST(Preflist, ReqID, {get, UUID}).

%%%===================================================================
%%% API - writes
%%%===================================================================

create_fn({_, Coordinator} = ReqID, not_found,
         [UUID, Iprange, Network, Gateway, Netmask, First, Last, Tag, Vlan]) ->
    I0 = ft_iprange:new(ReqID, First, Last),
    I1 = lists:foldl(
           fun ({F, A}, IPR) ->
                   erlang:apply(F, [ReqID | A] ++ [IPR])
           end, I0, [{fun ft_iprange:uuid/3, [UUID]},
                     {fun ft_iprange:name/3, [Iprange]},
                     {fun ft_iprange:network/3, [Network]},
                     {fun ft_iprange:gateway/3, [Gateway]},
                     {fun ft_iprange:netmask/3, [Netmask]},
                     {fun ft_iprange:tag/3, [Tag]},
                     {fun ft_iprange:vlan/3, [Vlan]}]),
    {write, ok, ft_obj:new(I1, Coordinator)};

create_fn({_, Coordinator} = ReqID, {ok, O},
          [UUID, Iprange, Network, Gateway, Netmask, First, Last, Tag, Vlan]) ->
    I0 = ft_obj:val(O),
    I1 = lists:foldl(
           fun ({F, A}, IPR) ->
                   erlang:apply(F, [ReqID | A] ++ [IPR])
           end, I0, [{fun ft_iprange:uuid/3, [UUID]},
                     {fun ft_iprange:name/3, [Iprange]},
                     {fun ft_iprange:network/3, [Network]},
                     {fun ft_iprange:gateway/3, [Gateway]},
                     {fun ft_iprange:netmask/3, [Netmask]},
                     {fun ft_iprange:tag/3, [Tag]},
                     {fun ft_iprange:vlan/3, [Vlan]}]),
    {write, ok, ft_obj:update(I1, Coordinator, O)}.

create(Preflist, ReqID, UUID, Args) ->
    ?REQUEST(Preflist, ReqID, {apply, UUID, fun create_fn/3, [UUID | Args]}).

sync_repair(Preflist, ReqID, UUID, Obj) ->
    ?REQUEST(Preflist, ReqID, {sync_repair, UUID, Obj}).

delete(Preflist, ReqID, UUID) ->
    ?REQUEST(Preflist, ReqID, {delete, UUID}).

claim_ip_fn({ReqID, Coordinator} = ID, {ok, O}, [IP]) ->
    H0 = ft_obj:val(O),
    H1 = ft_iprange:load(ID, H0),
    case ft_iprange:claim_ip(ID, IP, H1) of
        {ok, H2} ->
            Obj = ft_obj:update(H2, Coordinator, O),
            {write, {ok,
                     {ft_iprange:tag(H2),
                      IP,
                      ft_iprange:netmask(H2),
                      ft_iprange:gateway(H2),
                      ft_iprange:vlan(H2)}
                    }, Obj};
        _ ->
            {reply, {error, ReqID, duplicate}}
    end;

claim_ip_fn(_, _, _) ->
    {reply, {ok, not_found}}.

claim_ip(Preflist, ReqID, UUID, IP) ->
    ?REQUEST(Preflist, ReqID, {apply, UUID, fun claim_ip_fn/3, [IP]}).

release_ip_fn({_, Coordinator} = ID, {ok, O}, [IP]) ->
    H0 = ft_obj:val(O),
    H1 = ft_iprange:load(ID, H0),
    {ok, H2} = ft_iprange:release_ip(ID, IP, H1),
    Obj =  ft_obj:update(H2, Coordinator, O),
    {write, ok, Obj};

release_ip_fn(_, _, _) ->
    {reply, {ok, not_found}}.

release_ip(Preflist, ReqID, UUID, IP) ->
    ?REQUEST(Preflist, ReqID, {apply, UUID, fun release_ip_fn/3, [IP]}).


?VSET(name).
?VSET(uuid).
?VSET(network).
?VSET(netmask).
?VSET(gateway).
?VSET(set_metadata).
?VSET(tag).
?VSET(vlan).
