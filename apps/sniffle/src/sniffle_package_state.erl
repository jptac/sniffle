%%% @author Heinz Nikolaus Gies <heinz@licenser.net>
%%% @copyright (C) 2012, Heinz Nikolaus Gies
%%% @doc
%%%
%%% @end
%%% Created : 23 Aug 2012 by Heinz Nikolaus Gies <heinz@licenser.net>

-module(sniffle_package_state).

-include("sniffle.hrl").

-define(G(N, F),
        getter(#sniffle_obj{val=S0}, N) ->
               F(S0)).

-define(G(E),
        E(H) -> riak_dt_lwwreg:value(H#?PACKAGE.E)).

-define(S(E),
        E({T, _ID}, V, H) ->
               {ok, V1} = riak_dt_lwwreg:update({assign, V, T}, none, H#?PACKAGE.E),
               H#?PACKAGE{E = V1}).

-define(S(N, F),
        set(TID, N, Value, D) ->
               F(TID, Value, D)).

-export([
         to_json/1,
         load/2,
         new/1,
         merge/2,
         getter/2,
         set/4,
         set_metadata/4,
         blocksize/1, blocksize/3,
         compression/1, compression/3,
         cpu_cap/1, cpu_cap/3,
         cpu_shares/1, cpu_shares/3,
         max_swap/1, max_swap/3,
         name/1, name/3,
         quota/1, quota/3,
         ram/1, ram/3,
         uuid/1, uuid/3,
         zfs_io_priority/1, zfs_io_priority/3
        ]).

-ignore_xref([
              set_metadata/4,
              blocksize/1, blocksize/3,
              compression/1, compression/3,
              cpu_cap/1, cpu_cap/3,
              cpu_shares/1, cpu_shares/3,
              max_swap/1, max_swap/3,
              name/1, name/3,
              quota/1, quota/3,
              ram/1, ram/3,
              uuid/1, uuid/3,
              zfs_io_priority/1, zfs_io_priority/3
             ]).

-ignore_xref([merge/2, load/2, name/1, set/4, set/3, getter/2, uuid/1]).

to_json(P) ->
    J = [
         {<<"cpu_cap">>, cpu_cap(P)},
         {<<"cpu_shares">>, cpu_shares(P)},
         {<<"max_swap">>, max_swap(P)},
         {<<"metadata">>, metadata(P)},
         {<<"name">>, name(P)},
         {<<"quota">>, quota(P)},
         {<<"ram">>, ram(P)},
         {<<"uuid">>, uuid(P)}
        ],
    J1 = case blocksize(P) of
             undefined ->
                 J;
             BS ->
                 jsxd:set([<<"blocksize">>], BS, J)
         end,
    J2 = case compression(P) of
             undefined ->
                 J1;
             C ->
                 jsxd:set([<<"compression">>], C, J1)
         end,
    case zfs_io_priority(P) of
        undefined ->
            J2;
        Prio ->
            jsxd:set([<<"zfs_io_priority">>], Prio, J2)
    end.

?G(<<"uuid">>, uuid);
?G(<<"name">>, name);
?G(<<"blocksize">>, blocksize);
?G(<<"compression">>, compression);
?G(<<"cpu_cap">>, cpu_cap);
?G(<<"cpu_shares">>, cpu_shares);
?G(<<"max_swap">>, max_swap);
?G(<<"quota">>, quota);
?G(<<"ram">>, ram);
?G(<<"zfs_io_priority">>, zfs_io_priority).

?G(uuid).
?G(name).
?G(blocksize).
?G(compression).
?G(cpu_cap).
?G(cpu_shares).
?G(max_swap).
?G(quota).
?G(ram).
?G(zfs_io_priority).

?S(uuid).
?S(name).
?S(blocksize).
?S(compression).
?S(cpu_cap).
?S(cpu_shares).
?S(max_swap).
?S(quota).
?S(ram).
?S(zfs_io_priority).

metadata(H) ->
    fifo_map:value(H#?PACKAGE.metadata).

load(_, #?PACKAGE{} = P) ->
    P;

load({T, ID}, D) ->
    {ok, UUID} = jsxd:get(<<"uuid">>, D),
    {ok, Name} = jsxd:get(<<"name">>, D),
    {ok, BlockSize} = jsxd:get(<<"blocksize">>, undefined, D),
    {ok, Compression} = jsxd:get(<<"compression">>, undefined, D),
    {ok, CpuCap} = jsxd:get(<<"cpu_cap">>, D),
    {ok, CpuShares} = jsxd:get(<<"cpu_shares">>, D),
    {ok, MaxSwap} = jsxd:get(<<"max_swap">>, undefined, D),
    {ok, Quota} = jsxd:get(<<"quota">>, D),
    {ok, RAM} = jsxd:get(<<"ram">>, D),
    {ok, ZFSIOPriority} = jsxd:get(<<"zfs_io_priority">>, undefined, D),
    {ok, Metadata} = jsxd:get(<<"metadata">>, [], D),

    {ok, UUID1} = ?NEW_LWW(T, UUID),
    {ok, Name1} = ?NEW_LWW(T, Name),
    {ok, BlockSize1} = ?NEW_LWW(T, BlockSize),
    {ok, Compression1} = ?NEW_LWW(T, Compression),
    {ok, CpuCap1} = ?NEW_LWW(T, CpuCap),
    {ok, CpuShares1} = ?NEW_LWW(T, CpuShares),
    {ok, MaxSwap1} = ?NEW_LWW(T, MaxSwap),
    {ok, Quota1} = ?NEW_LWW(T, Quota),
    {ok, MaxSwap1} = ?NEW_LWW(T, MaxSwap),
    {ok, RAM1} = ?NEW_LWW(T, RAM),
    {ok, ZFSIOPriority1} = ?NEW_LWW(T, ZFSIOPriority),
    Metadata1 = fifo_map:from_orddict(Metadata, ID, T),

    D1 =
        #package_0_1_0{
           uuid            = UUID1,
           name            = Name1,
           metadata        = Metadata1,

           blocksize       = BlockSize1,
           compression     = Compression1,
           cpu_cap         = CpuCap1,
           cpu_shares      = CpuShares1,
           max_swap        = MaxSwap1,
           quota           = Quota1,
           ram             = RAM1,
           zfs_io_priority = ZFSIOPriority1
          },
    load({T, ID}, D1).

new({T, _ID}) ->
    {ok, Undefined} = ?NEW_LWW(undefined, T),
    #?PACKAGE{
        blocksize = Undefined,
        compression = Undefined,
        max_swap = Undefined,
        zfs_io_priority = Undefined
       }.

?S(<<"uuid">>, uuid);
?S(<<"name">>, name);
?S(<<"blocksize">>, blocksize);
?S(<<"compression">>, compression);
?S(<<"cpu_cap">>, cpu_cap);
?S(<<"cpu_shares">>, cpu_shares);
?S(<<"max_swap">>, max_swap);
?S(<<"quota">>, quota);
?S(<<"ram">>, ram);
?S(<<"zfs_io_priority">>, zfs_io_priority);

set(ID, K = <<"metadata.", _/binary>>, V, H) ->
    set(ID, re:split(K, "\\."), V, H);
set(ID, [<<"metadata">> | R], V, H) ->
    set_metadata(ID, R, V, H).

set_metadata({T, ID}, P, Value, User) when is_binary(P) ->
    set_metadata({T, ID}, fifo_map:split_path(P), Value, User);

set_metadata({_T, ID}, Attribute, delete, G) ->
    {ok, M1} = fifo_map:remove(Attribute, ID, G#?PACKAGE.metadata),
    G#?PACKAGE{metadata = M1};

set_metadata({T, ID}, Attribute, Value, G) ->
    {ok, M1} = fifo_map:set(Attribute, Value, ID, T, G#?PACKAGE.metadata),
    G#?PACKAGE{metadata = M1}.

merge(#?PACKAGE{
          uuid            = UUID1,
          name            = Name1,
          metadata        = Metadata1,

          blocksize       = BlockSize1,
          compression     = Compression1,
          cpu_cap         = CpuCap1,
          cpu_shares      = CpuShares1,
          max_swap        = MaxSwap1,
          quota           = Quota1,
          ram             = RAM1,
          zfs_io_priority = ZFSIOPriority1

         },
      #?PACKAGE{
          uuid            = UUID2,
          name            = Name2,
          metadata        = Metadata2,

          blocksize       = BlockSize2,
          compression     = Compression2,
          cpu_cap         = CpuCap2,
          cpu_shares      = CpuShares2,
          max_swap        = MaxSwap2,
          quota           = Quota2,
          ram             = RAM2,
          zfs_io_priority = ZFSIOPriority2
         }) ->
    #?PACKAGE{
        uuid            = riak_dt_lwwreg:merge(UUID1, UUID2),
        name            = riak_dt_lwwreg:merge(Name1, Name2),
        metadata       = fifo_map:merge(Metadata1, Metadata2),

        blocksize       = riak_dt_lwwreg:merge(BlockSize1, BlockSize2),
        compression     = riak_dt_lwwreg:merge(Compression1, Compression2),
        cpu_cap         = riak_dt_lwwreg:merge(CpuCap1, CpuCap2),
        cpu_shares      = riak_dt_lwwreg:merge(CpuShares1, CpuShares2),
        max_swap        = riak_dt_lwwreg:merge(MaxSwap1, MaxSwap2),
        quota           = riak_dt_lwwreg:merge(Quota1, Quota2),
        ram             = riak_dt_lwwreg:merge(RAM1, RAM2),
        zfs_io_priority = riak_dt_lwwreg:merge(ZFSIOPriority1, ZFSIOPriority2)

       }.
