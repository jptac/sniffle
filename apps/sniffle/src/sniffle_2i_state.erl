-module(sniffle_2i_state).

-export([new/1, load/2, target/3, target/1, merge/2]).

-ignore_xref([load/2, merge/2]).

-record(sniffle_2i_0,
        {time=0, target=not_found}).

-define(S2I, sniffle_2i_0).

new(_) ->
    #?S2I{}.

load(_, #?S2I{} = S2i) ->
    S2i.

target(#?S2I{target = T}) ->
    T.

target({T, _}, _, #?S2I{time = T0} = S2i) when T0 >= T ->
    S2i;
target({T, _}, Target, _) ->
    #?S2I{time = T, target = Target}.

merge(#?S2I{time = T0},
      #?S2I{time = T1} = S2i) when T1 >= T0 ->
    S2i;

merge(S2i, _) ->
    S2i.
