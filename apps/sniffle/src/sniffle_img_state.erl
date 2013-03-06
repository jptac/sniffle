-module(sniffle_img_state).
-export([new/0, data/2]).
-ignore_xref([new/0, data/2]).

new() ->
    <<>>.

data(D, _) ->
    D.
