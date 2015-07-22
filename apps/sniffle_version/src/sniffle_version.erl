-module(sniffle_version).

-include("sniffle_version.hrl").

-export([v/0]).

v() ->
    ?VERSION.
