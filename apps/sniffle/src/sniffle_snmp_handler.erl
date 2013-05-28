-module(sniffle_snmp_handler).
-include("sniffle_version.hrl").

%% External exports
-export([start/0, name/1, reload/0, version/1,
         p999/2, p99/2, p95/2, p75/2, p25/2,
         count/2, min/2, median/2, mean/2, max/2,
         ring_status/1, keys/2
        ]).

-ignore_xref([name/1, nintynine/1, reload/0, version/1,
              p999/2, p99/2, p95/2, p75/2, p25/2,
              count/2, min/2, median/2, mean/2, max/2,
              ring_status/1, keys/2
             ]).

%% Internal exports

-define(status_col, 4).

-define(active, 1).
-define(notInService, 2).
-define(notReady, 3).
-define(createAndGo, 4).   % Action; written, not read
-define(createAndWait, 5). % Action; written, not read
-define(destroy, 6).       % Action; written, not read


%% c("apps/sniffle/src/sniffle_snmp_handler.erl").
reload() ->
    snmpa:unload_mibs(["SNIFFLE-MIB"]),
    snmpc:compile("apps/sniffle/mibs/SNIFFLE-MIB"),
    snmpa:load_mibs(["SNIFFLE-MIB"]).

start() ->
    snmpa:load_mibs([code:priv_dir(sniffle) ++ "/mibs/SNIFFLE-MIB"]).

%%----------------------------------------------------------------
%% Instrumentation function for variable myName.
%% Returns: (get) {value, Name}
%%          (set) noError
%%----------------------------------------------------------------
name(get) ->
    {value, "Sniffle"}.

version(get) ->
    {value, binary_to_list(?VERSION)}.


ring_status(get) ->
    try riak_core_status:ringready() of
        {ok, _Nodes} ->
            {value, 0};
        {error, {different_owners, _N1, _N2}} ->
            {value, 1};
        {error, {nodes_down, _Down}} ->
            {value, 2}
    catch
        Exception:Reason ->
            lager:error("Ringready failed ~p:~p", [Exception, Reason]),
            {value, 4}
    end.

keys(get, <<"hypervisor">>) ->
    {ok, L} = sniffle_hypervisor:list(),
    {value, length(L)};
keys(get, <<"dtrace">>) ->
    {ok, L} = sniffle_dtrace:list(),
    {value, length(L)};
keys(get, <<"vm">>) ->
    {ok, L} = sniffle_vm:list(),
    {value, length(L)};
keys(get, <<"iprange">>) ->
    {ok, L} = sniffle_iprange:list(),
    {value, length(L)};
keys(get, <<"dataset">>) ->
    {ok, L} = sniffle_dataset:list(),
    {value, length(L)};
keys(get, <<"package">>) ->
    {ok, L} = sniffle_package:list(),
    {value, length(L)};
keys(get, <<"image">>) ->
    {ok, L} = sniffle_img:list(),
    {value, length(L)}.


p999(get, Prefix) ->
    percentile_get(p999, Prefix, total).

p99(get, Prefix) ->
    percentile_get(p99, Prefix, total).

p95(get, Prefix) ->
    percentile_get(p95, Prefix, total).

p75(get, Prefix) ->
    percentile_get(p75, Prefix, total).

p25(get, Prefix) ->
    percentile_get(p25, Prefix, total).

count(get, Prefix) ->
    percentile_get(observations, Prefix, total).

min(get, Prefix) ->
    percentile_get(min, Prefix, total).

median(get, Prefix) ->
    percentile_get(median, Prefix, total).

mean(get, Prefix) ->
    percentile_get(mean, Prefix, total).

max(get, Prefix) ->
    percentile_get(max, Prefix, total).

percentile_get(mean, Prefix, Grouping) ->
    Data = generate_percentile(Prefix, Grouping),
    case lists:keyfind(mean, 1, Data) of
        {mean, R} ->
            {value, trunc(R)};
        _ ->
            {value, 0}
    end;

percentile_get(What, Prefix, Grouping) ->
    Data = generate_percentile(Prefix, Grouping),
    case lists:keyfind(What, 1, Data) of
        {What, R} ->
            {value, R};
        _ ->
            {value, 0}
    end.

generate_percentile(Prefix, Grouping) ->
    L = byte_size(Prefix),
    {ok, Data} = statman_aggregator:get_window(60),
    Data1 = [ D || [{key,{
                       <<ThisPrefix:L/binary, _/binary>>,
                       ThisGrouping}},
                    _,
                    {type,histogram},
                    {value, D},
                    _] <- Data,
                   ThisPrefix =:= Prefix,
                   ThisGrouping =:= Grouping
            ],
    Data2 = lists:sort(lists:flatten(Data1)),
    Data3 = lists:foldl(fun ({T, C}, [{T, C1} | Acc]) ->
                                [{T, C1 + C} | Acc];
                            (E, Acc) ->
                                [E | Acc]
                        end, [], Data2),
    statman_histogram:summary(Data3).

%% [{observations,12},
%% {min,15986},
%% {median,17581},
%% {mean,17796.416666666668},
%% {max,19912},
%% {sd,1135.098911055232},
%% {sum,213557},
%% {sum2,3814722299},
%% {p25,16831},
%% {p75,17995},
%% {p95,19912},
%% {p99,19912},
%% {p999,19912}]


