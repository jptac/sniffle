-module(sniffle_dataset).
-define(CMD, sniffle_dataset_cmd).
-define(BUCKET, <<"dataset">>).
-define(S, ft_dataset).
-include("sniffle.hrl").

-define(FM(Met, Mod, Fun, Args),
        folsom_metrics:histogram_timed_update(
          {sniffle, dataset, Met},
          Mod, Fun, Args)).

-export([
         create/1,
         delete/1,
         import/1,
         remove_requirement/2,
         add_requirement/2,
         remove_network/2,
         add_network/2,
         available/0,
         available/1
        ]).

-export([
         set_metadata/2,
         description/2,
         disk_driver/2,
         homepage/2,
         image_size/2,
         name/2,
         type/2,
         zone_type/2,
         nic_driver/2,
         os/2,
         sha1/2,
         users/2,
         status/2,
         imported/2,
         version/2,
         kernel_version/2
        ]).


%%%===================================================================
%%% General section
%%%===================================================================
-spec get(UUID::fifo:dtrace_id()) ->
                 not_found | {ok, Dataset::fifo:dataset()} | {error, timeout}.

-spec list() ->
                  {ok, [UUID::fifo:dataset_id()]} | {error, timeout}.

-spec list([fifo:matcher()], boolean()) ->
                  {error, timeout} | {ok, [fifo:uuid()]}.

-include("sniffle_api.hrl").
%%%===================================================================
%%% Custom section
%%%===================================================================

available() ->
    Clients = available_clients(),
    get_datasets(Clients, []).

available(Send) ->
    Clients = available_clients(),
    send_datasets(Clients, Send).

http_opts() ->
    case sniffle_opt:get(network, http, proxy) of
        undefined ->
            [{follow_redirect, true}];
        P ->
            [{proxy, P}, {follow_redirect, true}]
    end.

available_clients() ->
    Servers = case sniffle_opt:get("endpoints", "datasets", "servers") of
                  undefined -> [];
                  L -> L
              end,
    Opts = http_opts(),
    [{Server, hackney:request(get, Server, [], <<>>, [async | Opts])}
     || Server <- Servers].



read_body(Ref, Acc) ->
    receive
        {hackney_response, Ref, done} ->
            Acc;
        {hackney_response, Ref, Bin} when is_binary(Bin) ->
            read_body(Ref, <<Acc/binary, Bin/binary>>);
        {hackney_response, Ref, {redirect, To, _}} ->
            case hackney:request(get, To, [], <<>>, http_opts()) of
                {ok, 200, _, Client} ->
                    {ok, Body} = hackney:body(Client),
                    Body;
                _ ->
                    <<>>
            end;
        {hackney_response, Ref, _} ->
            read_body(Ref, Acc)
    after
        5000 ->
            Acc
    end.

client_to_json(Server, Client) ->
    Body = read_body(Client, <<>>),
    JSON = try
               jsxd:from_list(jsx:decode(Body))
           catch
               _:_ ->
                   []
           end,
    [jsxd:set(<<"server">>, Server, E) || E <- JSON].

send_datasets([{Server, {ok, Client}} | Rest], Send) ->
    Send(client_to_json(Server, Client)),
    send_datasets(Rest, Send);

send_datasets([_ | Rest], Send) ->
    send_datasets(Rest, Send);

send_datasets([], _Send) ->
    ok.


get_datasets([{Server, {ok, Client}} | Rest], Acc) ->
    get_datasets(Rest, [client_to_json(Server, Client) | Acc]);

get_datasets([_ | Rest], Acc) ->
    get_datasets(Rest, Acc);

get_datasets([], Acc) ->
    {ok, lists:flatten(Acc)}.

-spec create(UUID::fifo:dataset_id()) ->
                    duplicate | ok | {error, timeout}.
create(UUID) ->
    case sniffle_dataset:get(UUID) of
        not_found ->
            do_write(UUID, create, []);
        {ok, _RangeObj} ->
            duplicate
    end.

-spec delete(UUID::fifo:dataset_id()) ->
                    not_found | {error, timeout} | ok.

delete(UUID) ->
    case do_write(UUID, delete) of
        ok ->
            sniffle_s3:delete(image, binary_to_list(UUID));
        E ->
            E
    end.

import(URL) ->
    sniffle_dataset_download_fsm:download(URL).

?SET(set_metadata).
?SET(description).
?SET(disk_driver).
?SET(homepage).
?SET(image_size).
?SET(name).
?SET(nic_driver).
?SET(os).
?SET(type).
?SET(zone_type).
?SET(users).
?SET(version).
?SET(kernel_version).
?SET(sha1).
?SET(status).
?SET(imported).
?SET(remove_requirement).
?SET(add_requirement).
?SET(remove_network).
?SET(add_network).
