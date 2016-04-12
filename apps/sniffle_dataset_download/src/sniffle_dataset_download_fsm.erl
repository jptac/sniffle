%%%-------------------------------------------------------------------
%%% @author Heinz Nikolaus Gies <heinz@Schroedinger.fritz.box>
%%% @copyright (C) 2014, Heinz Nikolaus Gies
%%% @doc
%%%
%%% @end
%%% Created : 10 Dec 2014 by Heinz Nikolaus Gies <heinz@Schroedinger.fritz.box>
%%%-------------------------------------------------------------------
-module(sniffle_dataset_download_fsm).

-behaviour(gen_fsm).

%% API
-export([start_link/3,
         download/1]).

-export([get_manifest/2,
         init_download/2,
         download/2,
         finalize/2,
         verify_size/2,
         calculate_sha/2,
         verify_sha/2
        ]).

%% gen_fsm callbacks
-export([init/1, handle_event/3,
         handle_sync_event/4, handle_info/3, terminate/3, code_change/4]).

-ignore_xref([start_link/3]).

-define(SERVER, ?MODULE).

-record(state, {uuid, url, sha1, img_sha1, upload, total_size, img_url,
                chunk_size, from, ref, acc = <<>>, downloaded = 0,
                http_opts = [], http_client, done = false}).

%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Creates a gen_fsm process which calls Module:init/1 to
%% initialize. To ensure a synchronized start-up procedure, this
%% function does not return until Module:init/1 has returned.
%%
%% @end
%%--------------------------------------------------------------------
-spec start_link(URL::binary(), From::pid(), Ref::reference()) ->
                        {ok, pid()} | ignore | {error, atom()}.
start_link(URL, From, Ref) ->
    gen_fsm:start_link(?MODULE, [URL, From, Ref], []).

download(URL) ->
    Ref = make_ref(),
    supervisor:start_child(sniffle_dataset_download_sup, [URL, self(), Ref]),
    receive
        {Ref, Reply} ->
            Reply
    after
        2500 ->
            {error, timeout}
    end.

%%%===================================================================
%%% gen_fsm callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever a gen_fsm is started using gen_fsm:start/[3,4] or
%% gen_fsm:start_link/[3,4], this function is called by the new
%% process to initialize.
%%
%% @spec init(Args) -> {ok, StateName, State} |
%%                     {ok, StateName, State, Timeout} |
%%                     ignore |
%%                     {stop, StopReason}
%% @end
%%--------------------------------------------------------------------
init([URL, From, Ref]) ->
    process_flag(trap_exit, true),
    Opts = case sniffle_opt:get(network, http, proxy) of
               undefined ->
                   [];
               P ->
                   [{proxy, P}]
           end,
    {ok, get_manifest,
     #state{url = URL, from = From, ref = Ref, http_opts = Opts}, 0}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% There should be one instance of this function for each possible
%% state name. Whenever a gen_fsm receives an event sent using
%% gen_fsm:send_event/2, the instance of this function with the same
%% name as the current state name StateName is called to handle
%% the event. It is also called if a timeout occurs.
%%
%% @end
%%--------------------------------------------------------------------
get_manifest(_E, State = #state{url = URL, from = From, ref = Ref,
                                http_opts = HTTPOpts}) ->
    lager:info("[img:import] Importing: ~s", [URL]),
    Client = client(URL, HTTPOpts),
    {ok, Body} = hackney:body(Client),
    hackney:close(Client),
    JSON = jsxd:from_list(jsx:decode(Body)),
    {ok, UUID} = jsxd:get([<<"uuid">>], JSON),
    ImgURL = case jsxd:get([<<"files">>, 0, <<"url">>], JSON) of
                 {ok, FileImgURL} -> FileImgURL; %% dataset API!
                 _ -> <<URL/binary, "/file">> %% img API!
             end,
    {ok, TotalSize} = jsxd:get([<<"files">>, 0, <<"size">>], JSON),
    {ok, SHA1} = jsxd:get([<<"files">>, 0, <<"sha1">>], JSON),
    sniffle_dataset:create(UUID),
    import_manifest(UUID, JSON),
    progress(UUID, 0),
    {ok, {Host, Port, AKey, SKey, Bucket}} = sniffle_s3:config(image),
    ChunkSize = application:get_env(sniffle, image_chunk_size,
                                    5*1024*1024),
    {ok, U} = fifo_s3_upload:new(AKey, SKey, Host, Port, Bucket, UUID),
    From ! {Ref, {ok, UUID}},
    {next_state, init_download,
     State#state{
       uuid = UUID,
       img_sha1 = SHA1,
       total_size = TotalSize,
       img_url = ImgURL,
       chunk_size = ChunkSize,
       upload = U
      }, 0}.

init_download(_E, State = #state{img_url = URL, http_opts = HTTPOpts}) ->
    lager:info("[img:import:~s] Downloading: ~s", [State#state.uuid, URL]),
    Client = client(URL, HTTPOpts),
    SHA1 = crypto:hash_init(sha),
    {next_state, download,
     State#state{
       sha1 = SHA1,
       http_client = Client
      }, 0}.

client(URL, HTTPOpts) ->
    {ok, 200, _, Client} = hackney:request(get, URL, [], <<>>, HTTPOpts),
    Client.

download(_E, State = #state{acc = Acc, chunk_size = ChunkSize, upload = U,
                            uuid = UUID, downloaded = D, total_size = T})
  when byte_size(Acc) >= ChunkSize ->
    <<Chunk:ChunkSize/binary, Acc1/binary>> = Acc,
    ok = fifo_s3_upload:part(U, binary:copy(Chunk)),
    progress(UUID, D / T),
    lager:info("[img:import:~s] Progress: ~p of ~p", [UUID, D, T]),
    {next_state, download, State#state{acc = Acc1}, 0};

download(_D, State = #state{done = true}) ->
    {next_state, finalize, State, 0};

download(_, State = #state{http_client = Client, acc = Acc, downloaded = Done,
                           total_size = TotalSize, sha1 = SHA1, uuid=UUID}) ->
    case hackney:stream_body(Client) of
        {ok, Data} ->
            SHA11 = crypto:hash_update(SHA1, Data),
            Acc1 = <<Acc/binary, Data/binary>>,
            {next_state, download,
             State#state{downloaded = Done + byte_size(Data), acc = Acc1,
                         sha1 = SHA11, http_client = Client}, 0};
        done ->
            lager:info("[img:import:~s] Download complete after ~p of ~p byte",
                       [UUID, Done, TotalSize]),
            hackney:close(Client),
            {next_state, download, State#state{done = true}, 0}
    end.

finalize(_Event, State = #state{acc = <<>>, upload = U, uuid = UUID}) ->
    fifo_s3_upload:done(U),
    sniffle_dataset:status(UUID, <<"verifying">>),
    {next_state, verify_size, State, 0};

finalize(_E, State = #state{acc = Acc, upload = U}) ->
    ok = fifo_s3_upload:part(U, binary:copy(Acc)),
    {next_state, finalize, State#state{acc = <<>>}, 0}.


verify_size(_E, State = #state{uuid = UUID, downloaded = ActualSize,
                               total_size = ExpectedSize}) when
      ActualSize =/= ExpectedSize ->
    lager:error("[img:import:~s] Incorrect size, expected ~p but git ~p byte.",
                [UUID, ActualSize, ExpectedSize]),
    {stop, size_verification_failed, State};

verify_size(_E, State = #state{uuid = UUID, downloaded = ActualSize}) ->
    lager:info("[img:import:~s] Downloaded size matches with ~p.",
               [UUID, ActualSize]),
    {next_state, calculate_sha, State, 0}.


calculate_sha(_E, State = #state{uuid = UUID, sha1 = SHA1}) ->
    Digest = base16:encode(crypto:hash_final(SHA1)),
    lager:info("[img:import:~s] Calculating digest: ~s.", [UUID, Digest]),
    {next_state, verify_sha, State#state{sha1 = Digest}, 0}.

verify_sha(_E, State = #state{uuid = UUID, sha1 = SHA1, img_sha1 = SHA1}) ->
    sniffle_dataset:status(UUID, <<"imported">>),
    progress(UUID, 1),
    lager:info("[img:import:~s] Digest verify with: ~s.", [UUID, SHA1]),
    lager:info("[img:import:~s] Download successful!", [UUID]),
    {stop, normal, State};

verify_sha(_E, State = #state{uuid = UUID, sha1 = ActualSHA1,
                          img_sha1 = ExpectedSHA1}) ->
    lager:error("[img:import:~s] SHA validation failed, got ~s instead of ~s.",
                [UUID, ActualSHA1, ExpectedSHA1]),
    {stop, ssh_verification_failed, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever a gen_fsm receives an event sent using
%% gen_fsm:send_all_state_event/2, this function is called to handle
%% the event.
%%
%% @spec handle_event(Event, StateName, State) ->
%%                   {next_state, NextStateName, NextState} |
%%                   {next_state, NextStateName, NextState, Timeout} |
%%                   {stop, Reason, NewState}
%% @end
%%--------------------------------------------------------------------
handle_event(_Event, StateName, State) ->
    {next_state, StateName, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever a gen_fsm receives an event sent using
%% gen_fsm:sync_send_all_state_event/[2,3], this function is called
%% to handle the event.
%%
%% @spec handle_sync_event(Event, From, StateName, State) ->
%%                   {next_state, NextStateName, NextState} |
%%                   {next_state, NextStateName, NextState, Timeout} |
%%                   {reply, Reply, NextStateName, NextState} |
%%                   {reply, Reply, NextStateName, NextState, Timeout} |
%%                   {stop, Reason, NewState} |
%%                   {stop, Reason, Reply, NewState}
%% @end
%%--------------------------------------------------------------------
handle_sync_event(_Event, _From, StateName, State) ->
    Reply = ok,
    {reply, Reply, StateName, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_fsm when it receives any
%% message other than a synchronous or asynchronous event
%% (or a system message).
%%
%% @spec handle_info(Info,StateName,State)->
%%                   {next_state, NextStateName, NextState} |
%%                   {next_state, NextStateName, NextState, Timeout} |
%%                   {stop, Reason, NewState}
%% @end
%%--------------------------------------------------------------------
handle_info(_Info, StateName, State) ->
    {next_state, StateName, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_fsm when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_fsm terminates with
%% Reason. The return value is ignored.
%%
%% @spec terminate(Reason, StateName, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(normal, _StateName, _State) ->
    ok;
terminate(Reason, _StateName,
          #state{uuid = undefined, from = From, ref = Ref}) ->
    From ! {Ref, {error, Reason}},
    ok;
terminate(Reason, _StateName,
          #state{uuid = UUID, http_client = Client, upload = U}) ->
    libhowl:send(UUID,
                 [{<<"event">>, <<"error">>},
                  {<<"data">>,
                   [{<<"message">>,
                     list_to_binary(io_lib:format("~p", Reason))}]}]),
    sniffle_dataset:status(UUID, <<"failed">>),
    hackney:close(Client),
    fifo_s3_upload:abort(U),
    ok.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, StateName, State, Extra) ->
%%                   {ok, StateName, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

import_manifest(UUID, D1) ->
    do_import(
      [
       {<<"metadata">>, fun sniffle_dataset:set_metadata/2},
       {<<"name">>, fun sniffle_dataset:name/2},
       {<<"version">>, fun sniffle_dataset:version/2},
       {<<"description">>, fun sniffle_dataset:description/2},
       {<<"disk_driver">>, fun sniffle_dataset:disk_driver/2},
       {<<"nic_driver">>, fun sniffle_dataset:nic_driver/2},
       {<<"users">>, fun sniffle_dataset:users/2},
       {[<<"tags">>, <<"kernel_version">>],
        fun sniffle_dataset:kernel_version/2}
      ], UUID, D1),
    sniffle_dataset:image_size(
      UUID,
      ensure_integer(
        jsxd:get(<<"image_size">>,
                 jsxd:get([<<"files">>, 0, <<"size">>], 0, D1), D1))),
    RS = jsxd:get(<<"requirements">>, [], D1),
    Networks = jsxd:get(<<"networks">>, [], RS),
    [sniffle_dataset:add_network(UUID, {NName, NDesc}) ||
        [{<<"description">>, NDesc}, {<<"name">>, NName}] <- Networks],
    case jsxd:get(<<"homepage">>, D1) of
        {ok, HomePage} ->
            sniffle_dataset:set_metadata(
              UUID,
              [{<<"homepage">>, HomePage}]);
        _ ->
            ok
    end,
    case jsxd:get(<<"min_platform">>, RS) of
        {ok, Min} ->
            Min1 = [V || {_, V} <- Min],
            [M | _] = lists:sort(Min1),
            R = {must, '>=', <<"sysinfo.Live Image">>, M},
            sniffle_dataset:add_requirement(UUID, R);
        _ ->
            ok
    end,
    case jsxd:get(<<"os">>, D1) of
        {ok, <<"smartos">>} ->
            sniffle_dataset:os(UUID, <<"smartos">>),
            sniffle_dataset:type(UUID, <<"zone">>);
        {ok, OS} ->
            case jsxd:get(<<"type">>, D1) of
                {ok, <<"lx-dataset">>} ->
                    sniffle_dataset:os(UUID, OS),
                    sniffle_dataset:type(UUID, <<"zone">>),
                    sniffle_dataset:zone_type(UUID, <<"lx">>);
                _ ->
                    sniffle_dataset:os(UUID, OS),
                    sniffle_dataset:type(UUID, <<"kvm">>)
            end
    end.

do_import([], _UUID, _O) ->
    ok;

do_import([{K, F} | R], UUID, O) ->
    case jsxd:get(K, O) of
        {ok, V}  ->
            F(UUID, V);
        _ ->
            ok
    end,
    do_import(R, UUID, O).


ensure_integer(I) when is_integer(I) ->
    I;
ensure_integer(L) when is_list(L) ->
    list_to_integer(L);
ensure_integer(B) when is_binary(B) ->
    list_to_integer(binary_to_list(B)).


progress(UUID, Imported) ->
    sniffle_dataset:imported(UUID, Imported),
    libhowl:send(UUID,
                 [{<<"event">>, <<"progress">>},
                  {<<"data">>, [{<<"imported">>, Imported}]}]).
