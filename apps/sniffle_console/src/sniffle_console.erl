%% @doc Interface for sniffle-admin commands.
%% A lot of the code is taken from
%% https://github.com/basho/riak_kv/blob/develop/src/riak_kv_console.erl
-module(sniffle_console).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([
         init_leo/1,
         db_keys/1,
         db_get/1,
         db_delete/1,
         get_ring/1,
         connections/1,
         db_update/1,
         update_snarl_meta/1,
         ensemble_status/1
        ]).

-export([
         aae_status/1,
         vnode_status/1,
         status/1,
         cluster_info/1,
         down/1,
         join/1,
         leave/1,
         reip/1,
         remove/1,
         ringready/1,
         staged_join/1
        ]).

-export([
         config/1,
         ds/1,
         dtrace/1,
         hvs/1,
         ips/1,
         networks/1,
         pkgs/1,
         vms/1
        ]).

-export([
         pp_json/1
        ]).

-ignore_xref([
              init_leo/1,
              db_keys/1,
              db_get/1,
              db_delete/1,
              get_ring/1,
              connections/1,
              aae_status/1,
              vnode_status/1,
              ensemble_status/1,
              status/1,
              cluster_info/1,
              config/1,
              down/1,
              ds/1,
              dtrace/1,
              hvs/1,
              ips/1,
              networks/1,
              join/1,
              leave/1,
              pkgs/1,
              reip/1,
              remove/1,
              ringready/1,
              staged_join/1,
              vms/1,
              db_update/1,
              update_snarl_meta/1
             ]).

update_snarl_meta([]) ->
    update_metadata("Organisation", ls_org, ft_org),
    update_metadata("User", ls_user, ft_user),
    update_metadata("Role", ls_role, ft_role).

update_metadata(Name, LS, FT) ->
    {ok, Es} = LS:list(),
    io:format("[~s] Updating ~p elements: ", [Name, length(Es)]),
    [update_meta_element(LS, FT, E) || E <- Es],
    io:format("done.~n").

update_meta_element(LS, FT, ID) ->
    {ok, E} = LS:get(ID),
    M = FT:metadata(E),
    MChanges = lists:foldl(fun({<<"public">>, _V}, Cs) ->
                                   Cs;
                              ({K, V}, Cs) ->
                                   [{K, delete}, {[<<"public">>, K], V} | Cs]
                           end, [], M),
    LS:set_metadata(ID, MChanges),
    io:format(".").

init_leo([Host]) ->
    init_leo([Host, Host]);

init_leo([Manager, Gateway]) ->
    P = 10020,
    User = "fifo",
    OK = {ok,[{<<"result">>,<<"OK">>}]},

    %% Create the user and store access and secret key
    {ok,[{<<"access_key_id">>,AKey}, {<<"secret_access_key">>, SKey}]} =
        libleofs:create_user(Manager, P, User),
    sniffle_opt:set(["storage", "s3", "access_key"], AKey),
    sniffle_opt:set(["storage", "s3", "secret_key"], SKey),
    io:format("Created user ~s:~n"
              "Access Key: ~s~n"
              "Secret Key: ~s~n",
              [User, AKey, SKey]),

    %% Create the general bucket
    GenBucket = "fifo",
    OK = libleofs:add_bucket(Manager, P, GenBucket, AKey),
    sniffle_opt:set(["storage", "s3", "general_bucket"], GenBucket),
    io:format("Created bucket general bucket: ~s~n", [GenBucket]),

    %% Create the image bucket
    ImgBucket = "fifo-images",
    OK = libleofs:add_bucket(Manager, P, ImgBucket, AKey),
    sniffle_opt:set(["storage", "s3", "image_bucket"], ImgBucket),
    io:format("Created bucket image bucket: ~s~n", [ImgBucket]),

    %% Create the backup bucket
    SnapBucket = "fifo-backups",
    OK = libleofs:add_bucket(Manager, P, SnapBucket, AKey),
    sniffle_opt:set(["storage", "s3", "snapshot_bucket"], SnapBucket),
    io:format("Created bucket snapshot bucket: ~s~n", [SnapBucket]),

    %% Add the enpoint
    OK = libleofs:add_endpoint(Manager, P, Gateway),
    sniffle_opt:set(["storage", "s3", "host"], Gateway),
    ok = sniffle_opt:set(["storage", "s3", "port"], 443),
    io:format("Configuring endpoint as: https://~s:~p~n", [Gateway, P]),
    %% Set s3 as storage system
    ok = sniffle_opt:set(["storage", "general", "backend"], s3),
    io:format("Setting storage backend to s3, please reastart sniffle for this "
              "to take full effect!~n"),
    ok.


db_update([]) ->
    [db_update([E]) || E <- ["vms", "datasets", "dtraces", "hypervisors",
                             "ipranges", "networks", "packages"]],
    ok;

db_update(["datasets"]) ->
    io:format("Updating datasets...~n"),
    do_update(sniffle_dataset, ft_dataset);

db_update(["dtraces"]) ->
    io:format("Updating dtrace scripts...~n"),
    do_update(sniffle_dtrace, ft_dtrace);

db_update(["hypervisors"]) ->
    io:format("Updating hypervisors...~n"),
    do_update(sniffle_hypervisor, ft_hypervisor);

db_update(["ipranges"]) ->
    io:format("Updating ipranges...~n"),
    do_update(sniffle_iprange, ft_iprange);

db_update(["networks"]) ->
    io:format("Updating networks...~n"),
    do_update(sniffle_network, ft_network);

db_update(["packages"]) ->
    io:format("Updating packages...~n"),
    do_update(sniffle_package, ft_package);

db_update(["vms"]) ->
    io:format("Updating vms...~n"),
    do_update(sniffle_vm, ft_vm).

print_endpoints(Es) ->
    io:format("Hostname            "
              "                    "
              " Node               "
              " Errors    ~n"),
    io:format("--------------------"
              "--------------------"
              "----------"
              " ---------------~n", []),
    [print_endpoint(E) || E <- Es].

print_endpoint([{{Hostname, [{port,Port},{ip,IP}]}, _, Fails}]) ->
    HostPort = <<IP/binary, ":", Port/binary>>,
    io:format("~40s ~-19s ~9b~n", [Hostname, HostPort, Fails]).

connections(["snarl"]) ->
    io:format("Snarl endpoints.~n"),
    print_endpoints(libsnarl:servers());

connections(["howl"]) ->
    io:format("Howl endpoints.~n"),
    print_endpoints(libhowl:servers());

connections([]) ->
    connections(["snarl"]),
    connections(["howl"]).

get_ring([]) ->
    {ok, RingData} = riak_core_ring_manager:get_my_ring(),
    {_S, CHash} = riak_core_ring:chash(RingData),
    io:format("Hash                "
              "                    "
              "          "
              " Node~n"),
    io:format("--------------------"
              "--------------------"
              "----------"
              " ---------------~n", []),
    lists:map(fun({K, H}) ->
                      io:format("~50b ~-15s~n", [K, H])
              end, CHash),
    ok.

is_prefix(Prefix, K) ->
    binary:longest_common_prefix([Prefix, K]) =:= byte_size(Prefix).

db_delete([CHashS, CatS, KeyS]) ->
    Cat = list_to_binary(CatS),
    Key = list_to_binary(KeyS),
    CHash = list_to_integer(CHashS),
    {ok, RingData} = riak_core_ring_manager:get_my_ring(),
    {_S, CHashs} = riak_core_ring:chash(RingData),
    case lists:keyfind(CHash, 1, CHashs) of
        false ->
            io:format("C-Hash ~b does not exist.~n", [CHash]),
            error;
        _ ->
            CHashA = list_to_atom(CHashS),
            fifo_db:delete(CHashA, Cat, Key)
    end.
db_get([CHashS, CatS, KeyS]) ->
    Cat = list_to_binary(CatS),
    Key = list_to_binary(KeyS),
    CHash = list_to_integer(CHashS),
    {ok, RingData} = riak_core_ring_manager:get_my_ring(),
    {_S, CHashs} = riak_core_ring:chash(RingData),
    case lists:keyfind(CHash, 1, CHashs) of
        false ->
            io:format("C-Hash ~b does not exist.~n", [CHash]),
            error;
        _ ->
            CHashA = list_to_atom(CHashS),
            case fifo_db:get(CHashA, Cat, Key) of
                {ok, E} ->
                    io:format("~p~n", [E]);
                _ ->
                    io:format("Not found.~n", []),
                    error
            end
    end.

db_keys([]) ->
    db_keys(["-p", ""]);

db_keys(["-p", PrefixS]) ->
    Prefix = list_to_binary(PrefixS),
    {ok, RingData} = riak_core_ring_manager:get_my_ring(),
    {_S, CHashs} = riak_core_ring:chash(RingData),
    lists:map(fun({Hash, H}) ->
                      io:format("~n~50b ~-15s~n", [Hash, H]),
                      io:format("--------------------"
                                "--------------------"
                                "--------------------"
                                "------~n", []),
                      CHashA = list_to_atom(integer_to_list(Hash)),
                      Keys = fifo_db:list_keys(CHashA, <<>>),
                      [io:format("~s~n", [K])
                       || K <- Keys,
                          is_prefix(Prefix, K) =:= true]
              end, CHashs),
    ok;

db_keys([CHashS]) ->
    db_keys([CHashS, ""]);

db_keys([CHashS, PrefixS]) ->
    Prefix = list_to_binary(PrefixS),
    CHash = list_to_integer(CHashS),
    {ok, RingData} = riak_core_ring_manager:get_my_ring(),
    {_S, CHashs} = riak_core_ring:chash(RingData),
    case lists:keyfind(CHash, 1, CHashs) of
        false ->
            io:format("C-Hash ~b does not exist.", [CHash]),
            error;
        _ ->
            CHashA = list_to_atom(CHashS),
            Keys = fifo_db:list_keys(CHashA, Prefix),
            [io:format("~s~n", [K]) || K <- Keys],
            ok
    end.

aae_status([]) ->
    Services = [{sniffle_hypervisor, "Hypervisor"}, {sniffle_vm, "VM"},
                {sniffle_iprange, "IP Range"}, {sniffle_package, "Package"},
                {sniffle_dataset, "Dataset"}, {sniffle_network, "Network"},
                {sniffle_dtrace, "DTrace"}],
    [aae_status(E) || E <- Services];

%% aae_status([]) ->
%%     ExchangeInfo = riak_kv_entropy_info:compute_exchange_info(),
%%     aae_exchange_status(ExchangeInfo),
%%     io:format("~n"),
%%     TreeInfo = riak_kv_entropy_info:compute_tree_info(),
%%     aae_tree_status(TreeInfo),
%%     io:format("~n"),
%%     aae_repair_status(ExchangeInfo).

aae_status({System, Name}) ->
    ExchangeInfo = riak_core_entropy_info:compute_exchange_info(System),
    io:format("~s~n~n", [Name]),
    aae_exchange_status(ExchangeInfo),
    io:format("~n"),
    aae_tree_status(System),
    io:format("~n"),
    aae_repair_status(ExchangeInfo).

dtrace([C, "-j" | R]) ->
    sniffle_console_dtrace:command(json, [C | R]);

dtrace([]) ->
    sniffle_console_dtrace:help(),
    ok;

dtrace(R) ->
    sniffle_console_dtrace:command(text, R).

vms([C, "-j" | R]) ->
    sniffle_console_vms:command(json, [C | R]);

vms([]) ->
    sniffle_console_vms:help(),
    ok;

vms(R) ->
    sniffle_console_vms:command(text, R).

hvs([C, "-j" | R]) ->
    sniffle_console_hypervisors:command(json, [C | R]);

hvs([]) ->
    sniffle_console_hypervisors:help(),
    ok;

hvs(R) ->
    sniffle_console_hypervisors:command(text, R).

pkgs([C, "-j" | R]) ->
    sniffle_console_packages:command(json, [C | R]);

pkgs([]) ->
    sniffle_console_packages:help(),
    ok;

pkgs(R) ->
    sniffle_console_packages:command(text, R).

ds([C, "-j" | R]) ->
    sniffle_console_datasets:command(json, [C | R]);

ds([]) ->
    sniffle_console_datasets:help(),
    ok;

ds(R) ->
    sniffle_console_datasets:command(text, R).

ips([]) ->
    sniffle_console_ipranges:help(),
    ok;

ips([C, "-j" | R]) ->
    sniffle_console_ipranges:command(json, [C | R]);

ips(R) ->
    sniffle_console_ipranges:command(text, R).

networks([]) ->
    sniffle_console_networks:help(),
    ok;

networks([C, "-j" | R]) ->
    sniffle_console_networks:command(json, [C | R]);

networks(R) ->
    sniffle_console_networks:command(text, R).

config(["show"]) ->
    io:format("Storage~n  General Section~n"),
    fifo_console:print_config(<<"storage">>, <<"general">>),
    io:format("  S3 Section~n"),
    fifo_console:print_config(<<"storage">>, <<"s3">>),
    io:format("Network~n  HTTP~n"),
    fifo_console:print_config(<<"network">>, <<"http">>),
    ok;

config(["set", Ks, V]) ->
    Ks1 = [binary_to_list(K) || K <- re:split(Ks, "\\.")],
    config(["set" | Ks1] ++ [V]);

config(["set" | R]) ->
    [K1, K2, K3, V] = R,
    Ks = [K1, K2, K3],
    case sniffle_opt:set(Ks, V) of
        {invalid, key, K} ->
            io:format("Invalid key: ~p~n", [K]),
            error;
        {invalid, type, T} ->
            io:format("Invalid type: ~p~n", [T]),
            error;
        _ ->
            io:format("Setting changed~n", []),
            ok
    end;

config(["unset", Ks]) ->
    Ks1 = [binary_to_list(K) || K <- re:split(Ks, "\\.")],
    config(["unset" | Ks1]);

config(["unset" | Ks]) ->
    sniffle_opt:unset(Ks),
    io:format("Setting changed~n", []),
    ok.

%%%===================================================================
%%% From riak_kv/riak_console
%%%===================================================================

join([NodeStr]) ->
    join(NodeStr, fun riak_core:join/1,
         "Sent join request to ~s~n", [NodeStr]).

staged_join([NodeStr]) ->
    Node = list_to_atom(NodeStr),
    join(NodeStr, fun riak_core:staged_join/1,
         "Success: staged join request for ~p to ~p~n", [node(), Node]).

join(NodeStr, JoinFn, SuccessFmt, SuccessArgs) ->
    try
        case JoinFn(NodeStr) of
            ok ->
                io:format(SuccessFmt, SuccessArgs),
                ok;
            {error, not_reachable} ->
                io:format("Node ~s is not reachable!~n", [NodeStr]),
                error;
            {error, different_ring_sizes} ->
                io:format("Failed: ~s has a different ring_creation_size~n",
                          [NodeStr]),
                error;
            {error, unable_to_get_join_ring} ->
                io:format("Failed: Unable to get ring from ~s~n", [NodeStr]),
                error;
            {error, not_single_node} ->
                io:format("Failed: This node is already a member of a "
                          "cluster~n"),
                error;
            {error, self_join} ->
                io:format("Failed: This node cannot join itself in a "
                          "cluster~n"),
                error;
            {error, _} ->
                io:format("Join failed. Try again in a few moments.~n", []),
                error
        end
    catch
        Exception:Reason ->
            lager:error("Join failed ~p:~p", [Exception, Reason]),
            io:format("Join failed, see log for details~n"),
            error
    end.

leave([]) ->
    try
        case riak_core:leave() of
            ok ->
                io:format("Success: ~p will shutdown after handing off "
                          "its data~n", [node()]),
                ok;
            {error, already_leaving} ->
                io:format("~p is already in the process of leaving the "
                          "cluster.~n", [node()]),
                ok;
            {error, not_member} ->
                io:format("Failed: ~p is not a member of the cluster.~n",
                          [node()]),
                error;
            {error, only_member} ->
                io:format("Failed: ~p is the only member.~n", [node()]),
                error
        end
    catch
        Exception:Reason ->
            lager:error("Leave failed ~p:~p", [Exception, Reason]),
            io:format("Leave failed, see log for details~n"),
            error
    end.

remove([Node]) ->
    try
        case riak_core:remove(list_to_atom(Node)) of
            ok ->
                io:format("Success: ~p removed from the cluster~n", [Node]),
                ok;
            {error, not_member} ->
                io:format("Failed: ~p is not a member of the cluster.~n",
                          [Node]),
                error;
            {error, only_member} ->
                io:format("Failed: ~p is the only member.~n", [Node]),
                error
        end
    catch
        Exception:Reason ->
            lager:error("Remove failed ~p:~p", [Exception, Reason]),
            io:format("Remove failed, see log for details~n"),
            error
    end.

down([Node]) ->
    try
        case riak_core:down(list_to_atom(Node)) of
            ok ->
                io:format("Success: ~p marked as down~n", [Node]),
                ok;
            {error, is_up} ->
                io:format("Failed: ~s is up~n", [Node]),
                error;
            {error, not_member} ->
                io:format("Failed: ~p is not a member of the cluster.~n",
                          [Node]),
                error;
            {error, only_member} ->
                io:format("Failed: ~p is the only member.~n", [Node]),
                error
        end
    catch
        Exception:Reason ->
            lager:error("Down failed ~p:~p", [Exception, Reason]),
            io:format("Down failed, see log for details~n"),
            error
    end.

-spec(status([]) -> ok).
status([]) ->
    try
        Stats = riak_kv_status:statistics(),
	StatString = format_stats(Stats,
                    ["-------------------------------------------\n",
		     io_lib:format("1-minute stats for ~p~n",[node()])]),
	io:format("~s\n", [StatString])
    catch
        Exception:Reason ->
            lager:error("Status failed ~p:~p", [Exception,
                    Reason]),
            io:format("Status failed, see log for details~n"),
            error
    end.

-spec(vnode_status([]) -> ok).
vnode_status([]) ->
    try
        case riak_kv_status:vnode_status() of
            [] ->
                io:format("There are no active vnodes.~n");
            Statuses ->
                io:format("~s~n-------------------------------------------~n~n",
                          ["Vnode status information"]),
                print_vnode_statuses(lists:sort(Statuses))
        end
    catch
        Exception:Reason ->
            lager:error("Backend status failed ~p:~p", [Exception,
                    Reason]),
            io:format("Backend status failed, see log for details~n"),
            error
    end.

reip([OldNode, NewNode]) ->
    try
        %% reip is called when node is down (so riak_core_ring_manager is not running),
        %% so it has to use the basic ring operations.
        %%
        %% Do *not* convert to use riak_core_ring_manager:ring_trans.
        %%
        case application:load(riak_core) of
            %% a process, such as cuttlefish, may have already loaded riak_core
            {error,{already_loaded,riak_core}} -> ok;
            ok -> ok
        end,
        RingStateDir = app_helper:get_env(riak_core, ring_state_dir),
        {ok, RingFile} = riak_core_ring_manager:find_latest_ringfile(),
        BackupFN = filename:join([RingStateDir, filename:basename(RingFile)++".BAK"]),
        {ok, _} = file:copy(RingFile, BackupFN),
        io:format("Backed up existing ring file to ~p~n", [BackupFN]),
        Ring = riak_core_ring_manager:read_ringfile(RingFile),
        NewRing = riak_core_ring:rename_node(Ring, OldNode, NewNode),
        ok = riak_core_ring_manager:do_write_ringfile(NewRing),
        io:format("New ring file written to ~p~n",
            [element(2, riak_core_ring_manager:find_latest_ringfile())])
    catch
        Exception:Reason ->
            io:format("Reip failed ~p:~p", [Exception, Reason]),
            error
    end.

%% Check if all nodes in the cluster agree on the partition assignment
-spec(ringready([]) -> ok | error).
ringready([]) ->
    try
        case riak_core_status:ringready() of
            {ok, Nodes} ->
                io:format("TRUE All nodes agree on the ring ~p\n", [Nodes]);
            {error, {different_owners, N1, N2}} ->
                io:format("FALSE Node ~p and ~p list different partition owners\n", [N1, N2]),
                error;
            {error, {nodes_down, Down}} ->
                io:format("FALSE ~p down.  All nodes need to be up to check.\n", [Down]),
                error
        end
    catch
        Exception:Reason ->
            lager:error("Ringready failed ~p:~p", [Exception,
                    Reason]),
            io:format("Ringready failed, see log for details~n"),
            error
    end.

cluster_info([OutFile|Rest]) ->
    try
        case lists:reverse(atomify_nodestrs(Rest)) of
            [] ->
                cluster_info:dump_all_connected(OutFile);
            Nodes ->
                cluster_info:dump_nodes(Nodes, OutFile)
        end
    catch
        error:{badmatch, {error, eacces}} ->
            io:format("Cluster_info failed, permission denied writing to ~p~n", [OutFile]);
        error:{badmatch, {error, enoent}} ->
            io:format("Cluster_info failed, no such directory ~p~n", [filename:dirname(OutFile)]);
        error:{badmatch, {error, enotdir}} ->
            io:format("Cluster_info failed, not a directory ~p~n", [filename:dirname(OutFile)]);
        Exception:Reason ->
            lager:error("Cluster_info failed ~p:~p",
                [Exception, Reason]),
            io:format("Cluster_info failed, see log for details~n"),
            error
    end.

ensemble_status([]) ->
    sniffle_ensemble_console:ensemble_overview();
ensemble_status(["root"]) ->
    sniffle_ensemble_console:ensemble_detail(root);
ensemble_status([Str]) ->
    N = parse_int(Str),
    case N of
        undefined ->
            io:format("No such ensemble: ~s~n", [Str]);
        _ ->
            sniffle_ensemble_console:ensemble_detail(N)
    end.

parse_int(IntStr) ->
    try
        list_to_integer(IntStr)
    catch
        error:badarg ->
            undefined
    end.



format_stats([], Acc) ->
    lists:reverse(Acc);
format_stats([{Stat, V}|T], Acc) ->
    format_stats(T, [io_lib:format("~p : ~p~n", [Stat, V])|Acc]).

atomify_nodestrs(Strs) ->
    lists:foldl(fun("local", Acc) -> [node()|Acc];
                   (NodeStr, Acc) -> try
                                         [list_to_existing_atom(NodeStr)|Acc]
                                     catch error:badarg ->
                                         io:format("Bad node: ~s\n", [NodeStr]),
                                         Acc
                                     end
                end, [], Strs).

print_vnode_statuses([]) ->
    ok;
print_vnode_statuses([{VNodeIndex, StatusData} | RestStatuses]) ->
    io:format("VNode: ~p~n", [VNodeIndex]),
    print_vnode_status(StatusData),
    io:format("~n"),
    print_vnode_statuses(RestStatuses).

print_vnode_status([]) ->
    ok;
print_vnode_status([{backend_status,
                     Backend,
                     StatusItem} | RestStatusItems]) ->
    if is_binary(StatusItem) ->
            StatusString = binary_to_list(StatusItem),
            io:format("Backend: ~p~nStatus: ~n~s~n",
                      [Backend, string:strip(StatusString)]);
       true ->
            io:format("Backend: ~p~nStatus: ~n~p~n",
                      [Backend, StatusItem])
    end,
    print_vnode_status(RestStatusItems);
print_vnode_status([StatusItem | RestStatusItems]) ->
    if is_binary(StatusItem) ->
            StatusString = binary_to_list(StatusItem),
            io:format("Status: ~n~s~n",
                      [string:strip(StatusString)]);
       true ->
            io:format("Status: ~n~p~n", [StatusItem])
    end,
    print_vnode_status(RestStatusItems).

%%%===================================================================
%%% Private
%%%===================================================================



pp_json(Obj) ->
    io:format("~s~n", [jsx:prettify(jsx:encode(Obj))]).


aae_exchange_status(ExchangeInfo) ->
    io:format("~s~n", [string:centre(" Exchanges ", 79, $=)]),
    io:format("~-49s  ~-12s  ~-12s~n", ["Index", "Last (ago)", "All (ago)"]),
    io:format("~79..-s~n", [""]),
    [begin
         Now = os:timestamp(),
         LastStr = format_timestamp(Now, LastTS),
         AllStr = format_timestamp(Now, AllTS),
         io:format("~-49b  ~-12s  ~-12s~n", [Index, LastStr, AllStr]),
         ok
     end || {Index, LastTS, AllTS, _Repairs} <- ExchangeInfo],
    ok.

aae_repair_status(ExchangeInfo) ->
    io:format("~s~n", [string:centre(" Keys Repaired ", 79, $=)]),
    io:format("~-49s  ~s  ~s  ~s~n", ["Index",
                                      string:centre("Last", 8),
                                      string:centre("Mean", 8),
                                      string:centre("Max", 8)]),
    io:format("~79..-s~n", [""]),
    [begin
         io:format("~-49b  ~s  ~s  ~s~n",
                   [Index,
                    string:centre(integer_to_list(Last), 8),
                    string:centre(integer_to_list(Mean), 8),
                    string:centre(integer_to_list(Max), 8)]),
         ok
     end || {Index, _, _, {Last,_Min,Max,Mean}} <- ExchangeInfo],
    ok.

aae_tree_status(System) ->
    TreeInfo = riak_core_entropy_info:compute_tree_info(System),
    io:format("~s~n", [string:centre(" Entropy Trees ", 79, $=)]),
    io:format("~-49s  Built (ago)~n", ["Index"]),
    io:format("~79..-s~n", [""]),
    [begin
         Now = os:timestamp(),
         BuiltStr = format_timestamp(Now, BuiltTS),
         io:format("~-49b  ~s~n", [Index, BuiltStr]),
         ok
     end || {Index, BuiltTS} <- TreeInfo],
    ok.

format_timestamp(_Now, undefined) ->
    "--";
format_timestamp(Now, TS) ->
    riak_core_format:human_time_fmt("~.1f", timer:now_diff(Now, TS)).


do_update(MainMod, StateMod) ->
    {ok, US} = MainMod:list_(),
    io:format("  Entries found: ~p~n", [length(US)]),

    io:format("  Grabbing UUIDs"),
    US1 = [begin
               io:format("."),
               {StateMod:uuid(ft_obj:val(U)), ft_obj:update(U)}
           end|| U <- US],
    io:format(" done.~n"),

    io:format("  Wipeing old entries"),
    [begin
         io:format("."),
         MainMod:wipe(UUID)
     end || {UUID, _} <- US1],
    io:format(" done.~n"),

    io:format("  Restoring entries"),
    [begin
         io:format("."),
         MainMod:sync_repair(UUID, O)
     end || {UUID, O} <- US1],
    io:format(" done.~n"),
    io:format("Update complete.~n"),
    ok.
