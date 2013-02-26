%%%-------------------------------------------------------------------
%%% @author Heinz Nikolaus Gies <heinz@licenser.net>
%%% @copyright (C) 2013, Heinz Nikolaus Gies
%%% @doc
%%%
%%% @end
%%% Created : 10 Jan 2013 by Heinz Nikolaus Gies <heinz@licenser.net>
%%%-------------------------------------------------------------------
-module(sniffle_dtrace_server).

-behaviour(gen_server).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% API
-export([
         run/3,
         start_link/1
        ]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-ignore_xref([
              start_link/1,
              run/3
             ]).

-record(state, {data = [],
                servers,
                script,
                compiled,
                runners,
                listeners = []}).

-include("hanoidb.hrl").

%%%===================================================================
%%% API
%%%===================================================================

run(ID, Config, Listener) ->
    case global:whereis_name({dtrace, ID}) of
        undefined ->
            sniffle_dtrace_sup:start_child([ID, Config, Listener]);
        Pid ->
            gen_server:cast(Pid, {listen, Listener}),
            {ok, Pid}
    end.


%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link([ID, Config, Listener]) ->
    gen_server:start_link({global, {dtrace, ID}}, ?MODULE, [ID, Config, Listener], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
init([ID, Config, Listener]) ->
    {ok, ScriptObj} = sniffle_dtrace:get(ID),
    {ok, Servers} = jsxd:get(<<"servers">>, Config),
    {ok, Script} = generate_script(ScriptObj, Config),
    Servers1 = [{binary_to_list(jsxd:get(<<"host">>, <<"">>, S1)),
                 jsxd:get(<<"port">>, 4200, S1)}
                || {ok, S1} <- [sniffle_hypervisor:get(S0) || S0 <- Servers]],
    Runners = [ {L, Host, Port} ||
                  {{ok, L}, Host, Port} <-
                      [{libchunter_dtrace_server:dtrace(Host, Port, Script), Host, Port}
                       || {Host, Port} <- Servers1]],
    erlang:monitor(process, Listener),
    timer:send_interval(1000, tick),
    {ok, #state{runners = Runners,
                servers = Servers1,
                listeners = [Listener],
                script = ScriptObj,
                compiled = Script}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @spec handle_call(Request, From, State) ->
%%                                   {reply, Reply, State} |
%%                                   {reply, Reply, State, Timeout} |
%%                                   {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, Reply, State} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------

handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%                                  {noreply, State, Timeout} |
%%                                  {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------

handle_cast({listen, Listener}, State) ->
    erlang:monitor(process, Listener),
    {noreply, State};

handle_cast(_Msg, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_info(tick, State) ->
    {Composed, Runners} = lists:foldr(fun({S, Host, Port} = D, {Data, RunA}) ->
                                   case libchunter_dtrace_server:walk(S) of
                                       ok ->
                                           {Data, [D | RunA]};
                                       {ok, R} ->
                                           {jsxd:merge(fun merge_fn/3, Data, to_jsxd(R)), [D | RunA]};
                                       E ->
                                           lager:error("DTrace host (~p) died with: ~p.", [S, E]),
                                           libchunter_dtrace_server:close(S),
                                           {ok, S1} = libchunter_dtrace_server:dtrace(Host, Port, State#state.compiled),
                                           {Data, [{S1, Host, Port} | RunA]}
                                   end
                           end, {[], []}, State#state.runners),
    [Pid ! {dtrace, Composed} || Pid <- State#state.listeners],
    {noreply, State#state{runners = Runners}};

handle_info({'DOWN', _Ref, process, _Pid, _Reason}, State = #state{ listeners = []}) ->
    {stop, normal, State};

handle_info({'DOWN', _Ref, process, Pid, _Reason}, State = #state{ listeners = [Pid]}) ->
    {stop, normal, State};

handle_info({'DOWN', _Ref, process, Pid, _Reason}, State = #state{ listeners = Ls}) ->
    {stop, normal, State#state{listeners = lists:delete(Pid, Ls)}};

handle_info(_Info, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(_Reason, #state{runners = Runners} = _State) ->

    [ libchunter_dtrace_server:close(R) || {R, _,_} <- Runners],
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

merge_fn(_, A, B) when is_integer(A),
                       is_integer(B) ->
    A + B;

merge_fn(_, A, B) when is_list(A),
                       is_list(B) ->
    jsxd:merge(fun merge_fn/3, A, B);

merge_fn(_, A, _B) when is_list(A) ->
    A;

merge_fn(_, _A, B) when is_list(B) ->
    B.

to_jsxd(Data) ->
    lists:foldr(fun ({_, Path, Vals}, Obj) ->
                        BPath = lists:map(fun(L) when is_list(L) ->
                                                  list_to_binary(L);
                                             (B) when is_binary(B) ->
                                                  B;
                                             (N) when is_number(N) ->
                                                  list_to_binary(integer_to_list(N))
                                          end, Path),
                        lists:foldr(fun({{Start, End}, Value}, Obj1) ->
                                            B = list_to_binary(io_lib:format("~p-~p", [Start, End])),
                                            jsxd:set(BPath ++ [B], Value, Obj1)
                                    end, Obj, Vals)
                end, [], Data).


generate_script(ScriptObj, Config) ->
    Script = case jsxd:get(<<"script">>, ScriptObj) of
                 {ok, S} when is_binary(S) ->
                     binary_to_list(S);
                 {ok, S} when is_list(S) ->
                     S
             end,
    Config0 = jsxd:get(<<"config">>, [], ScriptObj),
    Config1 = jsxd:merge(Config, Config0),

    Filter = jsxd:get(<<"filter">>, [], Config1),
    Config2 = case compile_filters(Filter) of
                  <<>> ->
                      jsxd:thread([{set, <<"filter">>, <<>>},
                                   {set, <<"partial_filter">>, <<"1==1">>}],
                                  Config1);
                  F ->
                      jsxd:thread([{set, <<"filter">>, <<"/", F/binary, "/">>},
                                   {set, <<"partial_filter">>, F}],
                                  Config1)
              end,
    {ok, Script1} = sgte:compile(Script),
    Script2 = sgte:render_str(Script1, Config2),
    {ok, Script2}.

compile_filters(Filters) ->
    Fs0 = lists:map(fun filter_matcher/1, Filters),
    case join_filter(Fs0, <<"&&">>) of
        [] ->
            <<>>;
        Fs1 ->
            Fs2 = iolist_to_binary(Fs1),
            Fs2
    end.

join_filter([_] = L, _) ->
    L;

join_filter([], _) ->
    [];

join_filter([E | R], S) ->
    [E, S | join_filter(R, S)].


filter_matcher({E, [V]}) ->
    filter_matcher({E, [<<"==">>, V]});

filter_matcher({<<"args">> = E, [I, V]}) when is_integer(I) ->
    filter_matcher({E, [I, <<"==">>, V]});

filter_matcher({<<"arg">> = E, [I, V]}) when is_integer(I) ->
    filter_matcher({E, [I, <<"==">>, V]});

filter_matcher({<<"pid">>, [Cmp, P]}) when is_number(P)->
    [<<"pid">>, Cmp, format_value(P)];

filter_matcher({<<"execname">>, [Cmp, P]}) when is_binary(P)->
    [<<"execname">>, Cmp,  format_value(P)];

filter_matcher({<<"provider">>, [Cmp, P]}) when is_binary(P)->
    [<<"probeprov">>, Cmp,  format_value(P)];

filter_matcher({<<"module">>, [Cmp, P]}) when is_binary(P)->
    [<<"probemod">>, Cmp,  format_value(P)];

filter_matcher({<<"probe">>, [Cmp, P]}) when is_binary(P)->
    [<<"probename">>, Cmp,  format_value(P)];

filter_matcher({<<"zonename">>, [Cmp, P]}) when is_binary(P)->
    [<<"zonename">>, Cmp,  format_value(P)];

filter_matcher({<<"args">>, [I, Cmp, V]}) when is_integer(I) ->
    [<<"args[">>, format_value(I), "]", Cmp, format_value(V)];

filter_matcher({<<"arg">>, [I, Cmp, V]}) when is_integer(I),
                                       I >= 0,
                                       I =< 9->
    [<<"arg">>, format_value(I), Cmp, format_value(V)];

filter_matcher({<<"custom">>, C}) when is_binary(C)->
    C.

format_value(V) when is_integer(V) ->
    integer_to_list(V);

format_value(V) when is_float(V) ->
    io_lib:format("~p", [V]);

format_value(V) when is_binary(V) ->
    <<"\"", V/binary, "\"">>.

-ifdef(TEST).

join_filter_test() ->
    ?assertEqual([], join_filter([], s)),
    ?assertEqual([a], join_filter([a], s)),
    ?assertEqual([a, s, b], join_filter([a, b], s)),
    ?assertEqual([a, s, b, s, c], join_filter([a, b, c], s)).

iolist_test() ->
    ?assertEqual(<<"a&&b">>, iolist_to_binary([<<"a">>, <<"&&">>, <<"b">>])).

compile_filters_test() ->
    ?assertEqual(<<>>, compile_filters([])),
    ?assertEqual(<<"a==1">>, compile_filters([{<<"custom">>, <<"a==1">>}])),
    ?assertEqual(<<"a==1&&b==2">>, compile_filters([{<<"custom">>, <<"a==1">>}, {<<"custom">>, <<"b==2">>}])).

fmt_h(V) ->
    R =format_value(V),
    iolist_to_binary(R).

format_value_test() ->
    ?assertEqual(<<"1">>, fmt_h(1)),
    ?assertEqual(<<"1.0">>, fmt_h(1.0)),
    ?assertEqual(<<"\"1\"">>, fmt_h(<<"1">>)).

fm_helper(F) ->
    R = filter_matcher(F),
    io:format("~p->~p~n", [F, R]),

    iolist_to_binary(R).

filter_matcher_correct_test() ->
    ?assertEqual(<<"pid==1">>, fm_helper({<<"pid">>, [1]})),
    ok.

-endif.
