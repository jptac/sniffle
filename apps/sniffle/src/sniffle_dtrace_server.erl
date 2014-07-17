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

%%%===================================================================
%%% API
%%%===================================================================

run(ID, Config, Listener) ->
    sniffle_dtrace_sup:start_child([ID, Config, Listener]).


%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link([ID, Config, Listener]) ->
    gen_server:start_link(?MODULE, [ID, Config, Listener], []).

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
    {ok, ScriptObj} = sniffle_dtrace:get_(ID),
    {ok, Servers} = jsxd:get(<<"servers">>, Config),
    {ok, Script} = generate_script(ScriptObj, Config),
    Servers1 = [ft_hypervisor:endpoint(S1)
                || {ok, S1} <- [sniffle_hypervisor:get_(S0) || S0 <- Servers]],
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
                                   case libchunter_dtrace_server:walk(S, llquantize) of
                                       ok ->
                                           {Data, [D | RunA]};
                                       {ok, R} ->
                                           {jsxd:merge(fun merge_fn/3, Data, R), [D | RunA]};
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

generate_script(ScriptObj, Config) ->
    Script = case ft_dtrace:script(ScriptObj) of
                 S when is_binary(S) ->
                     binary_to_list(S);
                 S when is_list(S) ->
                     S
             end,
    Config0 = ft_dtrace:config(ScriptObj),
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


compile_filters(F) ->
    iolist_to_binary(compile_filters_(F)).

compile_filters_([{<<"or">>,  Filters}]) ->
    Fs0 = compile_filters_(Filters),
    [<<"(">>, join_filter(Fs0, <<"||">>), <<")">>];

compile_filters_([{<<"and">>,  Filters}]) ->
    Fs0 = compile_filters_(Filters),
    [<<"(">>, join_filter(Fs0, <<"&&">>), <<")">>];

compile_filters_(Filters) ->
    lists:map(fun filter_matcher/1, Filters).

join_filter([_] = L, _) ->
    L;

join_filter([], _) ->
    [];

join_filter([E | R], S) ->
    [E, S | join_filter(R, S)].

filter_matcher([{_, _}] = F) ->
    compile_filters(F);

filter_matcher([<<"custom">>, C]) when is_binary(C)->
    C;

filter_matcher([E, V]) ->
    filter_matcher([E, <<"==">>, V]);

filter_matcher([<<"args">> = E, I, V]) when is_integer(I) ->
    filter_matcher([E, I, <<"==">>, V]);

filter_matcher([<<"arg">> = E, I, V]) when is_integer(I) ->
    filter_matcher([E, I, <<"==">>, V]);

filter_matcher([<<"pid">>, Cmp, P]) when is_number(P)->
    [<<"pid">>, Cmp, format_value(P)];

filter_matcher([<<"execname">>, Cmp, P]) when is_binary(P)->
    [<<"execname">>, Cmp,  format_value(P)];

filter_matcher([<<"provider">>, Cmp, P]) when is_binary(P)->
    [<<"probeprov">>, Cmp,  format_value(P)];

filter_matcher([<<"module">>, Cmp, P]) when is_binary(P)->
    [<<"probemod">>, Cmp,  format_value(P)];

filter_matcher([<<"probe">>, Cmp, P]) when is_binary(P)->
    [<<"probename">>, Cmp,  format_value(P)];

filter_matcher([<<"zonename">>, Cmp, P]) when is_binary(P)->
    [<<"zonename">>, Cmp,  format_value(P)];

filter_matcher([<<"args">>, I, Cmp, V]) when is_integer(I) ->
    [<<"args[">>, format_value(I), "]", Cmp, format_value(V)];

filter_matcher([<<"arg">>, I, Cmp, V]) when is_integer(I),
                                       I >= 0,
                                       I =< 9->
    [<<"arg">>, format_value(I), Cmp, format_value(V)].


format_value(V) when is_integer(V) ->
    integer_to_list(V);

format_value(V) when is_float(V) ->
    io_lib:format("~p", [V]);

format_value(null = V) ->
    io_lib:format("~p", [V]);

format_value(true = V) ->
    io_lib:format("~p", [V]);

format_value(false = V) ->
    io_lib:format("~p", [V]);

format_value(V) when is_binary(V) ->
    <<"\"", V/binary, "\"">>.

-ifdef(TEST).

join_filter_test() ->
    ?assertEqual([], join_filter([], s)),
    ?assertEqual([a], join_filter([a], s)),
    ?assertEqual([a, s, b], join_filter([a, b], s)),
    ?assertEqual([a, s, b, s, c], join_filter([a, b, c], s)).

compile_filters_test() ->
    ?assertEqual(<<>>, compile_filters([])),
    ?assertEqual(<<"a==1">>, compile_filters([[<<"custom">>, <<"a==1">>]])),
    ?assertEqual(<<"(c==3&&b==2)">>, compile_filters([{<<"and">>,
                                                     [[<<"custom">>, <<"c==3">>],
                                                      [<<"custom">>, <<"b==2">>]]}])).

compile_nested_test() ->
    ?assertEqual(<<"(a==1&&(b==2||zonename==\"c\"))">>, compile_filters([{<<"and">>,
                                                     [[<<"custom">>, <<"a==1">>],
                                                      [{<<"or">>,
                                                        [[<<"custom">>, <<"b==2">>],
                                                         [<<"zonename">>, <<"c">>]]}]]}])).

fmt_h(V) ->
    R =format_value(V),
    iolist_to_binary(R).

format_value_test() ->
    ?assertEqual(<<"1">>, fmt_h(1)),
    ?assertEqual(<<"1.0">>, fmt_h(1.0)),
    ?assertEqual(<<"\"1\"">>, fmt_h(<<"1">>)).

fm_helper(F) ->
    R = filter_matcher(F),
    iolist_to_binary(R).

filter_matcher_correct_test() ->
    ?assertEqual(<<"pid==1">>, fm_helper([<<"pid">>, 1])),
    ok.

-endif.
