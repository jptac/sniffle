-module(sniffle_dtrace).
-define(CMD, sniffle_dtrace_cmd).
-define(BUCKET, <<"dtrace">>).
-define(S, ft_dtrace).
-include("sniffle.hrl").

-define(FM(Met, Mod, Fun, Args),
        folsom_metrics:histogram_timed_update(
          {sniffle, dtrace, Met},
          Mod, Fun, Args)).

-export(
   [
    add/2,
    delete/1,
    name/2,
    uuid/2,
    script/2,
    set_metadata/2,
    set_config/2
   ]
  ).

%%%===================================================================
%%% General section
%%%===================================================================
-spec get(UUID::fifo:dtrace_id()) ->
                 not_found | {ok, DTrance::fifo:dtrace()} | {error, timeout}.

-spec list() ->
                  {ok, [UUID::fifo:dtrace_id()]} | {error, timeout}.

-spec list([fifo:matcher()], boolean()) ->
                  {error, timeout} |
                  {ok, [{integer(), fifo:uuid() | fifo:dtrace()}]}.


-include("sniffle_api.hrl").
%%%===================================================================
%%% Custom section
%%%===================================================================


-spec add(Name::binary(),
          Script::string()) ->
                 {ok, UUID::fifo:dtrace_id()} | {error, timeout}.
add(Name, Script) ->
    UUID = fifo_utils:uuid(dtrace),
    do_write(UUID, create, [Name, Script]),
    {ok, UUID}.

-spec delete(UUID::fifo:dtrace_id()) ->
                    not_found | {error, timeout} | ok.
delete(UUID) ->
    do_write(UUID, delete).

%%--------------------------------------------------------------------
%% @doc Lists all vm's and fiters by a given matcher set.
%% @end
%%--------------------------------------------------------------------

?SET(name).
?SET(uuid).
?SET(script).
?SET(set_metadata).
?SET(set_config).
