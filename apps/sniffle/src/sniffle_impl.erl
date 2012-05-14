-module(sniffle_impl).

%% @private

-type uuid() ::
	binary().

-type sniffle_call_machiens() ::
	{machines, list} |
	{machines, get, UUID :: uuid()} |
	{machines, info, UUID :: uuid()} |
	{machines, delete, UUID :: uuid()} |
	{machines, create, Name :: binary(), 
	 PackageUUID :: binary(), 
	 DatasetUUID :: uuid(), 
	 Metadata :: term(), 
	 Tags :: term()} |
	{machines, start, UUID :: uuid()} |
	{machines, start, UUID :: uuid(), ImageName :: binary()} |
	{machines, stop, UUID :: uuid()} |
	{machines, reboot, UUID :: uuid()}.

-type sniffle_call_packages() ::
	{packages, list}.

-type sniffle_call_datasets() ::
	{datasets, list}.

-type sniffle_call_images() ::
	{images, list}.

-type sniffle_call_keys() ::
	{keys, create, Auth :: term(), Pass :: term(),  KeyID :: term(), PublicKey :: binary()} | 
	{keys, list}.

-type sniffle_call() ::
	sniffle_call_machiens() |
	sniffle_call_packages() | 
	sniffle_call_datasets() |
	sniffle_call_images() |
	sniffle_call_keys().

-type sniffle_cast() ::
	term().

-type sniffle_info() ::
	timeout().
	
-callback init(UUID :: binary(), Host :: term()) ->
    {ok, State :: term()} | 
    {stop, Reason :: term()} | ignore.

-callback handle_call(Auth :: uuid(), Request :: sniffle_call(), From :: {pid(), Tag :: term()},
                      State :: term()) ->
    {reply, Reply :: term(), NewState :: term()} |
    {noreply, NewState :: term()} |
    {noreply, NewState :: term(), timeout() | hibernate} |
    {stop, Reason :: term(), Reply :: term(), NewState :: term()} |
    {stop, Reason :: term(), NewState :: term()}.

-callback handle_cast(Auth::uuid(), Request :: sniffle_cast(), State :: term()) ->
    {noreply, NewState :: term()} |
    {stop, Reason :: term(), NewState :: term()}.

-callback handle_info(Info :: sniffle_info() | term(), State :: term()) ->
    {noreply, NewState :: term()} |
    {noreply, NewState :: term(), timeout() | hibernate} |
    {stop, Reason :: term(), NewState :: term()}.

-callback terminate(Reason :: (normal | shutdown | {shutdown, term()} |
                               term()),
                    State :: term()) ->
    term().

-callback code_change(OldVsn :: (term() | {down, term()}), State :: term(),
                      Extra :: term()) ->
    {ok, NewState :: term()} | {error, Reason :: term()}.
