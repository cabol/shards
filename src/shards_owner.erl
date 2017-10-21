%%%-------------------------------------------------------------------
%%% @doc
%%% ETS owner.
%%% @end
%%%-------------------------------------------------------------------
-module(shards_owner).

-behaviour(gen_server).

%% API
-export([
  start_link/2,
  apply_ets_fun/3,
  stop/1
]).

%% gen_server callbacks
-export([
  init/1,
  handle_call/3,
  handle_cast/2,
  handle_info/2,
  terminate/2,
  code_change/3
]).

%%%===================================================================
%%% API
%%%===================================================================

-spec start_link(atom(), atom()) -> {ok, pid()} | ignore | {error, term()}.
start_link(Name, Options) ->
  gen_server:start_link({local, Name}, ?MODULE, {Name, Options}, []).

-spec apply_ets_fun(atom(), atom(), [term()]) -> term().
apply_ets_fun(Name, Fun, Args) ->
  gen_server:call(Name, {Fun, Args}).

-spec stop(atom()) -> ok.
stop(ShardName) ->
  true = exit(whereis(ShardName), normal),
  ok.

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%% @hidden
init({Name, Options}) ->
  _ = process_flag(trap_exit, true),

  NewOpts =
    case lists:keyfind(restore, 1, Options) of
      {restore, _, _} = Val -> Val;
      false                 -> Options
    end,

  init(Name, NewOpts).

%% @private
init(Name, {restore, ShardTabs, Options}) ->
  {Name, Filename} = lists:keyfind(Name, 1, ShardTabs),
  case ets:file2tab(Filename, Options) of
    {ok, _} -> {ok, #{name => Name, opts => []}};
    Error   -> {stop, {restore_error, Error}}
  end;
init(Name, Options) ->
  NewOpts = validate_options(Options),
  Name = ets:new(Name, NewOpts),
  {ok, #{name => Name, opts => NewOpts}}.

%% @private
validate_options(Options) ->
  Options1 =
    case lists:member(named_table, Options) of
      true -> Options;
      _    -> [named_table | Options]
    end,

  case lists:member(public, Options1) of
    true -> Options1;
    _    -> [public | Options1]
  end.

%% @hidden
handle_call({Fun, Args}, _From, #{name := Name} = State) ->
  Response = apply(ets, Fun, [Name | Args]),
  {reply, Response, State};
handle_call(_Request, _From, State) ->
  {reply, ok, State}.

%% @hidden
handle_cast(_Request, State) ->
  {noreply, State}.

%% @hidden
handle_info(_Info, State) ->
  {noreply, State}.

%% @hidden
terminate(_Reason, _State) ->
  ok.

%% @hidden
code_change(_OldVsn, State, _Extra) ->
  {ok, State}.
