%%%-------------------------------------------------------------------
%%% @doc
%%% This module provides a dynamic supervisor for creating and/or
%%% deleting tables dynamically in runtime and as part of an existing
%%% supervision tree; any application supervision tree using `shards'.
%%%
%%% <h2>Usage</h2>
%%% To use `shards' and make the creates tables part of your
%%% application supervision tree, you have to add to your main
%%% supervisor:
%%%
%%% ```
%%% % Supervisor init callback
%%% init(GroupName) ->
%%%   Children = [
%%%     shards_group:child_spec(GroupName)
%%%   ],
%%%
%%%   {ok, {{one_for_one, 10, 10}, Children}}.
%%% '''
%%%
%%% After your application starts, you can create and delete tables
%%% like so:
%%%
%%% ```
%%% > {ok, Pid, Tab} = shards_group:new_table(GroupName, mytab, [named_table]).
%%% {ok,<0.194.0>,#Ref<0.3052443831.2753691659.260835>}
%%%
%%% > shards_group:del_table(GroupName, mytab).
%%% ok
%%% '''
%%% @end
%%%-------------------------------------------------------------------
-module(shards_group).

-behavior(supervisor).

%% API
-export([
  start_link/0,
  start_link/1,
  stop/1,
  stop/2,
  child_spec/1,
  new_table/3,
  del_table/2,
  start_table/2
]).

%% Supervisor callbacks
-export([init/1]).

%%%===================================================================
%%% API
%%%===================================================================

%% @equiv start_link(shards_group)
start_link() ->
  start_link(?MODULE).

-spec start_link(Name) -> StartRet when
      Name     :: atom() | undefined,
      StartRet :: {ok, pid()} | {error, term()}.
start_link(undefined) ->
  supervisor:start_link(?MODULE, {?MODULE});
start_link(Name) ->
  supervisor:start_link({local, Name}, ?MODULE, {Name}).

%% @equiv stop(Pid, 5000)
stop(Pid) ->
  stop(Pid, 5000).

-spec stop(SupRef, Timeout) -> ok when
      SupRef  :: atom() | pid(),
      Timeout :: timeout().
stop(SupRef, Timeout) ->
  gen_server:stop(SupRef, normal, Timeout).

-spec child_spec(Name) -> ChildSpec when
      Name      :: atom() | undefined,
      ChildSpec :: supervisor:child_spec().
child_spec(Name) ->
  #{
    id    => Name,
    start => {?MODULE, start_link, [Name]},
    type  => supervisor
  }.

-spec new_table(SupRef, TabName, Options) -> {ok, TabPid, Tab} | {error, Reason} when
      SupRef  :: atom() | pid(),
      TabName :: atom(),
      Options :: [shards:option()],
      TabPid  :: pid(),
      Tab     :: shards:tab(),
      Reason  :: term().
new_table(SupRef, TabName, Options) when is_atom(SupRef); is_pid(SupRef) ->
  supervisor:start_child(SupRef, [TabName, Options]).

-spec del_table(SupRef, Tab) -> ok | {error, Reason} when
      SupRef :: atom(),
      Tab    :: shards:tab(),
      Reason :: not_found | simple_one_for_one.
del_table(SupRef, Tab) when is_atom(SupRef); is_pid(SupRef) ->
  TabPid = shards_meta:tab_pid(Tab),
  supervisor:terminate_child(SupRef, TabPid).

%% Helper for starting the table
%% @hidden
start_table(Name, Opts) ->
  Tab = shards:new(Name, Opts),
  Pid = shards_meta:tab_pid(Tab),
  {ok, Pid, Tab}.

%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================

%% @hidden
init({Name}) ->
  ChildSpec = partitioned_table_spec(Name),
  {ok, {{simple_one_for_one, 10, 10}, [ChildSpec]}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
partitioned_table_spec(Name) ->
  #{
    id       => Name,
    start    => {?MODULE, start_table, []},
    type     => supervisor,
    restart  => permanent,
    shutdown => infinity
  }.
