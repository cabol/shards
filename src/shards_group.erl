%%%-------------------------------------------------------------------
%%% @doc
%%% This module provides a feature for partitioned table groups.
%%% A group is represented by a dynamic supervisor so that the
%%% tables can be added or removed dynamically at runtime.
%%%
%%% <h2>Usage</h2>
%%% To use `shards_group' and make the created tables part of
%%% your application supervision tree, you have to add the
%%% group supervisor to the children list:
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

%% @doc Starts a new group with the name `Name'.
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

%% @doc Stops the group given by `SupRef'.
-spec stop(SupRef, Timeout) -> ok when
      SupRef  :: atom() | pid(),
      Timeout :: timeout().
stop(SupRef, Timeout) ->
  gen_server:stop(SupRef, normal, Timeout).

%% @doc Builds a child specification with the given `Name'.
-spec child_spec(Name) -> ChildSpec when
      Name      :: atom() | undefined,
      ChildSpec :: supervisor:child_spec().
child_spec(Name) ->
  #{
    id    => Name,
    start => {?MODULE, start_link, [Name]},
    type  => supervisor
  }.

%% @doc
%% Dynamically creates a new partitioned table named `TabName' and adds that
%% table to the supervisor `SupRef'.
%% @end
-spec new_table(SupRef, TabName, Options) -> {ok, TabPid, Tab} | {error, Reason} when
      SupRef  :: atom() | pid(),
      TabName :: atom(),
      Options :: [shards:option()],
      TabPid  :: pid(),
      Tab     :: shards:tab(),
      Reason  :: term().
new_table(SupRef, TabName, Options) when is_atom(SupRef); is_pid(SupRef) ->
  supervisor:start_child(SupRef, [TabName, Options]).

%% @doc
%% Deletes the given table `Tab` and removes it from the supervisor `SupRef'.
%% @end
-spec del_table(SupRef, Tab) -> ok | {error, Reason} when
      SupRef :: atom(),
      Tab    :: shards:tab(),
      Reason :: not_found | simple_one_for_one.
del_table(SupRef, Tab) when is_atom(SupRef); is_pid(SupRef) ->
  TabPid = shards_meta:get_owner(Tab),
  supervisor:terminate_child(SupRef, TabPid).

%% Helper for starting the table
%% @hidden
start_table(Name, Opts) ->
  Tab = shards:new(Name, Opts),
  Pid = shards_meta:get_owner(Tab),

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
    id       => {?MODULE, Name},
    start    => {?MODULE, start_table, []},
    type     => supervisor,
    restart  => permanent,
    shutdown => infinity
  }.
