%%%-------------------------------------------------------------------
%%% @doc
%%% Shards Pool Supervisor.
%%% @end
%%%-------------------------------------------------------------------
-module(shards_sup).

-behavior(supervisor).

%% API
-export([
  start_link/0,
  start_link/1,
  start_child/3,
  terminate_child/2
]).

%% Supervisor callbacks
-export([init/1]).

%%%===================================================================
%%% API
%%%===================================================================

%% @equiv start_link(?MODULE)
start_link() ->
  start_link(?MODULE).

-spec start_link(Name :: atom()) -> supervisor:startlink_ret().
start_link(Name) ->
  supervisor:start_link({local, Name}, ?MODULE, {Name}).

%%%===================================================================
%%% shards_supervisor callbacks
%%%===================================================================

-spec start_child(SupName, TabName, Options) -> Return when
  SupName :: atom(),
  TabName :: atom(),
  Options :: [shards_local:option()],
  Return  :: supervisor:startchild_ret().
start_child(SupName, TabName, Options) ->
  supervisor:start_child(SupName, [TabName, Options]).

-spec terminate_child(SupName, Tab) -> Return when
  SupName :: atom(),
  Tab     :: pid() | atom(),
  Error   :: not_found | simple_one_for_one,
  Return  :: ok | {error, Error}.
terminate_child(SupName, Tab) when is_atom(Tab) ->
  terminate_child(SupName, shards_lib:get_pid(Tab));
terminate_child(SupName, Tab) when is_pid(Tab) ->
  supervisor:terminate_child(SupName, Tab).

%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================

%% @hidden
init({Name}) ->
  ChildSpec = shards_owner_sup:child_spec(Name),
  {ok, {{simple_one_for_one, 10, 10}, [ChildSpec]}}.
