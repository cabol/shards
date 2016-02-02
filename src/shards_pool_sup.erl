%%%-------------------------------------------------------------------
%%% @doc
%%% Shards Pool Supervisor.
%%% @end
%%%-------------------------------------------------------------------
-module(shards_pool_sup).

-behavior(supervisor).

%% API
-export([
  start_link/0,
  start_child/1,
  terminate_child/2
]).

%% Supervisor callbacks
-export([init/1]).

%%%===================================================================
%%% API
%%%===================================================================

-spec start_link() -> supervisor:startlink_ret().
start_link() ->
  supervisor:start_link({local, ?MODULE}, ?MODULE, []).

-spec start_child([term()]) -> supervisor:startchild_ret().
start_child(Args) ->
  supervisor:start_child(?MODULE, Args).

-spec terminate_child(SupRef, Id) -> Result when
  SupRef :: supervisor:sup_ref(),
  Id     :: pid() | supervisor:child_id(),
  Result :: ok | {error, Error},
  Error  :: not_found | simple_one_for_one.
terminate_child(SupRef, Id) ->
  supervisor:terminate_child(SupRef, Id).

%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================

%% @hidden
init([]) ->
  ShardsSup = shards_supervisor_spec:supervisor(
    shards_sup, [], #{id => ?MODULE}),
  shards_supervisor_spec:supervise([ShardsSup], #{
    strategy => simple_one_for_one, intensity => 10, period => 10
  }).
