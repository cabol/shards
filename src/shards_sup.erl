%%%-------------------------------------------------------------------
%%% @doc
%%% ETS shards top level supervisor.
%%% @end
%%%-------------------------------------------------------------------
-module(shards_sup).

-behaviour(supervisor).

%% API
-export([start_link/3]).

%% Supervisor callbacks
-export([init/1]).

%%%===================================================================
%%% API functions
%%%===================================================================

-spec start_link(atom(), [term()], pos_integer()) -> supervisor:startlink_ret().
start_link(Name, Options, PoolSize) ->
  supervisor:start_link({local, Name}, ?MODULE, [Name, Options, PoolSize]).

%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================

%% @hidden
init([Name, Options, PoolSize]) ->
  % ETS table to hold control info.
  Name = ets:new(Name, [set, named_table, {read_concurrency, true}]),
  true = ets:insert(Name, {pool_size, PoolSize}),

  % create children
  Children = [begin
    % get a local name to shard
    LocalShardName = shards_owner:shard_name(Name, Shard),
    % save relationship between shard and shard name
    true = ets:insert(Name, {Shard, LocalShardName}),
    % shard worker spec
    shards_supervisor_spec:worker(
      shards_owner, [LocalShardName, Options], #{id => Shard})
  end || Shard <- lists:seq(0, PoolSize - 1)],

  % launch shards supervisor
  shards_supervisor_spec:supervise(Children, #{strategy => one_for_one}).
