%%%-------------------------------------------------------------------
%%% @doc
%%% Shards owners supervisor.
%%% @end
%%%-------------------------------------------------------------------
-module(shards_owner_sup).

-behaviour(supervisor).

%% API
-export([start_link/3]).

%% Supervisor callbacks
-export([init/1]).

%% Macros
-define(worker(Mod, Args, Spec), child(worker, Mod, Args, Spec)).

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
  true = ets:insert(Name, [{pool_size, PoolSize}, {options, Options}]),

  % create children
  Children = [begin
    % get a local name to shard
    LocalShardName = shards_owner:shard_name(Name, Shard),
    % save relationship between shard and shard name
    true = ets:insert(Name, {Shard, LocalShardName}),
    % shard worker spec
    ?worker(shards_owner, [LocalShardName, Options], #{id => Shard})
  end || Shard <- lists:seq(0, PoolSize - 1)],

  % launch shards supervisor
  supervise(Children, #{strategy => one_for_one}).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
child(Type, Module, Args, Spec) when is_map(Spec) ->
  #{
    id       => maps:get(id, Spec, Module),
    start    => maps:get(start, Spec, {Module, start_link, Args}),
    restart  => maps:get(restart, Spec, permanent),
    shutdown => maps:get(shutdown, Spec, 5000),
    type     => Type,
    modules  => maps:get(modules, Spec, [Module])
  }.

%% @private
supervise(Children, SupFlags) ->
  assert_unique_ids([Id || #{id := Id} <- Children]),
  {ok, {SupFlags, Children}}.

%% @private
assert_unique_ids([]) -> ok;
assert_unique_ids([Id | Rest]) ->
  case lists:member(Id, Rest) of
    true -> throw({badarg, duplicated_id});
    _    -> assert_unique_ids(Rest)
  end.
