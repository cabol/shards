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

-spec start_link(Name, Options, PoolSize) -> Response when
  Name     :: atom(),
  Options  :: [term()],
  PoolSize :: pos_integer(),
  Response :: supervisor:startlink_ret().
start_link(Name, Options, PoolSize) ->
  supervisor:start_link({local, Name}, ?MODULE, [Name, Options, PoolSize]).

%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================

%% @hidden
init([Name, Options, PoolSize]) ->
  % ETS table to hold control info.
  Name = ets:new(Name, [set, named_table, public, {read_concurrency, true}]),
  {Opts, #{module := Module, type := Type}} = parse_opts(Options),
  true = ets:insert(Name, {'$control', {Module, Type, PoolSize}}),

  % create children
  Children = [begin
    % get a local name to shard
    LocalShardName = shards_owner:shard_name(Name, Shard),
    % save relationship between shard and shard name
    true = ets:insert(Name, {Shard, LocalShardName}),
    % shard worker spec
    ?worker(shards_owner, [LocalShardName, Opts], #{id => Shard})
  end || Shard <- lists:seq(0, PoolSize - 1)],

  % init shards_dist pg2 group
  ok = init_shards_dist(Name),

  % launch shards supervisor
  supervise(Children, #{strategy => one_for_one}).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
child(Type, Module, Args, Spec) when is_map(Spec) ->
  #{id       => maps:get(id, Spec, Module),
    start    => maps:get(start, Spec, {Module, start_link, Args}),
    restart  => maps:get(restart, Spec, permanent),
    shutdown => maps:get(shutdown, Spec, 5000),
    type     => Type,
    modules  => maps:get(modules, Spec, [Module])}.

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

%% @private
parse_opts(Opts) ->
  parse_opts(Opts, [], #{module => shards, type => set}).

%% @private
parse_opts([], NOpts, C) ->
  {NOpts, C};
parse_opts([{module, Module} | Opts], NOpts, C) ->
  parse_opts(Opts, NOpts, C#{module := Module});
parse_opts([sharded_duplicate_bag | Opts], NOpts, C) ->
  parse_opts(Opts, [duplicate_bag | NOpts], C#{type := sharded_duplicate_bag});
parse_opts([sharded_bag | Opts], NOpts, C) ->
  parse_opts(Opts, [bag | NOpts], C#{type := sharded_bag});
parse_opts([duplicate_bag | Opts], NOpts, C) ->
  parse_opts(Opts, [duplicate_bag | NOpts], C#{type := duplicate_bag});
parse_opts([bag | Opts], NOpts, C) ->
  parse_opts(Opts, [bag | NOpts], C#{type := bag});
parse_opts([ordered_set | Opts], NOpts, C) ->
  parse_opts(Opts, [ordered_set | NOpts], C#{type := ordered_set});
parse_opts([set | Opts], NOpts, C) ->
  parse_opts(Opts, [set | NOpts], C#{type := set});
parse_opts([Opt | Opts], NOpts, C) ->
  parse_opts(Opts, [Opt | NOpts], C).

%% @private
init_shards_dist(Tab) ->
  ok = pg2:create(Tab),
  ok = pg2:join(Tab, self()).
