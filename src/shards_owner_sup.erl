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

-spec start_link(Name, Options, NumShards) -> Response when
  Name      :: atom(),
  Options   :: [term()],
  NumShards :: pos_integer(),
  Response  :: supervisor:startlink_ret().
start_link(Name, Options, NumShards) ->
  supervisor:start_link({local, Name}, ?MODULE, [Name, Options, NumShards]).

%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================

%% @hidden
init([Name, Options, NumShards]) ->
  % ETS table to hold state info.
  Name = ets:new(Name, [set, named_table, public, {read_concurrency, true}]),
  #{module := Module, type := Type, opts := Opts} = parse_opts(Options),
  true = ets:insert(Name, {'$shards_state', {Module, Type, NumShards}}),

  % create children
  Children = [begin
    % get a local name to shard
    LocalShardName = shards_owner:shard_name(Name, Shard),
    % save relationship between shard and shard name
    true = ets:insert(Name, {Shard, LocalShardName}),
    % shard worker spec
    ?worker(shards_owner, [LocalShardName, Opts], #{id => Shard})
  end || Shard <- lists:seq(0, NumShards - 1)],

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
  AccIn = #{module => shards_local, type  => set, opts  => []},
  parse_opts(Opts, AccIn).

%% @private
parse_opts([], Acc) ->
  Acc;
parse_opts([{scope, l} | Opts], Acc) ->
  parse_opts(Opts, Acc#{module := shards_local});
parse_opts([{scope, g} | Opts], Acc) ->
  parse_opts(Opts, Acc#{module := shards_dist});
parse_opts([sharded_duplicate_bag | Opts], #{opts := NOpts} = Acc) ->
  NAcc = Acc#{type := sharded_duplicate_bag, opts := [duplicate_bag | NOpts]},
  parse_opts(Opts, NAcc);
parse_opts([sharded_bag | Opts], #{opts := NOpts} = Acc) ->
  parse_opts(Opts, Acc#{type := sharded_bag, opts := [bag | NOpts]});
parse_opts([duplicate_bag | Opts], #{opts := NOpts} = Acc) ->
  NAcc = Acc#{type := duplicate_bag, opts := [duplicate_bag | NOpts]},
  parse_opts(Opts, NAcc);
parse_opts([bag | Opts], #{opts := NOpts} = Acc) ->
  parse_opts(Opts, Acc#{type := bag, opts := [bag | NOpts]});
parse_opts([ordered_set | Opts], #{opts := NOpts} = Acc) ->
  parse_opts(Opts, Acc#{type := ordered_set, opts := [ordered_set | NOpts]});
parse_opts([set | Opts], #{opts := NOpts} = Acc) ->
  parse_opts(Opts, Acc#{type := set, opts := [set | NOpts]});
parse_opts([Opt | Opts], #{opts := NOpts} = Acc) ->
  parse_opts(Opts, Acc#{opts := [Opt | NOpts]}).

%% @private
init_shards_dist(Tab) ->
  ok = pg2:create(Tab),
  ok = pg2:join(Tab, self()).
