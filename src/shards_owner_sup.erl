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

%% Macro to setup a supervisor worker
-define(worker(Mod, Args, Spec), child(worker, Mod, Args, Spec)).

%% Macro to check if option is table type
-define(is_ets_type(T_), T_ == set; T_ == ordered_set; T_ == bag; T_ == duplicate_bag).

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
  ParsedOpts = #{module := Module, opts := Opts} = parse_opts(Options),
  LocalState = local_state(ParsedOpts#{n_shards => NumShards}),
  DistState = dist_state(ParsedOpts),
  Metadata = {Module, LocalState, DistState},
  true = ets:insert(Name, {'$shards_meta', Metadata}),

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
  ok = init_shards_dist(Name, Module),

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
  AccIn = #{
    module          => shards_local,
    type            => set,
    pick_shard_fun  => fun shards_local:pick_shard/3,
    pick_node_fun   => fun shards_dist:pick_node/3,
    autoeject_nodes => true,
    opts            => []
  },
  parse_opts(Opts, AccIn).

%% @TODO: return error exception to invalid values
%% @private
parse_opts([], Acc) ->
  Acc;
parse_opts([{scope, l} | Opts], Acc) ->
  parse_opts(Opts, Acc#{module := shards_local});
parse_opts([{scope, g} | Opts], Acc) ->
  parse_opts(Opts, Acc#{module := shards_dist});
parse_opts([{pick_shard_fun, PickShard} | Opts], Acc) ->
  parse_opts(Opts, Acc#{pick_shard_fun := PickShard});
parse_opts([{pick_node_fun, PickNode} | Opts], Acc) ->
  parse_opts(Opts, Acc#{pick_node_fun := PickNode});
parse_opts([{autoeject_nodes, AutoEject} | Opts], Acc) ->
  parse_opts(Opts, Acc#{autoeject_nodes := AutoEject});
parse_opts([Opt | Opts], #{opts := NOpts} = Acc) when ?is_ets_type(Opt) ->
  parse_opts(Opts, Acc#{type := Opt, opts := [Opt | NOpts]});
parse_opts([Opt | Opts], #{opts := NOpts} = Acc) ->
  parse_opts(Opts, Acc#{opts := [Opt | NOpts]}).

%% @private
local_state(Opts) ->
  #{n_shards       := NumShards,
    type           := Type,
    pick_shard_fun := PickShard
  } = Opts,
  {NumShards, PickShard, Type}.

%% @private
dist_state(Opts) ->
  #{pick_node_fun   := PickNode,
    autoeject_nodes := AutoEject
  } = Opts,
  {PickNode, AutoEject}.

%% @private
init_shards_dist(Tab, shards_dist) ->
  ok = pg2:create(Tab),
  ok = pg2:join(Tab, self());
init_shards_dist(_, _) ->
  ok.
