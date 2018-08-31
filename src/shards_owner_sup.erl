%%%-------------------------------------------------------------------
%%% @doc
%%% This is the supervisor that holds and handles the supervision tree
%%% for a partitioned table, and it is the one that owns the metadata
%%% or control table. Every time a new partioned table is created,
%%% a new supervision tree is created to handle its lifecycle, and
%%% this module is the main supervisor for that tree.
%%% @end
%%%-------------------------------------------------------------------
-module(shards_owner_sup).

-behaviour(supervisor).

%% API
-export([
  start_link/2,
  child_spec/1
]).

%% Supervisor callbacks
-export([init/1]).

%% Macro to setup a supervisor worker
-define(worker(Mod, Args, Spec), child(worker, Mod, Args, Spec)).

%% Macro to check if restart strategy is allowed
-define(is_restart_strategy(S_), S_ == one_for_one; S_ == one_for_all).

%% Macro to check if option is table type
-define(is_ets_type(T_), T_ == set; T_ == ordered_set; T_ == bag; T_ == duplicate_bag).

%%%===================================================================
%%% API functions
%%%===================================================================

-spec start_link(Name :: atom(), Options :: [term()]) ->
        {ok, pid()} | ignore | {error, term()}.
start_link(Name, Options) ->
  supervisor:start_link(?MODULE, {Name, Options}).

-spec child_spec(Name :: atom()) -> supervisor:child_spec().
child_spec(Name) ->
  {
    Name,
    {?MODULE, start_link, []},
    permanent,
    infinity,
    supervisor,
    [?MODULE]
  }.

%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================

%% @hidden
init({Name, Options}) ->
  % ETS table to store state info.
  Name =
    ets:new(Name, [
      set,
      named_table,
      public,
      {read_concurrency, true}
    ]),

  % parse options and build metadata, local and dist state
  ParsedOpts = #{
    opts             := Opts,
    restart_strategy := RestartStrategy
  } = parse_opts(Options),
  State = shards_state:from_map(ParsedOpts),
  true = ets:insert(Name, State),

  % create children
  Children = [begin
    % get a local name to shard
    LocalShardName = shards_lib:shard_name(Name, Shard),

    % save relationship between shard and shard name
    true = ets:insert(Name, {Shard, LocalShardName}),

    % shard worker spec
    ?worker(shards_owner, [LocalShardName, Opts], #{id => Shard})
  end || Shard <- shards_lib:iterator(State)],

  % init shards_dist pg2 group
  Module = shards_state:module(State),
  ok = maybe_init_shards_dist(Name, Module),

  % launch shards supervisor
  supervise(Children, #{strategy => RestartStrategy}).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
child(Type, Module, Args, Spec) when is_map(Spec) ->
  {
    maps:get(id, Spec, Module),
    maps:get(start, Spec, {Module, start_link, Args}),
    maps:get(restart, Spec, permanent),
    maps:get(shutdown, Spec, 5000),
    Type,
    maps:get(modules, Spec, [Module])
  }.

%% @private
supervise(Children, SupFlagsMap) ->
  SupFlags = {
    maps:get(strategy, SupFlagsMap, one_for_one),
    maps:get(intensity, SupFlagsMap, 1),
    maps:get(period, SupFlagsMap, 5)
  },
  {ok, {SupFlags, Children}}.

%% @private
parse_opts(Opts) ->
  StateMap = shards_state:to_map(shards_state:new()),

  AccIn = StateMap#{
    opts             => [],
    restart_strategy => one_for_one
  },

  AccOut = parse_opts(Opts, AccIn),

  %% @TODO: this workaround must be fixed when a better strategy to support ordered_set be ready
  case maps:get(type, AccOut, set) of
    ordered_set -> AccOut#{n_shards := 1};
    _           -> AccOut
  end.

%% @private
parse_opts([], Acc) ->
  Acc;
parse_opts([{scope, l} | Opts], Acc) ->
  parse_opts(Opts, Acc#{module := shards_local});
parse_opts([{scope, g} | Opts], Acc) ->
  parse_opts(Opts, Acc#{module := shards_dist});
parse_opts([{sup_name, SupName} | Opts], Acc) when is_atom(SupName) ->
  parse_opts(Opts, Acc#{sup_name := SupName});
parse_opts([{n_shards, N} | Opts], Acc) when is_integer(N), N > 0 ->
  parse_opts(Opts, Acc#{n_shards := N});
parse_opts([{pick_shard_fun, Val} | Opts], Acc) when is_function(Val) ->
  parse_opts(Opts, Acc#{pick_shard_fun := Val});
parse_opts([{pick_node_fun, Val} | Opts], Acc) when is_function(Val) ->
  parse_opts(Opts, Acc#{pick_node_fun := Val});
parse_opts([{restart_strategy, Val} | Opts], Acc) when ?is_restart_strategy(Val) ->
  parse_opts(Opts, Acc#{restart_strategy := Val});
parse_opts([Opt | Opts], #{opts := NOpts} = Acc) when ?is_ets_type(Opt) ->
  parse_opts(Opts, Acc#{type => Opt, opts := [Opt | NOpts]});
parse_opts([Opt | Opts], #{opts := NOpts} = Acc) ->
  parse_opts(Opts, Acc#{opts := [Opt | NOpts]}).

%% @private
maybe_init_shards_dist(Tab, shards_dist) ->
  ok = pg2:create(Tab),
  ok = pg2:join(Tab, self());
maybe_init_shards_dist(_, _) ->
  ok.
