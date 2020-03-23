-module(shards_state_SUITE).

-include_lib("common_test/include/ct.hrl").
-include("support/shards_ct.hrl").

%% Common Test
-export([
  all/0,
  init_per_suite/1,
  end_per_suite/1
]).

%% Test Cases
-export([
  t_create_state/1,
  t_state_ops/1,
  t_get_state_badarg_error/1,
  t_eval_pick_shard/1
]).

-define(EXCLUDED_FUNS, [
  module_info,
  all,
  init_per_suite,
  end_per_suite
]).

%%%===================================================================
%%% Common Test
%%%===================================================================

-spec all() -> [atom()].
all() ->
  Exports = ?MODULE:module_info(exports),
  [F || {F, _} <- Exports, not lists:member(F, ?EXCLUDED_FUNS)].

-spec init_per_suite(shards_ct:config()) -> shards_ct:config().
init_per_suite(Config) ->
  ok = shards:start(),
  [{scope, l} | Config].

-spec end_per_suite(shards_ct:config()) -> shards_ct:config().
end_per_suite(Config) ->
  ok = shards:stop(),
  Config.

%%%===================================================================
%%% Tests Cases
%%%===================================================================

-spec t_create_state(shards_ct:config()) -> any().
t_create_state(_Config) ->
  % create state with all default attr values
  State0 = shards_state:new(),
  shards_local = shards_state:module(State0),
  shards_sup = shards_state:sup_name(State0),
  true = ?N_SHARDS == shards_state:n_shards(State0),
  true = fun shards_lib:pick/3 == shards_state:pick_shard_fun(State0),
  true = fun shards_lib:pick/3 == shards_state:pick_node_fun(State0),

  % create state using shards_state:new/1
  State1 = shards_state:new(4),
  shards_local = shards_state:module(State1),
  shards_sup = shards_state:sup_name(State0),
  4 = shards_state:n_shards(State1),
  true = fun shards_lib:pick/3 == shards_state:pick_shard_fun(State1),
  true = fun shards_lib:pick/3 == shards_state:pick_node_fun(State1),

  % create state using shards_state:new/2
  State2 = shards_state:new(2, shards_dist),
  shards_dist = shards_state:module(State2),
  shards_sup = shards_state:sup_name(State2),
  2 = shards_state:n_shards(State2),
  true = fun shards_lib:pick/3 == shards_state:pick_shard_fun(State2),
  true = fun shards_lib:pick/3 == shards_state:pick_node_fun(State2),

  % create state using shards_state:new/3
  State3 = shards_state:new(2, shards_dist, my_shards_sup),
  shards_dist = shards_state:module(State3),
  my_shards_sup = shards_state:sup_name(State3),
  2 = shards_state:n_shards(State3),
  true = fun shards_lib:pick/3 == shards_state:pick_shard_fun(State3),
  true = fun shards_lib:pick/3 == shards_state:pick_node_fun(State3),

  % create state using shards_state:from_map/1
  Fun = fun(X, Y, Z) -> (X + Y + Z) rem Y end,

  State4 =
    shards_state:from_map(#{
      module         => shards_dist,
      n_shards       => 4,
      sup_name       => my_shards_sup,
      pick_shard_fun => Fun
    }),

  shards_dist = shards_state:module(State4),
  my_shards_sup = shards_state:sup_name(State4),
  4 = shards_state:n_shards(State4),
  Fun = shards_state:pick_shard_fun(State4),
  true = fun shards_lib:pick/3 == shards_state:pick_node_fun(State4),

  % create state using shards_state:from_map/1
  State5 =
    shards_state:from_map(#{
      module         => shards_dist,
      n_shards       => 4,
      sup_name       => my_shards_sup,
      pick_shard_fun => Fun,
      pick_node_fun  => Fun
    }),


  shards_dist = shards_state:module(State5),
  my_shards_sup = shards_state:sup_name(State5),
  4 = shards_state:n_shards(State5),
  true = Fun == shards_state:pick_shard_fun(State5),
  true = Fun == shards_state:pick_node_fun(State5).

-spec t_state_ops(shards_ct:config()) -> any().
t_state_ops(_Config) ->
  test_set =
    shards:new(test_set, [
      {pick_node_fun, fun shards_tests:pick_node/3}
    ]),

  StateSet = shards_state:get(test_set),
  true = shards_state:is_state(StateSet),
  DefaultShards = ?N_SHARDS,
  #{n_shards := DefaultShards} = shards_state:to_map(StateSet),

  Mod = shards_state:module(test_set),
  true = Mod == shards_local orelse Mod == shards_dist,
  shards_sup = shards_state:sup_name(test_set),
  DefaultShards = shards_state:n_shards(test_set),
  1 = shards_state:keypos(test_set),
  Fun1 = fun shards_lib:pick/3,
  Fun1 = shards_state:pick_shard_fun(test_set),
  Fun2 = fun shards_tests:pick_node/3,
  Fun2 = shards_state:pick_node_fun(test_set),
  l = shards_state:scope(test_set),

  State0 = shards_state:new(),
  State1 = shards_state:module(State0, shards_dist),
  g = shards_state:scope(State1),
  State2 = shards_state:n_shards(State1, 100),
  Fun = fun(X, Y, Z) -> (X + Y + Z) rem Y end,
  State3 = shards_state:pick_shard_fun(State2, Fun),
  State4 = shards_state:pick_node_fun(State3, Fun),
  State5 = shards_state:sup_name(State4, my_sup),
  State6 = shards_state:keypos(State5, 2),

  #{
    module         := shards_dist,
    sup_name       := my_sup,
    n_shards       := 100,
    keypos         := 2,
    pick_shard_fun := Fun,
    pick_node_fun  := Fun
  } = shards_state:to_map(State6).

-spec t_get_state_badarg_error(shards_ct:config()) -> any().
t_get_state_badarg_error(_Config) ->
  wrong_tab = ets:new(wrong_tab, [public, named_table]),

  shards_ct:assert_error(fun() ->
    shards_state:get(wrong_tab)
  end, badarg).

-spec t_eval_pick_shard(shards_ct:config()) -> any().
t_eval_pick_shard(_Config) ->
  State = shards_state:new(),
  N = shards_state:n_shards(State),
  PickShardFun = shards_state:pick_shard_fun(State),
  Expected = PickShardFun(1, N, w),
  Expected = shards_state:eval_pick_shard(1, State).
