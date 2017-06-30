-module(shards_state_SUITE).

-include_lib("common_test/include/ct.hrl").

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
  t_get_state_badarg_error/1
]).

-define(EXCLUDED_FUNS, [
  module_info,
  all,
  init_per_suite,
  end_per_suite
]).

-include("support/shards_test_helper.hrl").

%%%===================================================================
%%% Common Test
%%%===================================================================

all() ->
  Exports = ?MODULE:module_info(exports),
  [F || {F, _} <- Exports, not lists:member(F, ?EXCLUDED_FUNS)].

init_per_suite(Config) ->
  shards:start(),
  [{scope, l} | Config].

end_per_suite(Config) ->
  shards:stop(),
  Config.

%%%===================================================================
%%% Tests Cases
%%%===================================================================

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
  true = 4 == shards_state:n_shards(State1),
  true = fun shards_lib:pick/3 == shards_state:pick_shard_fun(State1),
  true = fun shards_lib:pick/3 == shards_state:pick_node_fun(State1),

  % create state using shards_state:new/2
  State2 = shards_state:new(2, shards_dist),
  shards_dist = shards_state:module(State2),
  shards_sup = shards_state:sup_name(State2),
  true = 2 == shards_state:n_shards(State2),
  true = fun shards_lib:pick/3 == shards_state:pick_shard_fun(State2),
  true = fun shards_lib:pick/3 == shards_state:pick_node_fun(State2),

  % create state using shards_state:new/3
  State3 = shards_state:new(2, shards_dist, my_shards_sup),
  shards_dist = shards_state:module(State3),
  my_shards_sup = shards_state:sup_name(State3),
  true = 2 == shards_state:n_shards(State3),
  true = fun shards_lib:pick/3 == shards_state:pick_shard_fun(State3),
  true = fun shards_lib:pick/3 == shards_state:pick_node_fun(State3),

  % create state using shards_state:new/3
  Fun = fun(X, Y, Z) -> (X + Y + Z) rem Y end,
  State4 = shards_state:new(4, shards_dist, my_shards_sup, Fun),
  shards_dist = shards_state:module(State4),
  my_shards_sup = shards_state:sup_name(State4),
  true = 4 == shards_state:n_shards(State4),
  true = Fun == shards_state:pick_shard_fun(State4),
  true = fun shards_lib:pick/3 == shards_state:pick_node_fun(State4),

  % create state using shards_state:new/4
  State5 = shards_state:new(4, shards_dist, my_shards_sup, Fun, Fun),
  shards_dist = shards_state:module(State5),
  my_shards_sup = shards_state:sup_name(State5),
  true = 4 == shards_state:n_shards(State5),
  true = Fun == shards_state:pick_shard_fun(State5),
  true = Fun == shards_state:pick_node_fun(State5),

  ok.

t_state_ops(_Config) ->
  test_set = shards:new(test_set, [
    {pick_node_fun, fun shards_test_helper:pick_node/3}
  ]),
  StateSet = shards_state:get(test_set),
  DefaultShards = ?N_SHARDS,
  #{n_shards := DefaultShards} = shards_state:to_map(StateSet),

  Mod = shards_state:module(test_set),
  true = Mod == shards_local orelse Mod == shards_dist,
  shards_sup = shards_state:sup_name(test_set),
  DefaultShards = shards_state:n_shards(test_set),
  Fun1 = fun shards_lib:pick/3,
  Fun1 = shards_state:pick_shard_fun(test_set),
  Fun2 = fun shards_test_helper:pick_node/3,
  Fun2 = shards_state:pick_node_fun(test_set),

  State0 = shards_state:new(),
  State1 = shards_state:module(shards_dist, State0),
  State2 = shards_state:n_shards(100, State1),
  Fun = fun(X, Y, Z) -> (X + Y + Z) rem Y end,
  State3 = shards_state:pick_shard_fun(Fun, State2),
  State4 = shards_state:pick_node_fun(Fun, State3),
  State5 = shards_state:sup_name(my_sup, State4),

  #{module           := shards_dist,
    sup_name         := my_sup,
    n_shards         := 100,
    pick_shard_fun   := Fun,
    pick_node_fun    := Fun
  } = shards_state:to_map(State5),

  ok.

t_get_state_badarg_error(_Config) ->
  wrong_tab = ets:new(wrong_tab, [public, named_table]),
  _ = try shards_state:get(wrong_tab)
  catch _:badarg -> ok
  end, ok.
