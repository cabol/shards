-module(local_SUITE).

-include_lib("common_test/include/ct.hrl").

%% Common Test
-export([
  all/0,
  init_per_suite/1,
  end_per_suite/1,
  init_per_testcase/2,
  end_per_testcase/2
]).

%% Common Test Cases
-include_lib("mixer/include/mixer.hrl").
-mixin([
  {test_helper, [
    t_state/1,
    t_basic_ops/1,
    t_match_ops/1,
    t_select_ops/1,
    t_paginated_ops/1,
    t_paginated_ops_ordered_set/1,
    t_first_last_next_prev_ops/1,
    t_update_ops/1,
    t_fold_ops/1,
    t_info_ops/1,
    t_tab2list_tab2file_file2tab/1,
    t_equivalent_ops/1
  ]}
]).

%% Test Cases
-export([
  t_shard_restarted_when_down/1
]).

-include("test_helper.hrl").

-define(EXCLUDED_FUNS, [
  module_info,
  all,
  init_per_suite,
  end_per_suite,
  init_per_testcase,
  end_per_testcase
]).

%%%===================================================================
%%% Common Test
%%%===================================================================

all() ->
  Exports = ?MODULE:module_info(exports),
  [F || {F, _} <- Exports, not lists:member(F, ?EXCLUDED_FUNS)].

init_per_suite(Config) ->
  shards:start(),
  Config.

end_per_suite(Config) ->
  shards:stop(),
  Config.

init_per_testcase(_, Config) ->
  test_helper:init_shards(l),
  true = test_helper:cleanup_shards(),
  Config.

end_per_testcase(_, Config) ->
  test_helper:delete_shards(),
  Config.

%%%===================================================================
%%% Tests Cases
%%%===================================================================

t_shard_restarted_when_down(_Config) ->
  % create some sharded tables
  {tab1, _} = shards:new(tab1, []),
  {tab2, _} = shards:new(tab2, [{restart_strategy, one_for_all}]),

  % insert some values
  true = shards:insert(tab1, [{1, 1}, {2, 2}, {3, 3}]),
  true = shards:insert(tab2, [{1, 1}, {2, 2}, {3, 3}]),

  assert_values(tab1, [1, 2, 3], [1, 2, 3]),
  assert_values(tab2, [1, 2, 3], [1, 2, 3]),

  NumShards = shards_state:n_shards(tab1),
  ShardToKill = shards_local:pick(1, NumShards, w),
  ShardToKillTab1 = shards_local:shard_name(tab1, ShardToKill),
  exit(whereis(ShardToKillTab1), kill),
  timer:sleep(500),

  assert_values(tab1, [1, 2, 3], [nil, 2, 3]),

  ShardToKillTab2 = shards_local:shard_name(tab2, ShardToKill),
  exit(whereis(ShardToKillTab2), kill),
  timer:sleep(500),

  assert_values(tab2, [1, 2, 3], [nil, nil, nil]),

  % delete tables
  true = shards:delete(tab1),
  true = shards:delete(tab2),

  ok.

%%%===================================================================
%%% Internal functions
%%%===================================================================

assert_values(Tab, Keys, Expected) ->
  lists:foreach(fun({K, ExpectedV}) ->
    ExpectedV = case shards:lookup(Tab, K) of
      []       -> nil;
      [{K, V}] -> V
    end
  end, lists:zip(Keys, Expected)).
