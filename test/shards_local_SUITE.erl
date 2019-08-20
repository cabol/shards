-module(shards_local_SUITE).

-include_lib("common_test/include/ct.hrl").
-include("support/shards_ct.hrl").

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
  {shards_tests, [
    t_basic_ops/1,
    t_match_ops/1,
    t_select_ops/1,
    t_paginated_ops/1,
    t_paginated_ops_ordered_set/1,
    t_first_last_next_prev_ops/1,
    t_update_ops/1,
    t_fold_ops/1,
    t_info_ops/1,
    t_tab2list/1,
    t_tab2file_file2tab_tabfile_info/1,
    t_rename/1,
    t_equivalent_ops/1,
    t_keypos/1
  ]}
]).

%% Test Cases
-export([
  t_shard_restarted_when_down/1,
  t_custom_supervisor/1,
  t_shards_owner_unhandled_callbacks/1
]).

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

-spec all() -> [atom()].
all() ->
  Exports = ?MODULE:module_info(exports),
  [F || {F, _} <- Exports, not lists:member(F, ?EXCLUDED_FUNS)].

-spec init_per_suite(shards_ct:config()) -> shards_ct:config().
init_per_suite(Config) ->
  _ = shards:start(),
  [{scope, l} | Config].

-spec end_per_suite(shards_ct:config()) -> shards_ct:config().
end_per_suite(Config) ->
  _ = shards:stop(),
  Config.

-spec init_per_testcase(atom(), shards_ct:config()) -> shards_ct:config().
init_per_testcase(t_keypos, Config) ->
  _ = shards_tests:init_shards(l, [{keypos, #test_rec.name}]),
  true = shards_tests:cleanup_shards(),
  Config;
init_per_testcase(_, Config) ->
  _ = shards_tests:init_shards(l),
  true = shards_tests:cleanup_shards(),
  Config.

-spec end_per_testcase(atom(), shards_ct:config()) -> shards_ct:config().
end_per_testcase(_, Config) ->
  _ = shards_tests:delete_shards(),
  Config.

%%%===================================================================
%%% Tests Cases
%%%===================================================================

-spec t_shard_restarted_when_down(shards_ct:config()) -> any().
t_shard_restarted_when_down(_Config) ->
  % create some sharded tables
  tab1 = shards:new(tab1, []),
  tab2 = shards:new(tab2, [{restart_strategy, one_for_all}]),

  shards_ct:assert_error(fun() ->
    shards_lib:get_pid(wrong)
  end, badarg),

  % insert some values
  true = shards:insert(tab1, [{1, 1}, {2, 2}, {3, 3}]),
  true = shards:insert(tab2, [{1, 1}, {2, 2}, {3, 3}]),

  assert_values(tab1, [1, 2, 3], [1, 2, 3]),
  assert_values(tab2, [1, 2, 3], [1, 2, 3]),

  NumShards = shards_state:n_shards(tab1),
  ShardToKill = shards_lib:pick(1, NumShards, w),
  ShardToKillTab1 = shards_lib:shard_name(tab1, ShardToKill),

  exit(whereis(ShardToKillTab1), kill),
  timer:sleep(500),

  Expected = [begin
    Shard = shards_lib:pick(K, NumShards, w),
    case Shard == ShardToKill of
      true -> nil;
      _    -> K
    end
  end || K <- [1, 2, 3]],

  assert_values(tab1, [1, 2, 3], Expected),

  ShardToKillTab2 = shards_lib:shard_name(tab2, ShardToKill),
  exit(whereis(ShardToKillTab2), kill),
  timer:sleep(500),

  assert_values(tab2, [1, 2, 3], [nil, nil, nil]),

  % delete tables
  true = shards:delete(tab1),
  true = shards:delete(tab2).

-spec t_custom_supervisor(shards_ct:config()) -> any().
t_custom_supervisor(_Config) ->
  {ok, _Pid} = shards_sup:start_link(my_sup),

  test = shards:new(test, [{sup_name, my_sup}]),
  true = shards:insert(test, [{1, 1}, {2, 2}, {3, 3}]),
  assert_values(test, [1, 2, 3], [1, 2, 3]),

  true = shards:delete(test).

-spec t_shards_owner_unhandled_callbacks(shards_ct:config()) -> any().
t_shards_owner_unhandled_callbacks(_Config) ->
  tab1 = shards:new(tab1, []),
  Shard = shards_lib:shard_name(tab1, 1),

  ok = gen_server:call(Shard, hello),
  ok = gen_server:cast(Shard, hello),
  _ = Shard ! hello,
  _ = shards_owner:code_change(1, #{}, #{}),
  _ = timer:sleep(500),
  ok = shards_owner:stop(Shard),

  % delete tables
  true = shards:delete(tab1).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
assert_values(Tab, Keys, Expected) ->
  lists:foreach(fun({K, ExpectedV}) ->
    ExpectedV =
      case shards:lookup(Tab, K) of
        []       -> nil;
        [{K, V}] -> V
      end,

    true
  end, lists:zip(Keys, Expected)).
