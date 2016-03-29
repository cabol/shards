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

%% Test Cases
-include_lib("mixer/include/mixer.hrl").
-mixin([
  {test_helper, [
    t_basic_ops/1,
    t_match_ops/1,
    t_select_ops/1,
    t_paginated_ops/1,
    t_first_last_next_prev_ops/1,
    t_update_ops/1,
    t_fold_ops/1,
    t_info_ops/1,
    t_tab2list_tab2file_file2tab/1,
    t_equivalent_ops/1,
    t_unsupported_ops/1
  ]}
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
  test_helper:delete_shards_pool(),
  Config.
