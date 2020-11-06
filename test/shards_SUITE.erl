-module(shards_SUITE).

-include_lib("common_test/include/ct.hrl").
-include("shards_ct.hrl").

%% Common Test
-export([
  all/0,
  init_per_suite/1,
  end_per_suite/1,
  init_per_testcase/2,
  end_per_testcase/2
]).

%% Shared Tests
-include_lib("mixer/include/mixer.hrl").
-mixin([{shards_tests, [
  t_basic_ops/1,
  t_query_ops/1,
  t_match_ops/1,
  t_select_ops/1,
  t_select_replace/1,
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
]}]).

%% Tests
-export([
  t_table_deleted_when_partition_goes_down/1,
  t_shards_table_unhandled_callbacks/1,
  t_table_creation_errors/1
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
  Config.

-spec end_per_suite(shards_ct:config()) -> shards_ct:config().
end_per_suite(Config) ->
  Config.

-spec init_per_testcase(atom(), shards_ct:config()) -> shards_ct:config().
init_per_testcase(t_keypos, Config) ->
  _ = shards_tests:init_shards([{keypos, #test_rec.name}]),
  true = shards_tests:cleanup_shards(),
  Config;
init_per_testcase(_, Config) ->
  _ = shards_tests:init_shards(),
  true = shards_tests:cleanup_shards(),
  Config.

-spec end_per_testcase(atom(), shards_ct:config()) -> shards_ct:config().
end_per_testcase(_, Config) ->
  _ = shards_tests:delete_shards(),
  Config.

%%%===================================================================
%%% Tests Cases
%%%===================================================================

-spec t_table_deleted_when_partition_goes_down(shards_ct:config()) -> any().
t_table_deleted_when_partition_goes_down(_Config) ->
  % create some sharded tables
  Tab1 = shards:new(tab1, []),
  true = is_reference(Tab1),

  % insert some values
  true = shards:insert(Tab1, {1, 11}),
  11 = shards:lookup_element(Tab1, 1, 2),

  Meta = shards_meta:get(Tab1),
  KeyslotFun = shards_meta:keyslot_fun(Meta),
  Idx = KeyslotFun(1, shards_meta:partitions(Meta)),
  PidToKill = shards_partition:pid(Tab1, Idx),

  % unexpected message is ignored
  _ = PidToKill ! ping,

  true = exit(PidToKill, normal),
  ok = timer:sleep(500),
  [] = shards:lookup(Tab1, 1).

-spec t_shards_table_unhandled_callbacks(shards_ct:config()) -> any().
t_shards_table_unhandled_callbacks(_Config) ->
  tab1 = shards:new(tab1, [named_table]),
  Owner = lists:nth(1, shards:partition_owners(tab1)),

  ok = gen_server:call(Owner, hello),
  ok = gen_server:cast(Owner, hello),
  ok = shards_partition:stop(Owner),

  % delete tables
  true = shards:delete(tab1).

-spec t_table_creation_errors(shards_ct:config()) -> any().
t_table_creation_errors(_Config) ->
  shards_ct:with_table(fun(Tab) ->
    shards_ct:assert_error(fun() ->
      shards:new(Tab, [named_table])
    end, {conflict, conflict_test})
  end, conflict_test, [named_table]),

  shards_ct:assert_error(fun() ->
    shards:new(another_table, [wrongarg])
  end, {badoption, wrongarg}).
