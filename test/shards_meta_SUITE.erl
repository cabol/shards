-module(shards_meta_SUITE).

-include_lib("common_test/include/ct.hrl").
-include("shards_ct.hrl").

%% Common Test
-export([
  all/0
]).

%% Test Cases
-export([
  t_getters/1,
  t_getters_with_table/1,
  t_to_map/1,
  t_retrieve_tids_pids/1,
  t_store_and_retrieve_from_meta_table/1,
  t_errors/1
]).

-define(EXCLUDED_FUNS, [
  module_info,
  all
]).

%%%===================================================================
%%% Common Test
%%%===================================================================

-spec all() -> [atom()].
all() ->
  Exports = ?MODULE:module_info(exports),
  [F || {F, _} <- Exports, not lists:member(F, ?EXCLUDED_FUNS)].

%%%===================================================================
%%% Tests Cases
%%%===================================================================

-spec t_getters(shards_ct:config()) -> any().
t_getters(_Config) ->
  Meta0 = shards_meta:new(),
  undefined = shards_meta:tab_pid(Meta0),
  1 = shards_meta:keypos(Meta0),
  true = ?PARTITIONS == shards_meta:partitions(Meta0),
  true = fun erlang:phash2/2 == shards_meta:keyslot_fun(Meta0),
  false = shards_meta:parallel(Meta0),
  [] = shards_meta:ets_opts(Meta0),

  Meta1 = shards_meta:from_map(#{keypos => 2, partitions => 4, parallel => true}),
  Self = self(),
  Self = shards_meta:tab_pid(Meta1),
  2 = shards_meta:keypos(Meta1),
  4 = shards_meta:partitions(Meta1),
  true = fun erlang:phash2/2 == shards_meta:keyslot_fun(Meta1),
  true = shards_meta:parallel(Meta1),
  [] = shards_meta:ets_opts(Meta1),
  ok.

-spec t_getters_with_table(shards_ct:config()) -> any().
t_getters_with_table(_Config) ->
  Tab = shards:new(shards_meta_test, []),
  true = shards_meta:is_metadata(shards_meta:get(Tab)),
  false = shards_meta:is_metadata(invalid),

  Pid = shards_meta:tab_pid(Tab),
  true = is_pid(Pid),
  1 = shards_meta:keypos(Tab),
  true = ?PARTITIONS == shards_meta:partitions(Tab),
  true = fun erlang:phash2/2 == shards_meta:keyslot_fun(Tab),
  false = shards_meta:parallel(Tab),
  [] = shards_meta:ets_opts(Tab),

  true = shards:delete(Tab).

-spec t_to_map(shards_ct:config()) -> any().
t_to_map(_Config) ->
  Meta0 = shards_meta:new(),
  Parts = ?PARTITIONS,

  #{
    tab_pid     := undefined,
    keypos      := 1,
    partitions  := Parts,
    keyslot_fun := KeyslotFun,
    parallel    := false,
    ets_opts    := []
  } = shards_meta:to_map(Meta0),

  true = fun erlang:phash2/2 == KeyslotFun.

-spec t_retrieve_tids_pids(shards_ct:config()) -> any().
t_retrieve_tids_pids(_Config) ->
  Tab = shards:new(shards_meta_test, [{partitions, 2}]),

  [{_, Tid}, _] = shards_meta:get_partition_tids(Tab),
  true = is_reference(Tid),
  Pids = lists:usort([Pid || {_, Pid} <- shards_meta:get_partition_pids(Tab)]),
  true = is_pid(hd(Pids)),

  Pids = lists:usort(shards:partition_owners(Tab)),

  true = shards:delete(Tab).

-spec t_store_and_retrieve_from_meta_table(shards_ct:config()) -> any().
t_store_and_retrieve_from_meta_table(_Config) ->
  shards_ct:with_table(fun(Tab) ->
    ok = shards_meta:put(Tab, foo, bar),
    bar = shards_meta:lookup(Tab, foo)
  end, meta_table, []).

-spec t_errors(shards_ct:config()) -> any().
t_errors(_Config) ->
  shards_ct:assert_error(fun() ->
    shards_meta:get(unknown)
  end, {unknown_table, unknown}),

  shards_ct:assert_error(fun() ->
    shards_meta:get_partition_pids(unknown)
  end, {unknown_table, unknown}),

  shards_ct:assert_error(fun() ->
    shards_meta:get_partition_tids(unknown)
  end, {unknown_table, unknown}).
