-module(shards_opts_SUITE).

-include_lib("common_test/include/ct.hrl").

%% Common Test
-export([
  all/0
]).

%% Test Cases
-export([
  t_parse/1,
  t_parse_with_defaults/1,
  t_parse_ordered_set/1,
  t_parse_errors/1
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
%%% Test Cases
%%%===================================================================

-spec t_parse(shards_ct:config()) -> any().
t_parse(_Config) ->
  Self = self(),
  KeyslotFun = fun erlang:phash2/2,

  #{
    keypos := 2,
    keyslot_fun := KeyslotFun,
    parallel := true,
    partitions := 2,
    tab_pid := undefined,
    ets_opts := [
      set,
      ordered_set,
      bag,
      duplicate_bag,
      public,
      protected,
      private,
      named_table,
      compressed,
      {keypos, 2},
      {heir, Self, nil},
      {heir, none},
      {write_concurrency, true},
      {read_concurrency, true},
      {decentralized_counters, true}
    ]
  } = shards_opts:parse([
    set,
    ordered_set,
    bag,
    duplicate_bag,
    public,
    protected,
    private,
    named_table,
    compressed,
    {keypos, 2},
    {heir, Self, nil},
    {heir, none},
    {write_concurrency, true},
    {read_concurrency, true},
    {decentralized_counters, true},
    {partitions, 2},
    {keyslot_fun, KeyslotFun},
    {parallel, true}
  ]).

-spec t_parse_with_defaults(shards_ct:config()) -> any().
t_parse_with_defaults(_Config) ->
  KeyslotFun = fun erlang:phash2/2,
  Partitions = erlang:system_info(schedulers_online),

  #{
    ets_opts := [{restore,nil,nil}],
    keypos := 1,
    keyslot_fun := KeyslotFun,
    parallel := false,
    partitions := Partitions,
    tab_pid := undefined
  } = shards_opts:parse([{restore, nil, nil}]).

-spec t_parse_ordered_set(shards_ct:config()) -> any().
t_parse_ordered_set(_Config) ->
  KeyslotFun = fun erlang:phash2/2,

  #{
    ets_opts := [ordered_set],
    keypos := 1,
    keyslot_fun := KeyslotFun,
    parallel := false,
    partitions := 1,
    tab_pid := undefined
  } = shards_opts:parse([ordered_set]).

-spec t_parse_errors(shards_ct:config()) -> any().
t_parse_errors(_Config) ->
  shards_ct:assert_error(fun() ->
    shards_opts:parse([ordered_set, invalid])
  end, {badoption, invalid}).
