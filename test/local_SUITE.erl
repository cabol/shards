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

%% Tests
-export([t_basic_ops/1]).

-define(TAB, test_tab).
-define(ETS_TAB, ets_test_tab).

%%%===================================================================
%%% Common Test
%%%===================================================================

all() -> [t_basic_ops].

init_per_suite(Config) ->
  Config.

end_per_suite(Config) ->
  Config.

init_per_testcase(_, Config) ->
  init_tables(),
  Config.

end_per_testcase(_, Config) ->
  shards:delete(?TAB),
  ets:delete(?ETS_TAB),
  Config.

%%%===================================================================
%%% Exported Tests Functions
%%%===================================================================

t_basic_ops(_Config) ->
  true = cleanup_tables(),

  % insert some K/V pairs
  Obj1 = {kx, 1, a, "hi"},
  KVPairs = [
    {k1, 1}, {k1, 2}, {k1, 1},
    {k2, 2},
    {k11, 11},
    {k22, 22},
    Obj1
  ],
  true = shards:insert(?TAB, KVPairs),
  true = shards:insert(?TAB, Obj1),
  true = ets:insert(?ETS_TAB, KVPairs),
  true = ets:insert(?ETS_TAB, Obj1),

  % select and match
  R1 = lists:usort(ets:select(?ETS_TAB, [{{'$1', '$2'}, [], ['$$']}])),
  R1 = lists:usort(shards:select(?TAB, [{{'$1', '$2'}, [], ['$$']}])),
  R2 = lists:usort(ets:match(?ETS_TAB, '$1')),
  R2 = lists:usort(shards:match(?TAB, '$1')),

  % lookup
  R3 = ets:lookup_element(?ETS_TAB, k1, 2),
  R3 = shards:lookup_element(?TAB, k1, 2),
  R4 = lookup_keys(ets, ?ETS_TAB, [k1, k2, kx]),
  R4 = lookup_keys(shards, ?TAB, [k1, k2, kx]),

  % delete
  true = ets:delete_object(?ETS_TAB, Obj1),
  true = ets:delete(?ETS_TAB, k2),
  true = shards:delete_object(?TAB, Obj1),
  true = shards:delete(?TAB, k2),
  [] = lookup_keys(ets, ?ETS_TAB, [k1, k2, kx]),
  [] = lookup_keys(shards, ?TAB, [k1, k2, kx]),

  ct:print("\e[36m DEL_ALL: ~p \e[0m", [shards:delete_all_objects(?TAB)]),

  ct:print("\e[1;1m t_basic_ops: \e[0m\e[32m[OK] \e[0m"),
  ok.

%%%===================================================================
%%% Internal functions
%%%===================================================================

init_tables() ->
  shards:new(?TAB, [duplicate_bag, public, named_table]),
  shards_created(?TAB),
  ets:new(?ETS_TAB, [duplicate_bag, public, named_table]),
  ok.

cleanup_tables() ->
  true = ets:delete_all_objects(?ETS_TAB),
  true = shards:delete_all_objects(?TAB),
  All = ets:match(?ETS_TAB, '$1'),
  All = shards:match(?TAB, '$1'),
  true.

lookup_keys(Mod, Tab, Keys) ->
  lists:foldr(fun(Key, Acc) ->
    case Mod:lookup(Tab, Key) of
      [Value] -> [Value | Acc];
      _       -> Acc
    end
  end, [], Keys).

shards_created(Tab) ->
  lists:foreach(fun(Shard) ->
    true = lists:member(Shard, shards:all())
  end, shards:list(Tab)).
