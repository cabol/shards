-module(shards_test_helper).

%% Test Cases
-export([
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
  t_rename/1,
  t_equivalent_ops/1
]).

-export([
  t_match_ops_/1,
  t_select_ops_/1
]).

%% Helpers
-export([
  init_shards/1,
  cleanup_shards/0,
  delete_shards/0,
  build_args/2,
  get_module/2
]).

%% Pick Callbacks
-export([
  pick_shard/3,
  pick_node/3,
  pick_node_dist/3
]).

-include_lib("stdlib/include/ms_transform.hrl").
-include("shards_test_helper.hrl").

%%%===================================================================
%%% Tests Key Generator
%%%===================================================================

pick_shard(Key, N, w) ->
  erlang:phash2({Key, os:timestamp()}, N);
pick_shard(_, _, _) ->
  any.

pick_node(Key, Nodes, _) ->
  jchash:compute(erlang:phash2(Key), Nodes).

pick_node_dist(Key, Nodes, w) ->
  NewKey = {Key, os:timestamp()},
  jchash:compute(erlang:phash2(NewKey), Nodes);
pick_node_dist(_, _, _) ->
  any.

%%%===================================================================
%%% Test Cases
%%%===================================================================

t_basic_ops(Config) ->
  run_for_all_tables(Config, fun t_basic_ops_/1).

t_basic_ops_({Scope, Tab, EtsTab}) ->
  Mod = get_module(Scope, Tab),

  % insert some K/V pairs
  Obj1 = {kx, 1, a, "hi"},
  KVPairs = [
    {k1, 1}, {k1, 2}, {k1, 1},
    {k2, 2},
    {k11, 11},
    {k22, 22},
    Obj1
  ],
  true = Mod:insert(Tab, KVPairs),
  true = Mod:insert(Tab, Obj1),
  true = ets:insert(EtsTab, KVPairs),
  true = ets:insert(EtsTab, Obj1),

  % insert new
  false = ets:insert_new(EtsTab, [Obj1, {k3, <<"V3">>}]),
  [false, true] = Mod:insert_new(Tab, [Obj1, {k3, <<"V3">>}]),
  true = ets:insert_new(EtsTab, {k3, <<"V3">>}),
  false = Mod:insert_new(Tab, {k3, <<"V3">>}),

  % lookup element
  case Tab of
    ?SHARDED_DUPLICATE_BAG ->
      R0 = ets:lookup_element(EtsTab, k1, 2),
      LenR3 = length(R0),
      R01 = Mod:lookup_element(Tab, k1, 2),
      LenR3 = length(R01),
      SortR3 = lists:usort(R0),
      SortR3 = lists:usort(R01),

      try Mod:lookup_element(Tab, wrong, 2)
      catch _:badarg -> ok
      end;
    _ ->
      R0 = ets:lookup_element(EtsTab, k1, 2),
      R0 = Mod:lookup_element(Tab, k1, 2)
  end,

  % lookup
  R1 = lists:sort(lookup_keys(ets, EtsTab, [k1, k2, k3, kx])),
  R1 = lists:sort(lookup_keys(Mod, Tab, [k1, k2, k3, kx])),

  % delete
  true = ets:delete_object(EtsTab, Obj1),
  true = ets:delete(EtsTab, k2),
  true = Mod:delete_object(Tab, Obj1),
  true = Mod:delete(Tab, k2),
  R2 = lists:sort(lookup_keys(ets, EtsTab, [k1, k2, kx])),
  R2 = lists:sort(lookup_keys(Mod, Tab, [k1, k2, kx])),

  % member
  true = Mod:member(Tab, k1),
  true = ets:member(EtsTab, k1),
  false = Mod:member(Tab, kx),
  false = ets:member(EtsTab, kx),

  % take
  R3 = lists:sort(ets:take(EtsTab, k1)),
  R3 = lists:sort(Mod:take(Tab, k1)),
  [] = ets:lookup(EtsTab, k1),
  [] = Mod:lookup(Tab, k1),

  % delete all
  true = Mod:delete_all_objects(Tab),

  ok.

t_match_ops(Config) ->
  run_for_all_tables(Config, fun t_match_ops_/1).

t_match_ops_({Scope, Tab, EtsTab}) ->
  Mod = get_module(Scope, Tab),

  % insert some values
  KV = [
    {k1, 1}, {k2, 2}, {k3, 2}, {k1, 1}, {k4, 22}, {k5, 33},
    {k11, 1}, {k22, 2}, {k33, 2}, {k44, 11}, {k55, 22}, {k55, 33}
  ],
  true = ets:insert(EtsTab, KV),
  true = Mod:insert(Tab, KV),

  % match/2
  R1 = maybe_sort(EtsTab, ets:match(EtsTab, '$1')),
  R1 = maybe_sort(Tab, Mod:match(Tab, '$1')),

  % match_object/2
  R2 = maybe_sort(EtsTab, ets:match_object(EtsTab, '$1')),
  R2 = maybe_sort(Tab, Mod:match_object(Tab, '$1')),

  % match_delete/2
  true = ets:match_delete(EtsTab, {'$1', 2}),
  true = Mod:match_delete(Tab, {'$1', 2}),
  R3 = maybe_sort(EtsTab, ets:match_object(EtsTab, '$1')),
  R3 = maybe_sort(Tab, Mod:match_object(Tab, '$1')),

  ok.

t_select_ops(Config) ->
  run_for_all_tables(Config, fun t_select_ops_/1).

t_select_ops_({Scope, Tab, EtsTab}) ->
  Mod = get_module(Scope, Tab),

  % insert some values
  KV = [
    {k1, 1}, {k2, 2}, {k3, 2}, {k1, 1}, {k4, 22}, {k5, 33},
    {k11, 1}, {k22, 2}, {k33, 2}, {k44, 11}, {k55, 22}, {k55, 33}
  ],
  true = ets:insert(EtsTab, KV),
  true = Mod:insert(Tab, KV),

  % select/2
  MS1 = ets:fun2ms(fun({K, V}) -> {K, V} end),
  R1 = maybe_sort(EtsTab, ets:select(EtsTab, MS1)),
  R1 = maybe_sort(Tab, Mod:select(Tab, MS1)),

  % select_reverse/2
  R2 = maybe_sort(EtsTab, ets:select_reverse(EtsTab, MS1)),
  R2 = maybe_sort(Tab, Mod:select_reverse(Tab, MS1)),

  % select_count/2
  MS2 = ets:fun2ms(fun({_K, V}) when V rem 2 == 0 -> true end),
  L1 = ets:select_count(EtsTab, MS2),
  L1 = Mod:select_count(Tab, MS2),

  % select_delete/2
  L2 = ets:select_delete(EtsTab, MS2),
  L2 = Mod:select_delete(Tab, MS2),
  R11 = maybe_sort(EtsTab, ets:select(EtsTab, MS1)),
  R11 = maybe_sort(Tab, Mod:select(Tab, MS1)),

  ok.

t_paginated_ops(Config) ->
  MS = ets:fun2ms(fun({K, V}) -> {K, V} end),
  Ops = [
    {select, MS},
    {select_reverse, MS},
    {match, '$1'},
    {match_object, '$1'}
  ],
  Tabs = ?SHARDS_TABS -- [?ORDERED_SET],
  {_, Scope} = lists:keyfind(scope, 1, Config),
  Args = [{Scope, Tab, Op} || Tab <- Tabs, Op <- Ops],
  lists:foreach(fun(X) ->
    true = cleanup_shards(),
    t_paginated_ops_(X)
  end, Args).

t_paginated_ops_({Scope, Tab, {Op, Q}}) ->
  Mod = get_module(Scope, Tab),

  % test empty
  '$end_of_table' = Mod:Op(Tab, Q, 10),

  % insert some values
  KVPairs = [
    {k1, 1}, {k2, 2}, {k3, 2}, {k1, 1}, {k4, 22}, {k5, 33},
    {k11, 1}, {k22, 2}, {k33, 2}, {k44, 11}, {k55, 22}, {k55, 33}
  ],
  true = Mod:insert(Tab, KVPairs),

  %% length
  Len = case Tab of
    _ when ?is_tab_sharded(Tab) -> 12;
    _                           -> 10
  end,

  % select/3
  {R1, C1} = Mod:Op(Tab, Q, 1),
  1 = length(R1),
  {R2, _} = Mod:Op(Tab, Q, 20),
  Len = length(R2),

  % select/1 - by 1
  {R11, Calls1} = select_by({Mod, Op}, C1, 1),
  Calls1 = Len,
  R2 = R11 ++ R1,

  % select/1 - by 2
  {R3, C2} = Mod:Op(Tab, Q, 2),
  2 = length(R3),
  {R22, Calls2} = select_by({Mod, Op}, C2, 2),
  Calls2 = round(Len / 2),
  R2 = R22 ++ R3,

  % select/1 - by 4
  {R4, C3} = Mod:Op(Tab, Q, 4),
  4 = length(R4),
  {R44, Calls3} = select_by({Mod, Op}, C3, 4),
  Calls3 = round(Len / 4),
  R2 = R44 ++ R4,

  ok.

t_paginated_ops_ordered_set(_Config) ->
  MS = ets:fun2ms(fun({K, V}) -> {K, V} end),
  Ops = [
    {select, MS},
    {select_reverse, MS},
    {match, '$1'},
    {match_object, '$1'}
  ],
  Args = [Op || Op <- Ops],
  lists:foreach(fun(X) ->
    true = cleanup_shards(),
    t_paginated_ops_ordered_set_(X)
  end, Args).

t_paginated_ops_ordered_set_({Op, Q}) ->
  % insert some values
  KVPairs = [
    {k1, 1}, {k2, 2}, {k3, 2}, {k1, 1}, {k4, 22}, {k5, 33},
    {k11, 1}, {k22, 2}, {k33, 2}, {k44, 11}, {k55, 22}, {k55, 33}
  ],
  true = ets:insert(?ETS_ORDERED_SET, KVPairs),
  true = shards:insert(?ORDERED_SET, KVPairs),

  % select/3
  {R1, EC1} = ets:Op(?ETS_ORDERED_SET, Q, 1),
  {R1, C1} = shards:Op(?ORDERED_SET, Q, 1),
  {R2, _} = ets:Op(?ETS_ORDERED_SET, Q, 20),
  {R2, _} = shards:Op(?ORDERED_SET, Q, 20),
  Len = length(R2),

  % select/1 - by 1
  {R11, Calls1} = select_by({ets, Op}, EC1, 1),
  {R11, Calls1} = select_by({shards, Op}, C1, 1),
  Calls1 = Len,

  % select/1 - by 2
  {R3, EC2} = ets:Op(?ETS_ORDERED_SET, Q, 2),
  {R3, C2} = shards:Op(?ORDERED_SET, Q, 2),
  {R22, Calls2} = select_by({ets, Op}, EC2, 2),
  {R22, Calls2} = select_by({shards, Op}, C2, 2),
  Calls2 = round(Len / 2),

  % select/1 - by 4
  {R4, EC3} = ets:Op(?ETS_ORDERED_SET, Q, 4),
  {R4, C3} = shards:Op(?ORDERED_SET, Q, 4),
  4 = length(R4),
  {R44, Calls3} = select_by({ets, Op}, EC3, 4),
  {R44, Calls3} = select_by({shards, Op}, C3, 4),
  Calls3 = round(Len / 4),

  ok.

t_first_last_next_prev_ops(Config) ->
  run_for_all_tables(Config, fun t_first_last_next_prev_ops_/1).

t_first_last_next_prev_ops_({_, ?SHARDED_DUPLICATE_BAG, _}) ->
  '$end_of_table' = shards:first(?SHARDED_DUPLICATE_BAG),
  '$end_of_table' = shards:last(?SHARDED_DUPLICATE_BAG),

  true = shards:insert(?SHARDED_DUPLICATE_BAG, {k1, 1}),
  F1 = shards:first(?SHARDED_DUPLICATE_BAG),
  F1 = shards:last(?SHARDED_DUPLICATE_BAG),

  % insert some values
  KVPairs = [
    {k2, 2}, {k3, 2}, {k4, 22}, {k5, 33},
    {k11, 1}, {k22, 2}, {k33, 2}, {k44, 11}, {k55, 22}
  ],
  true = shards:insert(?SHARDED_DUPLICATE_BAG, KVPairs),

  % check first-next against select
  _ = try first_next_traversal(shards, ?SHARDED_DUPLICATE_BAG, 10, [])
  catch _:bad_pick_fun_ret -> ok
  end,

  ok;
t_first_last_next_prev_ops_({_, ?ORDERED_SET, _}) ->
  '$end_of_table' = shards:first(?ORDERED_SET),
  '$end_of_table' = shards:last(?ORDERED_SET),

  true = shards:insert(?ORDERED_SET, {k1, 1}),
  F1 = shards:first(?ORDERED_SET),
  F1 = shards:last(?ORDERED_SET),

  % insert some values
  KVPairs = [
    {k1, 1}, {k2, 2}, {k3, 2}, {k4, 22}, {k5, 33},
    {k11, 1}, {k22, 2}, {k33, 2}, {k44, 11}, {k55, 22}
  ],
  true = shards:insert(?ORDERED_SET, KVPairs),
  true = ets:insert(?ETS_ORDERED_SET, KVPairs),

  % check first-next against select for 'ordered_set'
  L1 = [Last1 | _] = first_next_traversal(ets, ?ETS_ORDERED_SET, 10, []),
  L1 = first_next_traversal(shards, ?ORDERED_SET, 10, []),
  '$end_of_table' = ets:next(?ETS_ORDERED_SET, Last1),
  '$end_of_table' = shards:next(?ORDERED_SET, Last1),

  % check last-prev against select for 'ordered_set'
  L2 = [Last2 | _] = last_prev_traversal(ets, ?ETS_ORDERED_SET, 10, []),
  L2 = last_prev_traversal(shards, ?ORDERED_SET, 10, []),
  '$end_of_table' = ets:prev(?ETS_ORDERED_SET, Last2),
  '$end_of_table' = shards:prev(?ORDERED_SET, Last2),

  ok;
t_first_last_next_prev_ops_({Scope, Tab, _}) ->
  Mod = get_module(Scope, Tab),

  '$end_of_table' = Mod:first(Tab),
  '$end_of_table' = Mod:last(Tab),

  true = Mod:insert(Tab, {k1, 1}),
  F1 = Mod:first(Tab),
  F1 = Mod:last(Tab),

  % insert some values
  KVPairs = [
    {k2, 2}, {k3, 2}, {k4, 22}, {k5, 33},
    {k11, 1}, {k22, 2}, {k33, 2}, {k44, 11}, {k55, 22}
  ],
  true = Mod:insert(Tab, KVPairs),

  % match spec
  MS = ets:fun2ms(fun({K, V}) -> {K, V} end),

  % check first-next against select
  L1 = [Last | _] = first_next_traversal(Mod, Tab, 10, []),
  '$end_of_table' = Mod:next(Tab, Last),
  {L11, _} = Mod:select(Tab, MS, 10),
  L1 = [K || {K, _} <- L11],

  % check last-prev against select
  L3 = [Last3 | _] = last_prev_traversal(Mod, Tab, 10, []),
  '$end_of_table' = Mod:prev(Tab, Last3),
  {L33, _} = Mod:select(Tab, MS, 10),
  L3 = L1 = [K || {K, _} <- L33],

  ok.

t_update_ops(Config) ->
  {_, Scope} = lists:keyfind(scope, 1, Config),
  Mod = get_module(Scope, ?SET),

  % update counter
  _ = ets:insert(?ETS_SET, {counter1, 0}),
  _ = Mod:insert(?SET, {counter1, 0}),
  R1 = ets:update_counter(?ETS_SET, counter1, 1),
  R1 = Mod:update_counter(?SET, counter1, 1),

  % update with default
  R2 = ets:update_counter(?ETS_SET, counter2, 1, {counter2, 0}),
  R2 = shards:update_counter(?SET, counter2, 1, {counter2, 0}),

  % update element
  _ = ets:insert(?ETS_SET, {elem0, 0}),
  _ = Mod:insert(?SET, {elem0, 0}),
  R3 = ets:update_element(?ETS_SET, elem0, {2, 10}),
  R3 = Mod:update_element(?SET, elem0, {2, 10}),

  ok.

t_fold_ops(Config) ->
  {_, Scope} = lists:keyfind(scope, 1, Config),
  Mod = get_module(Scope, ?SET),

  % insert some values
  true = ets:insert(?ETS_SET, [{k1, 1}, {k2, 2}, {k3, 3}]),
  true = Mod:insert(?SET, [{k1, 1}, {k2, 2}, {k3, 3}]),

  % foldl
  Foldl = fun({_, V}, Acc) -> [V | Acc] end,
  R1 = lists:usort(Mod:foldl(Foldl, [], ?SET)),
  R1 = lists:usort(ets:foldl(Foldl, [], ?ETS_SET)),

  % foldr
  Foldr = fun({_, V}, Acc) -> [V | Acc] end,
  R2 = lists:usort(Mod:foldr(Foldr, [], ?SET)),
  R2 = lists:usort(ets:foldr(Foldr, [], ?ETS_SET)),

  ok.

t_info_ops(Config) ->
  {_, Scope} = lists:keyfind(scope, 1, Config),
  Mod = get_module(Scope, ?SET),

  % test i/0
  R0 = shards:i(),
  R0 = ets:i(),

  % test info/1,2
  DefaultShards = ?N_SHARDS,
  DefaultShards = length(Mod:info(?SET)),
  5 = length(shards:info(?DUPLICATE_BAG)),
  L1 = lists:duplicate(DefaultShards, public),
  L1 = Mod:info(?SET, protection),
  L2 = lists:duplicate(5, public),
  L2 = shards:info(?DUPLICATE_BAG, protection),

  % test info_shard/2,3
  SetShards = shards:list(?SET),
  ok = lists:foreach(fun({Shard, ShardName}) ->
    R1 = shards:info_shard(?SET, Shard),
    R1 = ets:info(ShardName)
  end, lists:zip(lists:seq(0, length(SetShards) - 1), SetShards)),
  ok = lists:foreach(fun({Shard, ShardName}) ->
    R1 = shards:info_shard(?SET, Shard, protection),
    R1 = ets:info(ShardName, protection)
  end, lists:zip(lists:seq(0, length(SetShards) - 1), SetShards)),

  % test info_shard/2,3
  DupBagShards = shards:list(?DUPLICATE_BAG),
  ok = lists:foreach(fun({Shard, ShardName}) ->
    R1 = shards:info_shard(?DUPLICATE_BAG, Shard),
    R1 = ets:info(ShardName)
  end, lists:zip(lists:seq(0, length(DupBagShards) - 1), DupBagShards)),
  ok = lists:foreach(fun({Shard, ShardName}) ->
    R1 = shards:info_shard(?DUPLICATE_BAG, Shard, protection),
    R1 = ets:info(ShardName, protection)
  end, lists:zip(lists:seq(0, length(DupBagShards) - 1), DupBagShards)),

  % test invalid values
  R2 = ets:info(undefined_tab),
  R2 = Mod:info(undefined_tab),
  R2 = shards_local:info(undefined_tab, nil),
  R2 = shards:info_shard(undefined_tab, 0),
  R3 = ets:info(undefined_tab, name),
  R3 = shards:info(undefined_tab, name),
  R3 = shards_local:info(undefined_tab, name, nil),
  R3 = shards:info_shard(undefined_tab, 0, name),

  ok.

t_tab2list_tab2file_file2tab(Config) ->
  {_, Scope} = lists:keyfind(scope, 1, Config),
  Mod = get_module(Scope, ?SET),

  % insert some values
  KVPairs = [{k1, 1}, {k2, 2}, {k3, 3}, {k4, 4}],
  true = ets:insert(?ETS_SET, KVPairs),
  true = Mod:insert(?SET, KVPairs),

  % check tab2list/1
  R1 = lists:usort(Mod:tab2list(?SET)),
  R1 = lists:usort(ets:tab2list(?ETS_SET)),
  4 = length(R1),

  % save tab to file
  DefaultShards = ?N_SHARDS,
  ok = Mod:tab2file(?SET, test_tab),
  ok = Mod:tab2file(?SET, <<"test_tab2">>, []),

  % errors
  {error, _} = Mod:tab2file(?SET, "mydir/test_tab3"),
  _ = try Mod:tab2file(?SET, [1, 2, 3])
  catch _:{badarg, _} -> ok
  end,
  _ = try Mod:tab2file(?SET, self())
  catch _:{badarg, _} -> ok
  end,

  % delete table
  true = Mod:delete(?SET),

  % tab info
  {ok, {_, _}} = Mod:tabfile_info("test_tab"),
  {error, _} = Mod:tabfile_info(123),
  {error, _} = Mod:tabfile_info(1.2),

  % errors
  {error, _} = Mod:file2tab("wrong_file"),
  ok = file:delete("test_tab2.1"),
  {error, _} = Mod:file2tab("test_tab2"),

  % restore table from files
  {ok, ?SET} = Mod:file2tab("test_tab"),

  % check
  DefaultShards = length(Mod:info(?SET)),
  KVPairs = lookup_keys(Mod, ?SET, [k1, k2, k3, k4]),

  ok.

t_rename(Config) ->
  run_for_all_tables(Config, fun t_rename_/1).

t_rename_({Scope, Tab, _}) ->
  Mod = get_module(Scope, Tab),

  % rename non existent table
  _ = try Mod:rename(wrong, new_name)
  catch _:badarg -> ok
  end,

  % insert some values
  L = lists:seq(1, 100),
  KVPairs = lists:zip(L, L),
  true = Mod:insert(Tab, KVPairs),
  R1 = lists:sort(lookup_keys(Mod, Tab, L)),
  100 = length(R1),

  % rename table
  foo = Mod:rename(Tab, foo),
  R1 = lists:sort(lookup_keys(Mod, foo, L)),

  % rename it back
  Tab = Mod:rename(foo, Tab),
  R1 = lists:sort(lookup_keys(Mod, Tab, L)),

  ok.

t_equivalent_ops(Config) ->
  {_, Scope} = lists:keyfind(scope, 1, Config),
  Mod = get_module(Scope, ?SET),

  MS = ets:fun2ms(fun({K, V}) -> {K, V} end),

  R1 = ets:is_compiled_ms(MS),
  R1 = shards:is_compiled_ms(MS),

  R2 = ets:match_spec_compile(MS),
  R2 = shards:match_spec_compile(MS),

  R3 = ets:match_spec_run([{k1, 1}], R2),
  R3 = shards:match_spec_run([{k1, 1}], R2),

  R4 = ets:test_ms({k1, 1}, MS),
  R4 = shards:test_ms({k1, 1}, MS),

  Shards = shards:list(?SET),
  R5 = [ets:table(T) || T <- Shards],
  R5 = Mod:table(?SET),
  R5 = Mod:table(?SET, []),

  true = Mod:setopts(?SET, [{heir, none}]),
  true = shards:setopts(?SET, []),

  true = Mod:safe_fixtable(?SET, true),
  true = shards:safe_fixtable(?SET, false),

  true = Mod:give_away(?SET, self(), []),

  ok.

%%%===================================================================
%%% Helpers
%%%===================================================================

init_shards(Scope) ->
  _ = init_shards_new(Scope),

  set = shards_local:info_shard(?SET, 0, type),
  duplicate_bag = shards_local:info_shard(?DUPLICATE_BAG, 0, type),
  ordered_set = shards_local:info_shard(?ORDERED_SET, 0, type),
  duplicate_bag = shards_local:info_shard(?SHARDED_DUPLICATE_BAG, 0, type),
  _ = shards_created(?SHARDS_TABS),

  _ = ets:new(?ETS_SET, [set, public, named_table]),
  _ = ets:give_away(?ETS_SET, whereis(?SET), []),
  _ = ets:new(?ETS_DUPLICATE_BAG, [duplicate_bag, public, named_table]),
  _ = ets:give_away(?ETS_DUPLICATE_BAG, whereis(?DUPLICATE_BAG), []),
  _ = ets:new(?ETS_ORDERED_SET, [ordered_set, public, named_table]),
  _ = ets:give_away(?ETS_ORDERED_SET, whereis(?ORDERED_SET), []),
  _ = ets:new(?ETS_SHARDED_DUPLICATE_BAG, [duplicate_bag, public, named_table]),
  _ = ets:give_away(?ETS_SHARDED_DUPLICATE_BAG, whereis(?SHARDED_DUPLICATE_BAG), []),
  ok.

%% @private
init_shards_new(Scope) ->
  Mod = case Scope of
    g -> shards_dist;
    _ -> shards_local
  end,

  % test badarg
  _ = try shards:new(?SET, [{scope, Scope}, wrongarg])
  catch _:{badarg, _} -> ok
  end,

  DefaultShards = ?N_SHARDS,
  ?SET = shards:new(?SET, [
    named_table,
    public,
    {scope, Scope},
    {pick_node_fun, fun ?MODULE:pick_node/3}
  ]),
  StateSet = shards:state(?SET),
  #{module := Mod,
    n_shards := DefaultShards
  } = shards_state:to_map(StateSet),

  ?DUPLICATE_BAG = shards:new(?DUPLICATE_BAG, [
    {n_shards, 5},
    {scope, Scope},
    duplicate_bag,
    {pick_node_fun, fun ?MODULE:pick_node/3}
  ]),
  StateDupBag = shards:state(?DUPLICATE_BAG),
  #{module := Mod,
    n_shards := 5
  } = shards_state:to_map(StateDupBag),

  ?ORDERED_SET = shards:new(?ORDERED_SET, [
    {scope, Scope},
    ordered_set
  ]),
  StateOrderedSet = shards:state(?ORDERED_SET),
  #{module := Mod,
    n_shards := 1
  } = shards_state:to_map(StateOrderedSet),

  ?SHARDED_DUPLICATE_BAG = shards:new(?SHARDED_DUPLICATE_BAG, [
    {n_shards, 5},
    {scope, Scope},
    duplicate_bag,
    {pick_shard_fun, fun ?MODULE:pick_shard/3},
    {pick_node_fun, fun ?MODULE:pick_node_dist/3}
  ]),
  StateShardedDupBag = shards:state(?SHARDED_DUPLICATE_BAG),
  #{module := Mod,
    n_shards := 5
  } = shards_state:to_map(StateShardedDupBag).

cleanup_shards() ->
  L = lists:duplicate(4, true),
  L = [shards:delete_all_objects(Tab) || Tab <- ?SHARDS_TABS],
  L = [ets:delete_all_objects(Tab) || Tab <- ?ETS_TABS],
  All = [[] = ets:match(Tab, '$1') || Tab <- ?ETS_TABS],
  All = [[] = shards:match(Tab, '$1') || Tab <- ?SHARDS_TABS],
  true.

delete_shards() ->
  L = lists:duplicate(4, true),
  L = [shards:delete(Tab) || Tab <- ?SHARDS_TABS],
  Count = supervisor:count_children(shards_sup),
  {_, 0} = lists:keyfind(workers, 1, Count),
  {_, 0} = lists:keyfind(supervisors, 1, Count),
  {_, 0} = lists:keyfind(active, 1, Count),
  ok.

build_args(Args, Config) ->
  {_, Scope} = lists:keyfind(scope, 1, Config),
  [{Scope, X, Y} || {X, Y} <- Args].

get_module(l, Tab) when ?has_default_opts(Tab) ->
  shards_local;
get_module(_, _) ->
  shards.

%%%===================================================================
%%% Internal functions
%%%===================================================================

run_for_all_tables(Config, Fun) ->
  Tables = lists:zip(?SHARDS_TABS, ?ETS_TABS),
  Args = build_args(Tables, Config),
  lists:foreach(fun(X) ->
    true = cleanup_shards(),
    Fun(X)
  end, Args).

lookup_keys(Mod, Tab, Keys) ->
  lists:foldr(fun(Key, Acc) ->
    case Mod:lookup(Tab, Key) of
      []     -> Acc;
      Values -> Values ++ Acc
    end
  end, [], Keys).

maybe_sort(Tab, List) when Tab == ?ORDERED_SET; Tab == ?ETS_ORDERED_SET ->
  List;
maybe_sort(_Tab, List) ->
  lists:usort(List).

shards_created(TabL) when is_list(TabL) ->
  lists:foreach(fun shards_created/1, TabL);
shards_created(Tab) ->
  lists:foreach(fun(Shard) ->
    true = lists:member(Shard, shards:all())
  end, shards:list(Tab)).

select_by({Mod, Fun} = ModFun, Continuation, Limit) ->
  select_by(ModFun, Mod:Fun(Continuation), Limit, {[], 1}).

select_by(_, '$end_of_table', _, Acc) ->
  Acc;
select_by({Mod, Fun} = ModFun, {L, Continuation}, Limit, {Acc, Calls}) ->
  select_by(ModFun, Mod:Fun(Continuation), Limit, {L ++ Acc, Calls + 1}).

first_next_traversal(_, _, 0, Acc) ->
  Acc;
first_next_traversal(_, _, _, ['$end_of_table' | Acc]) ->
  Acc;
first_next_traversal(Mod, Tab, Limit, []) ->
  first_next_traversal(Mod, Tab, Limit - 1, [Mod:first(Tab)]);
first_next_traversal(Mod, Tab, Limit, [Key | _] = Acc) ->
  first_next_traversal(Mod, Tab, Limit - 1, [Mod:next(Tab, Key) | Acc]).

last_prev_traversal(_, _, 0, Acc) ->
  Acc;
last_prev_traversal(_, _, _, ['$end_of_table' | Acc]) ->
  Acc;
last_prev_traversal(Mod, Tab, Limit, []) ->
  last_prev_traversal(Mod, Tab, Limit - 1, [Mod:last(Tab)]);
last_prev_traversal(Mod, Tab, Limit, [Key | _] = Acc) ->
  last_prev_traversal(Mod, Tab, Limit - 1, [Mod:prev(Tab, Key) | Acc]).
