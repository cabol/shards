-module(test_helper).

%% Test Cases
-export([
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
]).

-export([
  t_match_ops_/1,
  t_select_ops_/1
]).

%% Helpers
-export([
  init_shards/1,
  cleanup_shards/0,
  delete_shards/0
]).

%% Pick Callbacks
-export([
  pick_shard/3,
  pick_node/3,
  pick_node_dist/3
]).

-include_lib("stdlib/include/ms_transform.hrl").
-include("test_helper.hrl").

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

t_state(_Config) ->
  % test badarg
  wrong_tab = ets:new(wrong_tab, [public, named_table]),
  try shards_state:get(wrong_tab)
  catch _:{badarg, wrong_tab} -> ok
  end,

  StateSet = shards_state:get(?SET),
  DefaultShards = ?N_SHARDS,
  #{n_shards := DefaultShards} = shards_state:to_map(StateSet),

  Mod = shards_state:module(?SET),
  true = Mod == shards_local orelse Mod == shards_dist,
  DefaultShards = shards_state:n_shards(?SET),
  Fun1 = fun shards_local:pick/3,
  Fun1 = shards_state:pick_shard_fun(?SET),
  Fun2 = fun ?MODULE:pick_node/3,
  Fun2 = shards_state:pick_node_fun(?SET),
  true = shards_state:auto_eject_nodes(?SET),

  State0 = shards_state:new(),
  State1 = shards_state:module(shards_dist, State0),
  State2 = shards_state:n_shards(100, State1),
  Fun = fun(X, Y, Z) -> (X + Y + Z) rem Y end,
  State3 = shards_state:pick_shard_fun(Fun, State2),
  State4 = shards_state:pick_node_fun(Fun, State3),
  State5 = shards_state:auto_eject_nodes(false, State4),

  #{module           := shards_dist,
    n_shards         := 100,
    pick_shard_fun   := Fun,
    pick_node_fun    := Fun,
    auto_eject_nodes := false
  } = shards_state:to_map(State5),

  ok.

t_basic_ops(_Config) ->
  Tables = lists:zip(?SHARDS_TABS, ?ETS_TABS),
  lists:foreach(fun(X) ->
    true = cleanup_shards(),
    t_basic_ops_(X)
  end, Tables).

t_basic_ops_({Tab, EtsTab} = Args) ->
  % insert some K/V pairs
  Obj1 = {kx, 1, a, "hi"},
  KVPairs = [
    {k1, 1}, {k1, 2}, {k1, 1},
    {k2, 2},
    {k11, 11},
    {k22, 22},
    Obj1
  ],
  true = shards:insert(Tab, KVPairs),
  true = shards:insert(Tab, Obj1),
  true = ets:insert(EtsTab, KVPairs),
  true = ets:insert(EtsTab, Obj1),

  % insert new
  false = ets:insert_new(EtsTab, [Obj1, {k3, <<"V3">>}]),
  [false, true] = shards:insert_new(Tab, [Obj1, {k3, <<"V3">>}]),
  true = ets:insert_new(EtsTab, {k3, <<"V3">>}),
  false = shards:insert_new(Tab, {k3, <<"V3">>}),

  % lookup element
  case Tab of
    ?SHARDED_DUPLICATE_BAG ->
      R0 = ets:lookup_element(EtsTab, k1, 2),
      LenR3 = length(R0),
      R01 = shards:lookup_element(Tab, k1, 2),
      LenR3 = length(R01),
      SortR3 = lists:usort(R0),
      SortR3 = lists:usort(R01),

      try shards:lookup_element(Tab, wrong, 2)
      catch _:{badarg, _} -> ok
      end;
    _ ->
      R0 = ets:lookup_element(EtsTab, k1, 2),
      R0 = shards:lookup_element(Tab, k1, 2)
  end,

  % lookup
  R1 = lists:sort(lookup_keys(ets, EtsTab, [k1, k2, k3, kx])),
  R1 = lists:sort(lookup_keys(shards, Tab, [k1, k2, k3, kx])),

  % delete
  true = ets:delete_object(EtsTab, Obj1),
  true = ets:delete(EtsTab, k2),
  true = shards:delete_object(Tab, Obj1),
  true = shards:delete(Tab, k2),
  R2 = lists:sort(lookup_keys(ets, EtsTab, [k1, k2, kx])),
  R2 = lists:sort(lookup_keys(shards, Tab, [k1, k2, kx])),

  % member
  true = shards:member(Tab, k1),
  true = ets:member(EtsTab, k1),
  false = shards:member(Tab, kx),
  false = ets:member(EtsTab, kx),

  % take
  R3 = lists:sort(ets:take(EtsTab, k1)),
  R3 = lists:sort(shards:take(Tab, k1)),
  [] = ets:lookup(EtsTab, k1),
  [] = shards:lookup(Tab, k1),

  ct:log(" ---> t_basic_ops(~p): [OK]", [Args]),
  ok.

t_match_ops(_Config) ->
  Tables = lists:zip(?SHARDS_TABS, ?ETS_TABS),
  lists:foreach(fun(X) ->
    true = cleanup_shards(),
    t_match_ops_(X)
  end, Tables).

t_match_ops_({Tab, EtsTab} = Args) ->
  % insert some values
  KV = [
    {k1, 1}, {k2, 2}, {k3, 2}, {k1, 1}, {k4, 22}, {k5, 33},
    {k11, 1}, {k22, 2}, {k33, 2}, {k44, 11}, {k55, 22}, {k55, 33}
  ],
  true = ets:insert(EtsTab, KV),
  true = shards:insert(Tab, KV),

  % match/2
  R1 = maybe_sort(EtsTab, ets:match(EtsTab, '$1')),
  R1 = maybe_sort(Tab, shards:match(Tab, '$1')),

  % match_object/2
  R2 = maybe_sort(EtsTab, ets:match_object(EtsTab, '$1')),
  R2 = maybe_sort(Tab, shards:match_object(Tab, '$1')),

  % match_delete/2
  true = ets:match_delete(EtsTab, {'$1', 2}),
  true = shards:match_delete(Tab, {'$1', 2}),
  R3 = maybe_sort(EtsTab, ets:match_object(EtsTab, '$1')),
  R3 = maybe_sort(Tab, shards:match_object(Tab, '$1')),

  ct:log(" ---> t_match_ops(~p): [OK]", [Args]),
  ok.

t_select_ops(_Config) ->
  Tables = lists:zip(?SHARDS_TABS, ?ETS_TABS),
  lists:foreach(fun(X) ->
    true = cleanup_shards(),
    t_select_ops_(X)
  end, Tables).

t_select_ops_({Tab, EtsTab} = Args) ->
  % insert some values
  KV = [
    {k1, 1}, {k2, 2}, {k3, 2}, {k1, 1}, {k4, 22}, {k5, 33},
    {k11, 1}, {k22, 2}, {k33, 2}, {k44, 11}, {k55, 22}, {k55, 33}
  ],
  true = ets:insert(EtsTab, KV),
  true = shards:insert(Tab, KV),

  % select/2
  MS1 = ets:fun2ms(fun({K, V}) -> {K, V} end),
  R1 = maybe_sort(EtsTab, ets:select(EtsTab, MS1)),
  R1 = maybe_sort(Tab, shards:select(Tab, MS1)),

  % select_reverse/2
  R2 = maybe_sort(EtsTab, ets:select_reverse(EtsTab, MS1)),
  R2 = maybe_sort(Tab, shards:select_reverse(Tab, MS1)),

  % select_count/2
  MS2 = ets:fun2ms(fun({_K, V}) when V rem 2 == 0 -> true end),
  L1 = ets:select_count(EtsTab, MS2),
  L1 = shards:select_count(Tab, MS2),

  % select_delete/2
  L2 = ets:select_delete(EtsTab, MS2),
  L2 = shards:select_delete(Tab, MS2),
  R11 = maybe_sort(EtsTab, ets:select(EtsTab, MS1)),
  R11 = maybe_sort(Tab, shards:select(Tab, MS1)),

  ct:log(" ---> t_select_ops(~p): [OK]", [Args]),
  ok.

t_paginated_ops(_Config) ->
  MS = ets:fun2ms(fun({K, V}) -> {K, V} end),
  Ops = [
    {select, MS},
    {select_reverse, MS},
    {match, '$1'},
    {match_object, '$1'}
  ],
  Tabs = ?SHARDS_TABS -- [?ORDERED_SET],
  Args = [{Tab, Op} || Tab <- Tabs, Op <- Ops],
  lists:foreach(fun(X) ->
    true = cleanup_shards(),
    t_paginated_ops_(X)
  end, Args).

t_paginated_ops_({Tab, {Op, Q}} = Args) ->
  % test empty
  '$end_of_table' = shards:Op(Tab, Q, 10),

  % insert some values
  KVPairs = [
    {k1, 1}, {k2, 2}, {k3, 2}, {k1, 1}, {k4, 22}, {k5, 33},
    {k11, 1}, {k22, 2}, {k33, 2}, {k44, 11}, {k55, 22}, {k55, 33}
  ],
  true = shards:insert(Tab, KVPairs),

  %% length
  Len = case Tab of
    _ when ?is_tab_sharded(Tab) -> 12;
    _                           -> 10
  end,

  % select/3
  {R1, C1} = shards:Op(Tab, Q, 1),
  1 = length(R1),
  {R2, _} = shards:Op(Tab, Q, 20),
  Len = length(R2),

  % select/1 - by 1
  {R11, Calls1} = select_by({shards, Op}, C1, 1),
  Calls1 = Len,
  R2 = R11 ++ R1,

  % select/1 - by 2
  {R3, C2} = shards:Op(Tab, Q, 2),
  2 = length(R3),
  {R22, Calls2} = select_by({shards, Op}, C2, 2),
  Calls2 = round(Len / 2),
  R2 = R22 ++ R3,

  % select/1 - by 4
  {R4, C3} = shards:Op(Tab, Q, 4),
  4 = length(R4),
  {R44, Calls3} = select_by({shards, Op}, C3, 4),
  Calls3 = round(Len / 4),
  R2 = R44 ++ R4,

  ct:log(" ---> t_paginated_ops(~p): [OK]", [Args]),
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

t_paginated_ops_ordered_set_({Op, Q} = Args) ->
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

  ct:log(" ---> t_paginated_ops_ordered_set(~p): [OK]", [Args]),
  ok.

t_first_last_next_prev_ops(_Config) ->
  lists:foreach(fun(X) ->
    true = cleanup_shards(),
    t_first_last_next_prev_ops_(X)
  end, ?SHARDS_TABS).

t_first_last_next_prev_ops_(?SHARDED_DUPLICATE_BAG) ->
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
  try first_next_traversal(shards, ?SHARDED_DUPLICATE_BAG, 10, [])
  catch _:bad_pick_fun_ret -> ok
  end,

  ct:log(" ---> t_first_last_next_prev_ops(~p): [OK]", [?SHARDED_DUPLICATE_BAG]),
  ok;
t_first_last_next_prev_ops_(?ORDERED_SET) ->
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

  ct:log(" ---> t_first_last_next_prev_ops(~p): [OK]", [?ORDERED_SET]),
  ok;
t_first_last_next_prev_ops_(Tab) ->
  '$end_of_table' = shards:first(Tab),
  '$end_of_table' = shards:last(Tab),

  true = shards:insert(Tab, {k1, 1}),
  F1 = shards:first(Tab),
  F1 = shards:last(Tab),

  % insert some values
  KVPairs = [
    {k2, 2}, {k3, 2}, {k4, 22}, {k5, 33},
    {k11, 1}, {k22, 2}, {k33, 2}, {k44, 11}, {k55, 22}
  ],
  true = shards:insert(Tab, KVPairs),

  % match spec
  MS = ets:fun2ms(fun({K, V}) -> {K, V} end),

  % check first-next against select
  L1 = [Last | _] = first_next_traversal(shards, Tab, 10, []),
  '$end_of_table' = shards:next(Tab, Last),
  {L11, _} = shards:select(Tab, MS, 10),
  L1 = [K || {K, _} <- L11],

  % check last-prev against select
  L3 = [Last3 | _] = last_prev_traversal(shards, Tab, 10, []),
  '$end_of_table' = shards:prev(Tab, Last3),
  {L33, _} = shards:select(Tab, MS, 10),
  L3 = L1 = [K || {K, _} <- L33],

  ct:log(" ---> t_first_last_next_prev_ops(~p): [OK]", [Tab]),
  ok.

t_update_ops(_Config) ->
  % update counter
  ets:insert(?ETS_SET, {counter1, 0}),
  shards:insert(?SET, {counter1, 0}),
  R1 = ets:update_counter(?ETS_SET, counter1, 1),
  R1 = shards:update_counter(?SET, counter1, 1),

  % update with default
  R2 = ets:update_counter(?ETS_SET, counter2, 1, {counter2, 0}),
  R2 = shards:update_counter(?SET, counter2, 1, {counter2, 0}),

  % update element
  ets:insert(?ETS_SET, {elem0, 0}),
  shards:insert(?SET, {elem0, 0}),
  R3 = ets:update_element(?ETS_SET, elem0, {2, 10}),
  R3 = shards:update_element(?SET, elem0, {2, 10}),

  ok.

t_fold_ops(_Config) ->
  % insert some values
  true = ets:insert(?ETS_SET, [{k1, 1}, {k2, 2}, {k3, 3}]),
  true = shards:insert(?SET, [{k1, 1}, {k2, 2}, {k3, 3}]),

  % foldl
  Foldl = fun({_, V}, Acc) -> [V | Acc] end,
  R1 = lists:usort(shards:foldl(Foldl, [], ?SET)),
  R1 = lists:usort(ets:foldl(Foldl, [], ?ETS_SET)),

  % foldr
  Foldr = fun({_, V}, Acc) -> [V | Acc] end,
  R2 = lists:usort(shards:foldr(Foldr, [], ?SET)),
  R2 = lists:usort(ets:foldr(Foldr, [], ?ETS_SET)),

  ok.

t_info_ops(_Config) ->
  % test i/0
  R0 = shards:i(),
  R0 = ets:i(),

  % test info/1,2
  DefaultShards = ?N_SHARDS,
  DefaultShards = length(shards:info(?SET)),
  5 = length(shards:info(?DUPLICATE_BAG)),
  L1 = lists:duplicate(DefaultShards, public),
  L1 = shards:info(?SET, protection),
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
  R2 = shards:info(undefined_tab),
  R2 = shards_local:info(undefined_tab, nil),
  R2 = shards:info_shard(undefined_tab, 0),
  R3 = ets:info(undefined_tab, name),
  R3 = shards:info(undefined_tab, name),
  R3 = shards_local:info(undefined_tab, name, nil),
  R3 = shards:info_shard(undefined_tab, 0, name),

  ok.

t_tab2list_tab2file_file2tab(_Config) ->
  % insert some values
  KVPairs = [{k1, 1}, {k2, 2}, {k3, 3}, {k4, 4}],
  true = ets:insert(?ETS_SET, KVPairs),
  true = shards:insert(?SET, KVPairs),

  % check tab2list/1
  R1 = lists:usort(shards:tab2list(?SET)),
  R1 = lists:usort(ets:tab2list(?ETS_SET)),
  4 = length(R1),

  % save tab to files
  DefaultShards = ?N_SHARDS,
  L = lists:duplicate(DefaultShards, ok),
  FileL = [
    "myfile0", "myfile1", "myfile2", "myfile3",
    "myfile4", "myfile5", "myfile6", "myfile7"
  ],
  L = shards:tab2file(?SET, FileL),

  % delete table
  shards:delete(?SET),

  % restore table from files
  {error, _} = shards:file2tab(["myfile0", "wrong_file"]),
  {ok, ?SET} = shards:file2tab(FileL),

  % check
  [_, _, _, _, _, _, _, _] = shards:info(?SET),
  KVPairs = lookup_keys(shards, ?SET, [k1, k2, k3, k4]),

  ok.

t_equivalent_ops(_Config) ->
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
  R5 = shards:table(?SET),

  true = shards:setopts(?SET, [{heir, none}]),

  true = shards:give_away(?SET, self(), []),

  ok.

%%%===================================================================
%%% Helpers
%%%===================================================================

init_shards(Scope) ->
  init_shards_new(Scope),

  set = shards_local:info_shard(?SET, 0, type),
  duplicate_bag = shards_local:info_shard(?DUPLICATE_BAG, 0, type),
  ordered_set = shards_local:info_shard(?ORDERED_SET, 0, type),
  duplicate_bag = shards_local:info_shard(?SHARDED_DUPLICATE_BAG, 0, type),
  shards_created(?SHARDS_TABS),

  ets:new(?ETS_SET, [set, public, named_table]),
  ets:give_away(?ETS_SET, whereis(?SET), []),
  ets:new(?ETS_DUPLICATE_BAG, [duplicate_bag, public, named_table]),
  ets:give_away(?ETS_DUPLICATE_BAG, whereis(?DUPLICATE_BAG), []),
  ets:new(?ETS_ORDERED_SET, [ordered_set, public, named_table]),
  ets:give_away(?ETS_ORDERED_SET, whereis(?ORDERED_SET), []),
  ets:new(?ETS_SHARDED_DUPLICATE_BAG, [duplicate_bag, public, named_table]),
  ets:give_away(?ETS_SHARDED_DUPLICATE_BAG, whereis(?SHARDED_DUPLICATE_BAG), []),
  ok.

%% @private
init_shards_new(Scope) ->
  Mod = case Scope of
    g -> shards_dist;
    _ -> shards_local
  end,

  % test badarg
  try shards:new(?SET, [{scope, Scope}, wrongarg])
  catch _:badarg -> ok
  end,

  DefaultShards = ?N_SHARDS,
  {_, StateSet} = shards:new(?SET, [
    {scope, Scope},
    {pick_node_fun, fun ?MODULE:pick_node/3}
  ]),
  StateSet = shards:state(?SET),
  #{module := Mod,
    n_shards := DefaultShards
  } = shards_state:to_map(StateSet),

  {_, StateDupBag} = shards:new(?DUPLICATE_BAG, [
    {n_shards, 5},
    {scope, Scope},
    duplicate_bag,
    {pick_node_fun, fun ?MODULE:pick_node/3}
  ]),
  StateDupBag = shards:state(?DUPLICATE_BAG),
  #{module := Mod,
    n_shards := 5
  } = shards_state:to_map(StateDupBag),

  {_, StateOrderedSet} = shards:new(?ORDERED_SET, [
    {scope, Scope},
    ordered_set
  ]),
  StateOrderedSet = shards:state(?ORDERED_SET),
  #{module := Mod,
    n_shards := 1
  } = shards_state:to_map(StateOrderedSet),

  {_, StateShardedDupBag} = shards:new(?SHARDED_DUPLICATE_BAG, [
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

%%%===================================================================
%%% Internal functions
%%%===================================================================

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
