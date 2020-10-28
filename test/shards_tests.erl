-module(shards_tests).

-include_lib("stdlib/include/ms_transform.hrl").
-include_lib("shards_ct.hrl").

%% Test Cases
-export([
  t_basic_ops/1,
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
]).

%% Test Cases Helpers
-export([
  t_basic_ops_/1,
  t_match_ops_/1,
  t_select_ops_/1
]).

%% Helpers
-export([
  init_shards/0,
  init_shards/1,
  cleanup_shards/0,
  delete_shards/0,
  build_args/2
]).

%% Pick Callbacks
-export([
  pick_shard/2
]).

%%%===================================================================
%%% Tests Key Generator
%%%===================================================================

%% @doc This is a shards_meta:pick_fun().
%% @hidden
pick_shard(Key, N) ->
  erlang:phash2(Key, N).

%%%===================================================================
%%% Test Cases
%%%===================================================================

-spec t_basic_ops(shards_ct:config()) -> any().
t_basic_ops(Config) ->
  run_for_all_tables(Config, fun t_basic_ops_/1).

%% @private
t_basic_ops_({Mod, Tab, EtsTab}) ->
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
  true = ets:insert_new(EtsTab, {k3, <<"V3">>}),
  true = Mod:insert_new(Tab, [{1, 1}, {2, 2}, {3, 3}]),

  false = Mod:insert_new(Tab, [Obj1, {k3, <<"V3">>}]),
  false = Mod:insert_new(Tab, Obj1),
  true = Mod:insert_new(Tab, {k3, <<"V3">>}),

  % lookup element
  R0 = ets:lookup_element(EtsTab, k1, 2),
  R0 = Mod:lookup_element(Tab, k1, 2),

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
  true = Mod:delete_all_objects(Tab).

-spec t_match_ops(shards_ct:config()) -> any().
t_match_ops(Config) ->
  run_for_all_tables(Config, fun t_match_ops_/1).

%% @private
t_match_ops_({Mod, Tab, EtsTab}) ->
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
  R3 = maybe_sort(Tab, Mod:match_object(Tab, '$1')).

-spec t_select_ops(shards_ct:config()) -> any().
t_select_ops(Config) ->
  run_for_all_tables(Config, fun t_select_ops_/1).

%% @private
t_select_ops_({Mod, Tab, EtsTab}) ->
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
  R11 = maybe_sort(Tab, Mod:select(Tab, MS1)).

-spec t_select_replace(shards_ct:config()) -> any().
t_select_replace(Config) ->
  run_for_all_tables(Config, fun t_select_replace_/1).

% @private
t_select_replace_({Mod, Tab, EtsTab}) ->
  % insert some values
  KV = [
    {k1, 1}, {k2, 2}, {k3, 2}, {k1, 1}, {k4, 22}, {k5, 33},
    {k11, 1}, {k22, 2}, {k33, 2}, {k44, 11}, {k55, 22}, {k55, 33}
  ],
  true = ets:insert(EtsTab, KV),
  true = Mod:insert(Tab, KV),

  MS = ets:fun2ms(fun({K, V}) when V rem 2 == 0 -> {K, {marker, V}} end),
  Count = ets:select_replace(EtsTab, MS),
  Count = Mod:select_replace(Tab, MS),

  L = lists:usort(ets:tab2list(EtsTab)),
  L = lists:usort(Mod:tab2list(Tab)).

-spec t_paginated_ops(shards_ct:config()) -> any().
t_paginated_ops(Config) ->
  {_, Mod} = lists:keyfind(module, 1, Config),
  MS = ets:fun2ms(fun({K, V}) -> {K, V} end),

  Ops = [
    {select, MS},
    {select_reverse, MS},
    {match, '$1'},
    {match_object, '$1'}
  ],

  Tabs = ?SHARDS_TABS -- [?ORDERED_SET],
  Args = [{Mod, Tab, Op} || Tab <- Tabs, Op <- Ops],

  lists:foreach(fun(X) ->
    true = cleanup_shards(),
    t_paginated_ops_(X)
  end, Args).

%% @private
t_paginated_ops_({Mod, Tab, {Op, Q}}) ->
  % test empty
  '$end_of_table' = Mod:Op(Tab, Q, 10),

  % insert some values
  KVPairs = [
    {k1, 1}, {k2, 2}, {k3, 2}, {k1, 1}, {k4, 22}, {k5, 33},
    {k11, 1}, {k22, 2}, {k33, 2}, {k44, 11}, {k55, 22}, {k55, 33}
  ],
  true = Mod:insert(Tab, KVPairs),

  %% length
  Len =
    case Tab of
      ?DUPLICATE_BAG -> 12;
      _              -> 10
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
  R2 = R44 ++ R4.

-spec t_paginated_ops_ordered_set(shards_ct:config()) -> any().
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

%% @private
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
  Calls3 = round(Len / 4).

-spec t_first_last_next_prev_ops(shards_ct:config()) -> any().
t_first_last_next_prev_ops(Config) ->
  run_for_all_tables(Config, fun t_first_last_next_prev_ops_/1).

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
  '$end_of_table' = shards:prev(?ORDERED_SET, Last2);

t_first_last_next_prev_ops_({Mod, Tab, _}) ->
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
  L3 = L1 = [K || {K, _} <- L33].

-spec t_update_ops(shards_ct:config()) -> any().
t_update_ops(Config) ->
  {_, Mod} = lists:keyfind(module, 1, Config),

  % update_counter
  _ = ets:insert(?ETS_SET, {counter1, 0}),
  _ = Mod:insert(?SET, {counter1, 0}),
  R1 = ets:update_counter(?ETS_SET, counter1, 1),
  R1 = Mod:update_counter(?SET, counter1, 1),

  % update_counter several values
  _ = ets:insert(?ETS_SET, {counter11, 0, 0}),
  _ = Mod:insert(?SET, {counter11, 0, 0}),
  R11 = ets:update_counter(?ETS_SET, counter11, [{2, 1}, {3, 2}]),
  R11 = Mod:update_counter(?SET, counter11, [{2, 1}, {3, 2}]),

  % update_counter with default
  R2 = ets:update_counter(?ETS_SET, counter2, 1, {counter2, 0}),
  R2 = shards:update_counter(?SET, counter2, 1, {counter2, 0}),

  % update_counter with default and several values
  R22 = ets:update_counter(?ETS_SET, counter22, [{2, 1}, {3, 2}], {counter22, 0, 0}),
  R22 = shards:update_counter(?SET, counter22, [{2, 1}, {3, 2}], {counter22, 0, 0}),

  % update_element
  _ = ets:insert(?ETS_SET, {elem0, 0}),
  _ = Mod:insert(?SET, {elem0, 0}),
  R3 = ets:update_element(?ETS_SET, elem0, {2, 10}),
  R3 = Mod:update_element(?SET, elem0, {2, 10}).

-spec t_fold_ops(shards_ct:config()) -> any().
t_fold_ops(Config) ->
  run_for_all_tables(Config, fun t_fold_ops_/1).

%% @private
t_fold_ops_({Mod, Tab, EtsTab}) ->
  % insert some values
  It = lists:seq(1, 100),
  KVPairs = lists:zip(It, It),
  true = ets:insert(EtsTab, KVPairs),
  true = Mod:insert(Tab, KVPairs),

  % foldl
  Foldl = fun({_, V}, Acc) -> [V | Acc] end,
  R1 = lists:usort(Mod:foldl(Foldl, [], Tab)),
  R1 = lists:usort(ets:foldl(Foldl, [], EtsTab)),

  % foldr
  Foldr = fun({_, V}, Acc) -> [V | Acc] end,
  R2 = lists:usort(Mod:foldr(Foldr, [], Tab)),
  R2 = lists:usort(ets:foldr(Foldr, [], EtsTab)).

-spec t_info_ops(shards_ct:config()) -> any().
t_info_ops(Config) ->
  run_for_all_tables(Config, fun t_info_ops_/1).

%% @private
t_info_ops_({Mod, Tab, EtsTab}) ->
  R0 = Mod:i(),
  R0 = ets:i(),

  R1 = ets:info(EtsTab),
  R11 = Mod:info(Tab),
  undefined = Mod:info(invalid),

  Meta = shards:meta(Tab),
  Partitions = shards_meta:partitions(Meta),

  lists:foreach(fun
    ({partitions, V})  -> V = Partitions;
    ({keyslot_fun, V}) -> V = shards_meta:keyslot_fun(Meta);
    ({parallel, V})    -> V = shards_meta:parallel(Meta);
    ({memory, V})      -> V = shards_lib:keyfind(memory, R1) * Partitions;
    ({name, V})        -> V = ets:info(Tab, name);
    ({id, V})          -> V = ets:info(Tab, id);
    ({owner, V})       -> V = shards_meta:tab_pid(Meta);
    ({K, V})           -> V = shards_lib:keyfind(K, R1)
  end, R11).

-spec t_tab2list(shards_ct:config()) -> any().
t_tab2list(Config) ->
  run_for_all_tables(Config, fun t_tab2list_/1).

%% @private
t_tab2list_({Mod, Tab, EtsTab}) ->
  % insert some values
  It = lists:seq(1, 100),
  KVPairs = lists:zip(It, It),
  true = ets:insert(EtsTab, KVPairs),
  true = Mod:insert(Tab, KVPairs),

  % check tab2list/1
  R1 = lists:usort(Mod:tab2list(Tab)),
  R1 = lists:usort(ets:tab2list(EtsTab)),
  100 = length(R1).

-spec t_tab2file_file2tab_tabfile_info(shards_ct:config()) -> any().
t_tab2file_file2tab_tabfile_info(Config) ->
  run_for_all_tables(Config, fun t_tab2file_file2tab_tabfile_info_/1).

%% @private
t_tab2file_file2tab_tabfile_info_({Mod, Tab, _EtsTab}) ->
  FN = shards_lib:to_string(Tab) ++ "_test_tab",

  % insert some values
  It = lists:seq(1, 100),
  KVPairs = lists:zip(It, It),
  true = Mod:insert(Tab, KVPairs),

  % save tab to file
  ok = Mod:tab2file(Tab, FN),
  ok = Mod:tab2file(Tab, FN ++ "2", []),

  % errors
  {error, _} = Mod:tab2file(Tab, "mydir/" ++ FN ++ "3"),

  shards_ct:assert_error(fun() ->
    Mod:tab2file(Tab, [1, 2, 3])
  end, {badarg, [1, 2, 3]}),

  % tab info
  ShardFN = FN ++ "2.0",

  ok = file:delete(ShardFN),
  {ok, TabInfo} = Mod:tabfile_info(FN),
  {error, _} = Mod:tabfile_info(FN ++ "2"),

  % check tab info attrs
  {ok, ShardTabInfo} = ets:tabfile_info(FN ++ ".0"),
  {Items, _} = lists:unzip(ShardTabInfo),

  ok = lists:foreach(fun(Item) ->
    {Item, _} = lists:keyfind(Item, 1, TabInfo)
  end, Items),

  Partitions = shards_meta:partitions(Tab),
  Partitions = shards_lib:keyfind(partitions, TabInfo),

  % delete table
  true = Mod:delete(Tab),

  % errors
  {error, _} = Mod:file2tab("wrong_file"),
  {error, _} = Mod:file2tab(FN ++ "2"),
  {error, _} = Mod:file2tab([1, 2, 3]),

  % restore table from files
  {ok, Tab} = Mod:file2tab(FN, []),
  Partitions = Mod:info(Tab, partitions),
  KVPairs = lookup_keys(Mod, Tab, It).

-spec t_rename(shards_ct:config()) -> any().
t_rename(Config) ->
  run_for_all_tables(Config, fun t_rename_/1).

%% @private
t_rename_({Mod, Tab, _}) ->
  % rename non existent table
  shards_ct:assert_error(fun() ->
    Mod:rename(wrong, new_name)
  end, {unknown_table, wrong}),

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
  R1 = lists:sort(lookup_keys(Mod, Tab, L)).

-spec t_equivalent_ops(shards_ct:config()) -> any().
t_equivalent_ops(Config) ->
  run_for_all_tables(Config, fun t_equivalent_ops_/1).

%% @private
t_equivalent_ops_({Mod, Tab, _EtsTab}) ->
  MS = ets:fun2ms(fun({K, V}) -> {K, V} end),

  R1 = ets:is_compiled_ms(MS),
  R1 = shards:is_compiled_ms(MS),

  R21 = ets:match_spec_compile(MS),
  R22 = shards:match_spec_compile(MS),

  R3 = ets:match_spec_run([{k1, 1}], R21),
  R3 = shards:match_spec_run([{k1, 1}], R22),

  R4 = ets:test_ms({k1, 1}, MS),
  R4 = shards:test_ms({k1, 1}, MS),

  Ids = shards_meta:get_partition_tids(Tab),
  R5 = lists:usort([ets:table(Id) || {_, Id} <- Ids]),
  R5 = lists:usort(Mod:table(Tab)),
  R5 = lists:usort(Mod:table(Tab, [])),

  true = Mod:setopts(Tab, [{heir, none}]),
  true = shards:setopts(Tab, []),

  true = Mod:safe_fixtable(Tab, true),
  true = shards:safe_fixtable(Tab, false).

-spec t_keypos(shards_ct:config()) -> any().
t_keypos(Config) ->
  run_for_all_tables(Config, fun t_keypos_/1).

t_keypos_({Mod, Tab, _EtsTab}) ->
  true = Mod:insert(Tab, [#test_rec{name = a, age = 30}, #test_rec{name = b, age = 20}]),
  [#test_rec{name = a, age = 30}] = Mod:lookup(Tab, a),
  [#test_rec{name = b, age = 20}] = Mod:lookup(Tab, b),

  true = Mod:insert_new(Tab, #test_rec{name = c, age = 15}),
  [#test_rec{name = c, age = 15}] = Mod:lookup(Tab, c),

  true = Mod:delete_object(Tab, #test_rec{name = b, age = 20}),

  [#test_rec{name = a, age = 30}] = Mod:lookup(Tab, a),
  [#test_rec{name = c, age = 15}] = Mod:lookup(Tab, c),
  [] = Mod:lookup(Tab, b).

%%%===================================================================
%%% Helpers
%%%===================================================================

%% @equiv init_shards(Scope, [])
init_shards() ->
  init_shards([]).

-spec init_shards(list()) -> ok | no_return().
init_shards(Opts) ->
  _ = init_shards_new(Opts),

  set = shards:info(?SET, type),
  duplicate_bag = shards:info(?DUPLICATE_BAG, type),
  ordered_set = shards:info(?ORDERED_SET, type),

  ok = shards_created(?SHARDS_TABS),

  _ = ets:new(?ETS_SET, [set, public, named_table | Opts]),
  _ = ets:give_away(?ETS_SET, whereis(?SET), []),
  _ = ets:new(?ETS_DUPLICATE_BAG, [duplicate_bag, public, named_table | Opts]),
  _ = ets:give_away(?ETS_DUPLICATE_BAG, whereis(?DUPLICATE_BAG), []),
  _ = ets:new(?ETS_ORDERED_SET, [ordered_set, public, named_table | Opts]),
  _ = ets:give_away(?ETS_ORDERED_SET, whereis(?ORDERED_SET), []),
  ok.

%% @private
init_shards_new(Opts) ->
  ?SET =
    shards:new(?SET, [
      named_table,
      public
      | Opts
    ]),

  StateSet = shards:meta(?SET),
  SetPartitions = shards_meta:partitions(StateSet),
  SetPartitions = ?PARTITIONS,

  ?DUPLICATE_BAG =
    shards:new(?DUPLICATE_BAG, [
      named_table,
      duplicate_bag,
      {partitions, 5},
      {keyslot_fun, fun ?MODULE:pick_shard/2},
      {parallel, true},
      {parallel_timeout, 5000}
      | Opts
    ]),

  StateDupBag = shards:meta(?DUPLICATE_BAG),
  5 = shards_meta:partitions(StateDupBag),

  ?ORDERED_SET =
    shards:new(?ORDERED_SET, [
      named_table,
      ordered_set
      | Opts
    ]),

  StateOrderedSet = shards:meta(?ORDERED_SET),
  1 =  shards_meta:partitions(StateOrderedSet).

-spec cleanup_shards() -> true | no_return().
cleanup_shards() ->
  L = lists:duplicate(3, true),
  L = [shards:delete_all_objects(Tab) || Tab <- ?SHARDS_TABS],
  L = [ets:delete_all_objects(Tab) || Tab <- ?ETS_TABS],
  All = [[] = ets:match(Tab, '$1') || Tab <- ?ETS_TABS],
  All = [[] = shards:match(Tab, '$1') || Tab <- ?SHARDS_TABS],
  true.

-spec delete_shards() -> ok | no_return().
delete_shards() ->
  L = lists:duplicate(3, true),
  L = [shards:delete(Tab) || Tab <- ?SHARDS_TABS],
  ok.

-spec build_args([any()], shards_ct:config()) -> [any()].
build_args(Args, Config) ->
  {_, Mod} = lists:keyfind(module, 1, Config),
  [{Mod, X, Y} || {X, Y} <- Args].

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
run_for_all_tables(Config, Fun) ->
  Tables = lists:zip(?SHARDS_TABS, ?ETS_TABS),
  Args = build_args(Tables, Config),
  lists:foreach(fun(X) ->
    Fun(X)
  end, Args).

%% @private
lookup_keys(Mod, Tab, Keys) ->
  lists:foldr(fun(Key, Acc) ->
    case Mod:lookup(Tab, Key) of
      []     -> Acc;
      Values -> Values ++ Acc
    end
  end, [], Keys).

%% @private
maybe_sort(Tab, List) when Tab == ?ORDERED_SET; Tab == ?ETS_ORDERED_SET ->
  List;
maybe_sort(_Tab, List) ->
  lists:usort(List).

%% @private
shards_created(TabL) when is_list(TabL) ->
  lists:foreach(fun shards_created/1, TabL);
shards_created(Tab) ->
  lists:foreach(fun({_, Tid}) ->
    true = lists:member(Tid, shards:all())
  end, shards_meta:get_partition_tids(Tab)).

%% @private
select_by({Mod, Fun} = ModFun, Continuation, Limit) ->
  select_by(ModFun, Mod:Fun(Continuation), Limit, {[], 1}).

%% @private
select_by(_, '$end_of_table', _, Acc) ->
  Acc;
select_by({Mod, Fun} = ModFun, {L, Continuation}, Limit, {Acc, Calls}) ->
  select_by(ModFun, Mod:Fun(Continuation), Limit, {L ++ Acc, Calls + 1}).

%% @private
first_next_traversal(_, _, 0, Acc) ->
  Acc;
first_next_traversal(_, _, _, ['$end_of_table' | Acc]) ->
  Acc;
first_next_traversal(Mod, Tab, Limit, []) ->
  first_next_traversal(Mod, Tab, Limit - 1, [Mod:first(Tab)]);
first_next_traversal(Mod, Tab, Limit, [Key | _] = Acc) ->
  first_next_traversal(Mod, Tab, Limit - 1, [Mod:next(Tab, Key) | Acc]).

%% @private
last_prev_traversal(_, _, 0, Acc) ->
  Acc;
last_prev_traversal(_, _, _, ['$end_of_table' | Acc]) ->
  Acc;
last_prev_traversal(Mod, Tab, Limit, []) ->
  last_prev_traversal(Mod, Tab, Limit - 1, [Mod:last(Tab)]);
last_prev_traversal(Mod, Tab, Limit, [Key | _] = Acc) ->
  last_prev_traversal(Mod, Tab, Limit - 1, [Mod:prev(Tab, Key) | Acc]).
