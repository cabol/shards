-module(shards_SUITE).

-include_lib("stdlib/include/ms_transform.hrl").
-include_lib("shards_ct.hrl").

%% Common Test
-export([
  all/0,
  init_per_suite/1,
  end_per_suite/1,
  init_per_testcase/2,
  end_per_testcase/2
]).

%% Shards Tests
-export([
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
]).

%% Extra Tests
-export([
  t_metadata_function_wrappers/1,
  t_table_deleted_when_partition_goes_down/1,
  t_shards_table_unhandled_callbacks/1,
  t_table_creation_errors/1
]).

% %% Test Cases Helpers
% -export([
%   t_basic_ops_/1,
%   t_match_ops_/1,
%   t_select_ops_/1
% ]).

% %% Helpers
% -export([
%   init_shards/0,
%   init_shards/1,
%   cleanup_shards/0,
%   delete_shards/0
% ]).

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
  _ = init_shards([{keypos, #test_rec.name}]),
  true = cleanup_shards(),
  Config;
init_per_testcase(_, Config) ->
  _ = init_shards(),
  true = cleanup_shards(),
  Config.

-spec end_per_testcase(atom(), shards_ct:config()) -> shards_ct:config().
end_per_testcase(_, Config) ->
  _ = delete_shards(),
  Config.

%%%===================================================================
%%% Shards Tests
%%%===================================================================

-spec t_basic_ops(shards_ct:config()) -> any().
t_basic_ops(_Config) ->
  run_for_all_tables(fun t_basic_ops_/1).

%% @private
t_basic_ops_({Tab, EtsTab}) ->
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
  true = ets:insert_new(EtsTab, {k3, <<"V3">>}),
  true = shards:insert_new(Tab, [{1, 1}, {2, 2}, {3, 3}]),

  false = shards:insert_new(Tab, [Obj1, {k3, <<"V3">>}]),
  false = shards:insert_new(Tab, Obj1),
  true = shards:insert_new(Tab, {k3, <<"V3">>}),

  % lookup element
  R0 = ets:lookup_element(EtsTab, k1, 2),
  R0 = shards:lookup_element(Tab, k1, 2),

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

  % delete all
  true = shards:delete_all_objects(Tab).

-spec t_query_ops(shards_ct:config()) -> any().
t_query_ops(_Config) ->
  MS = ets:fun2ms(fun({K, V}) -> {K, V} end),

  Ops = [
    {select, MS},
    {select_reverse, MS},
    {match, '$1'},
    {match_object, '$1'}
  ],

  Tabs = lists:zip(?ETS_TABS, ?SHARDS_TABS),

  lists:foreach(fun(X) ->
    true = cleanup_shards(),
    t_query_ops_(X)
  end, [{Ets, Shards, Op} || {Ets, Shards} <- Tabs, Op <- Ops]).

%% @private
t_query_ops_({EtsTab, Tab, {Op, MS}}) ->
  % insert some values
  KV = KV = [{X, X} || X <- lists:seq(1, 10000)],
  true = ets:insert(EtsTab, KV),
  true = shards:insert(Tab, KV),

  lists:foreach(fun(N) ->
    R1 = length(element(1, ets:Op(EtsTab, MS, N))),
    R1 = length(element(1, shards:Op(Tab, MS, N)))
  end, [100, 1000, 2000]),

  R11 = lists:sort(element(1, ets:Op(EtsTab, MS, 100000))),
  R11 = lists:sort(element(1, shards:Op(Tab, MS, 100000))),

  F = fun(Mod, AccIn) ->
    try
      lists:foldl(fun
        (_, {{R, C}, Acc}) ->
          {Mod:Op(C), R ++ Acc};

        (_, {'$end_of_table', Acc}) ->
          throw(Acc)
      end, {AccIn, []}, lists:seq(1, 1000000))
    catch
      throw:R -> R
    end
  end,

  {R2, C2} = ets:Op(EtsTab, MS, 10),
  {R22, C22} = shards:Op(Tab, MS, 10),

  R3 = lists:sort(F(ets, {R2, C2})),
  R3 = lists:sort(F(shards, {R22, C22})).

-spec t_match_ops(shards_ct:config()) -> any().
t_match_ops(_Config) ->
  run_for_all_tables(fun t_match_ops_/1).

%% @private
t_match_ops_({Tab, EtsTab}) ->
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
  R3 = maybe_sort(Tab, shards:match_object(Tab, '$1')).

-spec t_select_ops(shards_ct:config()) -> any().
t_select_ops(_Config) ->
  run_for_all_tables(fun t_select_ops_/1).

%% @private
t_select_ops_({Tab, EtsTab}) ->
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
  R11 = maybe_sort(Tab, shards:select(Tab, MS1)).

-spec t_select_replace(shards_ct:config()) -> any().
t_select_replace(_Config) ->
  run_for_all_tables(fun t_select_replace_/1).

% @private
t_select_replace_({Tab, EtsTab}) ->
  % insert some values
  KV = [
    {k1, 1}, {k2, 2}, {k3, 2}, {k1, 1}, {k4, 22}, {k5, 33},
    {k11, 1}, {k22, 2}, {k33, 2}, {k44, 11}, {k55, 22}, {k55, 33}
  ],
  true = ets:insert(EtsTab, KV),
  true = shards:insert(Tab, KV),

  MS = ets:fun2ms(fun({K, V}) when V rem 2 == 0 -> {K, {marker, V}} end),
  Count = ets:select_replace(EtsTab, MS),
  Count = shards:select_replace(Tab, MS),

  L = lists:sort(ets:tab2list(EtsTab)),
  L = lists:sort(shards:tab2list(Tab)).

-spec t_paginated_ops(shards_ct:config()) -> any().
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

%% @private
t_paginated_ops_({Tab, {Op, Q}}) ->
  % test empty
  '$end_of_table' = shards:Op(Tab, Q, 10),

  % insert some values
  KVPairs = [
    {k1, 1}, {k2, 2}, {k3, 2}, {k1, 1}, {k4, 22}, {k5, 33},
    {k11, 1}, {k22, 2}, {k33, 2}, {k44, 11}, {k55, 22}, {k55, 33}
  ],
  true = shards:insert(Tab, KVPairs),

  %% length
  Len =
    case Tab of
      ?DUPLICATE_BAG -> 12;
      _              -> 10
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
  {R22, _} = select_by({shards, Op}, C2, 2),
  R2 = R22 ++ R3,

  % select/1 - by 4
  {R4, C3} = shards:Op(Tab, Q, 4),
  4 = length(R4),
  {R44, _} = select_by({shards, Op}, C3, 4),
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

  lists:foreach(fun(X) ->
    true = cleanup_shards(),
    t_paginated_ops_ordered_set_(X)
  end, [Op || Op <- Ops]).

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
t_first_last_next_prev_ops(_Config) ->
  run_for_all_tables(fun t_first_last_next_prev_ops_/1).

t_first_last_next_prev_ops_({?ORDERED_SET, _}) ->
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

t_first_last_next_prev_ops_({Tab, _}) ->
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
  L3 = L1 = [K || {K, _} <- L33].

-spec t_update_ops(shards_ct:config()) -> any().
t_update_ops(_Config) ->
  % update_counter
  _ = ets:insert(?ETS_SET, {counter1, 0}),
  _ = shards:insert(?SET, {counter1, 0}),
  R1 = ets:update_counter(?ETS_SET, counter1, 1),
  R1 = shards:update_counter(?SET, counter1, 1),

  % update_counter several values
  _ = ets:insert(?ETS_SET, {counter11, 0, 0}),
  _ = shards:insert(?SET, {counter11, 0, 0}),
  R11 = ets:update_counter(?ETS_SET, counter11, [{2, 1}, {3, 2}]),
  R11 = shards:update_counter(?SET, counter11, [{2, 1}, {3, 2}]),

  % update_counter with default
  R2 = ets:update_counter(?ETS_SET, counter2, 1, {counter2, 0}),
  R2 = shards:update_counter(?SET, counter2, 1, {counter2, 0}),

  % update_counter with default and several values
  R22 = ets:update_counter(?ETS_SET, counter22, [{2, 1}, {3, 2}], {counter22, 0, 0}),
  R22 = shards:update_counter(?SET, counter22, [{2, 1}, {3, 2}], {counter22, 0, 0}),

  % update_element
  _ = ets:insert(?ETS_SET, {elem0, 0}),
  _ = shards:insert(?SET, {elem0, 0}),
  R3 = ets:update_element(?ETS_SET, elem0, {2, 10}),
  R3 = shards:update_element(?SET, elem0, {2, 10}).

-spec t_fold_ops(shards_ct:config()) -> any().
t_fold_ops(_Config) ->
  run_for_all_tables(fun t_fold_ops_/1).

%% @private
t_fold_ops_({Tab, EtsTab}) ->
  % insert some values
  It = lists:seq(1, 100),
  KVPairs = lists:zip(It, It),
  true = ets:insert(EtsTab, KVPairs),
  true = shards:insert(Tab, KVPairs),

  % foldl
  Foldl = fun({_, V}, Acc) -> [V | Acc] end,
  R1 = lists:sort(shards:foldl(Foldl, [], Tab)),
  R1 = lists:sort(ets:foldl(Foldl, [], EtsTab)),

  % foldr
  Foldr = fun({_, V}, Acc) -> [V | Acc] end,
  R2 = lists:sort(shards:foldr(Foldr, [], Tab)),
  R2 = lists:sort(ets:foldr(Foldr, [], EtsTab)).

-spec t_info_ops(shards_ct:config()) -> any().
t_info_ops(_Config) ->
  run_for_all_tables(fun t_info_ops_/1).

%% @private
t_info_ops_({Tab, EtsTab}) ->
  R0 = shards:i(),
  R0 = ets:i(),

  R1 = ets:info(EtsTab),
  R11 = shards:info(Tab),
  undefined = shards:info(invalid),

  Meta = shards:table_meta(Tab),
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
t_tab2list(_Config) ->
  run_for_all_tables(fun t_tab2list_/1).

%% @private
t_tab2list_({Tab, EtsTab}) ->
  % insert some values
  It = lists:seq(1, 100),
  KVPairs = lists:zip(It, It),
  true = ets:insert(EtsTab, KVPairs),
  true = shards:insert(Tab, KVPairs),

  % check tab2list/1
  R1 = lists:sort(shards:tab2list(Tab)),
  R1 = lists:sort(ets:tab2list(EtsTab)),
  100 = length(R1).

-spec t_tab2file_file2tab_tabfile_info(shards_ct:config()) -> any().
t_tab2file_file2tab_tabfile_info(_Config) ->
  run_for_all_tables(fun t_tab2file_file2tab_tabfile_info_/1).

%% @private
t_tab2file_file2tab_tabfile_info_({Tab, _EtsTab}) ->
  FN = shards_lib:to_string(Tab) ++ "_test_tab",

  % insert some values
  It = lists:seq(1, 100),
  KVPairs = lists:zip(It, It),
  true = shards:insert(Tab, KVPairs),

  % save tab to file
  ok = shards:tab2file(Tab, FN),
  ok = shards:tab2file(Tab, FN ++ "2", []),

  % errors
  {error, _} = shards:tab2file(Tab, "mydir/" ++ FN ++ "3"),

  shards_ct:assert_error(fun() ->
    shards:tab2file(Tab, [1, 2, 3])
  end, {badarg, [1, 2, 3]}),

  % tab info
  ShardFN = FN ++ "2.0",

  ok = file:delete(ShardFN),
  {ok, TabInfo} = shards:tabfile_info(FN),
  {error, _} = shards:tabfile_info(FN ++ "2"),

  % check tab info attrs
  {ok, ShardTabInfo} = ets:tabfile_info(FN ++ ".0"),
  {Items, _} = lists:unzip(ShardTabInfo),

  ok = lists:foreach(fun(Item) ->
    {Item, _} = lists:keyfind(Item, 1, TabInfo)
  end, Items),

  Partitions = shards_meta:partitions(Tab),
  Partitions = shards_lib:keyfind(partitions, TabInfo),

  % delete table
  true = shards:delete(Tab),

  % errors
  {error, _} = shards:file2tab("wrong_file"),
  {error, _} = shards:file2tab(FN ++ "2"),
  {error, _} = shards:file2tab([1, 2, 3]),

  % restore table from files
  {ok, Tab} = shards:file2tab(FN, []),
  Partitions = shards:info(Tab, partitions),
  KVPairs = lookup_keys(shards, Tab, It).

-spec t_rename(shards_ct:config()) -> any().
t_rename(_Config) ->
  run_for_all_tables(fun t_rename_/1).

%% @private
t_rename_({Tab, _}) ->
  % rename non existent table
  shards_ct:assert_error(fun() ->
    shards:rename(wrong, new_name)
  end, {unknown_table, wrong}),

  % insert some values
  L = lists:seq(1, 100),
  KVPairs = lists:zip(L, L),
  true = shards:insert(Tab, KVPairs),
  R1 = lists:sort(lookup_keys(shards, Tab, L)),
  100 = length(R1),

  % rename table
  foo = shards:rename(Tab, foo),
  R1 = lists:sort(lookup_keys(shards, foo, L)),

  % rename it back
  Tab = shards:rename(foo, Tab),
  R1 = lists:sort(lookup_keys(shards, Tab, L)).

-spec t_equivalent_ops(shards_ct:config()) -> any().
t_equivalent_ops(_Config) ->
  run_for_all_tables(fun t_equivalent_ops_/1).

%% @private
t_equivalent_ops_({Tab, _EtsTab}) ->
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
  R5 = lists:sort([ets:table(Id) || {_, Id} <- Ids]),
  R5 = lists:sort(shards:table(Tab)),
  R5 = lists:sort(shards:table(Tab, [])),

  true = shards:setopts(Tab, [{heir, none}]),
  true = shards:setopts(Tab, []),

  true = shards:safe_fixtable(Tab, true),
  true = shards:safe_fixtable(Tab, false).

-spec t_keypos(shards_ct:config()) -> any().
t_keypos(_Config) ->
  run_for_all_tables(fun t_keypos_/1).

t_keypos_({Tab, _EtsTab}) ->
  true = shards:insert(Tab, [#test_rec{name = a, age = 30}, #test_rec{name = b, age = 20}]),
  [#test_rec{name = a, age = 30}] = shards:lookup(Tab, a),
  [#test_rec{name = b, age = 20}] = shards:lookup(Tab, b),

  true = shards:insert_new(Tab, #test_rec{name = c, age = 15}),
  [#test_rec{name = c, age = 15}] = shards:lookup(Tab, c),

  true = shards:delete_object(Tab, #test_rec{name = b, age = 20}),

  [#test_rec{name = a, age = 30}] = shards:lookup(Tab, a),
  [#test_rec{name = c, age = 15}] = shards:lookup(Tab, c),
  [] = shards:lookup(Tab, b).

%%%===================================================================
%%% Extra Tests
%%%===================================================================

-spec t_metadata_function_wrappers(shards_ct:config()) -> any().
t_metadata_function_wrappers(_Config) ->
  shards_ct:with_table(fun(Tab) ->
    ok = shards:put_meta(Tab, foo, bar),
    bar = shards:get_meta(Tab, foo),
    undefined = shards:get_meta(Tab, foo_foo),
    bar_bar = shards:get_meta(Tab, foo_foo, bar_bar),

    shards_ct:assert_error(fun() ->
      shards:get_meta(unknown, foo)
    end, {unknown_table, unknown})
  end, meta_table, []).

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

  StateSet = shards:table_meta(?SET),
  SetPartitions = shards_meta:partitions(StateSet),
  SetPartitions = ?PARTITIONS,

  ?DUPLICATE_BAG =
    shards:new(?DUPLICATE_BAG, [
      named_table,
      duplicate_bag,
      {partitions, 5},
      {keyslot_fun, fun shards_ct:pick_shard/2},
      {parallel, true},
      {parallel_timeout, 5000}
      | Opts
    ]),

  StateDupBag = shards:table_meta(?DUPLICATE_BAG),
  5 = shards_meta:partitions(StateDupBag),

  ?ORDERED_SET =
    shards:new(?ORDERED_SET, [
      named_table,
      ordered_set
      | Opts
    ]),

  StateOrderedSet = shards:table_meta(?ORDERED_SET),
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

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
run_for_all_tables(Fun) ->
  lists:foreach(fun(X) ->
    Fun(X)
  end, lists:zip(?SHARDS_TABS, ?ETS_TABS)).

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
  lists:sort(List).

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
