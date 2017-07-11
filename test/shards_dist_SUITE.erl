-module(shards_dist_SUITE).

-include_lib("common_test/include/ct.hrl").

%% Common Test
-export([
  all/0,
  groups/0,
  init_per_suite/1,
  end_per_suite/1,
  init_per_testcase/2,
  end_per_testcase/2
]).

%% Common Test Cases
-include_lib("mixer/include/mixer.hrl").
-mixin([
  {shards_test_helper, [
    t_basic_ops/1,
    t_update_ops/1,
    t_fold_ops/1,
    t_rename/1,
    t_info_ops/1,
    t_tab2list/1,
    t_tab2file_file2tab_tabfile_info/1
  ]}
]).

%% Tests Cases
-export([
  t_join_leave_ops/1,
  t_match_ops/1,
  t_select_ops/1,
  t_eject_node_on_failure/1,
  t_delete_and_auto_setup_tab/1
]).

-include("support/shards_test_helper.hrl").

-define(SLAVES, [a, b, c, d, e, f]).

%%%===================================================================
%%% Common Test
%%%===================================================================

all() ->
  [{group, dist_test_group}].

groups() ->
  [{dist_test_group, [sequence], [
    t_join_leave_ops,
    t_basic_ops,
    t_update_ops,
    t_fold_ops,
    t_match_ops,
    t_select_ops,
    t_rename,
    t_info_ops,
    t_tab2list,
    t_tab2file_file2tab_tabfile_info,
    t_eject_node_on_failure,
    t_delete_and_auto_setup_tab
  ]}].

init_per_suite(Config) ->
  _ = shards:start(),
  Nodes = start_slaves(?SLAVES),
  [{nodes, Nodes}, {scope, g} | Config].

end_per_suite(Config) ->
  _ = shards:stop(),
  Config.

init_per_testcase(_, Config) ->
  Config.

end_per_testcase(_, Config) ->
  _ = cleanup_tabs(Config),
  Config.

%%%===================================================================
%%% Tests Cases
%%%===================================================================

t_join_leave_ops(Config) ->
  {_, Nodes} = lists:keyfind(nodes, 1, Config),
  AllNodes = lists:usort([node() | Nodes]),
  OkNodes1 = lists:usort([node() | lists:droplast(Nodes)]),
  ENode = lists:last(OkNodes1),

  % setup tables
  _ = setup_tabs(Config),
  _ = timer:sleep(500),

  % join and check nodes
  Tabs = ?SHARDS_TABS -- [?ORDERED_SET],
  lists:foreach(fun(Tab) ->
    AllNodes = shards:join(Tab, AllNodes),
    timer:sleep(500),
    7 = length(shards:get_nodes(Tab))
  end, Tabs),

  % check no duplicate members
  Members = pg2:get_members(?SET),
  AllNodes = shards:join(?SET, AllNodes),
  _ = timer:sleep(500),
  Members = pg2:get_members(?SET),

  % stop F node
  _ = stop_slaves([f]),

  % check nodes
  6 = length(shards:get_nodes(?SET)),
  6 = length(shards:get_nodes(?DUPLICATE_BAG)),

  % check nodes
  OkNodes1 = shards:get_nodes(?SET),
  R1 = [A, B, C, CT, D, E] = get_remote_nodes(AllNodes, ?SET),
  R1 = get_remote_nodes(AllNodes, ?DUPLICATE_BAG),
  OkNodes1 = A = B = C = CT = D = E,

  % leave an invalid node
  OkNodes1 = shards:leave(?SET, [wrongnode]),
  timer:sleep(500),

  % leave node E from SET
  OkNodes2 = lists:usort([node() | lists:droplast(OkNodes1)]),
  OkNodes2 = shards:leave(?SET, [ENode]),
  _ = timer:sleep(500),
  OkNodes2 = shards:get_nodes(?SET),
  [A2, B2, C2, CT2, D2] = get_remote_nodes(OkNodes2, ?SET),
  OkNodes2 = A2 = B2 = C2 = CT2 = D2,

  % check nodes
  5 = length(shards:get_nodes(?SET)),
  6 = length(shards:get_nodes(?DUPLICATE_BAG)),

  % join E node
  OkNodes1 = shards:join(?SET, [ENode]),
  _ = timer:sleep(500),
  R1 = get_remote_nodes(OkNodes1, ?SET),
  R1 = get_remote_nodes(AllNodes, ?DUPLICATE_BAG),

  % check nodes
  6 = length(shards:get_nodes(?SET)),
  6 = length(shards:get_nodes(?DUPLICATE_BAG)),

  ok.

t_match_ops(Config) ->
  Tabs = ?SHARDS_TABS -- [?ORDERED_SET],
  EtsTabs = ?ETS_TABS -- [?ETS_ORDERED_SET],
  Tables = lists:zip(Tabs, EtsTabs),
  Args = shards_test_helper:build_args(Tables, Config),
  lists:foreach(fun(X) ->
    true = shards_test_helper:cleanup_shards(),
    shards_test_helper:t_match_ops_(X)
  end, Args).

t_select_ops(Config) ->
  Tabs = ?SHARDS_TABS -- [?ORDERED_SET],
  EtsTabs = ?ETS_TABS -- [?ETS_ORDERED_SET],
  Tables = lists:zip(Tabs, EtsTabs),
  Args = shards_test_helper:build_args(Tables, Config),
  lists:foreach(fun(X) ->
    true = shards_test_helper:cleanup_shards(),
    shards_test_helper:t_select_ops_(X)
  end, Args).

t_eject_node_on_failure(Config) ->
  ok = cleanup_tabs(Config),

  UpNodes = shards:get_nodes(?SET),
  6 = length(UpNodes),

  % add new node
  NewNodes = [Z] = start_slaves([z]),
  ok = rpc:call(Z, shards_test_helper, init_shards, [g]),
  UpNodes1 = shards:join(?SET, NewNodes),
  _ = timer:sleep(500),
  UpNodes1 = shards:get_nodes(?SET),

  % insert some data on that node
  Z = lists:last(UpNodes1),
  Z = lists:nth(shards_test_helper:pick_node(2, length(UpNodes1), r) + 1, UpNodes1),
  true = shards:insert(?SET, {2, 2}),
  _ = timer:sleep(500),

  % cause an error
  ok = rpc:call(Z, shards, stop, []),
  _ = timer:sleep(500),
  7 = length(UpNodes1),

  % new node should be ejected on failure
  UpNodes2 = lists:usort(UpNodes1 -- NewNodes),
  UpNodes2 = shards:get_nodes(?SET),
  6 = length(UpNodes2),

  % stop failure node
  _ = stop_slaves([z]),

  ok.

t_delete_and_auto_setup_tab(Config) ->
  ok = cleanup_tabs(Config),

  UpNodes = shards:get_nodes(?SET),
  6 = length(UpNodes),

  true = shards:delete(?SET),
  _ = timer:sleep(500),
  [] = shards:get_nodes(?SET),
  [A, B, C, CT, D, E] = get_remote_nodes(UpNodes, ?SET),
  [] = A = B = C = CT = D = E,

  ?SET = shards:new(?SET, [{scope, g}, {nodes, UpNodes}]),
  6 = length(shards:get_nodes(?SET)),

  ok.

%%%===================================================================
%%% Internal functions
%%%===================================================================

start_slaves(Slaves) ->
  start_slaves(Slaves, []).

start_slaves([], Acc) ->
  lists:usort(Acc);
start_slaves([Node | T], Acc) ->
  ErlFlags = "-pa ../../lib/*/ebin",
  {ok, HostNode} = ct_slave:start(Node, [
    {kill_if_fail, true},
    {monitor_master, true},
    {boot_timeout, 5000},
    {init_timeout, 3000},
    {startup_timeout, 5000},
    {startup_functions, [{shards, start, []}]},
    {erl_flags, ErlFlags}
  ]),
  ok = ensure_shards_started(HostNode),
  start_slaves(T, [HostNode | Acc]).

ensure_shards_started(Node) ->
  Res = rpc:call(Node, shards, i, []),
  ensure_shards_started(Node, Res, 10).

ensure_shards_started(_Node, ok, _) ->
  ok;
ensure_shards_started(Node, _, N) when N =< 0 ->
  error({startup_node_error, Node});
ensure_shards_started(Node, _, N) when N > 0 ->
  timer:sleep(500),
  Res = rpc:call(Node, shards, i, []),
  ensure_shards_started(Node, Res, N - 1).

stop_slaves(Slaves) ->
  stop_slaves(Slaves, []).

stop_slaves([], Acc) ->
  lists:usort(Acc);
stop_slaves([Node | T], Acc) ->
  {ok, _Name} = ct_slave:stop(Node),
  pang = net_adm:ping(Node),
  stop_slaves(T, [Node | Acc]).

get_remote_nodes(Nodes, Tab) ->
  {ResL, _} = rpc:multicall(Nodes, shards, get_nodes, [Tab]),
  ResL.

setup_tabs(Config) ->
  {_, Nodes} = lists:keyfind(nodes, 1, Config),
  AllNodes = lists:usort([node() | Nodes]),

  {_, []} = rpc:multicall(
    AllNodes, shards_test_helper, init_shards, [g]),
  ok.

cleanup_tabs(Config) ->
  {_, Nodes} = lists:keyfind(nodes, 1, Config),
  AllNodes = lists:usort([node() | Nodes]),

  {_, _} = rpc:multicall(
    AllNodes, shards_test_helper, cleanup_shards, []),
  ok.
