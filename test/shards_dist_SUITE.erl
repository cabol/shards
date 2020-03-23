-module(shards_dist_SUITE).

-include_lib("common_test/include/ct.hrl").
-include("support/shards_ct.hrl").

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
  {shards_tests, [
    t_update_ops/1,
    t_fold_ops/1,
    t_rename/1,
    t_info_ops/1,
    t_tab2list/1,
    t_tab2file_file2tab_tabfile_info/1,
    t_keypos/1
  ]}
]).

%% Tests Cases
-export([
  t_join_leave_ops/1,
  t_basic_ops/1,
  t_match_ops/1,
  t_select_ops/1,
  t_eject_node_on_failure/1,
  t_delete_and_auto_setup_tab/1
]).

-define(SLAVES, [
  'a@127.0.0.1',
  'b@127.0.0.1',
  'c@127.0.0.1',
  'd@127.0.0.1',
  'e@127.0.0.1',
  'f@127.0.0.1'
]).

%%%===================================================================
%%% Common Test
%%%===================================================================

-spec all() -> [{group, atom()}].
all() ->
  [{group, dist_test_group}].

-spec groups() -> [any()].
groups() ->
  [
    {dist_test_group, [sequence], [
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
    ]}
  ].

-spec init_per_suite(shards_ct:config()) -> shards_ct:config().
init_per_suite(Config) ->
  ok = start_primary_node(),
  ok = shards:start(),
  ok = allow_boot(),
  Nodes = start_slaves(?SLAVES),
  [{nodes, Nodes}, {scope, g} | Config].

-spec end_per_suite(shards_ct:config()) -> shards_ct:config().
end_per_suite(Config) ->
  _ = shards:stop(),
  Config.

-spec init_per_testcase(atom(), shards_ct:config()) -> shards_ct:config().
init_per_testcase(_, Config) ->
  Config.

-spec end_per_testcase(atom(), shards_ct:config()) -> shards_ct:config().
end_per_testcase(_, Config) ->
  _ = cleanup_tabs(Config),
  Config.

%%%===================================================================
%%% Tests Cases
%%%===================================================================

-spec t_join_leave_ops(shards_ct:config()) -> any().
t_join_leave_ops(Config) ->
  {_, Nodes} = lists:keyfind(nodes, 1, Config),
  AllNodes = lists:usort([node() | Nodes]),
  OkNodes1 = lists:usort([node() | lists:droplast(Nodes)]),
  ENode = lists:last(OkNodes1),

  % setup tables
  _ = setup_tabs(Config),
  _ = timer:sleep(3000),

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
  _ = stop_slaves(['f@127.0.0.1']),

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
  6 = length(shards:get_nodes(?DUPLICATE_BAG)).

-spec t_basic_ops(shards_ct:config()) -> any().
t_basic_ops(Config) ->
  exec_test(t_basic_ops_, Config).

-spec t_match_ops(shards_ct:config()) -> any().
t_match_ops(Config) ->
  exec_test(t_match_ops_, Config).

-spec t_select_ops(shards_ct:config()) -> any().
t_select_ops(Config) ->
  exec_test(t_select_ops_, Config).

-spec t_eject_node_on_failure(shards_ct:config()) -> any().
t_eject_node_on_failure(Config) ->
  ok = cleanup_tabs(Config),

  UpNodes = shards:get_nodes(?SET),
  6 = length(UpNodes),

  % add new node
  NewNodes = [Z] = start_slaves(['z@127.0.0.1']),
  ok = rpc:call(Z, shards_tests, init_shards, [g]),
  UpNodes1 = shards:join(?SET, NewNodes),
  _ = timer:sleep(500),
  UpNodes1 = shards:get_nodes(?SET),

  % insert some data on that node
  Z = lists:last(UpNodes1),
  Z = lists:nth(shards_tests:pick_node(2, length(UpNodes1), r) + 1, UpNodes1),
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
  stop_slaves(['z@127.0.0.1']).

-spec t_delete_and_auto_setup_tab(shards_ct:config()) -> any().
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
  6 = length(shards:get_nodes(?SET)).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
start_primary_node() ->
  {ok, _} = net_kernel:start(['ct@127.0.0.1']),
  true = erlang:set_cookie(node(), shards),
  ok.

%% @private
allow_boot() ->
  _ = erl_boot_server:start([]),
  {ok, IPv4} = inet:parse_ipv4_address("127.0.0.1"),
  erl_boot_server:add_slave(IPv4).

%% @private
start_slaves(Slaves) ->
  start_slaves(Slaves, []).

%% @private
start_slaves([], Acc) ->
  lists:usort(Acc);
start_slaves([Node | T], Acc) ->
  start_slaves(T, [spawn_node(Node) | Acc]).

%% @private
spawn_node(Node) ->
  Cookie = atom_to_list(erlang:get_cookie()),
  InetLoaderArgs = "-loader inet -hosts 127.0.0.1 -setcookie " ++ Cookie,

  {ok, Node} =
    slave:start(
      "127.0.0.1",
      node_name(Node),
      InetLoaderArgs
    ),

  ok = rpc:block_call(Node, code, add_paths, [code:get_path()]),
  {ok, _} = rpc:block_call(Node, application, ensure_all_started, [shards]),
  ok = load_support_files(Node),
  Node.

%% @private
node_name(Node) ->
  [Name, _] = binary:split(atom_to_binary(Node, utf8), <<"@">>),
  binary_to_atom(Name, utf8).

%% @private
load_support_files(Node) ->
  {module, shards_tests} = rpc:block_call(Node, code, load_file, [shards_tests]),
  ok.

%% @private
stop_slaves(Slaves) ->
  stop_slaves(Slaves, []).

%% @private
stop_slaves([], Acc) ->
  lists:usort(Acc);
stop_slaves([Node | T], Acc) ->
  ok = slave:stop(Node),
  pang = net_adm:ping(Node),
  stop_slaves(T, [Node | Acc]).

%% @private
get_remote_nodes(Nodes, Tab) ->
  {ResL, _} = rpc:multicall(Nodes, shards, get_nodes, [Tab]),
  ResL.

%% @private
setup_tabs(Config) ->
  {_, Nodes} = lists:keyfind(nodes, 1, Config),
  AllNodes = lists:usort([node() | Nodes]),
  {_, []} = rpc:multicall(AllNodes, shards_tests, init_shards, [g]),
  ok.

%% @private
cleanup_tabs(Config) ->
  {_, Nodes} = lists:keyfind(nodes, 1, Config),
  AllNodes = lists:usort([node() | Nodes]),
  {_, _} = rpc:multicall(AllNodes, shards_tests, cleanup_shards, []),
  ok.

%% @private
exec_test(TestCase, Config) ->
  Tabs = ?SHARDS_TABS -- [?ORDERED_SET],
  EtsTabs = ?ETS_TABS -- [?ETS_ORDERED_SET],
  Tables = lists:zip(Tabs, EtsTabs),
  Args = shards_tests:build_args(Tables, Config),
  lists:foreach(fun(X) ->
    true = shards_tests:cleanup_shards(),
    shards_tests:TestCase(X)
  end, Args).
