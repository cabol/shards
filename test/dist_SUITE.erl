-module(dist_SUITE).

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
  {test_helper, [
    t_basic_ops/1,
    t_match_ops/1,
    t_select_ops/1
  ]}
]).

%% Tests Cases
-export([
  t_join_leave_ops/1,
  t_eject_node_on_failure/1,
  t_delete_tabs/1
]).

-include("test_helper.hrl").

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
    t_match_ops,
    t_select_ops,
    t_eject_node_on_failure,
    t_delete_tabs
  ]}].

init_per_suite(Config) ->
  _ = shards:start(),
  Nodes = start_slaves(?SLAVES),
  [{nodes, Nodes} | Config].

end_per_suite(Config) ->
  shards:stop(),
  Config.

init_per_testcase(_, Config) ->
  Config.

end_per_testcase(_, Config) ->
  cleanup_tabs(Config),
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
  setup_tabs(Config),
  timer:sleep(500),

  % join and check nodes
  lists:foreach(fun(Tab) ->
    AllNodes = shards:join(Tab, AllNodes),
    7 = length(shards:get_nodes(Tab))
  end, ?SHARDS_TABS),

  % check no duplicate members
  Members = pg2:get_members(?SET),
  AllNodes = shards:join(?SET, AllNodes),
  Members = pg2:get_members(?SET),

  % stop F node
  stop_slaves([f]),

  % check nodes
  6 = length(shards:get_nodes(?SET)),
  6 = length(shards:get_nodes(?DUPLICATE_BAG)),

  % check nodes
  OkNodes1 = shards:get_nodes(?SET),
  R1 = [A, B, C, CT, D, E] = get_remote_nodes(AllNodes, ?SET),
  R1 = get_remote_nodes(AllNodes, ?DUPLICATE_BAG),
  OkNodes1 = A = B = C = CT = D = E,

  % leave node E from SET
  OkNodes2 = lists:usort([node() | lists:droplast(OkNodes1)]),
  OkNodes2 = shards:leave(?SET, [ENode]),
  OkNodes2 = shards:get_nodes(?SET),
  [A2, B2, C2, CT2, D2] = get_remote_nodes(OkNodes2, ?SET),
  OkNodes2 = A2 = B2 = C2 = CT2 = D2,

  % check nodes
  5 = length(shards:get_nodes(?SET)),
  6 = length(shards:get_nodes(?DUPLICATE_BAG)),

  % join E node
  OkNodes1 = shards:join(?SET, [ENode]),
  R1 = get_remote_nodes(OkNodes1, ?SET),
  R1 = get_remote_nodes(AllNodes, ?DUPLICATE_BAG),

  % check nodes
  6 = length(shards:get_nodes(?SET)),
  6 = length(shards:get_nodes(?DUPLICATE_BAG)),

  ok.

t_eject_node_on_failure(Config) ->
  ok = cleanup_tabs(Config),

  UpNodes = shards:get_nodes(?SET),
  6 = length(UpNodes),

  % add new node
  NewNodes = [Z] = start_slaves([z]),
  ok = rpc:call(Z, test_helper, init_shards, [g]),
  UpNodes1 = shards:join(?SET, NewNodes),
  UpNodes1 = shards:get_nodes(?SET),

  % insert some data on that node
  Z = lists:last(UpNodes1),
  Z = test_helper:pick_node(read, 2, UpNodes1),
  true = shards:insert(?SET, {2, 2}),

  % cause an error
  ok = rpc:call(Z, shards, stop, []),
  timer:sleep(500),
  7 = length(UpNodes1),

  % new node should be ejected on failure
  UpNodes2 = lists:usort(UpNodes1 -- NewNodes),
  UpNodes2 = shards:get_nodes(?SET),
  6 = length(UpNodes2),

  % stop failure node
  stop_slaves([z]),

  ok.

t_delete_tabs(Config) ->
  ok = cleanup_tabs(Config),

  UpNodes = shards:get_nodes(?SET),
  6 = length(UpNodes),

  true = shards:delete(?SET),
  timer:sleep(500),
  [] = shards:get_nodes(?SET),
  [A, B, C, CT, D, E] = get_remote_nodes(UpNodes, ?SET),
  [] = A = B = C = CT = D = E,

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
    {init_timeout, 3000},
    {startup_timeout, 3000},
    {startup_functions, [{shards, start, []}]},
    {erl_flags, ErlFlags}
  ]),
  ct:print("\e[36m ---> Node ~p \e[32m[OK] \e[0m", [HostNode]),
  pong = net_adm:ping(HostNode),
  start_slaves(T, [HostNode | Acc]).

stop_slaves(Slaves) ->
  stop_slaves(Slaves, []).

stop_slaves([], Acc) ->
  lists:usort(Acc);
stop_slaves([Node | T], Acc) ->
  {ok, Name} = ct_slave:stop(Node),
  ct:print("\e[36m ---> Node ~p \e[31m[STOPPED] \e[0m", [Name]),
  pang = net_adm:ping(Node),
  stop_slaves(T, [Node | Acc]).

get_remote_nodes(Nodes, Tab) ->
  {ResL, _} = rpc:multicall(Nodes, shards, get_nodes, [Tab]),
  ResL.

setup_tabs(Config) ->
  {_, Nodes} = lists:keyfind(nodes, 1, Config),
  AllNodes = lists:usort([node() | Nodes]),

  {_, []} = rpc:multicall(
    AllNodes, test_helper, init_shards, [g]),
  ok.

cleanup_tabs(Config) ->
  {_, Nodes} = lists:keyfind(nodes, 1, Config),
  AllNodes = lists:usort([node() | Nodes]),

  {_, _} = rpc:multicall(
    AllNodes, test_helper, cleanup_shards, []),
  ok.
