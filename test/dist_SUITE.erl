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

%% Tests Cases
-export([
  t_join_leave_ops/1,
  t_basic_ops/1
]).

-define(SLAVES, [a, b, c, d, e, f]).

-define(SET, test_set).
-define(DUPLICATE_BAG, test_duplicate_bag).
-define(ORDERED_SET, test_ordered_set).
-define(SHARDED_DUPLICATE_BAG, test_sharded_duplicate_bag).

-define(SHARDS_TABS, [
  ?SET,
  ?DUPLICATE_BAG,
  ?ORDERED_SET,
  ?SHARDED_DUPLICATE_BAG
]).

%%%===================================================================
%%% Common Test
%%%===================================================================

all() ->
  [{group, dist_test_group}].

groups() ->
  [{dist_test_group, [sequence], [
    t_join_leave_ops,
    t_basic_ops
  ]}].

init_per_suite(Config) ->
  _ = shards:start(),
  Nodes = start_slaves(?SLAVES),
  [{nodes, Nodes} | Config].

end_per_suite(Config) ->
  Config.

init_per_testcase(_, Config) ->
  Config.

end_per_testcase(_, Config) ->
  Config.

%%%===================================================================
%%% Tests Cases
%%%===================================================================

t_join_leave_ops(Config) ->
  {_, Nodes} = lists:keyfind(nodes, 1, Config),
  OkNodes1 = lists:droplast(Nodes),
  ENode = lists:last(OkNodes1),

  ?SET = shards_dist:new(Nodes, ?SET, [], 5),

  stop_slaves([f]),
  ok = shards_dist:join(?SET, [ENode]),

  OkNodes1 = shards_dist:get_nodes(?SET),
  [{NA, A}, {NB, B}, {NC, C}, {ND, D}, {NE, E}] =
    get_remote_nodes(OkNodes1, ?SET),
  A = lists:usort([node() | OkNodes1] -- [NA]),
  B = lists:usort([node() | OkNodes1] -- [NB]),
  C = lists:usort([node() | OkNodes1] -- [NC]),
  D = lists:usort([node() | OkNodes1] -- [ND]),
  E = lists:usort([node() | OkNodes1] -- [NE]),

  ok = shards_dist:leave(?SET, [ENode]),
  OkNodes2 = lists:droplast(OkNodes1),
  OkNodes2 = shards_dist:get_nodes(?SET),
  [{NA, A2}, {NB, B2}, {NC, C2}, {ND, D2}] =
    get_remote_nodes(OkNodes2, ?SET),
  A2 = lists:usort([node() | OkNodes2] -- [NA]),
  B2 = lists:usort([node() | OkNodes2] -- [NB]),
  C2 = lists:usort([node() | OkNodes2] -- [NC]),
  D2 = lists:usort([node() | OkNodes2] -- [ND]),

  ct:print("\e[1;1m t_join_leave_ops: \e[0m\e[32m[OK] \e[0m"),
  ok.

t_basic_ops(_Config) ->


  ct:print("\e[1;1m t_basic_ops: \e[0m\e[32m[OK] \e[0m"),
  ok.

%%%===================================================================
%%% Internal functions
%%%===================================================================

start_slaves(Slaves) ->
  start_slaves(Slaves, []).

start_slaves([], Acc) ->
  lists:usort(Acc);
start_slaves([Node | T], Acc) ->
  Profile = os:getenv("REBAR_PROFILE"),
  ErlFlags = "-pa ../../_build/" ++ Profile ++ "/lib/*/ebin ",
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
  {ResL, _} = rpc:multicall(Nodes, shards_dist, get_nodes, [Tab]),
  lists:zip(Nodes, ResL).
