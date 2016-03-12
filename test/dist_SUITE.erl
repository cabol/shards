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
  t_basic_ops/1,
  t_delete_tabs/1
]).

-include("test_helpers.hrl").

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
    t_delete_tabs
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
  AllNodes = lists:usort([node() | Nodes]),
  OkNodes1 = lists:usort([node() | lists:droplast(Nodes)]),
  ENode = lists:last(OkNodes1),

  % create tables
  setup_tabs(Config),

  % join
  AllNodes = shards_dist:join(?DUPLICATE_BAG, AllNodes),
  AllNodes = shards_dist:join(?SET, AllNodes),

  % check nodes
  7 = length(shards_dist:get_nodes(?SET)),
  7 = length(shards_dist:get_nodes(?DUPLICATE_BAG)),

  % check no duplicate members
  Members = pg2:get_members(?SET),
  AllNodes = shards_dist:join(?SET, AllNodes),
  Members = pg2:get_members(?SET),

  % stop F node
  stop_slaves([f]),

  % check nodes
  6 = length(shards_dist:get_nodes(?SET)),
  6 = length(shards_dist:get_nodes(?DUPLICATE_BAG)),

  % check nodes
  OkNodes1 = shards_dist:get_nodes(?SET),
  R1 = [A, B, C, CT, D, E] = get_remote_nodes(AllNodes, ?SET),
  R1 = get_remote_nodes(AllNodes, ?DUPLICATE_BAG),
  OkNodes1 = A = B = C = CT = D = E,

  % leave node E from SET
  OkNodes2 = lists:usort([node() | lists:droplast(OkNodes1)]),
  OkNodes2 = shards_dist:leave(?SET, [ENode]),
  OkNodes2 = shards_dist:get_nodes(?SET),
  [A2, B2, C2, CT2, D2] = get_remote_nodes(OkNodes2, ?SET),
  OkNodes2 = A2 = B2 = C2 = CT2 = D2,

  % check nodes
  5 = length(shards_dist:get_nodes(?SET)),
  6 = length(shards_dist:get_nodes(?DUPLICATE_BAG)),

  % join E node
  OkNodes1 = shards_dist:join(?SET, [ENode]),
  R1 = get_remote_nodes(OkNodes1, ?SET),
  R1 = get_remote_nodes(AllNodes, ?DUPLICATE_BAG),

  % check nodes
  6 = length(shards_dist:get_nodes(?SET)),
  6 = length(shards_dist:get_nodes(?DUPLICATE_BAG)),

  ct:print("\e[1;1m t_join_leave_ops: \e[0m\e[32m[OK] \e[0m"),
  ok.

t_basic_ops(_Config) ->
  lists:foreach(fun t_basic_ops_/1, ?SHARDS_TABS).

t_basic_ops_(Tab) ->
  ok = cleanup_tabs(),

  % insert some K/V pairs
  Obj1 = {kx, 1, a, "hi"},
  KVPairs = [
    {k1, 1}, {k1, 2}, {k1, 3},
    {k2, 2},
    {k11, 11},
    {k22, 22},
    Obj1
  ],
  true = shards_dist:insert(Tab, KVPairs),
  true = shards_dist:insert(Tab, Obj1),

  % insert new
  [false, true] = shards_dist:insert_new(Tab, [Obj1, {k3, <<"V3">>}]),
  false = shards_dist:insert_new(Tab, {k3, <<"V3">>}),

  % select and match
  %R1 = lists:usort(shards_dist:select(Tab, [{{'$1', '$2'}, [], ['$$']}])),
  %R2 = lists:usort(shards_dist:match(Tab, '$1')),

  % lookup element
  case Tab == ?DUPLICATE_BAG orelse Tab == ?SHARDED_DUPLICATE_BAG of
    true ->
      [1, 2, 3] = lists:usort(shards_dist:lookup_element(Tab, k1, 2)),

      try shards_dist:lookup_element(Tab, wrong, 2)
      catch _:{badarg, _} -> ok
      end;
    _ ->
      3 = shards_dist:lookup_element(Tab, k1, 2)
  end,

  % lookup
  %R4 = lists:sort(lookup_keys(shards, Tab, [k1, k2, k3, kx])),

  % delete
  %true = shards_dist:delete_object(Tab, Obj1),
  true = shards_dist:delete(Tab, k2),
  %R5 = lists:sort(lookup_keys(shards, Tab, [k1, k2, kx])),

  % member
  %true = shards_dist:member(Tab, k1),
  %false = shards_dist:member(Tab, kx),

%%  % take
%%  R6 = lists:sort(shards_dist:take(Tab, k1)),
%%  [] = shards_dist:lookup(Tab, k1),

  ct:print("\e[1;1m t_basic_ops(~p): \e[0m\e[32m[OK] \e[0m", [Tab]),
  ok.

t_delete_tabs(_Config) ->
  ok = cleanup_tabs(),

  UpNodes = shards_dist:get_nodes(?SET),
  6 = length(UpNodes),

  true = shards_dist:delete(?SET),
  [] = shards_dist:get_nodes(?SET),
  [A, B, C, CT, D, E] = get_remote_nodes(UpNodes, ?SET),
  [] = A = B = C = CT = D = E,

  ct:print("\e[1;1m t_delete_tabs: \e[0m\e[32m[OK] \e[0m"),
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
  ResL.

remote_new(Nodes, Tab, Opts, PoolSize) ->
  {_, []} = rpc:multicall(Nodes, shards_dist, new, [Tab, Opts, PoolSize]),
  ok.

setup_tabs(Config) ->
  {_, Nodes} = lists:keyfind(nodes, 1, Config),
  AllNodes = lists:usort([node() | Nodes]),

  Types = [set, duplicate_bag, ordered_set, sharded_duplicate_bag],
  lists:foreach(fun({Tab, Type}) ->
    ok = remote_new(AllNodes, Tab, [Type], 5)
  end, lists:zip(?SHARDS_TABS, Types)).

cleanup_tabs() ->
  lists:foreach(fun(Tab) ->
    true = shards_dist:delete_all_objects(Tab)
  end, ?SHARDS_TABS).

lookup_keys(Mod, Tab, Keys) ->
  lists:foldr(fun(Key, Acc) ->
    case Mod:lookup(Tab, Key) of
      []     -> Acc;
      Values -> Values ++ Acc
    end
  end, [], Keys).
