-module(shards_group_SUITE).

%% Common Test
-export([
  all/0
]).

%% Test Cases
-export([
  t_start_group/1,
  t_start_group_without_name/1,
  t_adding_removing_tables/1,
  t_child_spec/1
]).

-define(EXCLUDED_FUNS, [
  module_info,
  all
]).

%%%===================================================================
%%% Common Test
%%%===================================================================

-spec all() -> [atom()].
all() ->
  Exports = ?MODULE:module_info(exports),
  [F || {F, _} <- Exports, not lists:member(F, ?EXCLUDED_FUNS)].

%%%===================================================================
%%% Tests Cases
%%%===================================================================

-spec t_start_group(shards_ct:config()) -> any().
t_start_group(_Config) ->
  {ok, Pid} = shards_group:start_link(),
  true = is_pid(Pid),
  ok = shards_group:stop(shards_group).

-spec t_start_group_without_name(shards_ct:config()) -> any().
t_start_group_without_name(_Config) ->
  {ok, Pid} = shards_group:start_link(undefined),
  true = is_pid(Pid),
  ok = shards_group:stop(Pid).

-spec t_adding_removing_tables(shards_ct:config()) -> any().
t_adding_removing_tables(_Config) ->
  with_group(fun(SupPid) ->
    {ok, Pid1, T1} = shards_group:new_table(SupPid, t1, []),
    [Pid1] = children_pids(SupPid),
    T1 = shards:info(T1, id),

    {ok, Pid2, T2} = shards_group:new_table(SupPid, t2, []),
    [Pid1, Pid2] = children_pids(SupPid),

    ok = shards_group:del_table(SupPid, T2),
    [Pid1] = children_pids(SupPid)
  end, dynamic_sup).

-spec t_child_spec(shards_ct:config()) -> any().
t_child_spec(_Config) ->
  #{
    id    := test,
    start := {shards_group, start_link, [test]},
    type  := supervisor
  } = shards_group:child_spec(test).

%%%===================================================================
%%% Internal functions
%%%===================================================================

with_group(Fun, Name) ->
  {ok, SupPid} = shards_group:start_link(Name),

  try
    Fun(SupPid)
  after
    shards_group:stop(SupPid)
  end.

children_pids(SupPid) ->
  [Child || {_, Child, _, _} <- supervisor:which_children(SupPid)].
