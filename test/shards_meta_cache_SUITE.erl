-module(shards_meta_cache_SUITE).

-include_lib("common_test/include/ct.hrl").

%% Common Test
-export([
  all/0,
  init_per_testcase/2,
end_per_testcase/2
]).

%% Test Cases
-export([
  t_exit_signal/1,
  t_unhandled_message/1,
  t_call_ignored/1,
  t_cast_ignored/1
]).

-define(EXCLUDED_FUNS, [
  module_info,
  all,
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

-spec init_per_testcase(atom(), shards_ct:config()) -> shards_ct:config().
init_per_testcase(_, Config) ->
  {ok, Pid} = shards_meta_cache:start_link(test, self()),
  [{cache_pid, Pid} | Config].

-spec end_per_testcase(atom(), shards_ct:config()) -> shards_ct:config().
end_per_testcase(_, Config) ->
  CachePid = ?config(cache_pid, Config),

  case is_process_alive(CachePid) of
    true ->
      ok = gen_server:stop(CachePid),
      Config;

    _ ->
      Config
  end.

%%%===================================================================
%%% Tests Cases
%%%===================================================================

-spec t_exit_signal(shards_ct:config()) -> any().
t_exit_signal(Config) ->
  _ = process_flag(trap_exit, true),

  Pid = ?config(cache_pid, Config),

  _ = Pid ! {'EXIT', Pid, test},

  {'EXIT', Pid, normal} = shards_ct:wait_for_msg(1000),
  ok.

-spec t_unhandled_message(shards_ct:config()) -> any().
t_unhandled_message(Config) ->
  Pid = ?config(cache_pid, Config),

  _ = Pid ! hello,

  ok.

-spec t_call_ignored(shards_ct:config()) -> any().
t_call_ignored(Config) ->
  Pid = ?config(cache_pid, Config),

  ok = gen_server:call(Pid, hello).

-spec t_cast_ignored(shards_ct:config()) -> any().
t_cast_ignored(Config) ->
  Pid = ?config(cache_pid, Config),

  ok = gen_server:cast(Pid, hello).
