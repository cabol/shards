-module(task_SUITE).

-include_lib("common_test/include/ct.hrl").

%% Common Test
-export([all/0]).

%% Others
-export([wait_and_send/2, create_task_in_other_process/0]).

%% Tests
-export([t_async1/1, t_async2/1, t_async3/1]).
-export([t_start1/1, t_start2/1, t_start3/1]).
-export([t_start_link1/1, t_start_link2/1, t_start_link3/1]).

%% Test Errors
-export([
  t_await_timeout/1,
  t_await_normal/1,
  t_await_task_throw/1,
  t_await_task_error/1,
  t_await_undef_module_error/1,
  t_await_undef_fun_error/1,
  t_await_undef_mfa_error/1,
  t_await_task_exit/1,
  t_await_noconnection/1,
  t_await_noconnection_from_named_monitor/1,
  t_await_raises_from_non_owner_proc/1
]).

%%%===================================================================
%%% Common Test
%%%===================================================================

all() -> [
  t_async1, t_async2, t_async3,
  t_start1, t_start2, t_start3,
  t_start_link1, t_start_link2, t_start_link3,
  t_await_timeout,
  t_await_normal,
  t_await_task_throw,
  t_await_task_error,
  t_await_undef_module_error,
  t_await_undef_fun_error,
  t_await_undef_mfa_error,
  t_await_task_exit,
  t_await_noconnection,
  t_await_noconnection_from_named_monitor,
  t_await_raises_from_non_owner_proc
].

wait_and_send(Caller, Atom) ->
  Caller ! ready,
  receive true -> true end,
  Caller ! Atom.

create_task_in_other_process() ->
  Caller = self(),
  spawn(fun() -> Caller ! shards_task:async(fun() -> nil end) end),
  receive Task -> Task end.

%%%===================================================================
%%% Exported Tests Functions
%%%===================================================================

t_async1(_Config) ->
  Parent = self(),
  Fun = fun() -> wait_and_send(Parent, done) end,
  Task = shards_task:async(Fun),

  % Assert the task
  #{pid := Pid, ref := Ref, owner := _} = Task,
  true = is_pid(Pid),
  true = is_reference(Ref),

  % Assert the link
  {links, Links} = process_info(self(), links),
  true = lists:member(Pid, Links),

  receive ready -> ok end,

  % Assert the initial call
  {name, FunName} = erlang:fun_info(Fun, name),
  {?MODULE, FunName, 0} = proc_lib:translate_initial_call(Pid),

  % Run the task
  Pid ! true,

  % Assert response and monitoring messages
  done = wait_for_msg(5000),
  {Ref, done} = wait_for_msg(5000),
  {'DOWN', Ref, _, _, normal} = wait_for_msg(5000),

  ct:print("\e[1;1m async/1 \e[0m\e[32m[OK] \e[0m"),
  ok.

t_async2(_Config) ->
  Parent = self(),
  Fun = fun(Atom) -> wait_and_send(Parent, Atom) end,
  Task = shards_task:async(Fun, [done]),

  % Assert the task
  #{pid := Pid, ref := Ref, owner := _} = Task,
  true = is_pid(Pid),
  true = is_reference(Ref),

  % Assert the link
  {links, Links} = process_info(self(), links),
  true = lists:member(Pid, Links),

  receive ready -> ok end,

  % Assert the initial call
  {name, FunName} = erlang:fun_info(Fun, name),
  {?MODULE, FunName, 1} = proc_lib:translate_initial_call(Pid),

  % Run the task
  Pid ! true,

  % Assert response and monitoring messages
  done = shards_task:await(Task),
  done = wait_for_msg(5000),

  ct:print("\e[1;1m async/2 \e[0m\e[32m[OK] \e[0m"),
  ok.

t_async3(_Config) ->
  Task = shards_task:async(?MODULE, wait_and_send, [self(), done]),

  % Assert the task
  #{pid := Pid, ref := Ref, owner := _} = Task,
  true = is_pid(Pid),
  true = is_reference(Ref),

  % Assert the link
  {links, Links} = process_info(self(), links),
  true = lists:member(Pid, Links),

  receive ready -> ok end,

  % Assert the initial call
  {?MODULE, wait_and_send, 2} = proc_lib:translate_initial_call(Pid),

  % Run the task
  Pid ! true,

  % Assert response and monitoring messages
  done = shards_task:await(Task),
  done = wait_for_msg(5000),

  ct:print("\e[1;1m async/3 \e[0m\e[32m[OK] \e[0m"),
  ok.

t_start1(_Config) ->
  Parent = self(),
  Fun = fun() -> wait_and_send(Parent, done) end,
  {ok, Pid} = shards_task:start(Fun),

  {links, Links} = process_info(self(), links),
  false = lists:member(Pid, Links),

  receive ready -> ok end,

  {name, FunName} = erlang:fun_info(Fun, name),
  {?MODULE, FunName, 0} = proc_lib:translate_initial_call(Pid),

  Pid ! true,
  done = wait_for_msg(5000),

  ct:print("\e[1;1m start/1 \e[0m\e[32m[OK] \e[0m"),
  ok.

t_start2(_Config) ->
  Parent = self(),
  Fun = fun(Atom) -> wait_and_send(Parent, Atom) end,
  {ok, Pid} = shards_task:start(Fun, [done]),

  {links, Links} = process_info(self(), links),
  false = lists:member(Pid, Links),

  receive ready -> ok end,

  {name, FunName} = erlang:fun_info(Fun, name),
  {?MODULE, FunName, 1} = proc_lib:translate_initial_call(Pid),

  Pid ! true,
  done = wait_for_msg(5000),

  ct:print("\e[1;1m start/2 \e[0m\e[32m[OK] \e[0m"),
  ok.

t_start3(_Config) ->
  {ok, Pid} = shards_task:start(?MODULE, wait_and_send, [self(), done]),

  {links, Links} = process_info(self(), links),
  false = lists:member(Pid, Links),

  receive ready -> ok end,

  {?MODULE, wait_and_send, 2} = proc_lib:translate_initial_call(Pid),

  % Run the task
  Pid ! true,

  Pid ! true,
  done = wait_for_msg(5000),

  ct:print("\e[1;1m start/3 \e[0m\e[32m[OK] \e[0m"),
  ok.

t_start_link1(_Config) ->
  Parent = self(),
  Fun = fun() -> wait_and_send(Parent, done) end,
  {ok, Pid} = shards_task:start_link(Fun),

  {links, Links} = process_info(self(), links),
  true = lists:member(Pid, Links),

  receive ready -> ok end,

  {name, FunName} = erlang:fun_info(Fun, name),
  {?MODULE, FunName, 0} = proc_lib:translate_initial_call(Pid),

  Pid ! true,
  done = wait_for_msg(5000),

  ct:print("\e[1;1m start_link/1 \e[0m\e[32m[OK] \e[0m"),
  ok.

t_start_link2(_Config) ->
  Parent = self(),
  Fun = fun(Atom) -> wait_and_send(Parent, Atom) end,
  {ok, Pid} = shards_task:start_link(Fun, [done]),

  {links, Links} = process_info(self(), links),
  true = lists:member(Pid, Links),

  receive ready -> ok end,

  {name, FunName} = erlang:fun_info(Fun, name),
  {?MODULE, FunName, 1} = proc_lib:translate_initial_call(Pid),

  Pid ! true,
  done = wait_for_msg(5000),

  ct:print("\e[1;1m start_link/2 \e[0m\e[32m[OK] \e[0m"),
  ok.

t_start_link3(_Config) ->
  {ok, Pid} = shards_task:start_link(?MODULE, wait_and_send, [self(), done]),

  {links, Links} = process_info(self(), links),
  true = lists:member(Pid, Links),

  receive ready -> ok end,

  {?MODULE, wait_and_send, 2} = proc_lib:translate_initial_call(Pid),

  % Run the task
  Pid ! true,
  done = wait_for_msg(5000),

  ct:print("\e[1;1m start_link/3 \e[0m\e[32m[OK] \e[0m"),
  ok.

t_await_timeout(_Config) ->
  Task = #{ref => make_ref(), owner => self()},

  try shards_task:await(Task, 0)
  catch
    exit:{timeout, {shards_task, await, [Task, 0]}} -> ok
  end,

  ct:print("\e[1;1m await/2 exits on timeout \e[0m\e[32m[OK] \e[0m"),
  ok.

t_await_normal(_Config) ->
  Task = shards_task:async(fun() -> exit(normal) end),

  try shards_task:await(Task)
  catch
    exit:{normal, {shards_task, await, [Task, 5000]}} -> ok
  end,

  ct:print("\e[1;1m await/2 exits on normal exit \e[0m\e[32m[OK] \e[0m"),
  ok.

t_await_task_throw(_Config) ->
  process_flag(trap_exit, true),
  Task = shards_task:async(fun() -> throw(unknown) end),

  try shards_task:await(Task)
  catch
    exit:{{{nocatch, unknown}, _}, {shards_task, await, [Task, 5000]}} -> ok
  end,

  ct:print("\e[1;1m await/2 exits on task throw \e[0m\e[32m[OK] \e[0m"),
  ok.

t_await_task_error(_Config) ->
  process_flag(trap_exit, true),
  Task = shards_task:async(fun() ->
    erlang:raise(error, <<"oops">>, erlang:get_stacktrace())
  end),

  try shards_task:await(Task)
  catch
    exit:{{<<"oops">>, _}, {shards_task, await, [Task, 5000]}} -> ok
  end,

  ct:print("\e[1;1m await/2 exits on task error \e[0m\e[32m[OK] \e[0m"),
  ok.

t_await_undef_module_error(_Config) ->
  process_flag(trap_exit, true),
  Task = shards_task:async(fun module_does_not_exist:undef/0),

  try shards_task:await(Task)
  catch
    exit:Ex ->
      {{undef, [{module_does_not_exist, undef, _, _} | _]},
       {shards_task, await, [Task, 5000]}} = Ex
  end,

  ct:print("\e[1;1m await/2 exits on task undef module error \e[0m\e[32m[OK] \e[0m"),
  ok.

t_await_undef_fun_error(_Config) ->
  process_flag(trap_exit, true),
  Task = shards_task:async(fun ?MODULE:undef/0),

  try shards_task:await(Task)
  catch
    exit:Ex ->
      {{undef, [{?MODULE, undef, _, _} | _]},
       {shards_task, await, [Task, 5000]}} = Ex
  end,

  ct:print("\e[1;1m await/2 exits on task undef function error \e[0m\e[32m[OK] \e[0m"),
  ok.

t_await_undef_mfa_error(_Config) ->
  process_flag(trap_exit, true),
  Task = shards_task:async(?MODULE, undef, []),

  try shards_task:await(Task)
  catch
    exit:Ex ->
      {{undef, [{?MODULE, undef, _, _} | _]},
        {shards_task, await, [Task, 5000]}} = Ex
  end,

  ct:print("\e[1;1m await/2 exits on task undef MFA error \e[0m\e[32m[OK] \e[0m"),
  ok.

t_await_task_exit(_Config) ->
  process_flag(trap_exit, true),
  Task = shards_task:async(fun() -> exit(unknown) end),

  try shards_task:await(Task)
  catch
    exit:{unknown, {shards_task, await, [Task, 5000]}} -> ok
  end,

  ct:print("\e[1;1m await/2 exits on task exit \e[0m\e[32m[OK] \e[0m"),
  ok.

t_await_noconnection(_Config) ->
  Ref  = make_ref(),
  Task = #{ref => Ref, pid => self(), owner => self()},
  self() ! {'DOWN', Ref, process, self(), noconnection},

  try shards_task:await(Task)
  catch
    exit:Ex ->
      Node = node(),
      {nodedown, Node} = element(1, Ex)
  end,

  ct:print("\e[1;1m await/2 exits on noconnection \e[0m\e[32m[OK] \e[0m"),
  ok.

t_await_noconnection_from_named_monitor(_Config) ->
  Ref  = make_ref(),
  Task = #{ref => Ref, pid => nil, owner => self()},
  self() ! {'DOWN', Ref, process, {name, node}, noconnection},

  try shards_task:await(Task)
  catch
    exit:Ex ->
      {nodedown, node} = element(1, Ex)
  end,

  ct:print("\e[1;1m await/2 exits on noconnection from named monitor \e[0m\e[32m[OK] \e[0m"),
  ok.

t_await_raises_from_non_owner_proc(_Config) ->
  Task = create_task_in_other_process(),

  try shards_task:await(Task, 1)
  catch
    throw:{invalid_owner_error, Task} -> ok
  end,

  ct:print("\e[1;1m await/2 raises when invoked from a non-owner process \e[0m\e[32m[OK] \e[0m"),
  ok.

%%%===================================================================
%%% Internal functions
%%%===================================================================

wait_for_msg(Timeout) ->
  receive
    Msg -> Msg
  after
    Timeout -> {error, timeout}
  end.
