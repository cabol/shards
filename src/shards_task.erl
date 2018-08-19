%%%-------------------------------------------------------------------
%%% @doc
%%% Task utility.
%%%
%%% Conveniences for spawning and awaiting tasks.
%%%
%%% This module is based on
%%% <a href="http://elixir-lang.org/docs/stable/elixir/Task.html">Elixir Task</a>
%%% @end
%%%-------------------------------------------------------------------
-module(shards_task).

%% Task API
-export([
  start/1,
  start/2,
  start/3,
  start_link/1,
  start_link/2,
  start_link/3,
  async/1,
  async/2,
  async/3,
  await/1,
  await/2
]).

%% Task Supervised API
-export([
  sup_start/2,
  sup_start_link/2,
  sup_spawn_link/3,
  reply/3,
  noreply/2
]).

-define(TIMEOUT, 5000).

%%%===================================================================
%%% Types
%%%===================================================================

%% @type task() = #{
%%         pid   => pid() | nil,
%%         ref   => ref() | nil,
%%         owner => pid() | nil
%%       }.
%%
%% Task definition.
%%
%% It contains these fields:
%% <ul>
%% <li>`pid': the process reference of the task process; `nil' if the task does
%% not use a task process.</li>
%% <li>`ref': the task monitor reference.</li>
%% <li>`owner': the PID of the process that started the task.</li>
%% </ul>
-type task() :: #{
        pid   => pid() | nil,
        ref   => reference() | nil,
        owner => pid() | nil
      }.

%% @type link() = link | monitor | nolink.
%%
%% Process link options.
-type link() :: link | monitor | nolink.

%% @type info() = {node(), pid() | atom()}.
%%
%% Process info.
-type proc_info() :: {node(), pid() | atom()}.

%% @type task_fun() = fun(() -> term()) | fun((term()) -> term()).
%%
%% Task Function.
-type task_fun() :: fun(() -> term()) | fun((term()) -> term()).

%% @type callback() = {module(), atom(), [term()]}.
%%
%% MFA Callback.
-type callback() :: {module(), atom(), [term()]}.

%% Exported types
-export_type([task/0, link/0, proc_info/0, task_fun/0, callback/0]).

%%%===================================================================
%%% Task API
%%%===================================================================

%% @doc
%% Starts a task.
%%
%% This is only used when the task is used for side-effects
%% (i.e. no interest in the returned result) and it should not
%% be linked to the current process.
%% @end
-spec start(task_fun()) -> {ok, pid()}.
start(Fun) ->
  start(erlang, apply, [Fun, []]).

%% @doc
%% Starts a task.
%%
%% This is only used when the task is used for side-effects
%% (i.e. no interest in the returned result) and it should not
%% be linked to the current process.
%% @end
-spec start(task_fun(), [term()]) -> {ok, pid()}.
start(Fun, Args) ->
  start(erlang, apply, [Fun, Args]).

%% @doc
%% Starts a task.
%%
%% This is only used when the task is used for side-effects
%% (i.e. no interest in the returned result) and it should not
%% be linked to the current process.
%% @end
-spec start(module(), atom(), [term()]) -> {ok, pid()}.
start(Mod, Fun, Args) ->
  sup_start(get_info(self()), {Mod, Fun, Args}).

%% @doc
%% Starts a task as part of a supervision tree.
%% @end
-spec start_link(task_fun()) -> {ok, pid()}.
start_link(Fun) ->
  start_link(erlang, apply, [Fun, []]).

%% @doc
%% Starts a task as part of a supervision tree.
%% @end
-spec start_link(task_fun(), [term()]) -> {ok, pid()}.
start_link(Fun, Args) ->
  start_link(erlang, apply, [Fun, Args]).

%% @doc
%% Starts a task as part of a supervision tree.
%% @end
-spec start_link(module(), atom(), [term()]) -> {ok, pid()}.
start_link(Mod, Fun, Args) ->
  sup_start_link(get_info(self()), {Mod, Fun, Args}).

%% @equiv async(erlang, apply, [Fun, []])
async(Fun) ->
  async(erlang, apply, [Fun, []]).

%% @equiv async(erlang, apply, [Fun, Args])
async(Fun, Args) ->
  async(erlang, apply, [Fun, Args]).

%% @doc
%% Starts a task that must be awaited on.
%%
%% A `task()' type is returned containing the relevant information.
%% Developers must eventually call `await/2' on the returned task.
%% @end
-spec async(module(), atom(), [term()]) -> task().
async(Mod, Fun, Args) ->
  MFA = {Mod, Fun, Args},
  Owner = self(),
  Pid = sup_spawn_link(Owner, get_info(Owner), MFA),
  Ref = monitor(process, Pid),
  Pid ! {Owner, Ref},
  #{pid => Pid, ref => Ref, owner => Owner}.

%% @equiv await(Task, 5000)
await(Task) ->
  await(Task, 5000).

%% @doc
%% Awaits a task reply.
%%
%% A timeout, in milliseconds, can be given with default value
%% of `5000'. In case the task process dies, this function will
%% exit with the same reason as the task.
%%
%% If the timeout is exceeded, `await' will exit, however,
%% the task will continue to run. When the calling process exits, its
%% exit signal will terminate the task if it is not trapping exits.
%%
%% This function assumes the task's monitor is still active or the monitor's
%% `DOWN' message is in the message queue. If it has been demonitored, or the
%% message already received, this function may wait for the duration of the
%% timeout awaiting the message.
%%
%% This function will always exit and demonitor if the task crashes or if
%% it times out, so the task can not be used again.
%% @end
-spec await(task(), timeout()) -> term() | no_return().
await(#{owner := Owner} = Task, _) when Owner /= self() ->
  throw({invalid_owner_error, Task});
await(#{ref := Ref} = Task, Timeout) ->
  receive
    {Ref, Reply} ->
      demonitor(Ref, [flush]),
      Reply;
    
    {'DOWN', Ref, _, Proc, Reason} ->
      exit({reason(Reason, Proc), {?MODULE, await, [Task, Timeout]}})
  after
    Timeout ->
      demonitor(Ref, [flush]),
      exit({timeout, {?MODULE, await, [Task, Timeout]}})
  end.

%%%===================================================================
%%% Task Supervised API
%%%===================================================================

%% @hidden
-spec sup_start(proc_info(), callback()) -> {ok, pid()}.
sup_start(Info, Fun) ->
  {ok, proc_lib:spawn(?MODULE, noreply, [Info, Fun])}.

%% @hidden
-spec sup_start_link(proc_info(), callback()) -> {ok, pid()}.
sup_start_link(Info, Fun) ->
  {ok, proc_lib:spawn_link(?MODULE, noreply, [Info, Fun])}.

%% @hidden
-spec sup_spawn_link(pid(), proc_info(), callback()) -> pid().
sup_spawn_link(Caller, Info, Fun) ->
  proc_lib:spawn_link(?MODULE, reply, [Caller, Info, Fun]).

%% @hidden
-spec reply(pid(), proc_info(), callback()) -> term() | no_return().
reply(Caller, Info, MFA) ->
  _ = initial_call(MFA),
  receive
    {Caller, Ref} ->
      Caller ! {Ref, do_apply(Info, MFA)}
  end.

%% @hidden
-spec noreply(proc_info(), callback()) -> term() | no_return().
noreply(Info, MFA) ->
  initial_call(MFA),
  do_apply(Info, MFA).

%%%===================================================================
%%% Internal functions
%%%===================================================================

-ifdef(OTP_RELEASE).
%% OTP 21 or higher
%% @private
do_apply(Info, {Module, Fun, Args} = MFA) ->
  try
    apply(Module, Fun, Args)
  catch
    error:Value:Stacktrace ->
      Reason = {Value, Stacktrace},
      exit(Info, MFA, Reason, Reason);
    
    throw:Value:Stacktrace ->
      Reason = {{nocatch, Value}, Stacktrace},
      exit(Info, MFA, {{nocatch, Value}, Stacktrace}, Reason);
    
    exit:Value:Stacktrace ->
      exit(Info, MFA, {Value, Stacktrace}, Value)
  end.
-else.
%% OTP 20 or lower
%% @private
do_apply(Info, {Module, Fun, Args} = MFA) ->
  try
    apply(Module, Fun, Args)
  catch
    error:Value ->
      Reason = {Value, erlang:get_stacktrace()},
      exit(Info, MFA, Reason, Reason);
    
    throw:Value ->
      Reason = {{nocatch, Value}, erlang:get_stacktrace()},
      exit(Info, MFA, Reason, Reason);
    
    exit:Value ->
      exit(Info, MFA, {Value, erlang:get_stacktrace()}, Value)
  end.
-endif.

%% @private
get_info(Pid) ->
  SelfOrName =
    case process_info(Pid, registered_name) of
      {registered_name, Name} -> Name;
      []                      -> self()
    end,
  
  {node(), SelfOrName}.

%% @private
initial_call(MFA) ->
  put('$initial_call', get_initial_call(MFA)).

%% @private
get_initial_call({erlang, apply, [Fun, Args]}) when is_function(Fun, length(Args)) ->
  {module, Module} = erlang:fun_info(Fun, module),
  {name, Name} = erlang:fun_info(Fun, name),
  {Module, Name, length(Args)};
get_initial_call({Mod, Fun, Args}) ->
  {Mod, Fun, length(Args)}.

%% @private
-spec exit(proc_info(), callback(), term(), term()) -> no_return().
exit(_Info, _MFA, _LogReason, Reason)
    when Reason == normal orelse Reason == shutdown
    orelse (tuple_size(Reason) == 2 andalso element(1, Reason) == shutdown) ->
  exit(Reason);
exit(Info, MFA, LogReason, Reason) ->
  {Fun, Args} = get_running(MFA),
  error_logger:format(
    "\e[31m" ++
    "** Task ~p terminating~n" ++
    "** Started from ~p~n" ++
    "** When function  == ~p~n" ++
    "**      arguments == ~p~n" ++
    "** Reason for termination == ~n" ++
    "** ~p~n" ++
    "\e[0m", [self(), get_from(Info), Fun, Args, get_reason(LogReason)]
  ),
  exit(Reason).

%% @private
get_from({Node, PidOrName}) when Node == node() ->
  PidOrName.

%% @private
get_running({erlang, apply, [Fun, Args]}) when is_function(Fun, length(Args)) ->
  {Fun, Args};
get_running({Mod, Fun, Args}) ->
  {erlang:make_fun(Mod, Fun, length(Args)), Args}.

%% @private
get_reason({undef, [{Mod, Fun, Args, _Info} | _] = Stacktrace} = Reason) when is_atom(Mod) and is_atom(Fun) ->
  FunExported = fun
    (M, F, A) when is_list(A) ->
      erlang:function_exported(M, F, length(A));
    
    (M, F, A) when is_integer(A) ->
      erlang:function_exported(M, F, A)
  end,

  case code:is_loaded(Mod) of
    false ->
      {module_could_not_be_loaded, Stacktrace};
    
    _ when is_list(Args); is_integer(Args) ->
      case FunExported(Mod, Fun, Args) of
        false -> {function_not_exported, Stacktrace};
        _     -> Reason
      end
  end;
get_reason(Reason) ->
  Reason.

%% @private
reason(noconnection, Proc) ->
  {nodedown, monitor_node(Proc)};
reason(Reason, _) ->
  Reason.

%% @private
monitor_node(Pid) when is_pid(Pid) ->
  node(Pid);
monitor_node({_, Node}) ->
  Node.
