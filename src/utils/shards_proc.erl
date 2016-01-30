%%%-------------------------------------------------------------------
%%% @doc
%%% Process utilities.
%%% @end
%%%-------------------------------------------------------------------
-module(shards_proc).

%% API
-export([wait_for_msg/0, wait_for_msg/1]).
-export([messages/0, messages/1, r_messages/1]).
-export([spawn_timer_fun/1]).
-export([spawn_handler/1, spawn_handler/2, spawn_handler/3]).
-export([spawn_callback_handler/3, spawn_callback_handler/4]).

%%%===================================================================
%%% API
%%%===================================================================

%% @equiv wait_for_msg(infinity)
wait_for_msg() ->
  wait_for_msg(infinity).

%% @doc
%%
%% @end
-spec wait_for_msg(timeout()) -> term() | {error, timeout}.
wait_for_msg(Timeout) ->
  receive
    Msg -> Msg
  after
    Timeout -> {error, timeout}
  end.

%% @equiv process_messages(self())
messages() ->
  messages(self()).

%% @doc
%% Returns all queued messages for the process `Pid'.
%% @end
-spec messages(pid()) -> [term()].
messages(Pid) ->
  {messages, Messages} = erlang:process_info(Pid, messages),
  Messages.

%% @doc
%% Returns all queued messages for the process `Pid', but doesn't
%% matter if process is local or remote.
%% @end
-spec r_messages(pid()) -> [term()].
r_messages(Pid) ->
  {messages, Messages} = rpc:pinfo(Pid, messages),
  Messages.

%% @doc
%% Spawns a linked process that sleeps for the given `Timeout', once
%% timeout expires then process dies.
%% @end
-spec spawn_timer_fun(timeout()) -> pid().
spawn_timer_fun(Timeout) ->
  spawn_link(fun() -> timer:sleep(Timeout) end).

%% @equiv spawn_handler(Fun, [])
spawn_handler(Fun) ->
  spawn_handler(Fun, []).

%% @equiv spawn_handler(Fun, Args, [])
spawn_handler(Fun, Args) ->
  spawn_handler(Fun, Args, []).

%% @equiv spawn_callback_handler(erlang, apply, [Fun, Args], Opts)
%% @doc
%% Same as `spawn_callback_handler/4', but receives a `fun' as
%% callback. This `fun' is invoked as:
%%
%% ```
%% apply(erlang, apply, [Fun, [Message | Args]])
%% '''
%%
%% Where `Message' is inserted as 1st argument in the `fun' args.
%% @end
spawn_handler(Fun, Args, Opts) ->
  spawn_callback_handler(erlang, apply, [Fun, Args], Opts).

%% @equiv spawn_callback_handler(Module, Fun, Args, [])
spawn_callback_handler(Module, Fun, Args) ->
  spawn_callback_handler(Module, Fun, Args, []).

%% @doc
%% Spawns a process that stays receiving messages, and when a message
%% is received, it applies the given callback `{Mod, Fun, Args}'.
%%
%% Options:
%% <ul>
%% <li>`Opts': Spawn process options. See `erlang:spawn_opt/2'.</li>
%% </ul>
%% @see erlang:spawn_opt/2.
%% @end
-spec spawn_callback_handler(
  module(), atom(), [term()], [term()]
) -> pid() | {pid(), reference()}.
spawn_callback_handler(Module, Fun, Args, Opts)
    when is_atom(Module), is_atom(Fun) ->
  spawn_opt(fun() -> handle(Module, Fun, Args) end, Opts).

%% @private
handle(erlang, apply, [Fun, FunArgs] = Args) ->
  receive
    Message ->
      apply(erlang, apply, [Fun, [Message | FunArgs]]),
      handle(erlang, apply, Args)
  end;
handle(Module, Fun, Args) ->
  receive
    Message ->
      apply(Module, Fun, [Message | Args]),
      handle(Module, Fun, Args)
  end.
