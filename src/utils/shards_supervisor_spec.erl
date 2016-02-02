%%%-------------------------------------------------------------------
%%% @doc
%%% Utility module to create supervisor and worker specs.
%%% @end
%%%-------------------------------------------------------------------
-module(shards_supervisor_spec).

%% API
-export([supervise/2]).
-export([supervisor/2, supervisor/3]).
-export([worker/2, worker/3]).

%%%===================================================================
%%% API functions
%%%===================================================================

%% @doc
%% Receives a list of children (workers or supervisors) to
%% supervise and a set of options. Returns a tuple containing
%% the supervisor specification.
%%
%% Example:
%%
%% ```
%% ebus_supervisor_spec:supervise(Children, #{strategy => one_for_one}).
%% '''
%% @end
-spec supervise(
  [supervisor:child_spec()], supervisor:sup_flags()
) -> {ok, tuple()}.
supervise(Children, SupFlags) ->
  assert_unique_ids([Id || #{id := Id} <- Children]),
  {ok, {SupFlags, Children}}.

%% @equiv supervisor(Module, Args, [])
supervisor(Module, Args) ->
  supervisor(Module, Args, #{}).

%% @doc
%% Defines the given `Module' as a supervisor which will be started
%% with the given arguments.
%%
%% Example:
%%
%% ```
%% ebus_supervisor_spec:supervisor(my_sup, [], #{restart => permanent}).
%% '''
%%
%% By default, the function `start_link' is invoked on the given
%% module. Overall, the default values for the options are:
%%
%% ```
%% #{
%%   id       => Module,
%%   start    => {Module, start_link, Args},
%%   restart  => permanent,
%%   shutdown => infinity,
%%   modules  => [module]
%% }
%% '''
%% @end
-spec supervisor(
  module(), [term()], supervisor:child_spec()
) -> supervisor:child_spec().
supervisor(Module, Args, Spec) when is_map(Spec) ->
  child(supervisor, Module, Args, Spec#{shutdown => infinity});
supervisor(_, _, _) -> throw(invalid_child_spec).

%% @equiv worker(Module, Args, [])
worker(Module, Args) ->
  worker(Module, Args, #{}).

%% @doc
%% Defines the given `Module' as a worker which will be started
%% with the given arguments.
%%
%% Example:
%%
%% ```
%% ebus_supervisor_spec:worker(my_module, [], #{restart => permanent}).
%% '''
%%
%% By default, the function `start_link' is invoked on the given
%% module. Overall, the default values for the options are:
%%
%% ```
%% #{
%%   id       => Module,
%%   start    => {Module, start_link, Args},
%%   restart  => permanent,
%%   shutdown => 5000,
%%   modules  => [module]
%% }
%% '''
%% @end
-spec worker(
  module(), [term()], supervisor:child_spec()
) -> supervisor:child_spec().
worker(Module, Args, Spec) when is_map(Spec) ->
  child(worker, Module, Args, Spec);
worker(_, _, _) -> throw(invalid_child_spec).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
assert_unique_ids([]) ->
  ok;
assert_unique_ids([Id | Rest]) ->
  case lists:member(Id, Rest) of
    true -> throw({badarg, duplicated_id});
    _    -> assert_unique_ids(Rest)
  end.

%% @private
child(Type, Module, Args, Spec) when is_map(Spec) ->
  #{
    id       => maps:get(id, Spec, Module),
    start    => maps:get(start, Spec, {Module, start_link, Args}),
    restart  => maps:get(restart, Spec, permanent),
    shutdown => maps:get(shutdown, Spec, 5000),
    type     => Type,
    modules  => maps:get(modules, Spec, [Module])
  }.
