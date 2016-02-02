%%%-------------------------------------------------------------------
%%% @doc
%%% Utility module to start/work with supervisors.
%%% @end
%%%-------------------------------------------------------------------
-module(shards_supervisor).

-behaviour(supervisor).

%% API
-export([start_link/2, start_link/3]).

%% Supervisor callbacks
-export([init/1]).

%%%===================================================================
%%% Types
%%%===================================================================

%% @type name() = atom() | {global, term()} | {via, module(), term()}.
%%
%% The Supervisor name.
-type name() :: atom() | {global, term()} | {via, module(), term()}.

%% @type options() =
%% #{
%%   name      => name(),
%%   strategy  => supervisor:strategy(),
%%   intensity => non_neg_integer(),
%%   period    => pos_integer()
%% }.
%%
%% Options used by the `start*' functions.
-type options() :: #{
  name      => name(),
  strategy  => supervisor:strategy(),
  intensity => non_neg_integer(),
  period    => pos_integer()
}.

%%%===================================================================
%%% API functions
%%%===================================================================

-spec start_link(
  [supervisor:child_spec()], options()
) -> supervisor:startlink_ret().
start_link(Children, Options) when is_list(Children) ->
  Spec = shards_supervisor_spec:supervise(Children, Options),
  start_link(?MODULE, Spec, Options).

-spec start_link(module(), term(), options()) -> supervisor:startlink_ret().
start_link(Module, Arg, Options) ->
  case maps:get(name, Options, nil) of
    nil ->
      supervisor:start_link(Module, Arg);
    Atom when is_atom(Atom) ->
      supervisor:start_link({local, Atom}, Module, Arg);
    Other when is_tuple(Other) ->
      supervisor:start_link(Other, Module, Arg)
  end.

%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================

%% @hidden
init(Args) -> Args.
