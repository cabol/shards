%%%-------------------------------------------------------------------
%%% @doc
%%% Shards State Manager.
%%% This module encapsulates the `shards' state.
%%% @end
%%%-------------------------------------------------------------------
-module(shards_state).

%% API
-export([
  get/1,
  new/0,
  to_map/1,
  from_map/1
]).

%% API – Getters & Setters
-export([
  module/1,
  module/2,
  n_shards/1,
  n_shards/2,
  pick_shard_fun/1,
  pick_shard_fun/2,
  pick_node_fun/1,
  pick_node_fun/2,
  auto_eject_nodes/1,
  auto_eject_nodes/2
]).

%%%===================================================================
%%% Types & Macros
%%%===================================================================

%% Default number of shards
-define(N_SHARDS, erlang:system_info(schedulers_online)).

%% @type op() = r | w | d.
%%
%% Defines operation type.
%% <li>`r': Read operations.</li>
%% <li>`w': Write operation.</li>
%% <li>`d': Delete operations.</li>
-type op() :: r | w | d.

%% @type key() = term().
%%
%% Defines key type.
-type key() :: term().

%% @type n_shards() = pos_integer().
%%
%% Defines number of shards.
-type n_shards() :: pos_integer().

%% @type range() = pos_integer().
%%
%% Defines the range or set – `range > 0'.
-type range() :: pos_integer().

%% @type pick_fun() = fun((key(), range(), op()) -> non_neg_integer() | any).
%%
%% Defines spec function to pick or compute the shard/node.
%% The function returns a value for `Key' within the range 0..Range-1.
-type pick_fun() :: fun((key(), range(), op()) -> non_neg_integer() | any).

%% State definition
-record(state, {
  module           = shards_local            :: module(),
  n_shards         = ?N_SHARDS               :: pos_integer(),
  pick_shard_fun   = fun shards_local:pick/3 :: pick_fun(),
  pick_node_fun    = fun shards_local:pick/3 :: pick_fun(),
  auto_eject_nodes = true                    :: boolean()
}).

%% @type state() = #state{}.
%%
%% Defines `shards' state.
-type state() :: #state{}.

%% @type state_map() = #{
%%   module           => module(),
%%   n_shards         => pos_integer(),
%%   pick_shard_fun   => pick_fun(),
%%   pick_node_fun    => pick_fun(),
%%   auto_eject_nodes => boolean()
%% }.
%%
%% Defines the map representation of the `shards' state:
%% <ul>
%% <li>`module': Module to be used depending on the `scope':
%% `shards_local' or `shards_dist'.</li>
%% <li>`n_shards': Number of ETS shards/fragments.</li>
%% <li>`pick_shard_fun': Function callback to pick/compute the shard.</li>
%% <li>`pick_node_fun': Function callback to pick/compute the node.</li>
%% <li>`auto_eject_nodes': A boolean value that controls if node should be
%% ejected when it fails.</li>
%% </ul>
-type state_map() :: #{
  module           => module(),
  n_shards         => pos_integer(),
  pick_shard_fun   => pick_fun(),
  pick_node_fun    => pick_fun(),
  auto_eject_nodes => boolean()
}.

%% Exported types
-export_type([
  op/0,
  key/0,
  n_shards/0,
  range/0,
  pick_fun/0,
  state/0,
  state_map/0
]).

%%%===================================================================
%%% API
%%%===================================================================

%% @doc
%% Returns the `state' for the given table `Tab'.
%% @end
-spec get(Tab :: atom()) -> state().
get(Tab) when is_atom(Tab) ->
  case ets:lookup(Tab, state) of
    [State] -> State;
    _       -> throw({badarg, Tab})
  end.

%% @doc
%% Creates a new `state' with default values.
%% @end
-spec new() -> state().
new() ->
  #state{}.

%% @doc
%% Converts the given `state' into a `map'.
%% @end
-spec to_map(state()) -> state_map().
to_map(State) ->
  #{module           => State#state.module,
    n_shards         => State#state.n_shards,
    pick_shard_fun   => State#state.pick_shard_fun,
    pick_node_fun    => State#state.pick_node_fun,
    auto_eject_nodes => State#state.auto_eject_nodes}.

%% @doc
%% Builds a new `state' from the given `Map'.
%% @end
-spec from_map(state_map()) -> state().
from_map(Map) ->
  #state{
    module           = maps:get(module, Map, shards_local),
    n_shards         = maps:get(n_shards, Map, ?N_SHARDS),
    pick_shard_fun   = maps:get(pick_shard_fun, Map, fun shards_local:pick/3),
    pick_node_fun    = maps:get(pick_node_fun, Map, fun shards_local:pick/3),
    auto_eject_nodes = maps:get(auto_eject_nodes, Map, true)}.

%%%===================================================================
%%% API – Getters & Setters
%%%===================================================================

-spec module(state() | atom()) -> module().
module(#state{module = Module}) ->
  Module;
module(Tab) when is_atom(Tab) ->
  module(?MODULE:get(Tab)).

-spec module(module(), state()) -> state().
module(Module, #state{} = State) when is_atom(Module) ->
  State#state{module = Module}.

-spec n_shards(state() | atom()) -> pos_integer().
n_shards(#state{n_shards = NumShards}) ->
  NumShards;
n_shards(Tab) when is_atom(Tab) ->
  n_shards(?MODULE:get(Tab)).

-spec n_shards(pos_integer(), state()) -> state().
n_shards(Shards, #state{} = State) when is_integer(Shards), Shards > 0 ->
  State#state{n_shards = Shards}.

-spec pick_shard_fun(state() | atom()) -> pick_fun().
pick_shard_fun(#state{pick_shard_fun = PickShardFun}) ->
  PickShardFun;
pick_shard_fun(Tab) when is_atom(Tab) ->
  pick_shard_fun(?MODULE:get(Tab)).

-spec pick_shard_fun(pick_fun(), state()) -> state().
pick_shard_fun(Fun, #state{} = State) when is_function(Fun, 3) ->
  State#state{pick_shard_fun = Fun}.

-spec pick_node_fun(state() | atom()) -> pick_fun().
pick_node_fun(#state{pick_node_fun = PickNodeFun}) ->
  PickNodeFun;
pick_node_fun(Tab) when is_atom(Tab) ->
  pick_node_fun(?MODULE:get(Tab)).

-spec pick_node_fun(pick_fun(), state()) -> state().
pick_node_fun(Fun, #state{} = State) when is_function(Fun, 3) ->
  State#state{pick_node_fun = Fun}.

-spec auto_eject_nodes(state() | atom()) -> boolean().
auto_eject_nodes(#state{auto_eject_nodes = AutoEjectNodes}) ->
  AutoEjectNodes;
auto_eject_nodes(Tab) when is_atom(Tab) ->
  auto_eject_nodes(?MODULE:get(Tab)).

-spec auto_eject_nodes(boolean(), state()) -> state().
auto_eject_nodes(Flag, #state{} = State) when is_boolean(Flag) ->
  State#state{auto_eject_nodes = Flag}.
