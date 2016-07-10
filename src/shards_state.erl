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
  from_map/1,
  module/1,
  n_shards/1,
  type/1,
  pick_shard_fun/1,
  pick_node_fun/1,
  auto_eject_nodes/1
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

%% @type pick_shard_fun() = fun((op(), key(), n_shards()) -> non_neg_integer() | any).
%%
%% Defines spec function to pick or compute the shard.
-type pick_shard_fun() :: fun((op(), key(), n_shards()) -> non_neg_integer() | any).

%% @type pick_node_fun() = fun((op(), key(), [node()]) -> node() | any).
%%
%% Defines spec function to pick or compute the node.
-type pick_node_fun() :: fun((op(), key(), [node()]) -> node() | any).

%% State definition
-record(state, {
  module           = shards_local                  :: module(),
  n_shards         = ?N_SHARDS                     :: pos_integer(),
  type             = set                           :: ets:type(),
  pick_shard_fun   = fun shards_local:pick_shard/3 :: pick_shard_fun(),
  pick_node_fun    = fun shards_dist:pick_node/3   :: pick_node_fun(),
  auto_eject_nodes = true                          :: boolean()
}).

%% @type state() = #state{}.
%%
%% Defines `shards' state.
-type state() :: #state{}.

%% @type state_map() = #{
%%   module           => module(),
%%   n_shards         => pos_integer(),
%%   type             => ets:type(),
%%   pick_shard_fun   => pick_shard_fun(),
%%   pick_node_fun    => pick_node_fun(),
%%   auto_eject_nodes => boolean()
%% }.
%%
%% Defines the map representation of the `shards' state:
%% <ul>
%% <li>`module': Module to be used depending on the `scope':
%% `shards_local' or `shards_dist'.</li>
%% <li>`n_shards': Number of ETS shards/fragments.</li>
%% <li>`type': ETS Table type.</li>
%% <li>`pick_shard_fun': Function callback to pick/compute the shard.</li>
%% <li>`pick_node_fun': Function callback to pick/compute the node.</li>
%% <li>`auto_eject_nodes': A boolean value that controls if node should be
%% ejected when it fails.</li>
%% </ul>
-type state_map() :: #{
  module           => module(),
  n_shards         => pos_integer(),
  type             => ets:type(),
  pick_shard_fun   => pick_shard_fun(),
  pick_node_fun    => pick_node_fun(),
  auto_eject_nodes => boolean()
}.

%% Exported types
-export_type([
  state/0,
  state_map/0,
  pick_shard_fun/0,
  pick_node_fun/0,
  n_shards/0,
  op/0
]).

%%%===================================================================
%%% API
%%%===================================================================

%% @doc
%% Returns the `state' for the given table `Tab'.
%% @end
-spec get(Tab :: atom()) -> state().
get(Tab) when is_atom(Tab) ->
  [State] = ets:lookup(Tab, state),
  State.

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
    type             => State#state.type,
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
    type             = maps:get(type, Map, set),
    pick_shard_fun   = maps:get(pick_shard_fun, Map, fun shards_local:pick_shard/3),
    pick_node_fun    = maps:get(pick_node_fun, Map, fun shards_dist:pick_node/3),
    auto_eject_nodes = maps:get(auto_eject_nodes, Map, true)
  }.

%% @doc
%% Returns the used `module'.
%% @end
-spec module(atom() | state()) -> module().
module(Tab) when is_atom(Tab) ->
  module(?MODULE:get(Tab));
module(#state{module = Module}) ->
  Module.

%% @doc
%% Returns the number of ETS shards/fragments.
%% @end
-spec n_shards(atom() | state()) -> pos_integer().
n_shards(Tab) when is_atom(Tab) ->
  n_shards(?MODULE:get(Tab));
n_shards(#state{n_shards = NumShards}) ->
  NumShards.

%% @doc
%% Returns the `ets' table type.
%% @end
-spec type(atom() | state()) -> ets:type().
type(Tab) when is_atom(Tab) ->
  type(?MODULE:get(Tab));
type(#state{type = Type}) ->
  Type.

%% @doc
%% Returns the function callback to pick/compute the shard.
%% @end
-spec pick_shard_fun(atom() | state()) -> pick_shard_fun().
pick_shard_fun(Tab) when is_atom(Tab) ->
  pick_shard_fun(?MODULE:get(Tab));
pick_shard_fun(#state{pick_shard_fun = PickShardFun}) ->
  PickShardFun.

%% @doc
%% Returns the function callback to pick/compute the node.
%% @end
-spec pick_node_fun(atom() | state()) -> pick_node_fun().
pick_node_fun(Tab) when is_atom(Tab) ->
  pick_node_fun(?MODULE:get(Tab));
pick_node_fun(#state{pick_node_fun = PickNodeFun}) ->
  PickNodeFun.

%% @doc
%% Returns the boolean value that controls if node should be
%% ejected when it fails.
%% @end
-spec auto_eject_nodes(atom() | state()) -> boolean().
auto_eject_nodes(Tab) when is_atom(Tab) ->
  auto_eject_nodes(?MODULE:get(Tab));
auto_eject_nodes(#state{auto_eject_nodes = AutoEjectNodes}) ->
  AutoEjectNodes.
