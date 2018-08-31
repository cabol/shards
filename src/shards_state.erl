%%%-------------------------------------------------------------------
%%% @doc
%%% State Handler - This module encapsulates the `shards' state.
%%%
%%% There are different properties that have to be stored somewhere so
%%% `shards' can work properly. Remember, `shards' performs a logic on
%%% top of `ETS', for example, compute the shard and/or node based on
%%% the `Key' where the action will be applied. To do so, it needs the
%%% number of shards or partitions, the function to pick the shard
%%% and/or node (in case of global scope), the table type and
%%% of course, the module to use depending on the scope;
%%% `shards_local' or `shards_dist'.
%%%
%%% Because of that, when a new table is created using `shards',
%%% a new supervision tree is created as well to represent that table.
%%% The supervisor is `shards_owner_sup' and it has a control ETS
%%% table to save the `state' so it can be fetched later at any time.
%%% @end
%%%-------------------------------------------------------------------
-module(shards_state).

%% API
-export([
  new/0,
  new/1,
  new/2,
  new/3,
  new/4,
  new/5,
  get/1,
  is_state/1,
  from_map/1,
  to_map/1
]).

%% API – Getters & Setters
-export([
  module/1,
  module/2,
  sup_name/1,
  sup_name/2,
  n_shards/1,
  n_shards/2,
  pick_shard_fun/1,
  pick_shard_fun/2,
  pick_node_fun/1,
  pick_node_fun/2,
  scope/1,
  eval_pick_shard/2,
  eval_pick_shard/3
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
%% Defines spec function to pick or compute the shard and/or node.
%% The function returns a value for `Key' within the range 0..Range-1.
-type pick_fun() :: fun((key(), range(), op()) -> non_neg_integer() | any).

%% @type scope() = l | g.
%%
%% Defines the scope, if it is local `l' or global `g'.
-type scope() :: l | g.

%% State definition
-record(state, {
  module         = shards_local          :: module(),
  sup_name       = shards_sup            :: atom(),
  n_shards       = ?N_SHARDS             :: pos_integer(),
  pick_shard_fun = fun shards_lib:pick/3 :: pick_fun(),
  pick_node_fun  = fun shards_lib:pick/3 :: pick_fun()
}).

%% @type state() = #state{}.
%%
%% Defines `shards' state.
-type state() :: #state{}.

%% @type state_map() = #{
%%         module         => module(),
%%         sup_name       => atom(),
%%         n_shards       => pos_integer(),
%%         pick_shard_fun => pick_fun(),
%%         pick_node_fun  => pick_fun()
%%       }.
%%
%% Defines the map representation of the `shards' state:
%% <ul>
%% <li>`module': Module to be used depending on the `scope':
%% `shards_local' or `shards_dist'.</li>
%% <li>`sup_name': Registered name for `shards_sup'.</li>
%% <li>`n_shards': Number of ETS shards/fragments.</li>
%% <li>`pick_shard_fun': Function callback to pick/compute the shard.</li>
%% <li>`pick_node_fun': Function callback to pick/compute the node.</li>
%% </ul>
-type state_map() :: #{
        module         => module(),
        sup_name       => atom(),
        n_shards       => pos_integer(),
        pick_shard_fun => pick_fun(),
        pick_node_fun  => pick_fun()
      }.

%% Exported types
-export_type([
  op/0,
  key/0,
  n_shards/0,
  range/0,
  pick_fun/0,
  scope/0,
  state/0,
  state_map/0
]).

%%%===================================================================
%%% API
%%%===================================================================

-spec new() -> state().
new() ->
  #state{}.

-spec new(pos_integer()) -> state().
new(Shards) ->
  #state{n_shards = Shards}.

-spec new(pos_integer(), module()) -> state().
new(Shards, Module) ->
  #state{n_shards = Shards, module = Module}.

-spec new(pos_integer(), module(), atom()) -> state().
new(Shards, Module, SupName) ->
  #state{n_shards = Shards, module = Module, sup_name = SupName}.

-spec new(pos_integer(), module(), atom(), pick_fun()) -> state().
new(Shards, Module, SupName, PickShardFun) ->
  #state{
    n_shards       = Shards,
    module         = Module,
    sup_name       = SupName,
    pick_shard_fun = PickShardFun
  }.

-spec new(pos_integer(), module(), atom(), pick_fun(), pick_fun()) -> state().
new(Shards, Module, SupName, PickShardFun, PickNodeFun) ->
  #state{
    n_shards       = Shards,
    module         = Module,
    sup_name       = SupName,
    pick_shard_fun = PickShardFun,
    pick_node_fun  = PickNodeFun
  }.

%% @doc
%% Returns the `state' for the given table `Tab'.
%% @end
-spec get(Tab :: atom()) -> state().
get(Tab) when is_atom(Tab) ->
  case ets:lookup(Tab, state) of
    [State] -> State;
    _       -> error(badarg)
  end.

%% @doc
%% Returns `true' in the given argument is a valid state, otherwise
%% `false' is returned.
%% @end
-spec is_state(any()) -> boolean().
is_state(#state{}) ->
  true;
is_state(_) ->
  false.

%% @doc
%% Builds a new `state' from the given `Map'.
%% @end
-spec from_map(map()) -> state().
from_map(Map) ->
  #state{
    module         = maps:get(module, Map, shards_local),
    sup_name       = maps:get(sup_name, Map, shards_sup),
    n_shards       = maps:get(n_shards, Map, ?N_SHARDS),
    pick_shard_fun = maps:get(pick_shard_fun, Map, fun shards_lib:pick/3),
    pick_node_fun  = maps:get(pick_node_fun, Map, fun shards_lib:pick/3)
  }.

%% @doc
%% Converts the given `state' into a `map'.
%% @end
-spec to_map(state()) -> state_map().
to_map(State) ->
  #{
    module         => State#state.module,
    sup_name       => State#state.sup_name,
    n_shards       => State#state.n_shards,
    pick_shard_fun => State#state.pick_shard_fun,
    pick_node_fun  => State#state.pick_node_fun
  }.

%%%===================================================================
%%% API
%%%===================================================================

-spec module(state() | atom()) -> module().
module(#state{module = Module}) ->
  Module;
module(Tab) when is_atom(Tab) ->
  module(?MODULE:get(Tab)).

-spec module(module(), state()) -> state().
module(Module, #state{} = State) when is_atom(Module) ->
  State#state{module = Module}.

-spec sup_name(state() | atom()) -> atom().
sup_name(#state{sup_name = SupName}) ->
  SupName;
sup_name(Tab) when is_atom(Tab) ->
  sup_name(?MODULE:get(Tab)).

-spec sup_name(atom(), state()) -> state().
sup_name(SupName, #state{} = State) when is_atom(SupName) ->
  State#state{sup_name = SupName}.

-spec n_shards(state() | atom()) -> pos_integer().
n_shards(#state{n_shards = Shards}) ->
  Shards;
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

-spec scope(state() | atom()) -> scope().
scope(#state{module = shards_local}) ->
  l;
scope(#state{module = shards_dist}) ->
  g;
scope(Tab) when is_atom(Tab) ->
  scope(?MODULE:get(Tab)).

%% @equiv eval_pick_shard(Key, w, State)
eval_pick_shard(Key, State) ->
  eval_pick_shard(Key, w, State).

-spec eval_pick_shard(key(), op(), state()) -> non_neg_integer() | any.
eval_pick_shard(Key, Op, #state{pick_shard_fun = PickShardFun, n_shards = Shards}) ->
  PickShardFun(Key, Shards, Op).
