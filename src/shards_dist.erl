%%%-------------------------------------------------------------------
%%% @doc
%%% Distributed Shards.
%%% @end
%%%-------------------------------------------------------------------
-module(shards_dist).

%% API
-export([
  new/2, new/3, new/4
]).

%% Cluster API
-export([
  join/2,
  leave/2,
  get_nodes/1
]).

%%%===================================================================
%%% Shards API
%%%===================================================================

new(Name, Options) ->
  local_new(Name, Options).

new(Name, Options, PoolSize) when is_integer(PoolSize) ->
  local_new(Name, Options, PoolSize);
new(Nodes, Name, Options) when is_list(Options) ->
  new(Nodes, Name, Options, 0).

new(Nodes, Name, Options, PoolSize) ->
  NewNodes = [node() | Nodes],
  global:trans({?MODULE, Name}, fun() ->
    Args = [fun local_new/3, [Name, Options, PoolSize]],
    {_, BadNodes} = rpc:multicall(NewNodes, erlang, apply, Args),
    join_(Name, Nodes -- BadNodes)
  end),
  Name.

%% @private
local_new(Name, Options) ->
  local_new(Name, Options, 0).

%% @private
local_new(Name, Options, 0) ->
  Name = ets:new(Name, Options),
  setup_table(Name, [{module, ets} | Options]);
local_new(Name, Options, PoolSize) ->
  Name = shards:new(Name, Options, PoolSize),
  setup_table(Name, [{module, shards} | Options]).

%% @private
setup_table(Tab, Options) ->
  Module = get_module(Options),
  ControlInfo = [
    {nodes, []},
    {module, Module},
    {options, Options}
  ],
  true = ets:insert(Tab, ControlInfo).

%%%===================================================================
%%% Extended API
%%%===================================================================

-spec join(Tab, Nodes) -> ok when
  Tab   :: atom(),
  Nodes :: [node()].
join(Tab, Nodes) ->
  global:trans({?MODULE, Tab}, fun() ->
    case join_(Tab, Nodes) of
      {_, []}       -> ok;
      {_, BadNodes} -> leave_(Tab, BadNodes), ok
    end
  end).

%% @private
join_(Tab, Nodes) ->
  AllNodes = lists:usort([node() | get_nodes(Tab)] ++ Nodes),
  Args = [fun local_join/2, [Tab, AllNodes]],
  rpc:multicall(AllNodes, erlang, apply, Args).

%% @private
local_join(Tab, Nodes) ->
  NewNodes = lists:usort(Nodes -- [node() | get_nodes(Tab)]),
  add_nodes(Tab, NewNodes).

-spec leave(Tab, Nodes) -> ok when
  Tab   :: atom(),
  Nodes :: [node()].
leave(Tab, Nodes) ->
  global:trans({?MODULE, Tab}, fun() ->
    leave_(Tab, Nodes), ok
  end).

%% @private
leave_(Tab, Nodes) ->
  RemNodes = lists:usort(Nodes),
  RemainingNodes = [node() | get_nodes(Tab)] -- RemNodes,
  Args = [fun local_leave/2, [Tab, RemNodes]],
  rpc:multicall(RemainingNodes, erlang, apply, Args).

%% @private
local_leave(Tab, Nodes) ->
  rem_nodes(Tab, Nodes).

-spec get_nodes(Tab) -> Nodes when
  Tab   :: atom(),
  Nodes :: [node()].
get_nodes(Tab) ->
  ets:lookup_element(Tab, nodes, 2).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
add_nodes(Tab, Nodes) ->
  CurrNodes = ets:lookup_element(Tab, nodes, 2),
  true = ets:update_element(Tab, nodes, {2, Nodes ++ CurrNodes}),
  ok.

%% @private
rem_nodes(Tab, Nodes) ->
  CurrNodes = ets:lookup_element(Tab, nodes, 2),
  true = ets:update_element(Tab, nodes, {2, CurrNodes -- Nodes}),
  ok.

%% @private
get_module(Opts) ->
  case lists:keyfind(module, 1, Opts) of
    {module, Mod} -> Mod;
    false         -> shards
  end.
