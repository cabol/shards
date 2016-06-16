%%%-------------------------------------------------------------------
%%% @doc
%%% Distributed Shards.
%%% @end
%%%-------------------------------------------------------------------
-module(shards_dist).

%% Cluster API
-export([
  join/2,
  leave/2,
  get_nodes/1,
  pick_node/3
]).

%% Shards API
-export([
  delete/1, delete/3,
  delete_all_objects/2,
  delete_object/3,
  new/2,
  insert/3,
  insert_new/3,
  lookup/3,
  lookup_element/4,
  match/3,
  match_delete/3,
  match_object/3,
  member/3,
  select/3,
  select_count/3,
  select_delete/3,
  select_reverse/3,
  take/3
]).

%% Extended API
-export([
  state/1
]).

%%%===================================================================
%%% Types & Macros
%%%===================================================================

%% Macro to get the default module to use: `shards_local'.
-define(SHARDS, shards_local).

%% @type pick_node_fun() = shards_local:pick_node_fun().
%%
%% Defines spec function to pick or compute the node.
-type pick_node_fun() :: shards_local:pick_node_fun().

%% @type state() = {
%%  PickNode       :: pick_node_fun(),
%%  AutoEjectNodes :: boolean()
%% }.
%%
%% Defines the `shards' distributed state:
%% <ul>
%% <li>`PickNode': Function callback to pick/compute the node.</li>
%% <li>`AutoEjectNodes': A boolean value that controls if node should be
%% ejected when it fails.</li>
%% </ul>
-type state() :: {
  PickNode       :: pick_node_fun(),
  AutoEjectNodes :: boolean()
}.

-export_type([
  state/0
]).

%%%===================================================================
%%% Extended API
%%%===================================================================

-spec join(Tab, Nodes) -> JoinedNodes when
  Tab         :: atom(),
  Nodes       :: [node()],
  JoinedNodes :: [node()].
join(Tab, Nodes) ->
  FilteredNodes = lists:filter(fun(Node) ->
    not lists:member(Node, get_nodes(Tab))
  end, Nodes),
  global:trans({?MODULE, Tab}, fun() ->
    rpc:multicall(FilteredNodes, erlang, apply, [fun join_/1, [Tab]])
  end),
  get_nodes(Tab).

%% @private
join_(Tab) -> pg2:join(Tab, whereis(Tab)).

-spec leave(Tab, Nodes) -> LeavedNodes when
  Tab         :: atom(),
  Nodes       :: [node()],
  LeavedNodes :: [node()].
leave(Tab, Nodes) ->
  Members = [{node(Pid), Pid} || Pid <- pg2:get_members(Tab)],
  lists:foreach(fun(Node) ->
    case lists:keyfind(Node, 1, Members) of
      {Node, Pid} -> pg2:leave(Tab, Pid);
      _           -> ok
    end
  end, Nodes),
  get_nodes(Tab).

-spec get_nodes(Tab) -> Nodes when
  Tab   :: atom(),
  Nodes :: [node()].
get_nodes(Tab) ->
  lists:usort([node(Pid) || Pid <- pg2:get_members(Tab)]).

-spec pick_node(Op, Key, Nodes) -> Node when
  Op    :: shards_local:operation_t(),
  Key   :: term(),
  Nodes :: [node()],
  Node  :: node().
pick_node(_, Key, Nodes) ->
  Nth = jumping_hash:compute(erlang:phash2(Key), length(Nodes)) + 1,
  lists:nth(Nth, Nodes).

%%%===================================================================
%%% Shards API
%%%===================================================================

-spec delete(Tab :: atom()) -> true.
delete(Tab) ->
  mapred(Tab, {?SHARDS, delete, [Tab]}, nil, state(Tab), delete),
  true.

-spec delete(Tab, Key, State) -> true when
  Tab   :: atom(),
  Key   :: term(),
  State :: shards:state().
delete(Tab, Key, {Local, Dist}) ->
  Map = {?SHARDS, delete, [Tab, Key, Local]},
  mapred(Tab, Key, Map, nil, Dist, delete),
  true.

-spec delete_all_objects(Tab, State) -> true when
  Tab   :: atom(),
  State :: shards:state().
delete_all_objects(Tab, {Local, Dist}) ->
  Map = {?SHARDS, delete_all_objects, [Tab, Local]},
  mapred(Tab, Map, nil, Dist, delete),
  true.

-spec delete_object(Tab, Object, State) -> true when
  Tab    :: atom(),
  Object :: tuple(),
  State  :: shards:state().
delete_object(Tab, Object, {Local, Dist}) when is_tuple(Object) ->
  [Key | _] = tuple_to_list(Object),
  Map = {?SHARDS, delete_object, [Tab, Object, Local]},
  mapred(Tab, Key, Map, nil, Dist, delete),
  true.

-spec insert(Tab, ObjOrObjL, State) -> true when
  Tab       :: atom(),
  ObjOrObjL :: tuple() | [tuple()],
  State     :: shards:state().
insert(Tab, ObjOrObjL, State) when is_list(ObjOrObjL) ->
  lists:foreach(fun(Object) ->
    true = insert(Tab, Object, State)
  end, ObjOrObjL), true;
insert(Tab, ObjOrObjL, {Local, {PickNode, AutoEject}}) when is_tuple(ObjOrObjL) ->
  [Key | _] = tuple_to_list(ObjOrObjL),
  Node = PickNode(write, Key, get_nodes(Tab)),
  rpc_call(Node, {?SHARDS, insert, [Tab, ObjOrObjL, Local]}, Tab, AutoEject).

-spec insert_new(Tab, ObjOrObjL, State) -> Result when
  Tab       :: atom(),
  ObjOrObjL :: tuple() | [tuple()],
  State     :: shards:state(),
  Result    :: boolean() | [boolean()].
insert_new(Tab, ObjOrObjL, State) when is_list(ObjOrObjL) ->
  lists:foldr(fun(Object, Acc) ->
    [insert_new(Tab, Object, State) | Acc]
  end, [], ObjOrObjL);
insert_new(Tab, ObjOrObjL, {Local, {PickNode, AutoEject} = Dist}) when is_tuple(ObjOrObjL) ->
  [Key | _] = tuple_to_list(ObjOrObjL),
  Nodes = get_nodes(Tab),
  case PickNode(read, Key, Nodes) of
    any ->
      Map = {?SHARDS, lookup, [Tab, Key, Local]},
      Reduce = fun lists:append/2,
      case mapred(Tab, Map, Reduce, Dist, read) of
        [] ->
          Node = PickNode(write, Key, Nodes),
          rpc_call(Node, {?SHARDS, insert_new, [Tab, ObjOrObjL, Local]}, Tab, AutoEject);
        _ ->
          false
      end;
    _ ->
      Node = PickNode(write, Key, Nodes),
      rpc_call(Node, {?SHARDS, insert_new, [Tab, ObjOrObjL, Local]}, Tab, AutoEject)
  end.

-spec lookup(Tab, Key, State) -> Result when
  Tab    :: atom(),
  Key    :: term(),
  State  :: shards:state(),
  Result :: [tuple()].
lookup(Tab, Key, {Local, Dist}) ->
  Map = {?SHARDS, lookup, [Tab, Key, Local]},
  Reduce = fun lists:append/2,
  mapred(Tab, Key, Map, Reduce, Dist, read).

-spec lookup_element(Tab, Key, Pos, State) -> Elem when
  Tab   :: atom(),
  Key   :: term(),
  Pos   :: pos_integer(),
  State :: shards:state(),
  Elem  :: term() | [term()].
lookup_element(Tab, Key, Pos, {Local, {PickNode, _} = Dist}) ->
  Nodes = get_nodes(Tab),
  case PickNode(read, Key, Nodes) of
    any ->
      Map = {?SHARDS, lookup_element, [Tab, Key, Pos, Local]},
      Filter = lists:filter(fun
        ({badrpc, {'EXIT', _}}) -> false;
        (_)                     -> true
      end, mapred(Tab, Map, nil, Dist, read)),
      case Filter of
        [] -> exit({badarg, erlang:get_stacktrace()});
        _  -> lists:append(Filter)
      end;
    Node ->
      rpc:call(Node, ?SHARDS, lookup_element, [Tab, Key, Pos, Local])
  end.

-spec match(Tab, Pattern, State) -> [Match] when
  Tab     :: atom(),
  Pattern :: ets:match_pattern(),
  State   :: shards:state(),
  Match   :: [term()].
match(Tab, Pattern, {Local, Dist}) ->
  Map = {?SHARDS, match, [Tab, Pattern, Local]},
  Reduce = fun lists:append/2,
  mapred(Tab, Map, Reduce, Dist, read).

-spec match_delete(Tab, Pattern, State) -> true when
  Tab     :: atom(),
  Pattern :: ets:match_pattern(),
  State   :: shards:state().
match_delete(Tab, Pattern, {Local, Dist}) ->
  Map = {?SHARDS, match_delete, [Tab, Pattern, Local]},
  Reduce = {fun(Res, Acc) -> Acc and Res end, true},
  mapred(Tab, Map, Reduce, Dist, delete).

-spec match_object(Tab, Pattern, State) -> [Object] when
  Tab     :: atom(),
  Pattern :: ets:match_pattern(),
  State   :: shards:state(),
  Object  :: tuple().
match_object(Tab, Pattern, {Local, Dist}) ->
  Map = {?SHARDS, match_object, [Tab, Pattern, Local]},
  Reduce = fun lists:append/2,
  mapred(Tab, Map, Reduce, Dist, read).

-spec member(Tab, Key, State) -> boolean() when
  Tab   :: atom(),
  Key   :: term(),
  State :: shards:state().
member(Tab, Key, {Local, Dist}) ->
  Map = {?SHARDS, member, [Tab, Key, Local]},
  case mapred(Tab, Key, Map, nil, Dist, read) of
    R when is_list(R) -> lists:member(true, R);
    R                 -> R
  end.

-spec new(Name, Options) -> Result when
  Name       :: atom(),
  Options    :: [shards_local:option()],
  LocalState :: shards_local:state(),
  DistState  :: state(),
  Result     :: {Name, {LocalState, DistState}}.
new(Name, Options) ->
  {Name, LocalState} = shards_local:new(Name, Options),
  {Name, {LocalState, state(Name)}}.

-spec select(Tab, MatchSpec, State) -> [Match] when
  Tab       :: atom(),
  MatchSpec :: ets:match_spec(),
  State     :: shards:state(),
  Match     :: term().
select(Tab, MatchSpec, {Local, Dist}) ->
  Map = {?SHARDS, select, [Tab, MatchSpec, Local]},
  Reduce = fun lists:append/2,
  mapred(Tab, Map, Reduce, Dist, read).

-spec select_count(Tab, MatchSpec, State) -> NumMatched when
  Tab        :: atom(),
  MatchSpec  :: ets:match_spec(),
  State      :: shards:state(),
  NumMatched :: non_neg_integer().
select_count(Tab, MatchSpec, {Local, Dist}) ->
  Map = {?SHARDS, select_count, [Tab, MatchSpec, Local]},
  Reduce = {fun(Res, Acc) -> Acc + Res end, 0},
  mapred(Tab, Map, Reduce, Dist, read).

-spec select_delete(Tab, MatchSpec, State) -> NumDeleted when
  Tab        :: atom(),
  MatchSpec  :: ets:match_spec(),
  State      :: shards:state(),
  NumDeleted :: non_neg_integer().
select_delete(Tab, MatchSpec, {Local, Dist}) ->
  Map = {?SHARDS, select_delete, [Tab, MatchSpec, Local]},
  Reduce = {fun(Res, Acc) -> Acc + Res end, 0},
  mapred(Tab, Map, Reduce, Dist, delete).

-spec select_reverse(Tab, MatchSpec, State) -> [Match] when
  Tab       :: atom(),
  MatchSpec :: ets:match_spec(),
  State     :: shards:state(),
  Match     :: term().
select_reverse(Tab, MatchSpec, {Local, Dist}) ->
  Map = {?SHARDS, select_reverse, [Tab, MatchSpec, Local]},
  Reduce = fun lists:append/2,
  mapred(Tab, Map, Reduce, Dist, read).

-spec take(Tab, Key, State) -> [Object] when
  Tab    :: atom(),
  Key    :: term(),
  State  :: shards:state(),
  Object :: tuple().
take(Tab, Key, {Local, Dist}) ->
  Map = {?SHARDS, take, [Tab, Key, Local]},
  Reduce = fun lists:append/2,
  mapred(Tab, Key, Map, Reduce, Dist, read).

%%%===================================================================
%%% Extended API
%%%===================================================================

%% @doc
%% Returns the stored state information.
%% <ul>
%% <li>`TabName': Table name.</li>
%% </ul>
%% @end
-spec state(TabName) -> State when
  TabName :: atom(),
  State   :: state().
state(TabName) ->
  {_, _, State} = ets:lookup_element(TabName, '$shards_meta', 2),
  State.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
rpc_call(Node, {Module, Function, Args}, Tab, AutoEject) ->
  case rpc:call(Node, Module, Function, Args) of
    {badrpc, _} ->
      % unexpected to get here
      maybe_eject_node(Node, Tab, AutoEject),
      throw({unexpected_error, {badrpc, Node}});
    Response ->
      Response
  end.

%% @private
maybe_eject_node(Node, Tab, true) ->
  leave(Tab, [Node]),
  ok;
maybe_eject_node(_, _, _) ->
  ok.

%% @private
mapred(Tab, Map, Reduce, State, Op) ->
  mapred(Tab, nil, Map, Reduce, State, Op).

%% @private
mapred(Tab, Key, Map, nil, State, Op) ->
  mapred(Tab, Key, Map, fun(E, Acc) -> [E | Acc] end, State, Op);
mapred(Tab, nil, Map, Reduce, _, _) ->
  p_mapred(Tab, Map, Reduce);
mapred(Tab, Key, Map, Reduce, {PickNode, AutoEject}, Op) ->
  case PickNode(Op, Key, get_nodes(Tab)) of
    any ->
      p_mapred(Tab, Map, Reduce);
    Node ->
      rpc_call(Node, Map, Tab, AutoEject)
  end.

%% @private
p_mapred(Tab, {MapMod, MapFun, MapArgs}, {RedFun, AccIn}) ->
  Tasks = lists:foldl(fun(Node, Acc) ->
    AsyncTask = shards_task:async(fun() ->
      rpc:call(Node, MapMod, MapFun, MapArgs)
    end), [AsyncTask | Acc]
  end, [], get_nodes(Tab)),
  lists:foldl(fun(Task, Acc) ->
    MapRes = shards_task:await(Task),
    RedFun(MapRes, Acc)
  end, AccIn, Tasks);
p_mapred(Tab, MapFun, ReduceFun) ->
  p_mapred(Tab, MapFun, {ReduceFun, []}).
