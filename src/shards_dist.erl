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

%%%===================================================================
%%% Types & Macros
%%%===================================================================

%% Macro to get the default module to use: `shards_local'.
-define(SHARDS, shards_local).

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
  Op    :: shards_state:op(),
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
  mapred(Tab, {?SHARDS, delete, [Tab]}, nil, shards_state:get(Tab), d),
  true.

-spec delete(Tab, Key, State) -> true when
  Tab   :: atom(),
  Key   :: term(),
  State :: shards_state:state().
delete(Tab, Key, State) ->
  Map = {?SHARDS, delete, [Tab, Key, State]},
  mapred(Tab, Key, Map, nil, State, d),
  true.

-spec delete_all_objects(Tab, State) -> true when
  Tab   :: atom(),
  State :: shards_state:state().
delete_all_objects(Tab, State) ->
  Map = {?SHARDS, delete_all_objects, [Tab, State]},
  mapred(Tab, Map, nil, State, d),
  true.

-spec delete_object(Tab, Object, State) -> true when
  Tab    :: atom(),
  Object :: tuple(),
  State  :: shards_state:state().
delete_object(Tab, Object, State) when is_tuple(Object) ->
  [Key | _] = tuple_to_list(Object),
  Map = {?SHARDS, delete_object, [Tab, Object, State]},
  mapred(Tab, Key, Map, nil, State, d),
  true.

-spec insert(Tab, ObjOrObjL, State) -> true when
  Tab       :: atom(),
  ObjOrObjL :: tuple() | [tuple()],
  State     :: shards_state:state().
insert(Tab, ObjOrObjL, State) when is_list(ObjOrObjL) ->
  lists:foreach(fun(Object) ->
    true = insert(Tab, Object, State)
  end, ObjOrObjL), true;
insert(Tab, ObjOrObjL, State) when is_tuple(ObjOrObjL) ->
  [Key | _] = tuple_to_list(ObjOrObjL),
  PickNodeFun = shards_state:pick_node_fun(State),
  AutoEject = shards_state:auto_eject_nodes(State),
  Node = PickNodeFun(w, Key, get_nodes(Tab)),
  rpc_call(Node, {?SHARDS, insert, [Tab, ObjOrObjL, State]}, Tab, AutoEject).

-spec insert_new(Tab, ObjOrObjL, State) -> Result when
  Tab       :: atom(),
  ObjOrObjL :: tuple() | [tuple()],
  State     :: shards_state:state(),
  Result    :: boolean() | [boolean()].
insert_new(Tab, ObjOrObjL, State) when is_list(ObjOrObjL) ->
  lists:foldr(fun(Object, Acc) ->
    [insert_new(Tab, Object, State) | Acc]
  end, [], ObjOrObjL);
insert_new(Tab, ObjOrObjL, State) when is_tuple(ObjOrObjL) ->
  [Key | _] = tuple_to_list(ObjOrObjL),
  Nodes = get_nodes(Tab),
  PickNodeFun = shards_state:pick_node_fun(State),
  AutoEject = shards_state:auto_eject_nodes(State),
  case PickNodeFun(r, Key, Nodes) of
    any ->
      Map = {?SHARDS, lookup, [Tab, Key, State]},
      Reduce = fun lists:append/2,
      case mapred(Tab, Map, Reduce, State, r) of
        [] ->
          Node = PickNodeFun(w, Key, Nodes),
          rpc_call(Node, {?SHARDS, insert_new, [Tab, ObjOrObjL, State]}, Tab, AutoEject);
        _ ->
          false
      end;
    _ ->
      Node = PickNodeFun(w, Key, Nodes),
      rpc_call(Node, {?SHARDS, insert_new, [Tab, ObjOrObjL, State]}, Tab, AutoEject)
  end.

-spec lookup(Tab, Key, State) -> Result when
  Tab    :: atom(),
  Key    :: term(),
  State  :: shards_state:state(),
  Result :: [tuple()].
lookup(Tab, Key, State) ->
  Map = {?SHARDS, lookup, [Tab, Key, State]},
  Reduce = fun lists:append/2,
  mapred(Tab, Key, Map, Reduce, State, r).

-spec lookup_element(Tab, Key, Pos, State) -> Elem when
  Tab   :: atom(),
  Key   :: term(),
  Pos   :: pos_integer(),
  State :: shards_state:state(),
  Elem  :: term() | [term()].
lookup_element(Tab, Key, Pos, State) ->
  Nodes = get_nodes(Tab),
  PickNodeFun = shards_state:pick_node_fun(State),
  case PickNodeFun(r, Key, Nodes) of
    any ->
      Map = {?SHARDS, lookup_element, [Tab, Key, Pos, State]},
      Filter = lists:filter(fun
        ({badrpc, {'EXIT', _}}) -> false;
        (_)                     -> true
      end, mapred(Tab, Map, nil, State, r)),
      case Filter of
        [] -> exit({badarg, erlang:get_stacktrace()});
        _  -> lists:append(Filter)
      end;
    Node ->
      rpc:call(Node, ?SHARDS, lookup_element, [Tab, Key, Pos, State])
  end.

-spec match(Tab, Pattern, State) -> [Match] when
  Tab     :: atom(),
  Pattern :: ets:match_pattern(),
  State   :: shards_state:state(),
  Match   :: [term()].
match(Tab, Pattern, State) ->
  Map = {?SHARDS, match, [Tab, Pattern, State]},
  Reduce = fun lists:append/2,
  mapred(Tab, Map, Reduce, State, r).

-spec match_delete(Tab, Pattern, State) -> true when
  Tab     :: atom(),
  Pattern :: ets:match_pattern(),
  State   :: shards_state:state().
match_delete(Tab, Pattern, State) ->
  Map = {?SHARDS, match_delete, [Tab, Pattern, State]},
  Reduce = {fun(Res, Acc) -> Acc and Res end, true},
  mapred(Tab, Map, Reduce, State, delete).

-spec match_object(Tab, Pattern, State) -> [Object] when
  Tab     :: atom(),
  Pattern :: ets:match_pattern(),
  State   :: shards_state:state(),
  Object  :: tuple().
match_object(Tab, Pattern, State) ->
  Map = {?SHARDS, match_object, [Tab, Pattern, State]},
  Reduce = fun lists:append/2,
  mapred(Tab, Map, Reduce, State, r).

-spec member(Tab, Key, State) -> boolean() when
  Tab   :: atom(),
  Key   :: term(),
  State :: shards_state:state().
member(Tab, Key, State) ->
  Map = {?SHARDS, member, [Tab, Key, State]},
  case mapred(Tab, Key, Map, nil, State, r) of
    R when is_list(R) -> lists:member(true, R);
    R                 -> R
  end.

-spec new(Name, Options) -> Result when
  Name    :: atom(),
  Options :: [shards_local:option()],
  State   :: shards_state:state(),
  Result  :: {Name, State}.
new(Name, Options) ->
  shards_local:new(Name, Options).

-spec select(Tab, MatchSpec, State) -> [Match] when
  Tab       :: atom(),
  MatchSpec :: ets:match_spec(),
  State     :: shards_state:state(),
  Match     :: term().
select(Tab, MatchSpec, State) ->
  Map = {?SHARDS, select, [Tab, MatchSpec, State]},
  Reduce = fun lists:append/2,
  mapred(Tab, Map, Reduce, State, r).

-spec select_count(Tab, MatchSpec, State) -> NumMatched when
  Tab        :: atom(),
  MatchSpec  :: ets:match_spec(),
  State      :: shards_state:state(),
  NumMatched :: non_neg_integer().
select_count(Tab, MatchSpec, State) ->
  Map = {?SHARDS, select_count, [Tab, MatchSpec, State]},
  Reduce = {fun(Res, Acc) -> Acc + Res end, 0},
  mapred(Tab, Map, Reduce, State, r).

-spec select_delete(Tab, MatchSpec, State) -> NumDeleted when
  Tab        :: atom(),
  MatchSpec  :: ets:match_spec(),
  State      :: shards_state:state(),
  NumDeleted :: non_neg_integer().
select_delete(Tab, MatchSpec, State) ->
  Map = {?SHARDS, select_delete, [Tab, MatchSpec, State]},
  Reduce = {fun(Res, Acc) -> Acc + Res end, 0},
  mapred(Tab, Map, Reduce, State, delete).

-spec select_reverse(Tab, MatchSpec, State) -> [Match] when
  Tab       :: atom(),
  MatchSpec :: ets:match_spec(),
  State     :: shards_state:state(),
  Match     :: term().
select_reverse(Tab, MatchSpec, State) ->
  Map = {?SHARDS, select_reverse, [Tab, MatchSpec, State]},
  Reduce = fun lists:append/2,
  mapred(Tab, Map, Reduce, State, r).

-spec take(Tab, Key, State) -> [Object] when
  Tab    :: atom(),
  Key    :: term(),
  State  :: shards_state:state(),
  Object :: tuple().
take(Tab, Key, State) ->
  Map = {?SHARDS, take, [Tab, Key, State]},
  Reduce = fun lists:append/2,
  mapred(Tab, Key, Map, Reduce, State, r).

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
  _ = leave(Tab, [Node]),
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
mapred(Tab, Key, Map, Reduce, State, Op) ->
  PickNodeFun = shards_state:pick_node_fun(State),
  AutoEject = shards_state:auto_eject_nodes(State),
  case PickNodeFun(Op, Key, get_nodes(Tab)) of
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
