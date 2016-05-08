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
  pick_one/2
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
  member/3,
  take/3
]).

%%%===================================================================
%%% Types & Macros
%%%===================================================================

%% Macro to get the default module to use: `shards_local'.
-define(SHARDS, shards_local).

%% Macro to validate if table type is sharded or not
-define(is_sharded(T_), T_ =:= sharded_duplicate_bag; T_ =:= sharded_bag).

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

-spec pick_one(Key, Nodes) -> Node when
  Key   :: term(),
  Nodes :: [node()],
  Node  :: node().
pick_one(Key, Nodes) ->
  Nth = jumping_hash:calculate(erlang:phash2(Key), length(Nodes)) + 1,
  lists:nth(Nth, Nodes).

%%%===================================================================
%%% Shards API
%%%===================================================================

-spec delete(Tab :: atom()) -> true.
delete(Tab) ->
  mapred(Tab, {?SHARDS, delete, [Tab]}, nil),
  true.

-spec delete(Tab, Key, State) -> true when
  Tab   :: atom(),
  Key   :: term(),
  State :: shards_local:state().
delete(Tab, Key, {_, Type, _} = State) ->
  mapred(Tab, Key, Type, {?SHARDS, delete, [Tab, Key, State]}, nil),
  true.

-spec delete_all_objects(Tab, State) -> true when
  Tab   :: atom(),
  State :: shards_local:state().
delete_all_objects(Tab, {_, Type, _} = State) ->
  mapred(Tab, Type, {?SHARDS, delete_all_objects, [Tab, State]}, nil),
  true.

-spec delete_object(Tab, Object, State) -> true when
  Tab    :: atom(),
  Object :: tuple(),
  State  :: shards_local:state().
delete_object(Tab, Object, {_, Type, _} = State) when is_tuple(Object) ->
  [Key | _] = tuple_to_list(Object),
  mapred(Tab, Key, Type, {?SHARDS, delete_object, [Tab, Object, State]}, nil),
  true.

-spec insert(Tab, ObjOrObjL, State) -> true when
  Tab       :: atom(),
  ObjOrObjL :: tuple() | [tuple()],
  State     :: shards_local:state().
insert(Tab, ObjOrObjL, State) when is_list(ObjOrObjL) ->
  lists:foreach(fun(Object) ->
    true = insert(Tab, Object, State)
  end, ObjOrObjL), true;
insert(Tab, ObjOrObjL, {_, Type, _} = State) when is_tuple(ObjOrObjL) ->
  [Key | _] = tuple_to_list(ObjOrObjL),
  Node = case Type of
    _ when ?is_sharded(Type) ->
      pick_one({Key, os:timestamp()}, get_nodes(Tab));
    _ ->
      pick_one(Key, get_nodes(Tab))
  end,
  rpc_call(Node, ?SHARDS, insert, [Tab, ObjOrObjL, State]).

-spec insert_new(Tab, ObjOrObjL, State) -> Result when
  Tab       :: atom(),
  ObjOrObjL :: tuple() | [tuple()],
  State     :: shards_local:state(),
  Result    :: boolean() | [boolean()].
insert_new(Tab, ObjOrObjL, State) when is_list(ObjOrObjL) ->
  lists:foldr(fun(Object, Acc) ->
    [insert_new(Tab, Object, State) | Acc]
  end, [], ObjOrObjL);
insert_new(Tab, ObjOrObjL, {_, Type, _} = State) when is_tuple(ObjOrObjL) ->
  [Key | _] = tuple_to_list(ObjOrObjL),
  case Type of
    _ when ?is_sharded(Type) ->
      Map = {?SHARDS, lookup, [Tab, Key, State]},
      Reduce = fun lists:append/2,
      case mapred(Tab, Type, Map, Reduce) of
        [] ->
          Node = pick_one({Key, os:timestamp()}, get_nodes(Tab)),
          rpc_call(Node, ?SHARDS, insert_new, [Tab, ObjOrObjL, State]);
        _ ->
          false
      end;
    _ ->
      Node = pick_one(Key, get_nodes(Tab)),
      rpc_call(Node, ?SHARDS, insert_new, [Tab, ObjOrObjL, State])
  end.

-spec lookup(Tab, Key, State) -> Result when
  Tab    :: atom(),
  Key    :: term(),
  State  :: shards_local:state(),
  Result :: [tuple()].
lookup(Tab, Key, {_, Type, _} = State) ->
  Map = {?SHARDS, lookup, [Tab, Key, State]},
  Reduce = fun lists:append/2,
  mapred(Tab, Key, Type, Map, Reduce).

-spec lookup_element(Tab, Key, Pos, State) -> Elem when
  Tab   :: atom(),
  Key   :: term(),
  Pos   :: pos_integer(),
  State :: shards_local:state(),
  Elem  :: term() | [term()].
lookup_element(Tab, Key, Pos, {_, Type, _} = State) when ?is_sharded(Type) ->
  Map = {?SHARDS, lookup_element, [Tab, Key, Pos, State]},
  Filter = lists:filter(fun
    ({badrpc, {'EXIT', _}}) -> false;
    (_)                     -> true
  end, mapred(Tab, nil, Type, Map, nil)),
  case Filter of
    [] -> exit({badarg, erlang:get_stacktrace()});
    _  -> lists:append(Filter)
  end;
lookup_element(Tab, Key, Pos, State) ->
  Node = pick_one(Key, get_nodes(Tab)),
  rpc:call(Node, ?SHARDS, lookup_element, [Tab, Key, Pos, State]).

-spec member(Tab, Key, State) -> boolean() when
  Tab   :: atom(),
  Key   :: term(),
  State :: shards_local:state().
member(Tab, Key, {_, Type, _} = State) ->
  case mapred(Tab, Key, Type, {?SHARDS, member, [Tab, Key, State]}, nil) of
    R when is_list(R) -> lists:member(true, R);
    R                 -> R
  end.

%% @equiv shards_local:new(Name, Options)
new(Name, Options) ->
  shards_local:new(Name, Options).

-spec take(Tab, Key, State) -> [Object] when
  Tab    :: atom(),
  Key    :: term(),
  State  :: shards_local:state(),
  Object :: tuple().
take(Tab, Key, {_, Type, _} = State) ->
  Map = {?SHARDS, take, [Tab, Key, State]},
  Reduce = fun lists:append/2,
  mapred(Tab, Key, Type, Map, Reduce).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
rpc_call(Node, Module, Function, Args) ->
  case rpc:call(Node, Module, Function, Args) of
    {badrpc, _} -> throw(unexpected_error); % @TODO: call GC to remove this node
    Response    -> Response
  end.

%%mapred(Tab, Map) ->
%%  mapred(Tab, Map, nil).

mapred(Tab, Map, Reduce) ->
  mapred(Tab, nil, Map, Reduce).

%% @private
mapred(Tab, Type, Map, Reduce) ->
  mapred(Tab, nil, Type, Map, Reduce).

%% @private
mapred(Tab, Key, Type, Map, nil) ->
  mapred(Tab, Key, Type, Map, fun(E, Acc) -> [E | Acc] end);
mapred(Tab, nil, _, Map, Reduce) ->
  p_mapred(Tab, Map, Reduce);
mapred(Tab, _, Type, Map, Reduce) when ?is_sharded(Type) ->
  p_mapred(Tab, Map, Reduce);
mapred(Tab, Key, _, {MapMod, MapFun, MapArgs}, _) ->
  Node = pick_one(Key, get_nodes(Tab)),
  rpc_call(Node, MapMod, MapFun, MapArgs).

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
