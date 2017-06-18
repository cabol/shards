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
  get_nodes/1
]).

%% Shards API
-export([
  delete/1, delete/3,
  delete_all_objects/2,
  delete_object/3,
  insert/3,
  insert_new/3,
  lookup/3,
  lookup_element/4,
  match/3,
  match_delete/3,
  match_object/3,
  member/3,
  new/2,
  rename/3,
  select/3,
  select_count/3,
  select_delete/3,
  select_reverse/3,
  take/3,
  update_counter/4,
  update_counter/5,
  update_element/4
]).

%%%===================================================================
%%% Types & Macros
%%%===================================================================

%% Macro to get the default module to use: `shards_local'.
-define(SHARDS, shards_local).

%% @type option() = {nodes, [node()]} | shards_local:option().
-type option() :: {nodes, [node()]} | shards_local:option().

% Exported Types
-export_type([
  option/0
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
  _ = global:trans({?MODULE, Tab}, fun() ->
    rpc:multicall(FilteredNodes, erlang, apply, [fun join_/1, [Tab]])
  end),
  get_nodes(Tab).

%% @private
join_(Tab) ->
  pg2:join(Tab, shards_lib:get_pid(Tab)).

-spec leave(Tab, Nodes) -> LeavedNodes when
  Tab         :: atom(),
  Nodes       :: [node()],
  LeavedNodes :: [node()].
leave(Tab, Nodes) ->
  Members = [{node(Pid), Pid} || Pid <- pg2:get_members(Tab)],
  lists:foreach(fun(Node) ->
    case lists:keyfind(Node, 1, Members) of
      {Node, Pid} -> pg2:leave(Tab, Pid);
      _           -> noop
    end
  end, Nodes),
  get_nodes(Tab).

-spec get_nodes(Tab) -> Nodes when
  Tab   :: atom(),
  Nodes :: [node()].
get_nodes(Tab) ->
  lists:usort([node(Pid) || Pid <- pg2:get_members(Tab)]).

%%%===================================================================
%%% Shards API
%%%===================================================================

-spec delete(Tab :: atom()) -> true.
delete(Tab) ->
  _ = mapred(Tab, {?SHARDS, delete, [Tab]}, nil, shards_state:get(Tab), d),
  true.

-spec delete(Tab, Key, State) -> true when
  Tab   :: atom(),
  Key   :: term(),
  State :: shards_state:state().
delete(Tab, Key, State) ->
  Map = {?SHARDS, delete, [Tab, Key, State]},
  _ = mapred(Tab, Key, Map, nil, State, d),
  true.

-spec delete_all_objects(Tab, State) -> true when
  Tab   :: atom(),
  State :: shards_state:state().
delete_all_objects(Tab, State) ->
  Map = {?SHARDS, delete_all_objects, [Tab, State]},
  _ = mapred(Tab, Map, nil, State, d),
  true.

-spec delete_object(Tab, Object, State) -> true when
  Tab    :: atom(),
  Object :: tuple(),
  State  :: shards_state:state().
delete_object(Tab, Object, State) when is_tuple(Object) ->
  Key = hd(tuple_to_list(Object)),
  Map = {?SHARDS, delete_object, [Tab, Object, State]},
  _ = mapred(Tab, Key, Map, nil, State, d),
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
  Key = hd(tuple_to_list(ObjOrObjL)),
  PickNodeFun = shards_state:pick_node_fun(State),
  Node = pick_node(PickNodeFun, Key, get_nodes(Tab), w),
  rpc:call(Node, ?SHARDS, insert, [Tab, ObjOrObjL, State]).

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
  Key = hd(tuple_to_list(ObjOrObjL)),
  Nodes = get_nodes(Tab),
  PickNodeFun = shards_state:pick_node_fun(State),
  case pick_node(PickNodeFun, Key, Nodes, r) of
    any ->
      Map = {?SHARDS, lookup, [Tab, Key, State]},
      Reduce = fun lists:append/2,
      case mapred(Tab, Map, Reduce, State, r) of
        [] ->
          Node = pick_node(PickNodeFun, Key, Nodes, w),
          rpc:call(Node, ?SHARDS, insert_new, [Tab, ObjOrObjL, State]);
        _ ->
          false
      end;
    _ ->
      Node = pick_node(PickNodeFun, Key, Nodes, w),
      rpc:call(Node, ?SHARDS, insert_new, [Tab, ObjOrObjL, State])
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
  case pick_node(PickNodeFun, Key, Nodes, r) of
    any ->
      Map = {?SHARDS, lookup_element, [Tab, Key, Pos, State]},
      Filter = lists:filter(fun
        ({badrpc, {'EXIT', _}}) -> false;
        (_)                     -> true
      end, mapred(Tab, Map, nil, State, r)),
      case Filter of
        [] -> error(badarg);
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

-spec new(Name, Options) -> Name when
  Name    :: atom(),
  Options :: [option()].
new(Name, Options) ->
  case lists:keytake(nodes, 1, Options) of
    {value, {nodes, Nodes}, Options1} ->
      new(Name, Options1, Nodes);
    _ ->
      shards_local:new(Name, Options)
  end.

%% @private
new(Name, Options, Nodes) ->
  AllNodes = lists:usort([node() | Nodes]),
  _ = global:trans({?MODULE, Name}, fun() ->
    rpc:multicall(AllNodes, shards_local, new, [Name, Options])
  end),
  _ = join(Name, AllNodes),
  Name.

-spec rename(Tab, Name, State) -> Name | no_return() when
  Tab   :: atom(),
  Name  :: atom(),
  State :: shards_state:state().
rename(Tab, Name, State) ->
  Map = {?SHARDS, rename, [Tab, Name, State]},
  _ = mapred(Tab, nil, Map, nil, State, r),
  Nodes = get_nodes(Tab),
  ok = pg2:delete(Tab),
  ok = pg2:create(Name),
  Nodes = join(Name, Nodes),
  Name.

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

-spec update_counter(Tab, Key, UpdateOp, State) -> Result when
  Tab      :: atom(),
  Key      :: term(),
  UpdateOp :: term(),
  State    :: shards_state:state(),
  Result   :: integer().
update_counter(Tab, Key, UpdateOp, State) ->
  PickNodeFun = shards_state:pick_node_fun(State),
  Node = pick_node(PickNodeFun, Key, get_nodes(Tab), w),
  rpc:call(Node, ?SHARDS, update_counter, [Tab, Key, UpdateOp, State]).

-spec update_counter(Tab, Key, UpdateOp, Default, State) -> Result when
  Tab      :: atom(),
  Key      :: term(),
  UpdateOp :: term(),
  Default  :: tuple(),
  State    :: shards_state:state(),
  Result   :: integer().
update_counter(Tab, Key, UpdateOp, Default, State) ->
  PickNodeFun = shards_state:pick_node_fun(State),
  Node = pick_node(PickNodeFun, Key, get_nodes(Tab), w),
  rpc:call(Node, ?SHARDS, update_counter, [Tab, Key, UpdateOp, Default, State]).

-spec update_element(Tab, Key, ElementSpec, State) -> boolean() when
  Tab         :: atom(),
  Key         :: term(),
  Pos         :: pos_integer(),
  Value       :: term(),
  ElementSpec :: {Pos, Value} | [{Pos, Value}],
  State       :: shards_state:state().
update_element(Tab, Key, ElementSpec, State) ->
  PickNodeFun = shards_state:pick_node_fun(State),
  Node = pick_node(PickNodeFun, Key, get_nodes(Tab), w),
  rpc:call(Node, ?SHARDS, update_element, [Tab, Key, ElementSpec, State]).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
pick_node(Fun, Key, Nodes, Op) ->
  case Fun(Key, length(Nodes), Op) of
    Nth when is_integer(Nth) ->
      lists:nth(Nth + 1, Nodes);
    Nth ->
      Nth
  end.

%% @private
rpc_call(Node, {Module, Function, Args}) ->
  rpc:call(Node, Module, Function, Args).

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
  case pick_node(PickNodeFun, Key, get_nodes(Tab), Op) of
    any ->
      p_mapred(Tab, Map, Reduce);
    Node ->
      rpc_call(Node, Map)
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
