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
  delete/1, delete/2,
  delete_all_objects/1,
  new/2, new/3,
  insert/2,
  insert_new/2,
  lookup/2,
  lookup_element/3,
  member/2
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

delete(Tab) ->
  Module = get_module(Tab),
  mapred(Tab, nil, {Module, delete, [Tab]}, nil),
  true.

delete(Tab, Key) ->
  Module = get_module(Tab),
  mapred(Tab, Key, {Module, delete, [Tab, Key]}, nil),
  true.

delete_all_objects(Tab) ->
  Module = get_module(Tab),
  mapred(Tab, nil, {Module, delete_all_objects, [Tab]}, nil),
  true.

insert(Tab, ObjectOrObjects) when is_list(ObjectOrObjects) ->
  lists:foreach(fun(Object) ->
    true = insert(Tab, Object)
  end, ObjectOrObjects), true;
insert(Tab, ObjectOrObjects) when is_tuple(ObjectOrObjects) ->
  Module = get_module(Tab),
  Type = shards_local:type(Tab),
  [Key | _] = tuple_to_list(ObjectOrObjects),
  Node = case Type of
    _ when Type =:= sharded_duplicate_bag; Type =:= sharded_bag ->
      pick_one({Key, os:timestamp()}, get_nodes(Tab));
    _ ->
      pick_one(Key, get_nodes(Tab))
  end,
  rpc_call(Node, Module, insert, [Tab, ObjectOrObjects]).

insert_new(Tab, ObjectOrObjects) when is_list(ObjectOrObjects) ->
  lists:foldr(fun(Object, Acc) ->
    [insert_new(Tab, Object) | Acc]
  end, [], ObjectOrObjects);
insert_new(Tab, ObjectOrObjects) when is_tuple(ObjectOrObjects) ->
  Module = get_module(Tab),
  Type = shards_local:type(Tab),
  [Key | _] = tuple_to_list(ObjectOrObjects),
  case Type of
    _ when Type =:= sharded_duplicate_bag; Type =:= sharded_bag ->
      Map = {Module, lookup, [Tab, Key]},
      Reduce = fun lists:append/2,
      case mapred(Tab, nil, Type, Map, Reduce) of
        [] ->
          Node = pick_one({Key, os:timestamp()}, get_nodes(Tab)),
          rpc_call(Node, Module, insert_new, [Tab, ObjectOrObjects]);
        _ ->
          false
      end;
    _ ->
      Node = pick_one(Key, get_nodes(Tab)),
      rpc_call(Node, Module, insert_new, [Tab, ObjectOrObjects])
  end.

lookup(Tab, Key) ->
  Module = get_module(Tab),
  mapred(Tab, Key, {Module, lookup, [Tab, Key]}, nil).

lookup_element(Tab, Key, Pos) ->
  Module = get_module(Tab),
  Type = shards_local:type(Tab),
  case Type of
    _ when Type =:= sharded_duplicate_bag; Type =:= sharded_bag ->
      LookupElem = fun(Tx, Kx, Px) ->
        catch(Module:lookup_element(Tx, Kx, Px))
      end,
      Map = {erlang, apply, [LookupElem, [Tab, Key, Pos]]},
      Filter = lists:filter(fun
        ({badrpc, {'EXIT', _}}) -> false;
        (_)                     -> true
      end, mapred(Tab, nil, Type, Map, nil)),
      case Filter of
        [] -> exit({badarg, erlang:get_stacktrace()});
        _  -> lists:append(Filter)
      end;
    _ ->
      Node = pick_one(Key, get_nodes(Tab)),
      rpc:call(Node, Module, lookup_element, [Tab, Key, Pos])
  end.

member(Tab, Key) ->
  Module = get_module(Tab),
  case mapred(Tab, Key, {Module, member, [Tab, Key]}, nil) of
    R when is_list(R) -> lists:member(true, R);
    R                 -> R
  end.

new(Name, Options) ->
  Name = shards_local:new(Name, Options),
  true = ets:insert(Name, {'$shards_dist_mod', ets}),
  Name.

new(Name, Options, PoolSize) ->
  Name = shards_local:new(Name, Options, PoolSize),
  true = ets:insert(Name, {'$shards_dist_mod', shards_local}),
  Name.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
get_module(Tab) ->
  ets:lookup_element(Tab, '$shards_dist_mod', 2).

%% @private
rpc_call(Node, Module, Function, Args) ->
  case rpc:call(Node, Module, Function, Args) of
    {badrpc, _} -> throw(unexpected_error); % @TODO: call GC to remove this node
    Response    -> Response
  end.

%%mapred(Tab, Map) ->
%%  mapred(Tab, Map, nil).

%%mapred(Tab, Map, Reduce) ->
%%  mapred(Tab, nil, shards:type(Tab), Map, Reduce).

mapred(Tab, Key, Map, Reduce) ->
  mapred(Tab, Key, shards_local:type(Tab), Map, Reduce).

%% @private
mapred(Tab, Key, Type, Map, nil) ->
  mapred(Tab, Key, Type, Map, fun(E, Acc) -> [E | Acc] end);
mapred(Tab, nil, _, Map, Reduce) ->
  p_mapred(Tab, Map, Reduce);
mapred(Tab, _, Type, Map, Reduce)
    when Type =:= sharded_duplicate_bag; Type =:= sharded_bag ->
  p_mapred(Tab, Map, Reduce);
mapred(Tab, Key, _, {MapMod, MapFun, MapArgs}, _) ->
  Node = pick_one(Key, get_nodes(Tab)),
  rpc:call(Node, MapMod, MapFun, MapArgs).

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
