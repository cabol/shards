%%%-------------------------------------------------------------------
%%% @doc
%%% This module implemnets Sharding but globally (on multiple
%%% distributed Erlang nodes).
%%%
%%% <u><b>Example</b></u>
%%%
%%% First of all, we have to run our app in distributed way, setting
%%% a node name and maybe a cookie. Let's suppose we spawn a node
%%% `a@127.0.0.1', and from that node:
%%%
%%% ```
%%% % when a tables is created with {scope, g}, the module
%%% % shards_dist is used internally by shards
%%% > shards:new(mytab, [{scope, g}]).
%%% mytab
%%%
%%% % from node `a` (local), join `b` and `c` nodes:
%%% > shards:join(mytab, ['b@127.0.0.1', 'c@127.0.0.1']).
%%% ['a@127.0.0.1','b@127.0.0.1','c@127.0.0.1']
%%%
%%% % let's check the list of joined nodes:
%%% > shards:get_nodes(mytab).
%%% ['a@127.0.0.1','b@127.0.0.1','c@127.0.0.1']
%%% '''
%%%
%%% Other option is calling `shards:new/3' but passing the option
%%% `{nodes, Nodes}`, where `Nodes` is the list of nodes you want
%%% to join.
%%%
%%% ```
%%% > Nodes = ['b@127.0.0.1', 'c@127.0.0.1']}].
%%% ['b@127.0.0.1','c@127.0.0.1']
%%%
%%% > shards:new(mytab, [{scope, g}, {nodes, Nodes}]).
%%% mytab
%%% '''
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(shards_dist).

%% Cluster API
-export([
  join/2,
  leave/2,
  get_nodes/1
]).

%% Internal purpose
-export([
  do_join/1
]).

%% Shards API
-export([
  delete/1, delete/3,
  delete_all_objects/2,
  delete_object/3,
  file2tab/1, file2tab/2,
  foldl/4,
  foldr/4,
  info/2, info/3,
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
  tab2file/3, tab2file/4,
  tab2list/2,
  tabfile_info/1,
  take/3,
  update_counter/4, update_counter/5,
  update_element/4
]).

%%%===================================================================
%%% Types & Macros
%%%===================================================================

%% @type option() = {nodes, [node()]} | shards_local:option().
-type option() :: {nodes, [node()]} | shards_local:option().

% Exported Types
-export_type([
  option/0
]).

%% Macro to get the default module to use: `shards_local'.
-define(SHARDS, shards_local).

%% Macro to check if the given Filename has the right type
-define(is_filename(FN_), is_list(FN_); is_binary(FN_); is_atom(FN_)).

%%%===================================================================
%%% Extended API
%%%===================================================================

-spec join(Tab :: atom(), Nodes :: [node()]) -> CurrentNodes :: [node()].
join(Tab, Nodes) ->
  global:trans({?MODULE, Tab}, fun() ->
    FilteredNodes =
      lists:filter(fun(Node) ->
        not lists:member(Node, get_nodes(Tab))
      end, Nodes),

    _ = rpc:multicall(FilteredNodes, ?MODULE, do_join, [Tab]),
    get_nodes(Tab)
  end).

%% @doc This function is used internally by `join/2'.
%% @hidden
do_join(Tab) ->
  pg2:join(Tab, shards_lib:get_pid(Tab)).

-spec leave(Tab :: atom(), Nodes :: [node()]) -> CurrentNodes :: [node()].
leave(Tab, Nodes) ->
  global:trans({?MODULE, Tab}, fun() ->
    Members = [{node(Pid), Pid} || Pid <- pg2:get_members(Tab)],

    ok =
      lists:foreach(fun(Node) ->
        case lists:keyfind(Node, 1, Members) of
          {Node, Pid} -> pg2:leave(Tab, Pid);
          _           -> noop
        end
      end, Nodes),

    get_nodes(Tab)
  end).

-spec get_nodes(Tab :: atom()) -> Nodes :: [node()].
get_nodes(Tab) ->
  lists:usort([node(Pid) || Pid <- pg2:get_members(Tab)]).

%%%===================================================================
%%% Shards API
%%%===================================================================

-spec delete(Tab :: atom()) -> true.
delete(Tab) ->
  _ = mapred(Tab, {?SHARDS, delete, [Tab]}, nil, shards_state:get(Tab), d),
  true.

-spec delete(Tab :: atom(), Key :: term(), State :: shards_state:state()) -> true.
delete(Tab, Key, State) ->
  Map = {?SHARDS, delete, [Tab, Key, State]},
  _ = mapred(Tab, Key, Map, nil, State, d),
  true.

-spec delete_all_objects(Tab :: atom(), State :: shards_state:state()) -> true.
delete_all_objects(Tab, State) ->
  Map = {?SHARDS, delete_all_objects, [Tab, State]},
  _ = mapred(Tab, Map, nil, State, d),
  true.

-spec delete_object(
        Tab    :: atom(),
        Object :: tuple(),
        State  :: shards_state:state()
      ) -> true.
delete_object(Tab, Object, State) when is_tuple(Object) ->
  Key = element(1, Object),
  Map = {?SHARDS, delete_object, [Tab, Object, State]},
  _ = mapred(Tab, Key, Map, nil, State, d),
  true.

%% @equiv file2tab(Filename, [])
file2tab(Filename) ->
  file2tab(Filename, []).

-spec file2tab(
        Filename :: shards_local:filename(),
        Options  :: [Option]
      ) -> {ok, Tab :: atom()} | {error, Reason :: term()}
      when Option :: {verify, boolean()}.
file2tab(Filename, Options) when ?is_filename(Filename) ->
  try
    StrFilename = shards_lib:to_string(Filename),
    {Tab, Nodes} = tabfile_info_local(StrFilename),

    Res =
      shards_lib:reduce_while(fun(Node, Acc) ->
        NodeFilename = shards_lib:to_string(Node) ++ "." ++ StrFilename,

        case rpc:call(Node, ?SHARDS, file2tab, [NodeFilename, Options]) of
          {ok, Tab} ->
            {cont, [Node | Acc]};

          {error, _} = E ->
            ok = lists:foreach(fun(N) -> rpc:call(N, ?SHARDS, delete, [Tab]) end, Acc),
            {halt, E}
        end
      end, [], Nodes),

    case Res of
      {error, _} = ResErr ->
        ResErr;

      _ ->
        _ = join(Tab, Nodes),
        {ok, Tab}
    end
  catch
    throw:Error -> Error
  end.

-spec foldl(
        Fun   :: fun((Element :: term(), Acc) -> Acc),
        Acc   :: term(),
        Tab   :: atom(),
        State :: shards_state:state()
      ) -> Acc
      when Acc :: term().
foldl(Fun, Acc, Tab, State) ->
  fold(foldl, Fun, Acc, Tab, State).

-spec foldr(
        Fun   :: fun((Element :: term(), Acc) -> Acc),
        Acc   :: term(),
        Tab   :: atom(),
        State :: shards_state:state()
      ) -> Acc
      when Acc :: term().
foldr(Fun, Acc, Tab, State) ->
  fold(foldr, Fun, Acc, Tab, State).

-spec info(Tab :: atom(), State :: shards_state:state()) ->
        InfoList | undefined
      when InfoList :: [shards_local:info_tuple() | {nodes, [node()]}].
info(Tab, State) ->
  case whereis(Tab) of
    undefined ->
      undefined;

    _ ->
      Map = {?SHARDS, info, [Tab, State]},
      Reduce = fun(E, Acc) -> [E | Acc] end,
      InfoLists = mapred(Tab, Map, Reduce, State, r),
      shards_info(InfoLists, [memory], get_nodes(Tab))
  end.

-spec info(
        Tab   :: atom(),
        Item  :: shards_local:info_item() | nodes,
        State :: shards_state:state()
      ) -> any() | undefined.
info(Tab, Item, State) ->
  case info(Tab, State) of
    undefined -> undefined;
    TabInfo   -> shards_lib:keyfind(Item, TabInfo)
  end.

-spec insert(
        Tab       :: atom(),
        ObjOrObjs :: tuple() | [tuple()],
        State     :: shards_state:state()
      ) -> true | no_return().
insert(Tab, ObjOrObjs, State) when is_list(ObjOrObjs) ->
  maps:fold(fun(Node, Group, Acc) ->
    Acc = rpc:call(Node, ?SHARDS, insert, [Tab, Group, State])
  end, true, group_keys_by_node(Tab, ObjOrObjs, State));
insert(Tab, ObjOrObjs, State) when is_tuple(ObjOrObjs) ->
  Key = element(1, ObjOrObjs),
  PickNodeFun = shards_state:pick_node_fun(State),
  Node = pick_node(PickNodeFun, Key, get_nodes(Tab), w),
  true = rpc:call(Node, ?SHARDS, insert, [Tab, ObjOrObjs, State]).

-spec insert_new(Tab :: atom(), ObjOrObjs, State :: shards_state:state()) ->
        boolean() | {false, ObjOrObjs}
      when ObjOrObjs :: tuple() | [tuple()].
insert_new(Tab, ObjOrObjs, State) when is_list(ObjOrObjs) ->
  Result =
    maps:fold(fun(Node, Group, Acc) ->
      case do_insert_new(Tab, Node, Group, State) of
        true            -> Acc;
        false           -> Group ++ Acc;
        {false, Failed} -> Failed ++ Acc
      end
    end, [], group_keys_by_node(Tab, ObjOrObjs, State)),

  case Result of
    [] -> true;
    _  -> {false, Result}
  end;
insert_new(Tab, ObjOrObjs, State) when is_tuple(ObjOrObjs) ->
  do_insert_new(Tab, get_node(Tab, ObjOrObjs, State), ObjOrObjs, State).

%% @private
do_insert_new(Tab, Node, Objs, State) ->
  Key = shards_lib:key_from_object(Objs),

  case pick_node(shards_state:pick_node_fun(State), Key, get_nodes(Tab), r) of
    any ->
      Map = {?SHARDS, lookup, [Tab, Key, State]},
      Reduce = fun erlang:'++'/2,

      case mapred(Tab, Map, Reduce, State, r) of
        [] -> rpc:call(Node, ?SHARDS, insert_new, [Tab, Objs, State]);
        _  -> false
      end;

    _ ->
      rpc:call(Node, ?SHARDS, insert_new, [Tab, Objs, State])
  end.

-spec lookup(
        Tab   :: atom(),
        Key   :: term(),
        State :: shards_state:state()
      ) -> Result :: [tuple()].
lookup(Tab, Key, State) ->
  Map = {?SHARDS, lookup, [Tab, Key, State]},
  Reduce = fun erlang:'++'/2,
  mapred(Tab, Key, Map, Reduce, State, r).

-spec lookup_element(
        Tab   :: atom(),
        Key   :: term(),
        Pos   :: pos_integer(),
        State :: shards_state:state()
      ) -> Elem :: term() | [term()].
lookup_element(Tab, Key, Pos, State) ->
  Nodes = get_nodes(Tab),
  PickNodeFun = shards_state:pick_node_fun(State),

  case pick_node(PickNodeFun, Key, Nodes, r) of
    any ->
      Map = {?SHARDS, lookup_element, [Tab, Key, Pos, State]},

      Filter =
        lists:filter(fun
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

-spec match(
        Tab     :: atom(),
        Pattern :: ets:match_pattern(),
        State   :: shards_state:state()
      ) -> [Match :: [term()]].
match(Tab, Pattern, State) ->
  Map = {?SHARDS, match, [Tab, Pattern, State]},
  Reduce = fun erlang:'++'/2,
  mapred(Tab, Map, Reduce, State, r).

-spec match_delete(
        Tab     :: atom(),
        Pattern :: ets:match_pattern(),
        State   :: shards_state:state()
      ) -> true.
match_delete(Tab, Pattern, State) ->
  Map = {?SHARDS, match_delete, [Tab, Pattern, State]},
  Reduce = {fun erlang:'and'/2, true},
  mapred(Tab, Map, Reduce, State, delete).

-spec match_object(
        Tab     :: atom(),
        Pattern :: ets:match_pattern(),
        State   :: shards_state:state()
      ) -> [Object :: tuple()].
match_object(Tab, Pattern, State) ->
  Map = {?SHARDS, match_object, [Tab, Pattern, State]},
  Reduce = fun erlang:'++'/2,
  mapred(Tab, Map, Reduce, State, r).

-spec member(
        Tab   :: atom(),
        Key   :: term(),
        State :: shards_state:state()
      ) -> boolean().
member(Tab, Key, State) ->
  Map = {?SHARDS, member, [Tab, Key, State]},

  case mapred(Tab, Key, Map, nil, State, r) of
    Res when is_list(Res) ->
      lists:member(true, Res);

    Res ->
      Res
  end.

-spec new(Name, Options :: [option()]) -> Name when Name :: atom().
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
  Fun = fun() -> rpc:multicall(AllNodes, shards_local, new, [Name, Options]) end,
  _ = global:trans({?MODULE, Name}, Fun),
  _ = join(Name, AllNodes),
  Name.

-spec rename(Tab :: atom(), Name, State :: shards_state:state()) ->
        Name | no_return()
      when Name :: atom().
rename(Tab, Name, State) ->
  Map = {?SHARDS, rename, [Tab, Name, State]},
  _ = mapred(Tab, nil, Map, nil, State, r),
  Nodes = get_nodes(Tab),
  ok = pg2:delete(Tab),
  ok = pg2:create(Name),
  Nodes = join(Name, Nodes),
  Name.

-spec select(
        Tab       :: atom(),
        MatchSpec :: ets:match_spec(),
        State     :: shards_state:state()
      ) -> [Match :: term()].
select(Tab, MatchSpec, State) ->
  Map = {?SHARDS, select, [Tab, MatchSpec, State]},
  Reduce = fun erlang:'++'/2,
  mapred(Tab, Map, Reduce, State, r).

-spec select_count(
        Tab       :: atom(),
        MatchSpec :: ets:match_spec(),
        State     :: shards_state:state()
      ) -> NumMatched :: non_neg_integer().
select_count(Tab, MatchSpec, State) ->
  Map = {?SHARDS, select_count, [Tab, MatchSpec, State]},
  Reduce = {fun(Res, Acc) -> Acc + Res end, 0},
  mapred(Tab, Map, Reduce, State, r).

-spec select_delete(
        Tab       :: atom(),
        MatchSpec :: ets:match_spec(),
        State     :: shards_state:state()
      ) -> NumDeleted :: non_neg_integer().
select_delete(Tab, MatchSpec, State) ->
  Map = {?SHARDS, select_delete, [Tab, MatchSpec, State]},
  Reduce = {fun(Res, Acc) -> Acc + Res end, 0},
  mapred(Tab, Map, Reduce, State, delete).

-spec select_reverse(
        Tab       :: atom(),
        MatchSpec :: ets:match_spec(),
        State     :: shards_state:state()
      ) -> [Match :: term()].
select_reverse(Tab, MatchSpec, State) ->
  Map = {?SHARDS, select_reverse, [Tab, MatchSpec, State]},
  Reduce = fun erlang:'++'/2,
  mapred(Tab, Map, Reduce, State, r).

%% @equiv tab2file(Tab, Filename, [], State)
tab2file(Tab, Filename, State) ->
  tab2file(Tab, Filename, [], State).

-spec tab2file(
        Tab      :: atom(),
        Filename :: shards_local:filename(),
        Options  :: [Option],
        State    :: shards_state:state()
      ) -> ok | {error, Reason :: term()}
      when Option  :: {extended_info, [ExtInfo]} | {sync, boolean()},
           ExtInfo :: md5sum | object_count.
tab2file(Tab, Filename, Options, State) when ?is_filename(Filename) ->
  StrFilename = shards_lib:to_string(Filename),
  Nodes = get_nodes(Tab),

  shards_lib:reduce_while(fun(Node, Acc) ->
    NodeFilename = shards_lib:to_string(Node) ++ "." ++ StrFilename,
    NewOpts = lists:keystore(nodes, 1, Options, {nodes, Nodes}),

    case rpc:call(Node, ?SHARDS, tab2file, [Tab, NodeFilename, NewOpts, State]) of
      ok ->
        {cont, Acc};

      {error, _} = Error ->
        {halt, Error}
    end
  end, ok, Nodes).

-spec tab2list(Tab :: atom(), State :: shards_state:state()) ->
        [Object]
      when Object :: tuple().
tab2list(Tab, State) ->
  Map = {?SHARDS, tab2list, [Tab, State]},
  Reduce = fun erlang:'++'/2,
  mapred(Tab, Map, Reduce, State, r).

-spec tabfile_info(Filename :: shards_local:filename()) ->
        {ok, TableInfo} | {error, Reason :: term()}
      when TableInfo :: [shards_local:tabinfo_item() | {nodes, [node()]}].
tabfile_info(Filename) when ?is_filename(Filename) ->
  try
    StrFilename = shards_lib:to_string(Filename),
    {_Tab, Nodes} = tabfile_info_local(StrFilename),

    TabInfoList =
      shards_lib:reduce_while(fun(Node, Acc) ->
        NodeFilename = shards_lib:to_string(Node) ++ "." ++ StrFilename,

        case rpc:call(Node, ?SHARDS, tabfile_info, [NodeFilename]) of
          {ok, TabInfo} ->
            {cont, [TabInfo | Acc]};

          {error, _} = E ->
            {halt, E}
        end
      end, [], Nodes),

    case TabInfoList of
      {error, _} = ResErr ->
        ResErr;

      _ ->
        {ok, shards_info(TabInfoList, [], Nodes)}
    end
  catch
    throw:Error -> Error
  end.

-spec take(Tab :: atom(), Key :: term(), State :: shards_state:state()) ->
        [Object :: tuple()].
take(Tab, Key, State) ->
  Map = {?SHARDS, take, [Tab, Key, State]},
  Reduce = fun erlang:'++'/2,
  mapred(Tab, Key, Map, Reduce, State, r).

-spec update_counter(
        Tab      :: atom(),
        Key      :: term(),
        UpdateOp :: term(),
        State    :: shards_state:state()
      ) -> Result :: integer().
update_counter(Tab, Key, UpdateOp, State) ->
  PickNodeFun = shards_state:pick_node_fun(State),
  Node = pick_node(PickNodeFun, Key, get_nodes(Tab), w),
  rpc:call(Node, ?SHARDS, update_counter, [Tab, Key, UpdateOp, State]).

-spec update_counter(
        Tab      :: atom(),
        Key      :: term(),
        UpdateOp :: term(),
        Default  :: tuple(),
        State    :: shards_state:state()
      ) -> Result :: integer().
update_counter(Tab, Key, UpdateOp, Default, State) ->
  PickNodeFun = shards_state:pick_node_fun(State),
  Node = pick_node(PickNodeFun, Key, get_nodes(Tab), w),
  rpc:call(Node, ?SHARDS, update_counter, [Tab, Key, UpdateOp, Default, State]).

-spec update_element(
        Tab         :: atom(),
        Key         :: term(),
        ElementSpec :: {Pos, Value} | [{Pos, Value}],
        State       :: shards_state:state()
      ) -> boolean()
      when Pos :: pos_integer(), Value :: term().
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
  Tasks =
    lists:foldl(fun(Node, Acc) ->
      AsyncTask =
        shards_task:async(fun() ->
          rpc:call(Node, MapMod, MapFun, MapArgs)
        end),

      [AsyncTask | Acc]
    end, [], get_nodes(Tab)),

  lists:foldl(fun(Task, Acc) ->
    MapRes = shards_task:await(Task),
    RedFun(MapRes, Acc)
  end, AccIn, Tasks);
p_mapred(Tab, MapFun, ReduceFun) ->
  p_mapred(Tab, MapFun, {ReduceFun, []}).

%% @private
fold(Fold, Function, Acc0, Tab, State) ->
  lists:foldl(fun(Node, FoldAcc) ->
    rpc:call(Node, ?SHARDS, Fold, [Function, FoldAcc, Tab, State])
  end, Acc0, get_nodes(Tab)).

%% @private
shards_info([FirstInfo | RestInfoLists], Attrs, Nodes) ->
  lists:foldl(fun(InfoList, InfoListAcc) ->
    shards_lib:keyupdate(fun(K, V) ->
      {K, V1} = lists:keyfind(K, 1, InfoList),
      V + V1
    end, [size] ++ Attrs, InfoListAcc)
  end, [{nodes, Nodes} | FirstInfo], RestInfoLists).

%% @private
tabfile_info_local(Filename) ->
  NodeFilename = shards_lib:to_string(node()) ++ "." ++ Filename,
  TabInfo = shards_lib:read_tabfile(NodeFilename),
  {name, Tab} = lists:keyfind(name, 1, TabInfo),
  {nodes, Nodes} = lists:keyfind(nodes, 1, TabInfo),
  {Tab, Nodes}.

%% @private
get_node(Tab, Object, State) ->
  get_node(Tab, Object, w, State) .

%% @private
get_node(Tab, Object, Op, State) ->
  Key = element(1, Object),
  PickNodeFun = shards_state:pick_node_fun(State),
  pick_node(PickNodeFun, Key, get_nodes(Tab), Op).

%% @private
group_keys_by_node(Tab, Objects, State) ->
  lists:foldr(fun(Object, Acc) ->
    Node = get_node(Tab, Object, State),
    Acc#{Node => [Object | maps:get(Node, Acc, [])]}
  end, #{}, Objects).
