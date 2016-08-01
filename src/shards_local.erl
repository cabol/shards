%%%-------------------------------------------------------------------
%%% @doc
%%% This is the main module, which contains all Shards/ETS API
%%% functions, BUT works locally.
%%%
%%% <b>Shards</b> is compatible with ETS API, most of the functions
%%% preserves the same ETS semantics, with some exception which you
%%% will find on each function doc.
%%%
%%% Shards gives a top level view of a single logical ETS table,
%%% but inside, that logical table is split in multiple physical
%%% ETS tables called <b>shards</b>, where `Shards = [0 .. N-1]',
%%% and `N' is the number of shards into which you want to split
%%% the table.
%%%
%%% The K/V pairs are distributed across these shards, therefore,
%%% some of the functions does not follows the same semantics as
%%% the original ones ETS.
%%%
%%% A good example of that are the query-based functions, which
%%% returns multiple results, and in case of `ordered_set', with
%%% a particular order. E.g.:
%%% <ul>
%%% <li>`select/2', `select/3', `select/1'</li>
%%% <li>`select_reverse/2', `select_reverse/3', `select_reverse/1'</li>
%%% <li>`match/2', `match/3', `match/1'</li>
%%% <li>`match_object/2', `match_object/3', `match_object/1'</li>
%%% <li>etc...</li>
%%% </ul>
%%% For those cases, the order what results are returned is not
%%% guaranteed to be the same as the original ETS functions.
%%%
%%% Additionally to the ETS functions, `shards_local' module allows
%%% to pass an extra argument, the `State'. When `shards' is
%%% called without the `State', it must fetch the `state' first,
%%% and it is recovered doing an extra call to an ETS control table
%%% owned by `shards_owner_sup'. If any microsecond matters, you can
%%% skip it call by calling `shards_local' directly and passing
%%% the `State'. E.g.:
%%%
%%% ```
%%% % when you create the table by first time, the state is returned
%%% {tab_name, State} = shards:new(tab_name, [{n_shards, 4}]).
%%%
%%% % also you can get the state at any time by calling:
%%% State = shards_state:get(tab_name).
%%%
%%% % normal way
%%% shards:lookup(table, key1).
%%%
%%% % calling shards_local directly
%%% shards_local:lookup(table, key1, State).
%%% '''
%%%
%%% Pools of shards can be added/removed dynamically. For example,
%%% using `shards:new/2' you can add more pools, and `shards:delete/1'
%%% to remove the pool you wish.
%%% @end
%%%-------------------------------------------------------------------
-module(shards_local).

%% ETS API
-export([
  all/0,
  delete/1, delete/2, delete/3,
  delete_all_objects/1, delete_all_objects/2,
  delete_object/2, delete_object/3,
  file2tab/1, file2tab/2,
  first/1, first/2,
  foldl/3, foldl/4,
  foldr/3, foldr/4,
  give_away/3, give_away/4,
  i/0,
  info/1, info/2, info/3,
  info_shard/2, info_shard/3,
  insert/2, insert/3,
  insert_new/2, insert_new/3,
  is_compiled_ms/1,
  last/1, last/2,
  lookup/2, lookup/3,
  lookup_element/3, lookup_element/4,
  match/2, match/3, match/4, match/1,
  match_delete/2, match_delete/3,
  match_object/2, match_object/3, match_object/4, match_object/1,
  match_spec_compile/1,
  match_spec_run/2,
  member/2, member/3,
  new/2,
  next/2, next/3,
  prev/2, prev/3,
  select/2, select/3, select/4, select/1,
  select_count/2, select_count/3,
  select_delete/2, select_delete/3,
  select_reverse/2, select_reverse/3, select_reverse/4, select_reverse/1,
  setopts/2, setopts/3,
  tab2file/2, tab2file/3, tab2file/4,
  tab2list/1, tab2list/2,
  tabfile_info/1,
  table/1, table/2, table/3,
  test_ms/2,
  take/2, take/3,
  update_counter/3, update_counter/4, update_counter/5,
  update_element/3, update_element/4
]).

%% Extended API
-export([
  shard_name/2,
  pick/3,
  list/2
]).

%%%===================================================================
%%% Types & Macros
%%%===================================================================

%% @type tweaks() = {write_concurrency, boolean()}
%%                | {read_concurrency, boolean()}
%%                | compressed.
%%
%% ETS tweaks option
-type tweaks() :: {write_concurrency, boolean()}
                | {read_concurrency, boolean()}
                | compressed.

%% @type shards_opt() = {scope, l | g}
%%                    | {n_shards, pos_integer()}
%%                    | {pick_shard_fun, shards_state:pick_fun()}
%%                    | {pick_node_fun, shards_state:pick_fun()}
%%                    | {restart_strategy, one_for_one | one_for_all}.
%%
%% Shards extended options.
-type shards_opt() :: {scope, l | g}
                    | {n_shards, pos_integer()}
                    | {pick_shard_fun, shards_state:pick_fun()}
                    | {pick_node_fun, shards_state:pick_fun()}
                    | {restart_strategy, one_for_one | one_for_all}.

%% @type option() = ets:type() | ets:access() | named_table
%%                | {keypos, pos_integer()}
%%                | {heir, pid(), HeirData :: term()}
%%                | {heir, none} | tweaks()
%%                | shards_opt().
%%
%% Create table options – used by `new/2'.
-type option() :: ets:type() | ets:access() | named_table
                | {keypos, pos_integer()}
                | {heir, pid(), HeirData :: term()}
                | {heir, none} | tweaks()
                | shards_opt().

% ETS Info Tuple
-type info_tuple() :: {compressed, boolean()}
                    | {heir, pid() | none}
                    | {keypos, pos_integer()}
                    | {memory, non_neg_integer()}
                    | {name, atom()}
                    | {named_table, boolean()}
                    | {node, node()}
                    | {owner, pid()}
                    | {protection, ets:access()}
                    | {size, non_neg_integer()}
                    | {type, ets:type()}
                    | {write_concurrency, boolean()}
                    | {read_concurrency, boolean()}.

% ETS Info Item
-type info_item() :: compressed | fixed | heir | keypos | memory
                   | name | named_table | node | owner | protection
                   | safe_fixed | size | stats | type
                   | write_concurrency | read_concurrency.

%% @type continuation() = {
%%  Tab          :: atom(),
%%  MatchSpec    :: ets:match_spec(),
%%  Limit        :: pos_integer(),
%%  Shard        :: non_neg_integer(),
%%  Continuation :: ets:continuation()
%% }.
%%
%% Defines the convention to `ets:select/1,3' continuation:
%% <ul>
%% <li>`Tab': Table name.</li>
%% <li>`MatchSpec': The `ets:match_spec()'.</li>
%% <li>`Limit': Results limit.</li>
%% <li>`Shard': Shards number.</li>
%% <li>`Continuation': The `ets:continuation()'.</li>
%% </ul>
-type continuation() :: {
  Tab          :: atom(),
  MatchSpec    :: ets:match_spec(),
  Limit        :: pos_integer(),
  Shard        :: non_neg_integer(),
  Continuation :: ets:continuation()
}.

% Exported Types
-export_type([
  option/0,
  info_tuple/0,
  info_item/0,
  continuation/0
]).

%%%===================================================================
%%% ETS API
%%%===================================================================

%% @equiv ets:all()
all() ->
  ets:all().

%% @doc
%% This operation behaves like `ets:delete/1'.
%%
%% @see ets:delete/1.
%% @end
-spec delete(Tab :: atom()) -> true.
delete(Tab) ->
  ok = shards_sup:terminate_child(shards_sup, whereis(Tab)),
  true.

%% @equiv delete(Tab, Key, shards_state:new())
delete(Tab, Key) ->
  delete(Tab, Key, shards_state:new()).

%% @doc
%% This operation behaves like `ets:delete/2'.
%%
%% @see ets:delete/2.
%% @end
-spec delete(Tab, Key, State) -> true when
  Tab   :: atom(),
  Key   :: term(),
  State :: shards_state:state().
delete(Tab, Key, State) ->
  mapred(Tab, Key, {fun ets:delete/2, [Key]}, nil, State, d),
  true.

%% @equiv delete_all_objects(Tab, shards_state:new())
delete_all_objects(Tab) ->
  delete_all_objects(Tab, shards_state:new()).

%% @doc
%% This operation behaves like `ets:delete_all_objects/1'.
%%
%% @see ets:delete_all_objects/1.
%% @end
-spec delete_all_objects(Tab, State) -> true when
  Tab   :: atom(),
  State :: shards_state:state().
delete_all_objects(Tab, State) ->
  mapred(Tab, fun ets:delete_all_objects/1, State),
  true.

%% @equiv delete_object(Tab, Object, shards_state:new())
delete_object(Tab, Object) ->
  delete_object(Tab, Object, shards_state:new()).

%% @doc
%% This operation behaves like `ets:delete_object/2'.
%%
%% @see ets:delete_object/2.
%% @end
-spec delete_object(Tab, Object, State) -> true when
  Tab    :: atom(),
  Object :: tuple(),
  State  :: shards_state:state().
delete_object(Tab, Object, State) when is_tuple(Object) ->
  [Key | _] = tuple_to_list(Object),
  mapred(Tab, Key, {fun ets:delete_object/2, [Object]}, nil, State, d),
  true.

%% @equiv file2tab(Filenames, [])
file2tab(Filenames) ->
  file2tab(Filenames, []).

%% @doc
%% Similar to `shards:file2tab/2'. Moreover, it restores the
%% supervision tree for the `shards' corresponding to the given
%% files, such as if they had been created using `shards:new/2,3'.
%%
%% @see ets:file2tab/2.
%% @end
-spec file2tab(Filenames, Options) -> Response when
  Filenames :: [file:name()],
  Tab       :: atom(),
  Options   :: [Option],
  Option    :: {verify, boolean()},
  Reason    :: term(),
  Response  :: {ok, Tab} | {error, Reason}.
file2tab(Filenames, Options) ->
  try
    ShardTabs = [{First, _} | _] = [begin
      case tabfile_info(FN) of
        {ok, Info} ->
          {name, ShardTabName} = lists:keyfind(name, 1, Info),
          {ShardTabName, FN};
        {error, Reason} ->
          throw({error, Reason})
      end
    end || FN <- Filenames],
    Tab = name_from_shard(First),
    {Tab, _} = new(Tab, [
      {restore, ShardTabs, Options},
      {n_shards, length(Filenames)}
    ]),
    {ok, Tab}
  catch
    _:Error -> Error
  end.

%% @equiv first(Tab, shards_state:new())
first(Tab) ->
  first(Tab, shards_state:new()).

%% @doc
%% This operation behaves similar to `ets:first/1'.
%% The order in which results are returned, might be not the same
%% as the original ETS function. Remember shards architecture
%% described at the beginning.
%%
%% @see ets:first/1.
%% @end
-spec first(Tab, State) -> Key | '$end_of_table' when
  Tab   :: atom(),
  Key   :: term(),
  State :: shards_state:state().
first(Tab, State) ->
  N = shards_state:n_shards(State),
  Shard = N - 1,
  first(Tab, ets:first(shard_name(Tab, Shard)), Shard).

%% @private
first(Tab, '$end_of_table', Shard) when Shard > 0 ->
  NextShard = Shard - 1,
  first(Tab, ets:first(shard_name(Tab, NextShard)), NextShard);
first(_, '$end_of_table', _) ->
  '$end_of_table';
first(_, Key, _) ->
  Key.

%% @equiv foldl(Function, Acc0, Tab, shards_state:new())
foldl(Function, Acc0, Tab) ->
  foldl(Function, Acc0, Tab, shards_state:new()).

%% @doc
%% This operation behaves like `ets:foldl/3'.
%%
%% @see ets:foldl/3.
%% @end
-spec foldl(Function, Acc0, Tab, State) -> Acc1 when
  Function :: fun((Element :: term(), AccIn) -> AccOut),
  Tab      :: atom(),
  State    :: shards_state:state(),
  Acc0     :: term(),
  Acc1     :: term(),
  AccIn    :: term(),
  AccOut   :: term().
foldl(Function, Acc0, Tab, State) ->
  N = shards_state:n_shards(State),
  fold(Tab, N, foldl, [Function, Acc0]).

%% @equiv foldr(Function, Acc0, Tab, shards_state:new())
foldr(Function, Acc0, Tab) ->
  foldr(Function, Acc0, Tab, shards_state:new()).

%% @doc
%% This operation behaves like `ets:foldr/3'.
%%
%% @see ets:foldr/3.
%% @end
-spec foldr(Function, Acc0, Tab, State) -> Acc1 when
  Function :: fun((Element :: term(), AccIn) -> AccOut),
  Tab      :: atom(),
  State    :: shards_state:state(),
  Acc0     :: term(),
  Acc1     :: term(),
  AccIn    :: term(),
  AccOut   :: term().
foldr(Function, Acc0, Tab, State) ->
  N = shards_state:n_shards(State),
  fold(Tab, N, foldr, [Function, Acc0]).

%% @equiv give_away(Tab, Pid, GiftData, shards_state:new())
give_away(Tab, Pid, GiftData) ->
  give_away(Tab, Pid, GiftData, shards_state:new()).

%% @doc
%% Equivalent to `ets:give_away/3' for each shard table. It returns
%% a `boolean()' instead that just `true'. Returns `true' if the
%% function was applied successfully on each shard, otherwise
%% `false' is returned.
%%
%% <p><font color="red"><b>WARNING: It is not recommended execute
%% this function, since it might cause an unexpected behavior.
%% Once this function is executed, `shards' doesn't control/manage
%% the ETS shards anymore. So from this point, you should use
%% ETS API instead. Also it is recommended to run `shards:delete/1'
%% after run this function.
%% </b></font></p>
%%
%% @see ets:give_away/3.
%% @end
-spec give_away(Tab, Pid, GiftData, State) -> true when
  Tab      :: atom(),
  Pid      :: pid(),
  GiftData :: term(),
  State    :: shards_state:state().
give_away(Tab, Pid, GiftData, State) ->
  Map = {fun shards_owner:give_away/3, [Pid, GiftData]},
  Reduce = {fun(_, Acc) -> Acc end, true},
  mapred(Tab, Map, Reduce, State).

%% @equiv ets:i()
i() ->
  ets:i().

%% @equiv info(Tab, shards_state:new())
info(Tab) ->
  info(Tab, shards_state:new()).

%% @doc
%% If 2nd argument is `info_tuple()' this function behaves like
%% `ets:info/2', but if it is the `shards_state:state()',
%% it behaves like `ets:info/1', but instead of return the
%% information about one single table, it returns a list with
%% the information of each shard table.
%%
%% @see ets:info/1.
%% @see ets:info/2.
%% @see shards:info_shard/2.
%% @see shards:info_shard/3.
%% @end
-spec info(Tab, StateOrItem) -> Result when
  Tab         :: atom(),
  StateOrItem :: shards_state:state() | info_item(),
  InfoList    :: [info_tuple()],
  Result1     :: [InfoList] | undefined,
  Value       :: [term()] | undefined,
  Result      :: Result1 | Value.
info(Tab, Item) when is_atom(Item) ->
  info(Tab, Item, shards_state:new());
info(Tab, State) ->
  case whereis(Tab) of
    undefined -> undefined;
    _         -> mapred(Tab, fun ets:info/1, State)
  end.

%% @doc
%% This operation behaves like `ets:info/2', but instead of return
%% the information about one single table, it returns a list with
%% the information of each shard table.
%%
%% @see ets:info/2.
%% @see shards:info_shard/3.
%% @end
-spec info(Tab, Item, State) -> Value when
  Tab   :: atom(),
  State :: shards_state:state(),
  Item  :: info_item(),
  Value :: [term()] | undefined.
info(Tab, Item, State) ->
  case whereis(Tab) of
    undefined -> undefined;
    _         -> mapred(Tab, {fun ets:info/2, [Item]}, State)
  end.

%% @doc
%% This operation behaves like `ets:info/1'
%%
%% @see ets:info/1.
%% @end
-spec info_shard(Tab, Shard) -> InfoList | undefined when
  Tab      :: atom(),
  Shard    :: non_neg_integer(),
  InfoList :: [info_tuple()].
info_shard(Tab, Shard) ->
  ShardName = shard_name(Tab, Shard),
  ets:info(ShardName).

%% @doc
%% This operation behaves like `ets:info/2'.
%%
%% @see ets:info/2.
%% @end
-spec info_shard(Tab, Shard, Item) -> Value | undefined when
  Tab   :: atom(),
  Shard :: non_neg_integer(),
  Item  :: info_item(),
  Value :: term().
info_shard(Tab, Shard, Item) ->
  ShardName = shard_name(Tab, Shard),
  ets:info(ShardName, Item).

%% @equiv insert(Tab, ObjOrObjL, shards_state:new())
insert(Tab, ObjOrObjL) ->
  insert(Tab, ObjOrObjL, shards_state:new()).

%% @doc
%% This operation behaves similar to `ets:insert/2', with a big
%% difference, <b>it is not atomic</b>. This means if it fails
%% inserting some K/V pair, previous inserted KV pairs are not
%% rolled back.
%%
%% @see ets:insert/2.
%% @end
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
  N = shards_state:n_shards(State),
  PickShardFun = shards_state:pick_shard_fun(State),
  ShardName = shard_name(Tab, PickShardFun(Key, N, w)),
  ets:insert(ShardName, ObjOrObjL).

%% @equiv insert_new(Tab, ObjOrObjL, shards_state:new())
insert_new(Tab, ObjOrObjL) ->
  insert_new(Tab, ObjOrObjL, shards_state:new()).

%% @doc
%% This operation behaves like `ets:insert_new/2' BUT it is not atomic,
%% which means if it fails inserting some K/V pair, only that K/V
%% pair is affected, the rest may be successfully inserted.
%%
%% This function returns a list if the `ObjectOrObjects' is a list.
%%
%% @see ets:insert_new/2.
%% @end
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
  N = shards_state:n_shards(State),
  PickShardFun = shards_state:pick_shard_fun(State),
  case PickShardFun(Key, N, r) of
    any ->
      Map = {fun ets:lookup/2, [Key]},
      Reduce = fun lists:append/2,
      case mapred(Tab, Map, Reduce, State) of
        [] ->
          ShardName = shard_name(Tab, PickShardFun(Key, N, w)),
          ets:insert_new(ShardName, ObjOrObjL);
        _ ->
          false
      end;
    _ ->
      ShardName = shard_name(Tab, PickShardFun(Key, N, w)),
      ets:insert_new(ShardName, ObjOrObjL)
  end.

%% @equiv ets:is_compiled_ms(Term)
is_compiled_ms(Term) ->
  ets:is_compiled_ms(Term).

%% @equiv last(Tab, shards_state:new())
last(Tab) ->
  last(Tab, shards_state:new()).

%% @doc
%% This operation behaves similar to `ets:last/1'.
%% The order in which results are returned, might be not the same
%% as the original ETS function. Remember shards architecture
%% described at the beginning.
%%
%% @see ets:last/1.
%% @end
-spec last(Tab, State) -> Key | '$end_of_table' when
  Tab   :: atom(),
  State :: shards_state:state(),
  Key   :: term().
last(Tab, State) ->
  case ets:info(shard_name(Tab, 0), type) of
    ordered_set ->
      ets:last(shard_name(Tab, 0));
    _ ->
      first(Tab, State)
  end.

%% @equiv lookup(Tab, Key, shards_state:new())
lookup(Tab, Key) ->
  lookup(Tab, Key, shards_state:new()).

%% @doc
%% This operation behaves like `ets:lookup/2'.
%%
%% @see ets:lookup/2.
%% @end
-spec lookup(Tab, Key, State) -> Result when
  Tab    :: atom(),
  Key    :: term(),
  State  :: shards_state:state(),
  Result :: [tuple()].
lookup(Tab, Key, State) ->
  Map = {fun ets:lookup/2, [Key]},
  Reduce = fun lists:append/2,
  mapred(Tab, Key, Map, Reduce, State, r).

%% @equiv lookup_element(Tab, Key, Pos, shards_state:new())
lookup_element(Tab, Key, Pos) ->
  lookup_element(Tab, Key, Pos, shards_state:new()).

%% @doc
%% This operation behaves like `ets:lookup_element/3'.
%%
%% @see ets:lookup_element/3.
%% @end
-spec lookup_element(Tab, Key, Pos, State) -> Elem when
  Tab   :: atom(),
  Key   :: term(),
  Pos   :: pos_integer(),
  State :: shards_state:state(),
  Elem  :: term() | [term()].
lookup_element(Tab, Key, Pos, State) ->
  N = shards_state:n_shards(State),
  PickShardFun = shards_state:pick_shard_fun(State),
  case PickShardFun(Key, N, r) of
    any ->
      LookupElem = fun(Tx, Kx, Px) ->
        catch ets:lookup_element(Tx, Kx, Px)
      end,
      Filter = lists:filter(fun
        ({'EXIT', _}) -> false;
        (_)           -> true
      end, mapred(Tab, {LookupElem, [Key, Pos]}, State)),
      case Filter of
        [] -> exit({badarg, erlang:get_stacktrace()});
        _  -> lists:append(Filter)
      end;
    Shard ->
      ShardName = shard_name(Tab, Shard),
      ets:lookup_element(ShardName, Key, Pos)
  end.

match(Tab, Pattern) ->
  match(Tab, Pattern, shards_state:new()).

%% @doc
%% If 3rd argument is `pos_integer()' this function behaves
%% like `ets:match/3', but if it is the `shards_state:state()',
%% it behaves like `ets:match/2'.
%%
%% The order in which results are returned, might be not the same
%% as the original ETS function. Remember shards architecture
%% described at the beginning.
%%
%% @see ets:match/2.
%% @see ets:match/3.
%% @end
-spec match(Tab, Pattern, StateOrLimit) -> Response when
  Tab          :: atom(),
  Pattern      :: ets:match_pattern(),
  StateOrLimit :: shards_state:state() | pos_integer(),
  Match        :: [term()],
  Continuation :: continuation(),
  ResWithState :: [Match],
  ResWithLimit :: {[Match], Continuation} | '$end_of_table',
  Response     :: ResWithState | ResWithLimit.
match(Tab, Pattern, Limit) when is_integer(Limit), Limit > 0 ->
  match(Tab, Pattern, Limit, shards_state:new());
match(Tab, Pattern, State) ->
  Map = {fun ets:match/2, [Pattern]},
  Reduce = fun lists:append/2,
  mapred(Tab, Map, Reduce, State).

%% @doc
%% This operation behaves similar to `ets:match/3'.
%% The order in which results are returned, might be not the same
%% as the original ETS function. Remember shards architecture
%% described at the beginning.
%%
%% @see ets:match/3.
%% @end
-spec match(Tab, Pattern, Limit, State) -> Response when
  Tab          :: atom(),
  Pattern      :: ets:match_pattern(),
  Limit        :: pos_integer(),
  State        :: shards_state:state(),
  Match        :: term(),
  Continuation :: continuation(),
  Response     :: {[Match], Continuation} | '$end_of_table'.
match(Tab, Pattern, Limit, State) ->
  N = shards_state:n_shards(State),
  q(match, Tab, Pattern, Limit, q_fun(), Limit, N - 1, {[], nil}).

%% @doc
%% This operation behaves similar to `ets:match/1'.
%% The order in which results are returned, might be not the same
%% as the original ETS function. Remember shards architecture
%% described at the beginning.
%%
%% @see ets:match/1.
%% @end
-spec match(Continuation) -> Response when
  Match        :: term(),
  Continuation :: continuation(),
  Response     :: {[Match], Continuation} | '$end_of_table'.
match({_, _, Limit, _, _} = Continuation) ->
  q(match, Continuation, q_fun(), Limit, []).

%% @equiv match_delete(Tab, Pattern, shards_state:new())
match_delete(Tab, Pattern) ->
  match_delete(Tab, Pattern, shards_state:new()).

%% @doc
%% This operation behaves like `ets:match_delete/2'.
%%
%% @see ets:match_delete/2.
%% @end
-spec match_delete(Tab, Pattern, State) -> true when
  Tab     :: atom(),
  Pattern :: ets:match_pattern(),
  State   :: shards_state:state().
match_delete(Tab, Pattern, State) ->
  Map = {fun ets:match_delete/2, [Pattern]},
  Reduce = {fun(Res, Acc) -> Acc and Res end, true},
  mapred(Tab, Map, Reduce, State).

%% @equiv match_object(Tab, Pattern, shards_state:new())
match_object(Tab, Pattern) ->
  match_object(Tab, Pattern, shards_state:new()).

%% @doc
%% If 3rd argument is `pos_integer()' this function behaves like
%% `ets:match_object/3', but if it is the `shards_state:state()',
%% it behaves like `ets:match_object/2'.
%%
%% The order in which results are returned, might be not the same
%% as the original ETS function. Remember shards architecture
%% described at the beginning.
%%
%% @see ets:match_object/2.
%% @see ets:match_object/3.
%% @end
-spec match_object(Tab, Pattern, StateOrLimit) -> Response when
  Tab          :: atom(),
  Pattern      :: ets:match_pattern(),
  StateOrLimit :: shards_state:state() | pos_integer(),
  Object       :: tuple(),
  ResWithState :: [Object],
  ResWithLimit :: {[term()], Continuation} | '$end_of_table',
  Continuation :: continuation(),
  Response     :: ResWithState | ResWithLimit.
match_object(Tab, Pattern, Limit) when is_integer(Limit), Limit > 0 ->
  match_object(Tab, Pattern, Limit, shards_state:new());
match_object(Tab, Pattern, State) ->
  Map = {fun ets:match_object/2, [Pattern]},
  Reduce = fun lists:append/2,
  mapred(Tab, Map, Reduce, State).

%% @doc
%% This operation behaves similar to `ets:match_object/3'.
%% The order in which results are returned, might be not the same
%% as the original ETS function. Remember shards architecture
%% described at the beginning.
%%
%% @see ets:match_object/3.
%% @end
-spec match_object(Tab, Pattern, Limit, State) -> Response when
  Tab          :: atom(),
  Pattern      :: ets:match_pattern(),
  Limit        :: pos_integer(),
  State        :: shards_state:state(),
  Match        :: term(),
  Continuation :: continuation(),
  Response     :: {[Match], Continuation} | '$end_of_table'.
match_object(Tab, Pattern, Limit, State) ->
  N = shards_state:n_shards(State),
  q(match_object, Tab, Pattern, Limit, q_fun(), Limit, N - 1, {[], nil}).

%% @doc
%% This operation behaves similar to `ets:match_object/1'.
%% The order in which results are returned, might be not the same
%% as the original ETS function. Remember shards architecture
%% described at the beginning.
%%
%% @see ets:match_object/1.
%% @end
-spec match_object(Continuation) -> Response when
  Match        :: term(),
  Continuation :: continuation(),
  Response     :: {[Match], Continuation} | '$end_of_table'.
match_object({_, _, Limit, _, _} = Continuation) ->
  q(match_object, Continuation, q_fun(), Limit, []).

%% @equiv ets:match_spec_compile(MatchSpec)
match_spec_compile(MatchSpec) ->
  ets:match_spec_compile(MatchSpec).

%% @equiv ets:match_spec_run(List, CompiledMatchSpec)
match_spec_run(List, CompiledMatchSpec) ->
  ets:match_spec_run(List, CompiledMatchSpec).

%% @equiv member(Tab, Key, shards_state:new())
member(Tab, Key) ->
  member(Tab, Key, shards_state:new()).

%% @doc
%% This operation behaves like `ets:member/2'.
%%
%% @see ets:member/2.
%% @end
-spec member(Tab, Key, State) -> boolean() when
  Tab   :: atom(),
  Key   :: term(),
  State :: shards_state:state().
member(Tab, Key, State) ->
  case mapred(Tab, Key, {fun ets:member/2, [Key]}, nil, State, r) of
    R when is_list(R) -> lists:member(true, R);
    R                 -> R
  end.

%% @doc
%% This operation is analogous to `ets:new/2', BUT it behaves totally
%% different. When this function is called, instead of create a single
%% table, a new supervision tree is created and added to `shards_sup'.
%%
%% This supervision tree has a main supervisor `shards_sup' which
%% creates a control ETS table and also creates `N' number of
%% `shards_owner' (being `N' the number of shards). Each `shards_owner'
%% creates an ETS table to represent each shard, so this `gen_server'
%% acts as the table owner.
%%
%% Finally, when you create a table, internally `N' physical tables
%% are created (one per shard), but `shards' encapsulates all this
%% and you see only one logical table (similar to how a distributed
%% storage works).
%%
%% <b>IMPORTANT: By default, `NumShards = number of schedulers'.</b>
%%
%% @see ets:new/2.
%% @end
-spec new(Name, Options) -> Result when
  Name    :: atom(),
  Options :: [option()],
  State   :: shards_state:state(),
  Result  :: {Name, State}.
new(Name, Options) ->
  case shards_sup:start_child([Name, Options]) of
    {ok, _} -> {Name, shards_state:get(Name)};
    _       -> throw(badarg)
  end.

%% @equiv next(Tab, Key1, shards_state:new())
next(Tab, Key1) ->
  next(Tab, Key1, shards_state:new()).

%% @doc
%% This operation behaves similar to `ets:next/2'.
%% The order in which results are returned, might be not the same
%% as the original ETS function. Remember shards architecture
%% described at the beginning. It raises a `bad_pick_fun_ret'
%% exception in case of pick fun returns `any'.
%%
%% @see ets:next/2.
%% @end
-spec next(Tab, Key1, State) -> Key2 | '$end_of_table' when
  Tab   :: atom(),
  Key1  :: term(),
  State :: shards_state:state(),
  Key2  :: term().
next(Tab, Key1, State) ->
  N = shards_state:n_shards(State),
  PickShardFun = shards_state:pick_shard_fun(State),
  case PickShardFun(Key1, N, r) of
    any ->
      throw(bad_pick_fun_ret);
    Shard ->
      ShardName = shard_name(Tab, Shard),
      next_(Tab, ets:next(ShardName, Key1), Shard)
  end.

%% @private
next_(Tab, '$end_of_table', Shard) when Shard > 0 ->
  NextShard = Shard - 1,
  next_(Tab, ets:first(shard_name(Tab, NextShard)), NextShard);
next_(_, '$end_of_table', _) ->
  '$end_of_table';
next_(_, Key2, _) ->
  Key2.

%% @equiv prev(Tab, Key1, shards_state:new())
prev(Tab, Key1) ->
  prev(Tab, Key1, shards_state:new()).

%% @doc
%% This operation behaves similar to `ets:prev/2'.
%% The order in which results are returned, might be not the same
%% as the original ETS function. Remember shards architecture
%% described at the beginning.
%%
%% @see ets:prev/2.
%% @end
-spec prev(Tab, Key1, State) -> Key2 | '$end_of_table' when
  Tab   :: atom(),
  Key1  :: term(),
  State :: shards_state:state(),
  Key2  :: term().
prev(Tab, Key1, State) ->
  case ets:info(shard_name(Tab, 0), type) of
    ordered_set ->
      ets:prev(shard_name(Tab, 0), Key1);
    _ ->
      next(Tab, Key1, State)
  end.

%% @equiv select(Tab, MatchSpec, shards_state:new())
select(Tab, MatchSpec) ->
  select(Tab, MatchSpec, shards_state:new()).

%% @doc
%% If 3rd argument is `pos_integer()' this function behaves like
%% `ets:select/3', but if it is the `shards_state:state()',
%% it behaves like `ets:select/2'.
%%
%% The order in which results are returned, might be not the same
%% as the original ETS function. Remember shards architecture
%% described at the beginning.
%%
%% @see ets:select/2.
%% @see ets:select/3.
%% @end
-spec select(Tab, MatchSpec, StateOrLimit) -> Response when
  Tab          :: atom(),
  MatchSpec    :: ets:match_spec(),
  StateOrLimit :: shards_state:state() | pos_integer(),
  Match        :: term(),
  ResWithState :: [Match],
  ResWithLimit :: {[Match], Continuation} | '$end_of_table',
  Continuation :: continuation(),
  Response     :: ResWithState | ResWithLimit.
select(Tab, MatchSpec, Limit) when is_integer(Limit), Limit > 0 ->
  select(Tab, MatchSpec, Limit, shards_state:new());
select(Tab, MatchSpec, State) ->
  Map = {fun ets:select/2, [MatchSpec]},
  Reduce = fun lists:append/2,
  mapred(Tab, Map, Reduce, State).

%% @doc
%% This operation behaves similar to `ets:select/3'.
%% The order in which results are returned, might be not the same
%% as the original ETS function. Remember shards architecture
%% described at the beginning.
%%
%% @see ets:select/3.
%% @end
-spec select(Tab, MatchSpec, Limit, State) -> Response when
  Tab          :: atom(),
  MatchSpec    :: ets:match_spec(),
  Limit        :: pos_integer(),
  State        :: shards_state:state(),
  Match        :: term(),
  Continuation :: continuation(),
  Response     :: {[Match], Continuation} | '$end_of_table'.
select(Tab, MatchSpec, Limit, State) ->
  N = shards_state:n_shards(State),
  q(select, Tab, MatchSpec, Limit, q_fun(), Limit, N - 1, {[], nil}).

%% @doc
%% This operation behaves similar to `ets:select/1'.
%% The order in which results are returned, might be not the same
%% as the original ETS function. Remember shards architecture
%% described at the beginning.
%%
%% @see ets:select/1.
%% @end
-spec select(Continuation) -> Response when
  Match        :: term(),
  Continuation :: continuation(),
  Response     :: {[Match], Continuation} | '$end_of_table'.
select({_, _, Limit, _, _} = Continuation) ->
  q(select, Continuation, q_fun(), Limit, []).

%% @equiv select_count(Tab, MatchSpec, shards_state:new())
select_count(Tab, MatchSpec) ->
  select_count(Tab, MatchSpec, shards_state:new()).

%% @doc
%% This operation behaves like `ets:select_count/2'.
%%
%% @see ets:select_count/2.
%% @end
-spec select_count(Tab, MatchSpec, State) -> NumMatched when
  Tab        :: atom(),
  MatchSpec  :: ets:match_spec(),
  State      :: shards_state:state(),
  NumMatched :: non_neg_integer().
select_count(Tab, MatchSpec, State) ->
  Map = {fun ets:select_count/2, [MatchSpec]},
  Reduce = {fun(Res, Acc) -> Acc + Res end, 0},
  mapred(Tab, Map, Reduce, State).

%% @equiv select_delete(Tab, MatchSpec, shards_state:new())
select_delete(Tab, MatchSpec) ->
  select_delete(Tab, MatchSpec, shards_state:new()).

%% @doc
%% This operation behaves like `ets:select_delete/2'.
%%
%% @see ets:select_delete/2.
%% @end
-spec select_delete(Tab, MatchSpec, State) -> NumDeleted when
  Tab        :: atom(),
  MatchSpec  :: ets:match_spec(),
  State      :: shards_state:state(),
  NumDeleted :: non_neg_integer().
select_delete(Tab, MatchSpec, State) ->
  Map = {fun ets:select_delete/2, [MatchSpec]},
  Reduce = {fun(Res, Acc) -> Acc + Res end, 0},
  mapred(Tab, Map, Reduce, State).

%% @equiv select_reverse(Tab, MatchSpec, shards_state:new())
select_reverse(Tab, MatchSpec) ->
  select_reverse(Tab, MatchSpec, shards_state:new()).

%% @doc
%% If 3rd argument is `pos_integer()' this function behaves like
%% `ets:select_reverse/3', but if it is the `shards_state:state()',
%% it behaves like `ets:select_reverse/2'.
%%
%% The order in which results are returned, might be not the same
%% as the original ETS function. Remember shards architecture
%% described at the beginning.
%%
%% @see ets:select_reverse/2.
%% @see ets:select_reverse/3.
%% @end
-spec select_reverse(Tab, MatchSpec, StateOrLimit) -> Response when
  Tab          :: atom(),
  MatchSpec    :: ets:match_spec(),
  StateOrLimit :: shards_state:state() | pos_integer(),
  Match        :: term(),
  ResWithState :: [Match],
  ResWithLimit :: {[Match], Continuation} | '$end_of_table',
  Continuation :: continuation(),
  Response     :: ResWithState | ResWithLimit.
select_reverse(Tab, MatchSpec, Limit) when is_integer(Limit), Limit > 0 ->
  select_reverse(Tab, MatchSpec, Limit, shards_state:new());
select_reverse(Tab, MatchSpec, State) ->
  Map = {fun ets:select_reverse/2, [MatchSpec]},
  Reduce = fun lists:append/2,
  mapred(Tab, Map, Reduce, State).

%% @doc
%% This operation behaves similar to `ets:select_reverse/3'.
%% The order in which results are returned, might be not the same
%% as the original ETS function. Remember shards architecture
%% described at the beginning.
%%
%% @see ets:select_reverse/3.
%% @end
-spec select_reverse(Tab, MatchSpec, Limit, State) -> Response when
  Tab          :: atom(),
  MatchSpec    :: ets:match_spec(),
  Limit        :: pos_integer(),
  State        :: shards_state:state(),
  Match        :: term(),
  Continuation :: continuation(),
  Response     :: {[Match], Continuation} | '$end_of_table'.
select_reverse(Tab, MatchSpec, Limit, State) ->
  N = shards_state:n_shards(State),
  q(select_reverse, Tab, MatchSpec, Limit, q_fun(), Limit, N - 1, {[], nil}).

%% @doc
%% This operation behaves similar to `ets:select_reverse/1'.
%% The order in which results are returned, might be not the same
%% as the original ETS function. Remember shards architecture
%% described at the beginning.
%%
%% @see ets:select_reverse/1.
%% @end
-spec select_reverse(Continuation) -> Response when
  Match        :: term(),
  Continuation :: continuation(),
  Response     :: {[Match], Continuation} | '$end_of_table'.
select_reverse({_, _, Limit, _, _} = Continuation) ->
  q(select_reverse, Continuation, q_fun(), Limit, []).

%% @equiv setopts(Tab, Opts, shards_state:new())
setopts(Tab, Opts) ->
  setopts(Tab, Opts, shards_state:new()).

%% @doc
%% Equivalent to `ets:setopts/2' for each shard table. It returns
%% a `boolean()' instead that just `true'. Returns `true' if the
%% function was applied successfully on each shard, otherwise
%% `false' is returned.
%%
%% @see ets:setopts/2.
%% @end
-spec setopts(Tab, Opts, State) -> boolean() when
  Tab      :: atom(),
  Opts     :: Opt | [Opt],
  Opt      :: {heir, pid(), HeirData} | {heir, none},
  HeirData :: term(),
  State    :: shards_state:state().
setopts(Tab, Opts, State) ->
  Map = {fun shards_owner:setopts/2, [Opts]},
  Reduce = {fun(E, Acc) -> Acc and E end, true},
  mapred(Tab, Map, Reduce, State).

%% @equiv tab2file(Tab, Filenames, shards_state:new())
tab2file(Tab, Filenames) ->
  tab2file(Tab, Filenames, shards_state:new()).

%% @equiv tab2file/4
tab2file(Tab, Filenames, Options) when is_list(Options) ->
  tab2file(Tab, Filenames, Options, shards_state:new());
tab2file(Tab, Filenames, State) ->
  tab2file(Tab, Filenames, [], State).

%% @doc
%% Similar to `ets:tab2file/3', but it returns a list of
%% responses for each shard table instead.
%%
%% @see ets:tab2file/3.
%% @end
-spec tab2file(Tab, Filenames, Options, State) -> Response when
  Tab       :: atom(),
  Filenames :: [file:name()],
  Options   :: [Option],
  Option    :: {extended_info, [ExtInfo]} | {sync, boolean()},
  ExtInfo   :: md5sum | object_count,
  State     :: shards_state:state(),
  ShardTab  :: atom(),
  ShardRes  :: ok | {error, Reason :: term()},
  Response  :: [{ShardTab, ShardRes}].
tab2file(Tab, Filenames, Options, State) ->
  N = shards_state:n_shards(State),
  [begin
     ets:tab2file(Shard, Filename, Options)
   end || {Shard, Filename} <- lists:zip(list(Tab, N), Filenames)].

%% @equiv tab2list(Tab, shards_state:new())
tab2list(Tab) ->
  tab2list(Tab, shards_state:new()).

%% @doc
%% This operation behaves like `ets:tab2list/1'.
%%
%% @see ets:tab2list/1.
%% @end
-spec tab2list(Tab, State) -> [Object] when
  Tab    :: atom(),
  State  :: shards_state:state(),
  Object :: tuple().
tab2list(Tab, State) ->
  mapred(Tab, fun ets:tab2list/1, fun lists:append/2, State).

%% @equiv ets:tabfile_info(Filename)
tabfile_info(Filename) ->
  ets:tabfile_info(Filename).

%% @equiv table(Tab, shards_state:new())
table(Tab) ->
  table(Tab, shards_state:new()).

%% @equiv table/3
table(Tab, Options) when is_list(Options) ->
  table(Tab, Options, shards_state:new());
table(Tab, State) ->
  table(Tab, [], State).

%% @doc
%% Similar to `ets:table/2', but it returns a list of `ets:table/2'
%% responses, one for each shard table.
%%
%% @see ets:table/2.
%% @end
-spec table(Tab, Options, State) -> [QueryHandle] when
  Tab            :: atom(),
  QueryHandle    :: qlc:query_handle(),
  Options        :: [Option] | Option,
  Option         :: {n_objects, NObjects} | {traverse, TraverseMethod},
  NObjects       :: default | pos_integer(),
  State          :: shards_state:state(),
  MatchSpec      :: ets:match_spec(),
  TraverseMethod :: first_next | last_prev | select | {select, MatchSpec}.
table(Tab, Options, State) ->
  mapred(Tab, {fun ets:table/2, [Options]}, State).

%% @equiv ets:test_ms(Tuple, MatchSpec)
test_ms(Tuple, MatchSpec) ->
  ets:test_ms(Tuple, MatchSpec).

%% @equiv take(Tab, Key, shards_state:new())
take(Tab, Key) ->
  take(Tab, Key, shards_state:new()).

%% @doc
%% This operation behaves like `ets:take/2'.
%%
%% @see ets:take/2.
%% @end
-spec take(Tab, Key, State) -> [Object] when
  Tab    :: atom(),
  Key    :: term(),
  State  :: shards_state:state(),
  Object :: tuple().
take(Tab, Key, State) ->
  Map = {fun ets:take/2, [Key]},
  Reduce = fun lists:append/2,
  mapred(Tab, Key, Map, Reduce, State, r).

%% @equiv update_counter(Tab, Key, UpdateOp, shards_state:new())
update_counter(Tab, Key, UpdateOp) ->
  update_counter(Tab, Key, UpdateOp, shards_state:new()).

%% @doc
%% This operation behaves like `ets:update_counter/3'.
%%
%% @see ets:update_counter/3.
%% @end
-spec update_counter(Tab, Key, UpdateOp, State) -> Result when
  Tab      :: atom(),
  Key      :: term(),
  UpdateOp :: term(),
  State    :: shards_state:state(),
  Result   :: integer().
update_counter(Tab, Key, UpdateOp, State) ->
  Map = {fun ets:update_counter/3, [Key, UpdateOp]},
  mapred(Tab, Key, Map, nil, State, w).

%% @doc
%% This operation behaves like `ets:update_counter/4'.
%%
%% @see ets:update_counter/4.
%% @end
-spec update_counter(Tab, Key, UpdateOp, Default, State) -> Result when
  Tab      :: atom(),
  Key      :: term(),
  UpdateOp :: term(),
  Default  :: tuple(),
  State    :: shards_state:state(),
  Result   :: integer().
update_counter(Tab, Key, UpdateOp, Default, State) ->
  Map = {fun ets:update_counter/4, [Key, UpdateOp, Default]},
  mapred(Tab, Key, Map, nil, State, w).

%% @equiv update_element(Tab, Key, ElementSpec, shards_state:new())
update_element(Tab, Key, ElementSpec) ->
  update_element(Tab, Key, ElementSpec, shards_state:new()).

%% @doc
%% This operation behaves like `ets:update_element/3'.
%%
%% @see ets:update_element/3.
%% @end
-spec update_element(Tab, Key, ElementSpec, State) -> boolean() when
  Tab         :: atom(),
  Key         :: term(),
  Pos         :: pos_integer(),
  Value       :: term(),
  ElementSpec :: {Pos, Value} | [{Pos, Value}],
  State       :: shards_state:state().
update_element(Tab, Key, ElementSpec, State) ->
  Map = {fun ets:update_element/3, [Key, ElementSpec]},
  mapred(Tab, Key, Map, nil, State, w).

%%%===================================================================
%%% Extended API
%%%===================================================================

%% @doc
%% Builds a shard name `ShardName'.
%% <ul>
%% <li>`TabName': Table name from which the shard name is generated.</li>
%% <li>`ShardNum': Shard number – from `0' to `(NumShards - 1)'</li>
%% </ul>
%% @end
-spec shard_name(TabName, ShardNum) -> ShardName when
  TabName   :: atom(),
  ShardNum  :: non_neg_integer(),
  ShardName :: atom().
shard_name(TabName, Shard) ->
  shards_owner:shard_name(TabName, Shard).

%% @doc
%% Pick/computes the shard where the `Key' will be handled.
%% <ul>
%% <li>`Key': The key to be hashed to calculate the shard.</li>
%% <li>`Range': Range/set – number of shards/nodes.</li>
%% <li>`Op': Operation type: `r | w | d'.</li>
%% </ul>
%% @end
-spec pick(Key, Range, Op) -> Result when
  Key    :: shards_state:key(),
  Range  :: shards_state:range(),
  Op     :: shards_state:op(),
  Result :: non_neg_integer().
pick(Key, NumShards, _) ->
  erlang:phash2(Key, NumShards).

%% @doc
%% Returns the list of shard names associated to the given `TabName'.
%% The shard names that were created in the `shards:new/2,3' fun.
%% <ul>
%% <li>`TabName': Table name.</li>
%% <li>`NumShards': Number of shards.</li>
%% </ul>
%% @end
-spec list(TabName, NumShards) -> ShardTabNames when
  TabName       :: atom(),
  NumShards     :: pos_integer(),
  ShardTabNames :: [atom()].
list(TabName, NumShards) ->
  Shards = lists:seq(0, NumShards - 1),
  [shard_name(TabName, Shard) || Shard <- Shards].

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
mapred(Tab, Map, State) ->
  mapred(Tab, Map, nil, State).

%% @private
mapred(Tab, Map, Reduce, State) ->
  mapred(Tab, nil, Map, Reduce, State, r).

%% @private
mapred(Tab, Key, Map, nil, State, Op) ->
  mapred(Tab, Key, Map, fun(E, Acc) -> [E | Acc] end, State, Op);
mapred(Tab, nil, Map, Reduce, State, _) ->
  case shards_state:n_shards(State) of
    N when N =< 1 ->
      s_mapred(Tab, N, Map, Reduce);
    N ->
      p_mapred(Tab, N, Map, Reduce)
  end;
mapred(Tab, Key, {MapFun, Args} = Map, Reduce, State, Op) ->
  N = shards_state:n_shards(State),
  PickShardFun = shards_state:pick_shard_fun(State),
  case PickShardFun(Key, N, Op) of
    any ->
      s_mapred(Tab, N, Map, Reduce);
    Shard ->
      apply(MapFun, [shard_name(Tab, Shard) | Args])
  end.

%% @private
s_mapred(Tab, NumShards, {MapFun, Args}, {ReduceFun, AccIn}) ->
  lists:foldl(fun(Shard, Acc) ->
    MapRes = apply(MapFun, [shard_name(Tab, Shard) | Args]),
    ReduceFun(MapRes, Acc)
  end, AccIn, lists:seq(0, NumShards - 1));
s_mapred(Tab, NumShards, MapFun, ReduceFun) ->
  {Map, Reduce} = mapred_funs(MapFun, ReduceFun),
  s_mapred(Tab, NumShards, Map, Reduce).

%% @private
p_mapred(Tab, NumShards, {MapFun, Args}, {ReduceFun, AccIn}) ->
  Tasks = lists:foldl(fun(Shard, Acc) ->
    AsyncTask = shards_task:async(fun() ->
      apply(MapFun, [shard_name(Tab, Shard) | Args])
    end), [AsyncTask | Acc]
  end, [], lists:seq(0, NumShards - 1)),
  lists:foldl(fun(Task, Acc) ->
    MapRes = shards_task:await(Task),
    ReduceFun(MapRes, Acc)
  end, AccIn, Tasks);
p_mapred(Tab, NumShards, MapFun, ReduceFun) ->
  {Map, Reduce} = mapred_funs(MapFun, ReduceFun),
  p_mapred(Tab, NumShards, Map, Reduce).

%% @private
mapred_funs(MapFun, ReduceFun) ->
  Map = case is_function(MapFun) of
    true -> {MapFun, []};
    _    -> MapFun
  end,
  Reduce = {ReduceFun, []},
  {Map, Reduce}.

%% @private
fold(Tab, NumShards, Fold, [Fun, Acc]) ->
  lists:foldl(fun(Shard, FoldAcc) ->
    ShardName = shard_name(Tab, Shard),
    apply(ets, Fold, [Fun, FoldAcc, ShardName])
  end, Acc, lists:seq(0, NumShards - 1)).

%% @private
name_from_shard(ShardTabName) ->
  BinShardTabName = atom_to_binary(ShardTabName, utf8),
  Tokens = binary:split(BinShardTabName, <<"_">>, [global]),
  binary_to_atom(join_bin(lists:droplast(Tokens), <<"_">>), utf8).

%% @private
join_bin(BinL, Separator) when is_list(BinL) ->
  lists:foldl(fun
    (X, <<"">>) -> <<X/binary>>;
    (X, Acc)    -> <<Acc/binary, Separator/binary, X/binary>>
  end, <<"">>, BinL).

%% @private
q(_, Tab, MatchSpec, Limit, _, 0, Shard, {Acc, Continuation}) ->
  {Acc, {Tab, MatchSpec, Limit, Shard, Continuation}};
q(_, _, _, _, _, _, Shard, {[], _}) when Shard < 0 ->
  '$end_of_table';
q(_, Tab, MatchSpec, Limit, _, _, Shard, {Acc, _}) when Shard < 0 ->
  {Acc, {Tab, MatchSpec, Limit, Shard, '$end_of_table'}};
q(F, Tab, MatchSpec, Limit, QFun, I, Shard, {Acc, '$end_of_table'}) ->
  q(F, Tab, MatchSpec, Limit, QFun, I, Shard - 1, {Acc, nil});
q(F, Tab, MatchSpec, Limit, QFun, I, Shard, {Acc, _}) ->
  case ets:F(shard_name(Tab, Shard), MatchSpec, I) of
    {L, Cont} ->
      NewAcc = {QFun(L, Acc), Cont},
      q(F, Tab, MatchSpec, Limit, QFun, I - length(L), Shard, NewAcc);
    '$end_of_table' ->
      q(F, Tab, MatchSpec, Limit, QFun, I, Shard, {Acc, '$end_of_table'})
  end.

%% @private
q(_, {Tab, MatchSpec, Limit, Shard, Continuation}, _, 0, Acc) ->
  {Acc, {Tab, MatchSpec, Limit, Shard, Continuation}};
q(F, {Tab, MatchSpec, Limit, Shard, '$end_of_table'}, QFun, I, Acc) ->
  q(F, Tab, MatchSpec, Limit, QFun, I, Shard - 1, {Acc, nil});
q(F, {Tab, MatchSpec, Limit, Shard, Continuation}, QFun, I, Acc) ->
  case ets:F(Continuation) of
    {L, Cont} ->
      NewAcc = QFun(L, Acc),
      q(F, {Tab, MatchSpec, Limit, Shard, Cont}, QFun, I - length(L), NewAcc);
    '$end_of_table' ->
      q(F, {Tab, MatchSpec, Limit, Shard, '$end_of_table'}, QFun, I, Acc)
  end.

%% @private
q_fun() ->
  fun(L1, L0) -> L1 ++ L0 end.
