%%%-------------------------------------------------------------------
%%% @doc
%%% This is the main module, which contains all Shards/ETS API
%%% functions, BUT works locally.
%%%
%%% <b>Shards</b> is compatible with ETS API, most of the functions
%%% preserves the same ETS semantics, with som exception which you
%%% will find on the function doc.
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
%%% returns multiple results, and in case of `ordered_set', in
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
%%% Additionally to the ETS functions, Shards provides, in some cases,
%%% an additional function to receive the pool size (or number of
%%% shards). In the compatible ETS functions, this parameter
%%% `PoolSize' doesn't exist, so in those cases, Shards have to
%%% figure out the value of the pool size, which is stored in a
%%% control ETS table. For that reason, some additional functions
%%% have been created, receiving the `PoolSize' and skipping the
%%% call to the control ETS table (these functions promotes better
%%% performance. E.g.:
%%%
%%% ```
%%% % normal compatible ETS function
%%% shards:lookup(table, key1).
%%% % additional function (pool_size = 5)
%%% shards:lookup(table, key1, 5).
%%% '''
%%%
%%% Most of the cases, where the `PoolSize' is not specified, Shards
%%% has to do an additional call to the ETS control table.
%%%
%%% Pools of shards can be added/removed dynamically. For example,
%%% using `shards:new/2' or `shards:new/3' you can add more pools.
%%% And `shards:delete/1' to remove the pool you wish.
%%% @end
%%%-------------------------------------------------------------------
-module(shards_local).

%% ETS API
-export([
  all/0,
  delete/1, delete/3,
  delete_object/3,
  delete_all_objects/2,
  file2tab/1, file2tab/2,
  first/2,
  foldl/4,
  foldr/4,
  from_dets/2,
  fun2ms/1,
  give_away/4,
  i/0, i/1,
  info/2, info/3,
  info_shard/2, info_shard/3,
  init_table/3,
  insert/3,
  insert_new/3,
  is_compiled_ms/1,
  last/2,
  lookup/3,
  lookup_element/4,
  match/3, match/4, match/2,
  match_delete/3,
  match_object/3, match_object/4, match_object/2,
  match_spec_compile/1,
  match_spec_run/2,
  member/3,
  new/2, new/3,
  next/3,
  prev/3,
  rename/3,
  repair_continuation/3,
  safe_fixtable/3,
  select/3, select/4, select/2,
  select_count/3,
  select_delete/3,
  select_reverse/3, select_reverse/4, select_reverse/2,
  setopts/3,
  slot/3,
  tab2file/3, tab2file/4,
  tab2list/2,
  tabfile_info/1,
  table/2, table/3,
  test_ms/2,
  take/3,
  to_dets/3,
  update_counter/4, update_counter/5,
  update_element/4
]).

%% Extended API
-export([
  shard_name/2,
  shard/2,
  list/2,
  state/1
]).

%% Default pool size
-define(DEFAULT_POOL_SIZE, 2).

%% Macro to validate if table type is sharded or not
-define(is_sharded(T_), T_ =:= sharded_duplicate_bag; T_ =:= sharded_bag).

%%%===================================================================
%%% Types
%%%===================================================================

%% @type type() =
%% set | ordered_set | bag | duplicate_bag |
%% sharded_bad | sharded_duplicate_bag.
%%
%% Defines the table types, which are the same as ETS with two more
%% types: `sharded_bad' and `sharded_duplicate_bag'.
-type type() ::
  set | ordered_set | bag | duplicate_bag |
  sharded_bad | sharded_duplicate_bag.

%% @type state() = {
%%  Module   :: shards_local | shards_dist,
%%  Type     :: type(),
%%  PoolSize :: pos_integer()
%% }.
%%
%% Defines the `shards' local state:
%% <ul>
%% <li>`Module': Module to use: `shards_local' | `shards_dist'.</li>
%% <li>`Type': Table type.</li>
%% <li>`PoolSize': Number of ETS shards/fragments.</li>
%% </ul>
-type state() :: {
  Module   :: shards_local | shards_dist,
  Type     :: type(),
  PoolSize :: pos_integer()
}.

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

%% Exported types
-export_type([
  type/0,
  state/0,
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

%% @doc
%% This operation behaves like `ets:delete/2'.
%%
%% @see ets:delete/2.
%% @end
-spec delete(Tab, Key, State) -> true when
  Tab   :: atom(),
  Key   :: term(),
  State :: state().
delete(Tab, Key, State) ->
  mapred(Tab, Key, {fun ets:delete/2, [Key]}, nil, State),
  true.

%% @doc
%% This operation behaves like `ets:delete_all_objects/1'.
%%
%% @see ets:delete_all_objects/1.
%% @end
-spec delete_all_objects(Tab, State) -> true when
  Tab   :: atom(),
  State :: state().
delete_all_objects(Tab, State) ->
  mapred(Tab, fun ets:delete_all_objects/1, State),
  true.

%% @doc
%% This operation behaves like `ets:delete_object/2'.
%%
%% @see ets:delete_object/2.
%% @end
-spec delete_object(Tab, Object, State) -> true when
  Tab    :: atom(),
  Object :: tuple(),
  State  :: state().
delete_object(Tab, Object, State) when is_tuple(Object) ->
  [Key | _] = tuple_to_list(Object),
  mapred(Tab, Key, {fun ets:delete_object/2, [Object]}, nil, State),
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
  Response  :: [{Tab, state()} | {error, Reason}].
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
    new(Tab, [{restore, ShardTabs, Options}], length(Filenames))
  catch
    _:Error -> Error
  end.

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
  State :: state().
first(Tab, {_, _, PoolSize}) ->
  Shard = PoolSize - 1,
  first(Tab, ets:first(shard_name(Tab, Shard)), Shard).

%% @private
first(Tab, '$end_of_table', Shard) when Shard > 0 ->
  NextShard = Shard - 1,
  first(Tab, ets:first(shard_name(Tab, NextShard)), NextShard);
first(_, '$end_of_table', _) ->
  '$end_of_table';
first(_, Key, _) ->
  Key.

%% @doc
%% This operation behaves like `ets:foldl/3'.
%%
%% @see ets:foldl/3.
%% @end
-spec foldl(Function, Acc0, Tab, State) -> Acc1 when
  Function :: fun((Element :: term(), AccIn) -> AccOut),
  Tab      :: atom(),
  State    :: state(),
  Acc0     :: term(),
  Acc1     :: term(),
  AccIn    :: term(),
  AccOut   :: term().
foldl(Function, Acc0, Tab, {_, _, PoolSize}) ->
  fold(Tab, PoolSize, foldl, [Function, Acc0]).

%% @doc
%% This operation behaves like `ets:foldr/3'.
%%
%% @see ets:foldr/3.
%% @end
-spec foldr(Function, Acc0, Tab, State) -> Acc1 when
  Function :: fun((Element :: term(), AccIn) -> AccOut),
  Tab      :: atom(),
  State    :: state(),
  Acc0     :: term(),
  Acc1     :: term(),
  AccIn    :: term(),
  AccOut   :: term().
foldr(Function, Acc0, Tab, {_, _, PoolSize}) ->
  fold(Tab, PoolSize, foldr, [Function, Acc0]).

%% @doc
%% <p><font color="red"><b>NOT SUPPORTED!</b></font></p>
%% @end
from_dets(_Tab, _DetsTab) ->
  throw(unsupported_operation).

%% @doc
%% <p><font color="red"><b>WARNING:</b> Please use `ets:fun2ms/1'
%% instead.</font></p>
%%
%% Since this function uses `parse_transform', it isn't possible
%% to call `ets:fun2ms/1' from `shards'. Besides, it isn't
%% necessary, the effect is the same as you call directly
%% `ets:fun2ms/1' from you code.
%%
%% @see ets:fun2ms/1.
%% @end
fun2ms(_LiteralFun) ->
  throw(unsupported_operation).

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
  State    :: state().
give_away(Tab, Pid, GiftData, State) ->
  Map = {fun shards_owner:give_away/3, [Pid, GiftData]},
  Reduce = {fun(_, Acc) -> Acc end, true},
  mapred(Tab, Map, Reduce, State).

%% @equiv ets:i()
i() ->
  ets:i().

%% @doc
%% <p><font color="red"><b>NOT SUPPORTED!</b></font></p>
%% @end
i(_Tab) ->
  throw(unsupported_operation).

%% @doc
%% This operation behaves like `ets:info/1', but instead of return
%% the information about one single table, it returns a list with
%% the information of each shard table.
%%
%% @see ets:info/1.
%% @end
-spec info(Tab, State) -> Result when
  Tab      :: atom(),
  State    :: state(),
  Result   :: [InfoList],
  InfoList :: [term() | undefined].
info(Tab, State) ->
  mapred(Tab, fun ets:info/1, State).

%% @doc
%% This operation behaves like `ets:info/2', but instead of return
%% the information about one single table, it returns a list with
%% the information of each shard table.
%%
%% @see ets:info/2.
%% @end
-spec info(Tab, Item, State) -> Result when
  Tab    :: atom(),
  State  :: state(),
  Item   :: atom(),
  Result :: [Value],
  Value  :: [term() | undefined].
info(Tab, Item, State) ->
  mapred(Tab, {fun ets:info/2, [Item]}, State).

%% @doc
%% This operation behaves like `ets:info/1'
%%
%% @see ets:info/1.
%% @end
-spec info_shard(Tab, Shard) -> Result when
  Tab    :: atom(),
  Shard  :: non_neg_integer(),
  Result :: [term()] | undefined.
info_shard(Tab, Shard) ->
  ShardName = shard_name(Tab, Shard),
  ets:info(ShardName).

%% @doc
%% This operation behaves like `ets:info/2'.
%%
%% @see ets:info/2.
%% @end
-spec info_shard(Tab, Shard, Item) -> Result when
  Tab    :: atom(),
  Shard  :: non_neg_integer(),
  Item   :: atom(),
  Result :: term() | undefined.
info_shard(Tab, Shard, Item) ->
  ShardName = shard_name(Tab, Shard),
  ets:info(ShardName, Item).

%% @doc
%% <p><font color="red"><b>NOT SUPPORTED!</b></font></p>
%% @end
init_table(_Tab, _InitFun, _State) ->
  throw(unsupported_operation).

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
  State     :: state().
insert(Tab, ObjOrObjL, State) when is_list(ObjOrObjL) ->
  lists:foreach(fun(Object) ->
    true = insert(Tab, Object, State)
  end, ObjOrObjL), true;
insert(Tab, ObjOrObjL, {_, Type, PoolSize}) when is_tuple(ObjOrObjL) ->
  [Key | _] = tuple_to_list(ObjOrObjL),
  insert_(Tab, Key, ObjOrObjL, PoolSize, Type).

%% @private
insert_(Tab, Key, Obj, PoolSize, Type) when ?is_sharded(Type) ->
  ShardName = shard_name(Tab, shard({Key, os:timestamp()}, PoolSize)),
  ets:insert(ShardName, Obj);
insert_(Tab, Key, Obj, PoolSize, _) ->
  ShardName = shard_name(Tab, shard(Key, PoolSize)),
  ets:insert(ShardName, Obj).

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
  State     :: state(),
  Result    :: boolean() | [boolean()].
insert_new(Tab, ObjOrObjL, State) when is_list(ObjOrObjL) ->
  lists:foldr(fun(Object, Acc) ->
    [insert_new(Tab, Object, State) | Acc]
  end, [], ObjOrObjL);
insert_new(Tab, ObjOrObjL, {_, Type, PS} = State) when is_tuple(ObjOrObjL) ->
  [Key | _] = tuple_to_list(ObjOrObjL),
  case Type of
    _ when ?is_sharded(Type) ->
      Map = {fun ets:lookup/2, [Key]},
      Reduce = fun lists:append/2,
      case mapred(Tab, Map, Reduce, State) of
        [] ->
          NewKey = {Key, os:timestamp()},
          ShardName = shard_name(Tab, shard(NewKey, PS)),
          ets:insert_new(ShardName, ObjOrObjL);
        _ ->
          false
      end;
    _ ->
      ShardName = shard_name(Tab, shard(Key, PS)),
      ets:insert_new(ShardName, ObjOrObjL)
  end.

%% @equiv ets:is_compiled_ms(Term)
is_compiled_ms(Term) ->
  ets:is_compiled_ms(Term).

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
  State :: state(),
  Key   :: term().
last(Tab, {_, ordered_set, PoolSize}) ->
  last(Tab, ets:last(shard_name(Tab, 0)), 0, PoolSize - 1);
last(Tab, State) ->
  first(Tab, State).

%% @private
last(Tab, '$end_of_table', Shard, PoolSize) when Shard < PoolSize ->
  NextShard = Shard + 1,
  last(Tab, ets:last(shard_name(Tab, NextShard)), NextShard, PoolSize);
last(_, '$end_of_table', _, _) ->
  '$end_of_table';
last(_, Key, _, _) ->
  Key.

%% @doc
%% This operation behaves like `ets:lookup/2'.
%%
%% @see ets:lookup/2.
%% @end
-spec lookup(Tab, Key, State) -> Result when
  Tab    :: atom(),
  Key    :: term(),
  State  :: state(),
  Result :: [tuple()].
lookup(Tab, Key, State) ->
  mapred(Tab, Key, {fun ets:lookup/2, [Key]}, fun lists:append/2, State).

%% @doc
%% This operation behaves like `ets:lookup_element/3'.
%%
%% @see ets:lookup_element/3.
%% @end
-spec lookup_element(Tab, Key, Pos, State) -> Elem when
  Tab   :: atom(),
  Key   :: term(),
  Pos   :: pos_integer(),
  State :: state(),
  Elem  :: term() | [term()].
lookup_element(Tab, Key, Pos, {_, Type, _} = State) when ?is_sharded(Type) ->
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
lookup_element(Tab, Key, Pos, {_, _, PoolSize}) ->
  ShardName = shard_name(Tab, shard(Key, PoolSize)),
  ets:lookup_element(ShardName, Key, Pos).

%% @doc
%% This operation behaves similar to `ets:match/2'.
%% The order in which results are returned, might be not the same
%% as the original ETS function. Remember shards architecture
%% described at the beginning.
%%
%% @see ets:match/2.
%% @end
-spec match(Tab, Pattern, State) -> [Match] when
  Tab     :: atom(),
  Pattern :: ets:match_pattern(),
  State   :: state(),
  Match   :: [term()].
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
  State        :: state(),
  Match        :: term(),
  Continuation :: continuation(),
  Response     :: {[Match], Continuation} | '$end_of_table'.
match(Tab, Pattern, Limit, {_, Type, PoolSize}) ->
  q(match,
    Tab,
    Pattern,
    Limit,
    q_fun(Type),
    Limit,
    PoolSize - 1,
    {[], nil}).

%% @doc
%% This operation behaves similar to `ets:match/1'.
%% The order in which results are returned, might be not the same
%% as the original ETS function. Remember shards architecture
%% described at the beginning.
%%
%% @see ets:match/1.
%% @end
-spec match(Continuation, State) -> Response when
  Match        :: term(),
  Continuation :: continuation(),
  State        :: state(),
  Response     :: {[Match], Continuation} | '$end_of_table'.
match({_, _, Limit, _, _} = Continuation, {_, Type, _}) ->
  q(match, Continuation, q_fun(Type), Limit, []).

%% @doc
%% This operation behaves like `ets:match_delete/2'.
%%
%% @see ets:match_delete/2.
%% @end
-spec match_delete(Tab, Pattern, State) -> true when
  Tab     :: atom(),
  Pattern :: ets:match_pattern(),
  State   :: state().
match_delete(Tab, Pattern, State) ->
  Map = {fun ets:match_delete/2, [Pattern]},
  Reduce = {fun(Res, Acc) -> Acc and Res end, true},
  mapred(Tab, Map, Reduce, State).

%% @doc
%% This operation behaves similar to `ets:match_object/2'.
%% The order in which results are returned, might be not the same
%% as the original ETS function. Remember shards architecture
%% described at the beginning.
%%
%% @see ets:match_object/2.
%% @end
-spec match_object(Tab, Pattern, State) -> [Object] when
  Tab     :: atom(),
  Pattern :: ets:match_pattern(),
  State   :: state(),
  Object  :: tuple().
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
  State        :: state(),
  Match        :: term(),
  Continuation :: continuation(),
  Response     :: {[Match], Continuation} | '$end_of_table'.
match_object(Tab, Pattern, Limit, {_, Type, PoolSize}) ->
  q(match_object,
    Tab,
    Pattern,
    Limit,
    q_fun(Type),
    Limit,
    PoolSize - 1,
    {[], nil}).

%% @doc
%% This operation behaves similar to `ets:match_object/1'.
%% The order in which results are returned, might be not the same
%% as the original ETS function. Remember shards architecture
%% described at the beginning.
%%
%% @see ets:match_object/1.
%% @end
-spec match_object(Continuation, State) -> Response when
  Match        :: term(),
  Continuation :: continuation(),
  State        :: state(),
  Response     :: {[Match], Continuation} | '$end_of_table'.
match_object({_, _, Limit, _, _} = Continuation, {_, Type, _}) ->
  q(match_object, Continuation, q_fun(Type), Limit, []).

%% @equiv ets:match_spec_compile(MatchSpec)
match_spec_compile(MatchSpec) ->
  ets:match_spec_compile(MatchSpec).

%% @equiv ets:match_spec_run(List, CompiledMatchSpec)
match_spec_run(List, CompiledMatchSpec) ->
  ets:match_spec_run(List, CompiledMatchSpec).

%% @doc
%% This operation behaves like `ets:member/2'.
%%
%% @see ets:member/2.
%% @end
-spec member(Tab, Key, State) -> boolean() when
  Tab   :: atom(),
  Key   :: term(),
  State :: state().
member(Tab, Key, State) ->
  case mapred(Tab, Key, {fun ets:member/2, [Key]}, nil, State) of
    R when is_list(R) -> lists:member(true, R);
    R                 -> R
  end.

%% @equiv new(Name, Options, 2)
new(Name, Options) ->
  new(Name, Options, ?DEFAULT_POOL_SIZE).

%% @doc
%% This operation is analogous to `ets:new/2', BUT it behaves totally
%% different. When this function is called, instead of create a single
%% table, a new supervision tree is created and added to `shards_sup'.
%%
%% This supervision tree has a main supervisor `shards_sup' which
%% creates a control ETS table and also creates `N' number of
%% `shards_owner' (being `N' the pool size). Each `shards_owner'
%% creates an ETS table to represent each shard, so this `gen_server'
%% acts as the table owner.
%%
%% Finally, when you create a table, internally `N' physical tables
%% are created (one per shard), but `shards' encapsulates all this
%% and you see only one logical table (similar to how a distributed
%% storage works).
%%
%% <b>IMPORTANT: By default, `PoolSize = 2'.</b>
%%
%% @see ets:new/2.
%% @end
-spec new(Name, Options, PoolSize) -> Result when
  Name     :: atom(),
  Options  :: [Option],
  PoolSize :: pos_integer(),
  State    :: state(),
  Result   :: {Name, State},
  Option   :: type() | ets:access() | named_table | {keypos, pos_integer()} |
              {heir, pid(), HeirData :: term()} | {heir, none} | Tweaks |
              {scope, l | g},
  Tweaks   :: {write_concurrency, boolean()} |
              {read_concurrency, boolean()} |
              compressed.
new(Name, Options, PoolSize) ->
  case shards_sup:start_child([Name, Options, PoolSize]) of
    {ok, _} -> {Name, state(Name)};
    _       -> throw(badarg)
  end.

%% @doc
%% This operation behaves similar to `ets:next/2'.
%% The order in which results are returned, might be not the same
%% as the original ETS function. Remember shards architecture
%% described at the beginning.
%%
%% @see ets:next/2.
%% @end
-spec next(Tab, Key1, State) -> Key2 | '$end_of_table' when
  Tab   :: atom(),
  Key1  :: term(),
  State :: state(),
  Key2  :: term().
next(Tab, Key1, {_, _, PoolSize}) ->
  Shard = shard(Key1, PoolSize),
  ShardName = shard_name(Tab, Shard),
  next_(Tab, ets:next(ShardName, Key1), Shard).

%% @private
next_(Tab, '$end_of_table', Shard) when Shard > 0 ->
  NextShard = Shard - 1,
  next_(Tab, ets:first(shard_name(Tab, NextShard)), NextShard);
next_(_, '$end_of_table', _) ->
  '$end_of_table';
next_(_, Key2, _) ->
  Key2.

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
  State :: state(),
  Key2  :: term().
prev(Tab, Key1, {_, ordered_set, PoolSize}) ->
  Shard = shard(Key1, PoolSize),
  ShardName = shard_name(Tab, Shard),
  prev(Tab, ets:prev(ShardName, Key1), Shard, PoolSize - 1);
prev(Tab, Key1, State) ->
  next(Tab, Key1, State).

%% @private
prev(Tab, '$end_of_table', Shard, PoolSize) when Shard < PoolSize ->
  NextShard = Shard + 1,
  prev(Tab, ets:last(shard_name(Tab, NextShard)), NextShard, PoolSize);
prev(_, '$end_of_table', _, _) ->
  '$end_of_table';
prev(_, Key2, _, _) ->
  Key2.

%% @doc
%% <p><font color="red"><b>NOT SUPPORTED!</b></font></p>
%% @end
rename(_Tab, _Name, _State) ->
  throw(unsupported_operation).

%% @doc
%% <p><font color="red"><b>NOT SUPPORTED!</b></font></p>
%% @end
repair_continuation(_Continuation, _MatchSpec, _State) ->
  throw(unsupported_operation).

%% @doc
%% <p><font color="red"><b>NOT SUPPORTED!</b></font></p>
%% @end
safe_fixtable(_Tab, _Fix, _State) ->
  throw(unsupported_operation).

%% @doc
%% This operation behaves similar to `ets:select/2'.
%% The order in which results are returned, might be not the same
%% as the original ETS function. Remember shards architecture
%% described at the beginning.
%%
%% @see ets:select/2.
%% @end
-spec select(Tab, MatchSpec, State) -> [Match] when
  Tab       :: atom(),
  MatchSpec :: ets:match_spec(),
  State     :: state(),
  Match     :: term().
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
  State        :: state(),
  Match        :: term(),
  Continuation :: continuation(),
  Response     :: {[Match], Continuation} | '$end_of_table'.
select(Tab, MatchSpec, Limit, {_, Type, PoolSize}) ->
  q(select,
    Tab,
    MatchSpec,
    Limit,
    q_fun(Type),
    Limit,
    PoolSize - 1,
    {[], nil}).

%% @doc
%% This operation behaves similar to `ets:select/1'.
%% The order in which results are returned, might be not the same
%% as the original ETS function. Remember shards architecture
%% described at the beginning.
%%
%% @see ets:select/1.
%% @end
-spec select(Continuation, State) -> Response when
  Match        :: term(),
  Continuation :: continuation(),
  State        :: state(),
  Response     :: {[Match], Continuation} | '$end_of_table'.
select({_, _, Limit, _, _} = Continuation, {_, Type, _}) ->
  q(select, Continuation, q_fun(Type), Limit, []).

%% @doc
%% This operation behaves like `ets:select_count/2'.
%%
%% @see ets:select_count/2.
%% @end
-spec select_count(Tab, MatchSpec, State) -> NumMatched when
  Tab        :: atom(),
  MatchSpec  :: ets:match_spec(),
  State      :: state(),
  NumMatched :: non_neg_integer().
select_count(Tab, MatchSpec, State) ->
  Map = {fun ets:select_count/2, [MatchSpec]},
  Reduce = {fun(Res, Acc) -> Acc + Res end, 0},
  mapred(Tab, Map, Reduce, State).

%% @doc
%% This operation behaves like `ets:select_delete/2'.
%%
%% @see ets:select_delete/2.
%% @end
-spec select_delete(Tab, MatchSpec, State) -> NumDeleted when
  Tab        :: atom(),
  MatchSpec  :: ets:match_spec(),
  State      :: state(),
  NumDeleted :: non_neg_integer().
select_delete(Tab, MatchSpec, State) ->
  Map = {fun ets:select_delete/2, [MatchSpec]},
  Reduce = {fun(Res, Acc) -> Acc + Res end, 0},
  mapred(Tab, Map, Reduce, State).

%% @doc
%% This operation behaves similar to `ets:select_reverse/2'.
%% The order in which results are returned, might be not the same
%% as the original ETS function. Remember shards architecture
%% described at the beginning.
%%
%% @see ets:select_reverse/2.
%% @end
-spec select_reverse(Tab, MatchSpec, State) -> [Match] when
  Tab       :: atom(),
  MatchSpec :: ets:match_spec(),
  State     :: state(),
  Match     :: term().
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
  State        :: state(),
  Match        :: term(),
  Continuation :: continuation(),
  Response     :: {[Match], Continuation} | '$end_of_table'.
select_reverse(Tab, MatchSpec, Limit, {_, Type, PoolSize}) ->
  q(select_reverse,
    Tab, MatchSpec,
    Limit,
    q_fun(Type),
    Limit,
    PoolSize - 1,
    {[], nil}).

%% @doc
%% This operation behaves similar to `ets:select_reverse/1'.
%% The order in which results are returned, might be not the same
%% as the original ETS function. Remember shards architecture
%% described at the beginning.
%%
%% @see ets:select_reverse/1.
%% @end
-spec select_reverse(Continuation, State) -> Response when
  Match        :: term(),
  Continuation :: continuation(),
  State        :: state(),
  Response     :: {[Match], Continuation} | '$end_of_table'.
select_reverse({_, _, Limit, _, _} = Continuation, {_, Type, _}) ->
  q(select_reverse, Continuation, q_fun(Type), Limit, []).

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
  State    :: state().
setopts(Tab, Opts, State) ->
  Map = {fun shards_owner:setopts/2, [Opts]},
  Reduce = {fun(E, Acc) -> Acc and E end, true},
  mapred(Tab, Map, Reduce, State).

%% @doc
%% <p><font color="red"><b>NOT SUPPORTED!</b></font></p>
%% @end
slot(_Tab, _I, _State) ->
  throw(unsupported_operation).

%% @equiv tab2file(Tab, Filenames, [])
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
  State     :: state(),
  ShardTab  :: atom(),
  ShardRes  :: ok | {error, Reason :: term()},
  Response  :: [{ShardTab, ShardRes}].
tab2file(Tab, Filenames, Options, {_, _, PoolSize}) ->
  [begin
     ets:tab2file(Shard, Filename, Options)
   end || {Shard, Filename} <- lists:zip(list(Tab, PoolSize), Filenames)].

%% @doc
%% This operation behaves like `ets:tab2list/1'.
%%
%% @see ets:tab2list/1.
%% @end
-spec tab2list(Tab, State) -> [Object] when
  Tab    :: atom(),
  State  :: state(),
  Object :: tuple().
tab2list(Tab, State) ->
  mapred(Tab, fun ets:tab2list/1, fun lists:append/2, State).

%% @equiv ets:tabfile_info(Filename)
tabfile_info(Filename) ->
  ets:tabfile_info(Filename).

%% @equiv table(Tab, [])
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
  State          :: state(),
  MatchSpec      :: ets:match_spec(),
  TraverseMethod :: first_next | last_prev | select | {select, MatchSpec}.
table(Tab, Options, State) ->
  mapred(Tab, {fun ets:table/2, [Options]}, State).

%% @equiv ets:test_ms(Tuple, MatchSpec)
test_ms(Tuple, MatchSpec) ->
  ets:test_ms(Tuple, MatchSpec).

%% @doc
%% This operation behaves like `ets:take/2'.
%%
%% @see ets:take/2.
%% @end
-spec take(Tab, Key, State) -> [Object] when
  Tab    :: atom(),
  Key    :: term(),
  State  :: state(),
  Object :: tuple().
take(Tab, Key, State) ->
  mapred(Tab, Key, {fun ets:take/2, [Key]}, fun lists:append/2, State).

%% @doc
%% <p><font color="red"><b>NOT SUPPORTED!</b></font></p>
%% @end
to_dets(_Tab, _DetsTab, _State) ->
  throw(unsupported_operation).

%% @doc
%% This operation behaves like `ets:update_counter/3'.
%%
%% @see ets:update_counter/3.
%% @end
-spec update_counter(Tab, Key, UpdateOp, State) -> Result when
  Tab      :: atom(),
  Key      :: term(),
  UpdateOp :: term(),
  State    :: state(),
  Result   :: integer().
update_counter(Tab, Key, UpdateOp, State) ->
  mapred(Tab, Key, {fun ets:update_counter/3, [Key, UpdateOp]}, nil, State).

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
  State    :: state(),
  Result   :: integer().
update_counter(Tab, Key, UpdateOp, Default, State) ->
  Map = {fun ets:update_counter/4, [Key, UpdateOp, Default]},
  mapred(Tab, Key, Map, nil, State).

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
  State       :: state().
update_element(Tab, Key, ElementSpec, State) ->
  Map = {fun ets:update_element/3, [Key, ElementSpec]},
  mapred(Tab, Key, Map, nil, State).

%%%===================================================================
%%% Extended API
%%%===================================================================

%% @doc
%% Builds a shard name `ShardName'.
%% <ul>
%% <li>`TabName': Table name from which the shard name is generated.</li>
%% <li>`ShardNum': Shard number – from `0' to `(PoolSize - 1)'</li>
%% </ul>
%% @end
-spec shard_name(TabName, ShardNum) -> ShardName when
  TabName   :: atom(),
  ShardNum  :: non_neg_integer(),
  ShardName :: atom().
shard_name(TabName, Shard) ->
  shards_owner:shard_name(TabName, Shard).

%% @doc
%% Calculates the shard where the `Key' is handled.
%% <ul>
%% <li>`Key': The key to be hashed to calculate the shard.</li>
%% <li>`PoolSize': Number of shards.</li>
%% </ul>
%% @end
-spec shard(Key, PoolSize) -> ShardNum when
  Key      :: term(),
  PoolSize :: pos_integer(),
  ShardNum :: non_neg_integer().
shard(Key, PoolSize) ->
  erlang:phash2(Key, PoolSize).

%% @doc
%% Returns the list of shard names associated to the given `TabName'.
%% The shard names that were created in the `shards:new/2,3' fun.
%% <ul>
%% <li>`TabName': Table name.</li>
%% <li>`PoolSize': Number of shards.</li>
%% </ul>
%% @end
-spec list(TabName, PoolSize) -> ShardTabNames when
  TabName       :: atom(),
  PoolSize      :: pos_integer(),
  ShardTabNames :: [atom()].
list(TabName, PoolSize) ->
  Shards = lists:seq(0, PoolSize - 1),
  [shard_name(TabName, Shard) || Shard <- Shards].

%% @doc
%% Returns the local `shards' state.
%% <ul>
%% <li>`TabName': Table name.</li>
%% </ul>
%% @end
-spec state(TabName :: atom()) -> state().
state(TabName) ->
  ets:lookup_element(TabName, '$shards_state', 2).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
mapred(Tab, Map, State) ->
  mapred(Tab, Map, nil, State).

%% @private
mapred(Tab, Map, Reduce, State) ->
  mapred(Tab, nil, Map, Reduce, State).

%% @private
mapred(Tab, Key, Map, nil, State) ->
  mapred(Tab, Key, Map, fun(E, Acc) -> [E | Acc] end, State);
mapred(Tab, nil, Map, Reduce, {_, _, PoolSize}) ->
  p_mapred(Tab, PoolSize, Map, Reduce);
mapred(Tab, _, Map, Reduce, {_, Type, PoolSize}) when ?is_sharded(Type) ->
  s_mapred(Tab, PoolSize, Map, Reduce);
mapred(Tab, Key, {MapFun, Args}, _, {_, _, PoolSize}) ->
  ShardName = shard_name(Tab, shard(Key, PoolSize)),
  apply(MapFun, [ShardName | Args]).

%% @private
s_mapred(Tab, PoolSize, {MapFun, Args}, {ReduceFun, AccIn}) ->
  lists:foldl(fun(Shard, Acc) ->
    MapRes = apply(MapFun, [shard_name(Tab, Shard) | Args]),
    ReduceFun(MapRes, Acc)
  end, AccIn, lists:seq(0, PoolSize - 1));
s_mapred(Tab, PoolSize, MapFun, ReduceFun) ->
  {Map, Reduce} = mapred_funs(MapFun, ReduceFun),
  s_mapred(Tab, PoolSize, Map, Reduce).

%% @private
p_mapred(Tab, PoolSize, {MapFun, Args}, {ReduceFun, AccIn}) ->
  Tasks = lists:foldl(fun(Shard, Acc) ->
    AsyncTask = shards_task:async(fun() ->
      apply(MapFun, [shard_name(Tab, Shard) | Args])
    end), [AsyncTask | Acc]
  end, [], lists:seq(0, PoolSize - 1)),
  lists:foldl(fun(Task, Acc) ->
    MapRes = shards_task:await(Task),
    ReduceFun(MapRes, Acc)
  end, AccIn, Tasks);
p_mapred(Tab, PoolSize, MapFun, ReduceFun) ->
  {Map, Reduce} = mapred_funs(MapFun, ReduceFun),
  p_mapred(Tab, PoolSize, Map, Reduce).

%% @private
mapred_funs(MapFun, ReduceFun) ->
  Map = case is_function(MapFun) of
    true -> {MapFun, []};
    _    -> MapFun
  end,
  Reduce = case is_function(ReduceFun) of
   true -> {ReduceFun, []};
   _    -> ReduceFun
  end,
  {Map, Reduce}.

%% @private
fold(Tab, PoolSize, Fold, [Fun, Acc]) ->
  lists:foldl(fun(Shard, FoldAcc) ->
    ShardName = shard_name(Tab, Shard),
    apply(ets, Fold, [Fun, FoldAcc, ShardName])
  end, Acc, lists:seq(0, PoolSize - 1)).

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
q_fun(ordered_set) ->
  fun(L1, L0) -> lists:foldl(fun(E, Acc) -> [E | Acc] end, L0, L1) end;
q_fun(_) ->
  fun(L1, L0) -> L1 ++ L0 end.
