%%%-------------------------------------------------------------------
%%% @doc
%%% This is the main module, which contains all API functions.
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
-module(shards).

-behaviour(application).

%% Application callbacks and functions
-export([
  start/0, stop/0,
  start/2, stop/1
]).

%% ETS API
-export([
  all/0,
  delete/1, delete/2,
  delete_object/2,
  delete_all_objects/1,
  file2tab/1, file2tab/2,
  first/1,
  foldl/3,
  foldr/3,
  from_dets/2,
  fun2ms/1,
  give_away/3,
  i/0, i/1,
  info/1, info/2,
  info_shard/2, info_shard/3,
  init_table/2,
  insert/2,
  insert_new/2,
  is_compiled_ms/1,
  last/1,
  lookup/2,
  lookup_element/3,
  match/2, match/3, match/1,
  match_delete/2,
  match_object/2, match_object/3, match_object/1,
  match_spec_compile/1,
  match_spec_run/2,
  member/2,
  new/2, new/3,
  next/2,
  prev/2,
  rename/2,
  repair_continuation/2,
  safe_fixtable/2,
  select/2, select/3, select/1,
  select_count/2,
  select_delete/2,
  select_reverse/2, select_reverse/3, select_reverse/1,
  setopts/2,
  slot/2,
  tab2file/2, tab2file/3,
  tab2list/1,
  tabfile_info/1,
  table/1, table/2,
  test_ms/2,
  take/2,
  to_dets/2,
  update_counter/3, update_counter/4,
  update_element/3
]).

%% Extended API
-export([
  shard_name/2,
  shard/2,
  control_info/1,
  module/1,
  type/1,
  pool_size/1,
  list/1
]).

%% Default pool size
-define(DEFAULT_POOL_SIZE, 2).

%%%===================================================================
%%% Types
%%%===================================================================

%% @type continuation() =
%% {
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

%%%===================================================================
%%% Application callbacks and functions
%%%===================================================================

%% @doc Starts `shards' application.
-spec start() -> {ok, _} | {error, term()}.
start() -> application:ensure_all_started(shards).

%% @doc Stops `shards' application.
-spec stop() -> ok | {error, term()}.
stop() -> application:stop(shards).

%% @hidden
start(_StartType, _StartArgs) -> shards_sup:start_link().

%% @hidden
stop(_State) -> ok.

%%%===================================================================
%%% ETS API
%%%===================================================================

%% @doc
%% This operation behaves like `ets:all/0'.
%%
%% @see ets:all/0.
%% @end
all() -> ets:all().

%% @doc
%% This operation behaves like `ets:delete/1'.
%%
%% @see ets:delete/1.
%% @end
delete(Tab) ->
  ok = shards_sup:terminate_child(shards_sup, whereis(Tab)),
  true.

%% @doc
%% This operation behaves like `ets:delete/2'.
%%
%% @see ets:delete/2.
%% @end
delete(Tab, Key) ->
  mapred(Tab, Key, {fun ets:delete/2, [Key]}, nil),
  true.

%% @doc
%% This operation behaves like `ets:delete_all_objects/1'.
%%
%% @see ets:delete_all_objects/1.
%% @end
delete_all_objects(Tab) ->
  mapred(Tab, fun ets:delete_all_objects/1),
  true.

%% @doc
%% This operation behaves like `ets:delete_object/2'.
%%
%% @see ets:delete_object/2.
%% @end
delete_object(Tab, Object) when is_tuple(Object) ->
  [Key | _] = tuple_to_list(Object),
  mapred(Tab, Key, {fun ets:delete_object/2, [Object]}, nil),
  true.

%% @equiv file2tab(Filenames, [])
file2tab(Filenames) -> file2tab(Filenames, []).

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
  Response  :: [{ok, Tab} | {error, Reason}].
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
    TabName = name_from_shard(First),
    _ = new(TabName, [{restore, ShardTabs, Options}], length(Filenames)),
    {ok, TabName}
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
first(Tab) ->
  Shard = pool_size(Tab) - 1,
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
foldl(Function, Acc0, Tab) ->
  fold(Tab, pool_size(Tab), foldl, [Function, Acc0]).

%% @doc
%% This operation behaves like `ets:foldr/3'.
%%
%% @see ets:foldr/3.
%% @end
foldr(Function, Acc0, Tab) ->
  fold(Tab, pool_size(Tab), foldr, [Function, Acc0]).

%% @doc
%% <p><font color="red"><b>NOT SUPPORTED!</b></font></p>
%% @end
from_dets(_Tab, _DetsTab) -> throw(unsupported_operation).

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
fun2ms(_LiteralFun) -> throw(unsupported_operation).

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
give_away(Tab, Pid, GiftData) ->
  Map = {fun shards_owner:give_away/3, [Pid, GiftData]},
  Reduce = {fun(E, Acc) -> Acc and E end, true},
  mapred(Tab, Map, Reduce).

%% @doc
%% Equivalent to `ets:i/0'. You can also call `ets:i/0' directly
%% from your code, since `shards' here only works as a wrapper.
%%
%% @see ets:i/0.
%% @end
i() -> ets:i().

%% @doc
%% <p><font color="red"><b>NOT SUPPORTED!</b></font></p>
%% @end
i(_Tab) -> throw(unsupported_operation).

%% @doc
%% This operation behaves like `ets:info/1', but instead of return
%% the information about one single table, it returns a list with
%% the information of each shard table.
%%
%% @see ets:info/1.
%% @end
-spec info(Tab) -> Result when
  Tab      :: atom(),
  Result   :: [InfoList],
  InfoList :: [term() | undefined].
info(Tab) -> mapred(Tab, fun ets:info/1).

%% @doc
%% This operation behaves like `ets:info/2', but instead of return
%% the information about one single table, it returns a list with
%% the information of each shard table.
%%
%% @see ets:info/2.
%% @end
-spec info(Tab, Item) -> Result when
  Tab      :: atom(),
  Item     :: atom(),
  Result   :: [Value],
  Value    :: [term() | undefined].
info(Tab, Item) -> mapred(Tab, {fun ets:info/2, [Item]}).

%% @doc
%% This operation behaves like `ets:info/1'
%%
%% @see ets:info/1.
%% @end
-spec info_shard(Tab, Shard) -> Result when
  Tab      :: atom(),
  Shard    :: non_neg_integer(),
  Result   :: [term()] | undefined.
info_shard(Tab, Shard) ->
  ShardName = shard_name(Tab, Shard),
  ets:info(ShardName).

%% @doc
%% This operation behaves like `ets:info/2'.
%%
%% @see ets:info/2.
%% @end
-spec info_shard(Tab, Shard, Item) -> Result when
  Tab      :: atom(),
  Shard    :: non_neg_integer(),
  Item     :: atom(),
  Result   :: term() | undefined.
info_shard(Tab, Shard, Item) ->
  ShardName = shard_name(Tab, Shard),
  ets:info(ShardName, Item).

%% @doc
%% <p><font color="red"><b>NOT SUPPORTED!</b></font></p>
%% @end
init_table(_Tab, _InitFun) -> throw(unsupported_operation).

%% @doc
%% This operation behaves similar to `ets:insert/2', with a big
%% difference, <b>it is not atomic</b>. This means if it fails
%% inserting some K/V pair, previous inserted KV pairs are not
%% rolled back.
%%
%% @see ets:insert/2.
%% @end
insert(Tab, ObjectOrObjects) when is_list(ObjectOrObjects) ->
  lists:foreach(fun(Object) ->
    true = insert(Tab, Object)
  end, ObjectOrObjects), true;
insert(Tab, ObjectOrObjects) when is_tuple(ObjectOrObjects) ->
  {_, Type, PoolSize} = control_info(Tab),
  [Key | _] = tuple_to_list(ObjectOrObjects),
  Shard = case Type =:= sharded_duplicate_bag orelse Type =:= sharded_bag of
    true -> shard_name(Tab, shard({Key, os:timestamp()}, PoolSize));
    _    -> shard_name(Tab, shard(Key, PoolSize))
  end,
  ets:insert(Shard, ObjectOrObjects).

%% @doc
%% This operation behaves like `ets:insert_new/2' BUT it is not atomic,
%% which means if it fails inserting some K/V pair, only that K/V
%% pair is affected, the rest may be successfully inserted.
%%
%% This function returns a list if the `ObjectOrObjects' is a list.
%%
%% @see ets:insert_new/2.
%% @end
-spec insert_new(Tab, ObjectOrObjects) -> Result when
  Tab             :: atom(),
  ObjectOrObjects :: tuple() | [tuple()],
  Result          :: boolean() | [boolean()].
insert_new(Tab, ObjectOrObjects) when is_list(ObjectOrObjects) ->
  lists:foldr(fun(Object, Acc) ->
    [insert_new(Tab, Object) | Acc]
  end, [], ObjectOrObjects);
insert_new(Tab, ObjectOrObjects) when is_tuple(ObjectOrObjects) ->
  {_, Type, PoolSize} = control_info(Tab),
  [Key | _] = tuple_to_list(ObjectOrObjects),
  case Type =:= sharded_duplicate_bag orelse Type =:= sharded_bag of
    true ->
      Map = {fun ets:lookup/2, [Key]},
      Reduce = fun lists:append/2,
      case mapred(Tab, PoolSize, Map, Reduce) of
        [] ->
          NewKey = {Key, os:timestamp()},
          ShardName = shard_name(Tab, shard(NewKey, PoolSize)),
          ets:insert_new(ShardName, ObjectOrObjects);
        _ ->
          false
      end;
    _ ->
      ShardName = shard_name(Tab, shard(Key, PoolSize)),
      ets:insert_new(ShardName, ObjectOrObjects)
  end.

%% @doc
%% Equivalent to `ets:is_compiled_ms/1'.
%%
%% @see ets:is_compiled_ms/1.
%% @end
is_compiled_ms(Term) -> ets:is_compiled_ms(Term).

%% @doc
%% This operation behaves similar to `ets:last/1'.
%% The order in which results are returned, might be not the same
%% as the original ETS function. Remember shards architecture
%% described at the beginning.
%%
%% @see ets:last/1.
%% @end
last(Tab) ->
  {_, Type, PoolSize} = control_info(Tab),
  case Type of
    ordered_set ->
      last(Tab, ets:last(shard_name(Tab, 0)), 0, PoolSize - 1);
    _ ->
      first(Tab)
  end.

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
lookup(Tab, Key) ->
  mapred(Tab, Key, {fun ets:lookup/2, [Key]}, fun lists:append/2).

%% @doc
%% This operation behaves like `ets:lookup_element/3'.
%%
%% @see ets:lookup_element/3.
%% @end
lookup_element(Tab, Key, Pos) ->
  {_, Type, PoolSize} = control_info(Tab),
  case Type =:= sharded_duplicate_bag orelse Type =:= sharded_bag of
    true ->
      LookupElem = fun(Tx, Kx, Px) ->
        catch(ets:lookup_element(Tx, Kx, Px))
      end,
      Filter = lists:filter(fun
        ({'EXIT', _}) -> false;
        (_)           -> true
      end, mapred(Tab, {LookupElem, [Key, Pos]})),
      case Filter of
        [] -> exit({badarg, erlang:get_stacktrace()});
        _  -> lists:append(Filter)
      end;
    _ ->
      ShardName = shard_name(Tab, shard(Key, PoolSize)),
      ets:lookup_element(ShardName, Key, Pos)
  end.

%% @doc
%% This operation behaves similar to `ets:match/2'.
%% The order in which results are returned, might be not the same
%% as the original ETS function. Remember shards architecture
%% described at the beginning.
%%
%% @see ets:match/2.
%% @end
match(Tab, Pattern) ->
  Map = {fun ets:match/2, [Pattern]},
  Reduce = fun lists:append/2,
  mapred(Tab, Map, Reduce).

%% @doc
%% This operation behaves similar to `ets:match/3'.
%% The order in which results are returned, might be not the same
%% as the original ETS function. Remember shards architecture
%% described at the beginning.
%%
%% @see ets:match/3.
%% @end
-spec match(Tab, Pattern, Limit) -> Response when
  Tab          :: atom(),
  Pattern      :: ets:match_pattern(),
  Limit        :: pos_integer(),
  Match        :: term(),
  Continuation :: continuation(),
  Response     :: {[Match], Continuation} | '$end_of_table'.
match(Tab, Pattern, Limit) ->
  q(match,
    Tab,
    Pattern,
    Limit,
    q_fun(Tab),
    Limit,
    pool_size(Tab) - 1,
    {[], nil}).

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
match({Tab, _, Limit, _, _} = Continuation) ->
  q(match, Continuation, q_fun(Tab), Limit, []).

%% @doc
%% This operation behaves like `ets:match_delete/2'.
%%
%% @see ets:match_delete/2.
%% @end
match_delete(Tab, Pattern) ->
  Map = {fun ets:match_delete/2, [Pattern]},
  Reduce = {fun(Res, Acc) -> Acc and Res end, true},
  mapred(Tab, Map, Reduce).


%% @doc
%% This operation behaves similar to `ets:match_object/2'.
%% The order in which results are returned, might be not the same
%% as the original ETS function. Remember shards architecture
%% described at the beginning.
%%
%% @see ets:match_object/2.
%% @end
match_object(Tab, Pattern) ->
  Map = {fun ets:match_object/2, [Pattern]},
  Reduce = fun lists:append/2,
  mapred(Tab, Map, Reduce).

%% @doc
%% This operation behaves similar to `ets:match_object/3'.
%% The order in which results are returned, might be not the same
%% as the original ETS function. Remember shards architecture
%% described at the beginning.
%%
%% @see ets:match_object/3.
%% @end
-spec match_object(Tab, Pattern, Limit) -> Response when
  Tab          :: atom(),
  Pattern      :: ets:match_pattern(),
  Limit        :: pos_integer(),
  Match        :: term(),
  Continuation :: continuation(),
  Response     :: {[Match], Continuation} | '$end_of_table'.
match_object(Tab, Pattern, Limit) ->
  q(match_object,
    Tab,
    Pattern,
    Limit,
    q_fun(Tab),
    Limit,
    pool_size(Tab) - 1,
    {[], nil}).

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
match_object({Tab, _, Limit, _, _} = Continuation) ->
  q(match_object, Continuation, q_fun(Tab), Limit, []).

%% @doc
%% Equivalent to `ets:match_spec_compile/1'.
%%
%% @see ets:match_spec_compile/1.
%% @end
match_spec_compile(MatchSpec) ->
  ets:match_spec_compile(MatchSpec).

%% @doc
%% Equivalent to `ets:match_spec_run/2'.
%%
%% @see ets:match_spec_run/2.
%% @end
match_spec_run(List, CompiledMatchSpec) ->
  ets:match_spec_run(List, CompiledMatchSpec).

%% @doc
%% This operation behaves like `ets:member/2'.
%%
%% @see ets:member/2.
%% @end
member(Tab, Key) ->
  case mapred(Tab, Key, {fun ets:member/2, [Key]}, nil) of
    R when is_list(R) -> lists:member(true, R);
    R                 -> R
  end.

%% @doc
%% This operation is the mirror of `ets:new/2', BUT it behaves totally
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
%% <b>IMPORTANT: By default, `PoolSize = 2'. If you want yo set the
%% `PoolSize' explicitly, please us `shards:new/3' instead.</b>
%%
%% @see ets:new/2.
%% @end
new(Name, Options) ->
  new(Name, Options, ?DEFAULT_POOL_SIZE).

%% @doc
%% Same as `shards:new/2' but receives the `PoolSize' explicitly.
%% @end
new(Name, Options, PoolSize) ->
  case shards_sup:start_child([Name, Options, PoolSize]) of
    {ok, _} -> Name;
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
next(Tab, Key1) ->
  Shard = shard(Key1, pool_size(Tab)),
  ShardName = shard_name(Tab, Shard),
  next(Tab, ets:next(ShardName, Key1), Shard).

%% @private
next(Tab, '$end_of_table', Shard) when Shard > 0 ->
  NextShard = Shard - 1,
  next(Tab, ets:first(shard_name(Tab, NextShard)), NextShard);
next(_, '$end_of_table', _) ->
  '$end_of_table';
next(_, Key2, _) ->
  Key2.

%% @doc
%% This operation behaves similar to `ets:prev/2'.
%% The order in which results are returned, might be not the same
%% as the original ETS function. Remember shards architecture
%% described at the beginning.
%%
%% @see ets:prev/2.
%% @end
prev(Tab, Key1) ->
  {_, Type, PoolSize} = control_info(Tab),
  case Type of
    ordered_set ->
      Shard = shard(Key1, PoolSize),
      ShardName = shard_name(Tab, Shard),
      prev(Tab, ets:prev(ShardName, Key1), Shard, PoolSize - 1);
    _ ->
      next(Tab, Key1)
  end.

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
rename(_Tab, _Name) -> throw(unsupported_operation).

repair_continuation(_Continuation, _MatchSpec) -> throw(unsupported_operation).

safe_fixtable(_Tab, _Fix) -> throw(unsupported_operation).

%% @doc
%% This operation behaves similar to `ets:select/2'.
%% The order in which results are returned, might be not the same
%% as the original ETS function. Remember shards architecture
%% described at the beginning.
%%
%% @see ets:select/2.
%% @end
select(Tab, MatchSpec) ->
  Map = {fun ets:select/2, [MatchSpec]},
  Reduce = fun lists:append/2,
  mapred(Tab, Map, Reduce).

%% @doc
%% This operation behaves similar to `ets:select/3'.
%% The order in which results are returned, might be not the same
%% as the original ETS function. Remember shards architecture
%% described at the beginning.
%%
%% @see ets:select/3.
%% @end
-spec select(Tab, MatchSpec, Limit) -> Response when
  Tab          :: atom(),
  MatchSpec    :: ets:match_spec(),
  Limit        :: pos_integer(),
  Match        :: term(),
  Continuation :: continuation(),
  Response     :: {[Match], Continuation} | '$end_of_table'.
select(Tab, MatchSpec, Limit) ->
  q(select,
    Tab,
    MatchSpec,
    Limit,
    q_fun(Tab),
    Limit,
    pool_size(Tab) - 1,
    {[], nil}).

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
select({Tab, _, Limit, _, _} = Continuation) ->
  q(select, Continuation, q_fun(Tab), Limit, []).

%% @doc
%% This operation behaves like `ets:select_count/2'.
%%
%% @see ets:select_count/2.
%% @end
select_count(Tab, MatchSpec) ->
  Map = {fun ets:select_count/2, [MatchSpec]},
  Reduce = {fun(Res, Acc) -> Acc + Res end, 0},
  mapred(Tab, Map, Reduce).

%% @doc
%% This operation behaves like `ets:select_delete/2'.
%%
%% @see ets:select_delete/2.
%% @end
select_delete(Tab, MatchSpec) ->
  Map = {fun ets:select_delete/2, [MatchSpec]},
  Reduce = {fun(Res, Acc) -> Acc + Res end, 0},
  mapred(Tab, Map, Reduce).

%% @doc
%% This operation behaves similar to `ets:select_reverse/2'.
%% The order in which results are returned, might be not the same
%% as the original ETS function. Remember shards architecture
%% described at the beginning.
%%
%% @see ets:select_reverse/2.
%% @end
select_reverse(Tab, MatchSpec) ->
  Map = {fun ets:select_reverse/2, [MatchSpec]},
  Reduce = fun lists:append/2,
  mapred(Tab, Map, Reduce).

%% @doc
%% This operation behaves similar to `ets:select_reverse/3'.
%% The order in which results are returned, might be not the same
%% as the original ETS function. Remember shards architecture
%% described at the beginning.
%%
%% @see ets:select_reverse/3.
%% @end
-spec select_reverse(Tab, MatchSpec, Limit) -> Response when
  Tab          :: atom(),
  MatchSpec    :: ets:match_spec(),
  Limit        :: pos_integer(),
  Match        :: term(),
  Continuation :: continuation(),
  Response     :: {[Match], Continuation} | '$end_of_table'.
select_reverse(Tab, MatchSpec, Limit) ->
  q(select_reverse,
    Tab, MatchSpec,
    Limit,
    q_fun(Tab),
    Limit,
    pool_size(Tab) - 1,
    {[], nil}).

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
select_reverse({Tab, _, Limit, _, _} = Continuation) ->
  q(select_reverse, Continuation, q_fun(Tab), Limit, []).

%% @doc
%% Equivalent to `ets:setopts/2' for each shard table. It returns
%% a `boolean()' instead that just `true'. Returns `true' if the
%% function was applied successfully on each shard, otherwise
%% `false' is returned.
%%
%% @see ets:setopts/2.
%% @end
-spec setopts(Tab, Opts) -> boolean() when
  Tab      :: atom(),
  Opts     :: Opt | [Opt],
  Opt      :: {heir, pid(), HeirData} | {heir, none},
  HeirData :: term().
setopts(Tab, Opts) ->
  Map = {fun shards_owner:setopts/2, [Opts]},
  Reduce = {fun(E, Acc) -> Acc and E end, true},
  mapred(Tab, Map, Reduce).

%% @doc
%% <p><font color="red"><b>NOT SUPPORTED!</b></font></p>
%% @end
slot(_Tab, _I) -> throw(unsupported_operation).

%% @equiv tab2file(Tab, Filename, [])
tab2file(Tab, Filename) -> tab2file(Tab, Filename, []).

%% @doc
%% Similar to `ets:tab2file/3', but it returns a list of
%% responses for each shard table instead.
%%
%% @see ets:tab2file/3.
%% @end
-spec tab2file(Tab, Filenames, Options) -> Response when
  Tab       :: atom(),
  Filenames :: [file:name()],
  Options   :: [Option],
  Option    :: {extended_info, [ExtInfo]} | {sync, boolean()},
  ExtInfo   :: md5sum | object_count,
  ShardTab  :: atom(),
  ShardRes  :: ok | {error, Reason},
  Reason    :: term(),
  Response  :: [{ShardTab, ShardRes}].
tab2file(Tab, Filenames, Options) ->
  [begin
     ets:tab2file(Shard, Filename, Options)
   end || {Shard, Filename} <- lists:zip(list(Tab), Filenames)].

%% @doc
%% This operation behaves like `ets:tab2list/1'.
%%
%% @see ets:tab2list/1.
%% @end
tab2list(Tab) ->
  mapred(Tab, fun ets:tab2list/1, fun lists:append/2).

%% @doc
%% Equivalent to `ets:tabfile_info/1'.
%%
%% @see ets:tabfile_info/1.
%% @end
tabfile_info(Filename) -> ets:tabfile_info(Filename).

%% @equiv table(Tab, [])
table(Tab) -> table(Tab, []).

%% @doc
%% Similar to `ets:table/2', but it returns a list of `ets:table/2'
%% responses, one for each shard table.
%%
%% @see ets:table/2.
%% @end
-spec table(Tab, Options) -> [QueryHandle] when
  Tab            :: atom(),
  QueryHandle    :: qlc:query_handle(),
  Options        :: [Option] | Option,
  Option         :: {n_objects, NObjects} | {traverse, TraverseMethod},
  NObjects       :: default | pos_integer(),
  MatchSpec      :: ets:match_spec(),
  TraverseMethod :: first_next | last_prev | select | {select, MatchSpec}.
table(Tab, Options) ->
  mapred(Tab, {fun ets:table/2, [Options]}).

%% @doc
%% Equivalent to `ets:test_ms/2'.
%%
%% @see ets:test_ms/2.
%% @end
test_ms(Tuple, MatchSpec) -> ets:test_ms(Tuple, MatchSpec).

%% @doc
%% This operation behaves like `ets:take/2'.
%%
%% @see ets:take/2.
%% @end
take(Tab, Key) ->
  mapred(Tab, Key, {fun ets:take/2, [Key]}, fun lists:append/2).

%% @doc
%% <p><font color="red"><b>NOT SUPPORTED!</b></font></p>
%% @end
to_dets(_Tab, _DetsTab) -> throw(unsupported_operation).

%% @doc
%% This operation behaves like `ets:update_counter/3'.
%%
%% @see ets:update_counter/3.
%% @end
update_counter(Tab, Key, UpdateOp) ->
  mapred(Tab, Key, {fun ets:update_counter/3, [Key, UpdateOp]}, nil).

%% @doc
%% This operation behaves like `ets:update_counter/4'.
%%
%% @see ets:update_counter/4.
%% @end
update_counter(Tab, Key, UpdateOp, Default) ->
  Map = {fun ets:update_counter/4, [Key, UpdateOp, Default]},
  mapred(Tab, Key, Map, nil).

%% @doc
%% This operation behaves like `ets:update_element/3'.
%%
%% @see ets:update_element/3.
%% @end
update_element(Tab, Key, ElementSpec) ->
  Map = {fun ets:update_element/3, [Key, ElementSpec]},
  mapred(Tab, Key, Map, nil).

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
shard_name(TabName, Shard) -> shards_owner:shard_name(TabName, Shard).

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
shard(Key, PoolSize) -> erlang:phash2(Key, PoolSize).

%% @doc
%% Returns the stored control information..
%% <ul>
%% <li>`TabName': Table name.</li>
%% </ul>
%% @end
-spec control_info(TabName) -> Response when
  TabName  :: atom(),
  Type     :: atom(),
  PoolSize :: pos_integer(),
  Response :: {Type, PoolSize}.
control_info(TabName) ->
  [{_, CtrlInfo}] = ets:lookup(TabName, '$control'),
  CtrlInfo.

%% @doc
%% Returns the module to be used (`shards' | `ets').
%% <ul>
%% <li>`TabName': Table name.</li>
%% </ul>
%% @end
-spec module(TabName) -> Module when
  TabName :: atom(),
  Module  :: module().
module(TabName) ->
  {Module, _, _} = control_info(TabName),
  Module.

%% @doc
%% Returns the table type.
%% <ul>
%% <li>`TabName': Table name.</li>
%% </ul>
%% @end
-spec type(TabName) -> Type when
  TabName :: atom(),
  Type    :: atom().
type(TabName) ->
  {_, Type, _} = control_info(TabName),
  Type.

%% @doc
%% Returns the pool size or number of shards.
%% <ul>
%% <li>`TabName': Table name.</li>
%% </ul>
%% @end
-spec pool_size(TabName) -> PoolSize when
  TabName  :: atom(),
  PoolSize :: pos_integer().
pool_size(TabName) ->
  {_, _, PoolSize} = control_info(TabName),
  PoolSize.

%% @doc
%% Returns the list of shard names associated to the given `TabName'.
%% The shard names that were created in the `shards:new/2,3' fun.
%% <ul>
%% <li>`TabName': Table name.</li>
%% </ul>
%% @end
-spec list(TabName) -> ShardTabNames when
  TabName       :: atom(),
  ShardTabNames :: [atom()].
list(TabName) ->
  Shards = lists:seq(0, pool_size(TabName) - 1),
  [shard_name(TabName, Shard) || Shard <- Shards].

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
mapred(Tab, Map) ->
  mapred(Tab, Map, nil).

%% @private
mapred(Tab, Map, Reduce) ->
  mapred(Tab, nil, control_info(Tab), Map, Reduce).

%% @private
mapred(Tab, Key, Map, Reduce) ->
  mapred(Tab, Key, control_info(Tab), Map, Reduce).

%% @private
mapred(Tab, Key, Ctrl, Map, nil) ->
  mapred(Tab, Key, Ctrl, Map, fun(E, Acc) -> [E | Acc] end);
mapred(Tab, nil, {_, _, PoolSize}, Map, Reduce) ->
  p_mapred(Tab, PoolSize, Map, Reduce);
mapred(Tab, _, {_, Type, PoolSize}, Map, Reduce)
    when Type =:= sharded_duplicate_bag; Type =:= sharded_bag ->
  s_mapred(Tab, PoolSize, Map, Reduce);
mapred(Tab, Key, {_, _, PoolSize}, {MapFun, Args}, _) ->
  ShardName = shard_name(Tab, shard(Key, PoolSize)),
  apply(erlang, apply, [MapFun, [ShardName | Args]]).

%% @private
s_mapred(Tab, PoolSize, {MapFun, Args}, {ReduceFun, AccIn}) ->
  lists:foldl(fun(Shard, Acc) ->
    MapRes = apply(erlang, apply, [MapFun, [shard_name(Tab, Shard) | Args]]),
    ReduceFun(MapRes, Acc)
  end, AccIn, lists:seq(0, PoolSize - 1));
s_mapred(Tab, PoolSize, MapFun, ReduceFun) ->
  {Map, Reduce} = mapred_funs(MapFun, ReduceFun),
  s_mapred(Tab, PoolSize, Map, Reduce).

%% @private
p_mapred(Tab, PoolSize, {MapFun, Args}, {ReduceFun, AccIn}) ->
  Tasks = lists:foldl(fun(Shard, Acc) ->
    AsyncTask = shards_task:async(fun() ->
      apply(erlang, apply, [MapFun, [shard_name(Tab, Shard) | Args]])
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
q_fun(Tab) ->
  case type(Tab) of
    ordered_set ->
      fun(L1, L0) -> lists:foldl(fun(E, Acc) -> [E | Acc] end, L0, L1) end;
    _ ->
      fun(L1, L0) -> L1 ++ L0 end
  end.
