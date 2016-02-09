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
  delete/1, delete/2, delete/3,
  delete_object/2, delete_object/3,
  delete_all_objects/1, delete_all_objects/2,
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
  insert/2, insert/3,
  insert_new/2, insert_new/3,
  is_compiled_ms/1,
  last/1,
  lookup/2, lookup/3,
  lookup_element/3, lookup_element/4,
  match/2, match/3, match/1,
  match_delete/2,
  match_object/2, match_object/3, match_object/1,
  match_spec_compile/1,
  match_spec_run/2,
  member/2, member/3,
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
  update_counter/3, update_counter/4, update_counter/5,
  update_element/3, update_element/4
]).

%% Extended API
-export([
  shard_name/2,
  shard/2,
  pool_size/1,
  options/1,
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
stop() -> application:stop(ebus).

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
%% <b>IMPORTANT: This function makes an additional call to an ETS
%% table to fetch the pool size (used by `shards' internally).
%% If you want to skip this step you should use `shards:insert/3'
%% instead.</b>
%%
%% @see ets:delete/2.
%% @end
delete(Tab, Key) ->
  delete(Tab, Key, pool_size(Tab)).

%% @doc
%% Same as `shards:delete/2' but receives the `PoolSize' explicitly.
%% @end
delete(Tab, Key, PoolSize) ->
  call(Tab, Key, PoolSize, fun ets:delete/2, [Key]).

%% @doc
%% This operation behaves like `ets:delete_all_objects/1'.
%%
%% <b>IMPORTANT: This function makes an additional call to an ETS
%% table to fetch the pool size (used by `shards' internally).
%% If you want to skip this step you should use `shards:insert/3'
%% instead.</b>
%%
%% @see ets:delete_all_objects/1.
%% @end
delete_all_objects(Tab) ->
  delete_all_objects(Tab, pool_size(Tab)).

%% @doc
%% Same as `shards:delete_all_objects/1' but receives the `PoolSize'
%% explicitly.
%% @end
delete_all_objects(Tab, PoolSize) ->
  pmap(Tab, PoolSize, fun ets:delete_all_objects/1),
  true.

%% @doc
%% This operation behaves like `ets:delete_object/2'.
%%
%% <b>IMPORTANT: This function makes an additional call to an ETS
%% table to fetch the pool size (used by `shards' internally).
%% If you want to skip this step you should use `shards:insert/3'
%% instead.</b>
%%
%% @see ets:delete_object/2.
%% @end
delete_object(Tab, Object) ->
  delete_object(Tab, Object, pool_size(Tab)).

%% @doc
%% Same as `shards:delete_object/2' but receives the `PoolSize'
%% explicitly.
%% @end
delete_object(Tab, Object, PoolSize) when is_tuple(Object) ->
  [Key | _] = tuple_to_list(Object),
  call(Tab, Key, PoolSize, fun ets:delete_object/2, [Object]).

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
%% <b>IMPORTANT: This function makes an additional call to an ETS
%% table to fetch the pool size (used by `shards' internally).
%% If you want to skip this step you should use `shards:foldl/4'
%% instead.</b>
%%
%% @see ets:foldl/3.
%% @end
foldl(Function, Acc0, Tab) ->
  foldl(Function, Acc0, Tab, pool_size(Tab)).

%% @doc
%% Same as `shards:foldl/3' but receives the `PoolSize' explicitly.
%% @end
foldl(Function, Acc0, Tab, PoolSize) ->
  fold(Tab, PoolSize, foldl, [Function, Acc0]).

%% @doc
%% This operation behaves like `ets:foldr/3'.
%%
%% <b>IMPORTANT: This function makes an additional call to an ETS
%% table to fetch the pool size (used by `shards' internally).
%% If you want to skip this step you should use `shards:foldr/4'
%% instead.</b>
%%
%% @see ets:foldr/3.
%% @end
foldr(Function, Acc0, Tab) ->
  foldr(Function, Acc0, Tab, pool_size(Tab)).

%% @doc
%% Same as `shards:foldr/3' but receives the `PoolSize' explicitly.
%% @end
foldr(Function, Acc0, Tab, PoolSize) ->
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
%% <p><font color="red"><b>NOT SUPPORTED!</b></font></p>
%% @end
give_away(_Tab, _Pid, _GiftData) ->
  throw(unsupported_operation).

%% @doc
%% Equivalent to `ets:i/0'. You can also call `ets:i/0' directly
%% from your code, since `shards' here only works as a wrapper.
%%
%% @see ets:i/0.
%% @end
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
-spec info(Tab) -> Result when
  Tab      :: atom(),
  Result   :: [InfoList],
  InfoList :: [term() | undefined].
info(Tab) ->
  pmap(Tab, pool_size(Tab), fun ets:info/1).

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
info(Tab, Item) ->
  pmap(Tab, pool_size(Tab), fun ets:info/2, [Item]).

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
init_table(_Tab, _InitFun) ->
  throw(unsupported_operation).

%% @doc
%% This operation behaves like `ets:insert/2' BUT it is not atomic,
%% which means if it fails inserting some K/V pair, only that K/V
%% pair is affected, the rest may be successfully inserted.
%%
%% This function returns a list if the `ObjectOrObjects' is a list.
%%
%% <b>IMPORTANT: This function makes an additional call to an ETS
%% table to fetch the pool size (used by `shards' internally).
%% If you want to skip this step you should use `shards:insert/3'
%% instead.</b>
%%
%% @see ets:insert/2.
%% @end
insert(Tab, ObjectOrObjects) ->
  insert(Tab, ObjectOrObjects, pool_size(Tab)).

%% @doc
%% Same as `shards:insert/2' but receives the `PoolSize' explicitly.
%% @end
-spec insert(Tab, ObjectOrObjects, PoolSize) -> Result when
  Tab             :: atom(),
  ObjectOrObjects :: tuple() | [tuple()],
  PoolSize        :: pos_integer(),
  Result          :: true | [true].
insert(Tab, ObjectOrObjects, PoolSize) when is_list(ObjectOrObjects) ->
  lists:foldr(fun(Object, Acc) ->
    [insert(Tab, Object, PoolSize) | Acc]
  end, [], ObjectOrObjects);
insert(Tab, ObjectOrObjects, PoolSize) when is_tuple(ObjectOrObjects) ->
  [Key | _] = tuple_to_list(ObjectOrObjects),
  call(Tab, Key, PoolSize, fun ets:insert/2, [ObjectOrObjects]).

%% @doc
%% This operation behaves like `ets:insert_new/2' BUT it is not atomic,
%% which means if it fails inserting some K/V pair, only that K/V
%% pair is affected, the rest may be successfully inserted.
%%
%% This function returns a list if the `ObjectOrObjects' is a list.
%%
%% <b>IMPORTANT: This function makes an additional call to an ETS
%% table to fetch the pool size (used by `shards' internally).
%% If you want to skip this step you should use `shards:insert_new/3'
%% instead.</b>
%%
%% @see ets:insert_new/2.
%% @end
insert_new(Tab, ObjectOrObjects) ->
  insert_new(Tab, ObjectOrObjects, pool_size(Tab)).

%% @doc
%% Same as `shards:insert_new/2' but receives the `PoolSize' explicitly.
%% @end
-spec insert_new(Tab, ObjectOrObjects, PoolSize) -> Result when
  Tab             :: atom(),
  ObjectOrObjects :: tuple() | [tuple()],
  PoolSize        :: pos_integer(),
  Result          :: boolean() | [boolean()].
insert_new(Tab, ObjectOrObjects, PoolSize) when is_list(ObjectOrObjects) ->
  lists:foldr(fun(Object, Acc) ->
    [insert_new(Tab, Object, PoolSize) | Acc]
  end, [], ObjectOrObjects);
insert_new(Tab, ObjectOrObjects, PoolSize) when is_tuple(ObjectOrObjects) ->
  [Key | _] = tuple_to_list(ObjectOrObjects),
  call(Tab, Key, PoolSize, fun ets:insert_new/2, [ObjectOrObjects]).

%% @doc
%% Equivalent to `ets:is_compiled_ms/1'.
%%
%% @see ets:is_compiled_ms/1.
%% @end
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
last(Tab) ->
  Options = options(Tab),
  case lists:member(ordered_set, Options) of
    true ->
      last(Tab, ets:last(shard_name(Tab, 0)), 0, pool_size(Tab) - 1);
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
%% <b>IMPORTANT: This function makes an additional call to an ETS
%% table to fetch the pool size (used by `shards' internally).
%% If you want to skip this step you should use `shards:lookup/3'
%% instead.</b>
%%
%% @see ets:lookup/2.
%% @end
lookup(Tab, Key) ->
  lookup(Tab, Key, pool_size(Tab)).

%% @doc
%% Same as `shards:lookup/2' but receives the `PoolSize' explicitly.
%% @end
lookup(Tab, Key, PoolSize) ->
  call(Tab, Key, PoolSize, fun ets:lookup/2, [Key]).

%% @doc
%% This operation behaves like `ets:lookup_element/3'.
%%
%% <b>IMPORTANT: This function makes an additional call to an ETS
%% table to fetch the pool size (used by `shards' internally).
%% If you want to skip this step you should use
%% `shards:lookup_element/4' instead.</b>
%%
%% @see ets:lookup_element/3.
%% @end
lookup_element(Tab, Key, Pos) ->
  lookup_element(Tab, Key, Pos, pool_size(Tab)).

%% @doc
%% Same as `shards:lookup_element/3' but receives the `PoolSize'
%% explicitly.
%% @end
lookup_element(Tab, Key, Pos, PoolSize) ->
  call(Tab, Key, PoolSize, fun ets:lookup_element/3, [Key, Pos]).

%% @doc
%% This operation behaves similar to `ets:match/2'.
%% The order in which results are returned, might be not the same
%% as the original ETS function. Remember shards architecture
%% described at the beginning.
%%
%% @see ets:match/2.
%% @end
match(Tab, Pattern) ->
  lists:append(pmap(Tab, pool_size(Tab), fun ets:match/2, [Pattern])).

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
  Results = pmap(Tab, pool_size(Tab), fun ets:match_delete/2, [Pattern]),
  lists:foldl(fun(Res, Acc) -> Acc and Res end, true, Results).


%% @doc
%% This operation behaves similar to `ets:match_object/2'.
%% The order in which results are returned, might be not the same
%% as the original ETS function. Remember shards architecture
%% described at the beginning.
%%
%% @see ets:match_object/2.
%% @end
match_object(Tab, Pattern) ->
  lists:append(pmap(Tab, pool_size(Tab), fun ets:match_object/2, [Pattern])).

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
%% <b>IMPORTANT: This function makes an additional call to an ETS
%% table to fetch the pool size (used by `shards' internally).
%% If you want to skip this step you should use `shards:member/3'
%% instead.</b>
%%
%% @see ets:member/2.
%% @end
member(Tab, Key) ->
  member(Tab, Key, pool_size(Tab)).

%% @doc
%% Same as `shards:member/2' but receives the `PoolSize' explicitly.
%% @end
member(Tab, Key, PoolSize) ->
  call(Tab, Key, PoolSize, fun ets:member/2, [Key]).

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
%% <b>IMPORTANT: This function makes an additional call to an ETS
%% table to fetch the pool size (used by `shards' internally).
%% If you want to skip this step you should use
%% `shards:lookup_element/4' instead.</b>
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
  Options = options(Tab),
  case lists:member(ordered_set, Options) of
    true ->
      PoolSize = pool_size(Tab),
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
rename(_Tab, _Name) ->
  throw(unsupported_operation).

repair_continuation(_Continuation, _MatchSpec) ->
  %% @TODO: Implement this function.
  throw(unsupported_operation).

safe_fixtable(_Tab, _Fix) ->
  %% @TODO: Implement this function.
  throw(unsupported_operation).

%% @doc
%% This operation behaves similar to `ets:select/2'.
%% The order in which results are returned, might be not the same
%% as the original ETS function. Remember shards architecture
%% described at the beginning.
%%
%% @see ets:select/2.
%% @end
select(Tab, MatchSpec) ->
  lists:append(pmap(Tab, pool_size(Tab), fun ets:select/2, [MatchSpec])).

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
  Results = pmap(Tab, pool_size(Tab), fun ets:select_count/2, [MatchSpec]),
  lists:foldl(fun(Res, Acc) -> Acc + Res end, 0, Results).

%% @doc
%% This operation behaves like `ets:select_delete/2'.
%%
%% @see ets:select_delete/2.
%% @end
select_delete(Tab, MatchSpec) ->
  Results = pmap(Tab, pool_size(Tab), fun ets:select_delete/2, [MatchSpec]),
  lists:foldl(fun(Res, Acc) -> Acc + Res end, 0, Results).

%% @doc
%% This operation behaves similar to `ets:select_reverse/2'.
%% The order in which results are returned, might be not the same
%% as the original ETS function. Remember shards architecture
%% described at the beginning.
%%
%% @see ets:select_reverse/2.
%% @end
select_reverse(Tab, MatchSpec) ->
  lists:append(pmap(Tab, pool_size(Tab), fun ets:select_reverse/2, [MatchSpec])).

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

setopts(_Tab, _Opts) ->
  %% @TODO: Implement this function.
  throw(unsupported_operation).

%% @doc
%% <p><font color="red"><b>NOT SUPPORTED!</b></font></p>
%% @end
slot(_Tab, _I) ->
  throw(unsupported_operation).

%% @equiv tab2file(Tab, Filename, [])
tab2file(Tab, Filename) ->
  tab2file(Tab, Filename, []).

%% @doc
%% Similar to `shards:tab2file/3', but it returns a list of
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
  lists:append(pmap(Tab, pool_size(Tab), fun ets:tab2list/1)).

%% @doc
%% Equivalent to `ets:tabfile_info/1'.
%%
%% @see ets:tabfile_info/1.
%% @end
tabfile_info(Filename) ->
  ets:tabfile_info(Filename).

table(_Tab) ->
  %% @TODO: Implement this function.
  throw(unsupported_operation).

table(_Tab, _Options) ->
  %% @TODO: Implement this function.
  throw(unsupported_operation).

%% @doc
%% Equivalent to `ets:test_ms/2'.
%%
%% @see ets:test_ms/2.
%% @end
test_ms(Tuple, MatchSpec) ->
  ets:test_ms(Tuple, MatchSpec).

%% @doc
%% This operation behaves like `ets:take/2'.
%%
%% <b>IMPORTANT: This function makes an additional call to an ETS
%% table to fetch the pool size (used by `shards' internally).
%% If you want to skip this step you should use
%% `shards:take/3' instead.</b>
%%
%% @see ets:take/2.
%% @end
take(Tab, Key) ->
  take(Tab, Key, pool_size(Tab)).

%% @doc
%% Same as `shards:take/2' but receives the `PoolSize' explicitly.
%% @end
take(Tab, Key, PoolSize) ->
  call(Tab, Key, PoolSize, fun ets:take/2, [Key]).

%% @doc
%% <p><font color="red"><b>NOT SUPPORTED!</b></font></p>
%% @end
to_dets(_Tab, _DetsTab) ->
  throw(unsupported_operation).

%% @doc
%% This operation behaves like `ets:update_counter/3'.
%%
%% <b>IMPORTANT: This function makes an additional call to an ETS
%% table to fetch the pool size (used by `shards' internally).
%% If you want to skip this step you should use
%% `shards:update_counter/4' instead.</b>
%%
%% @see ets:update_counter/3.
%% @end
update_counter(Tab, Key, UpdateOp) ->
  update_counter(Tab, Key, UpdateOp, pool_size(Tab)).

%% @doc
%% This function can behaves in two ways:
%% <ul>
%% <li>If the 4th parameter is an integer, it's taken as the pool
%% size, and in this case it behaves like `shards:update_counter/3'
%% but receiving the `PoolSize' explicitly.</li>
%% <li>If the 4th parameter is a tuple, it behaves like
%% `ets:update_counter/4.'</li>
%% </ul>
%%
%% @see ets:update_counter/4.
%% @end
update_counter(Tab, Key, UpdateOp, PoolSize) when is_integer(PoolSize) ->
  call(Tab, Key, PoolSize, fun ets:update_counter/3, [Key, UpdateOp]);
update_counter(Tab, Key, UpdateOp, Default) when is_tuple(Default) ->
  update_counter(Tab, Key, UpdateOp, Default, pool_size(Tab)).

%% @doc
%% Same as `shards:update_counter/4' but receives the `PoolSize'
%% explicitly.
%% @end
update_counter(Tab, Key, UpdateOp, Default, PoolSize) ->
  call(Tab, Key, PoolSize, fun ets:update_counter/4, [Key, UpdateOp, Default]).

%% @doc
%% This operation behaves like `ets:update_element/3'.
%%
%% <b>IMPORTANT: This function makes an additional call to an ETS
%% table to fetch the pool size (used by `shards' internally).
%% If you want to skip this step you should use
%% `shards:update_element/4' instead.</b>
%%
%% @see ets:update_element/3.
%% @end
update_element(Tab, Key, ElementSpec) ->
  update_element(Tab, Key, ElementSpec, pool_size(Tab)).

%% @doc
%% Same as `shards:update_element/3' but receives the `PoolSize'
%% explicitly.
%% @end
update_element(Tab, Key, ElementSpec, PoolSize) ->
  call(Tab, Key, PoolSize, fun ets:update_element/3, [Key, ElementSpec]).

%%%===================================================================
%%% Extended API
%%%===================================================================

-spec shard_name(atom(), non_neg_integer()) -> atom().
shard_name(Tab, Shard) ->
  shards_owner:shard_name(Tab, Shard).

-spec shard(term(), pos_integer()) -> non_neg_integer().
shard(Key, PoolSize) ->
  erlang:phash2(Key) rem PoolSize.

-spec pool_size(atom()) -> pos_integer().
pool_size(Tab) ->
  [{_, PoolSize}] = ets:lookup(Tab, pool_size),
  PoolSize.

-spec options(atom()) -> [term()].
options(Tab) ->
  [{_, Options}] = ets:lookup(Tab, options),
  Options.

-spec list(atom()) -> [atom()].
list(Tab) ->
  [shard_name(Tab, Shard) || Shard <- lists:seq(0, pool_size(Tab) - 1)].

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
call(Tab, Key, PoolSize, Fun, Args) ->
  ShardName = shard_name(Tab, shard(Key, PoolSize)),
  apply(erlang, apply, [Fun, [ShardName | Args]]).

%% @private
pmap(Tab, PoolSize, Fun) ->
  pmap(Tab, PoolSize, Fun, []).

%% @private
pmap(Tab, PoolSize, Fun, Args) ->
  [shards_task:await(Task) || Task <- [shards_task:async(fun() ->
    ShardName = shard_name(Tab, Shard),
    apply(erlang, apply, [Fun, [ShardName | Args]])
  end) || Shard <- lists:seq(0, PoolSize - 1)]].

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
  case info_shard(Tab, 0, type) of
    ordered_set ->
      fun(L1, L0) -> lists:foldl(fun(E, Acc) -> [E | Acc] end, L0, L1) end;
    _ ->
      fun(L1, L0) -> L1 ++ L0 end
  end.
