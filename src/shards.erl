%%%-------------------------------------------------------------------
%%% @doc
%%% This is the main module, which contains all API functions.
%%%
%%% <b>Shards</b> is compatible with ETS API, most of the functions
%%% preserves the same ETS semantics, with som exception which you
%%% will find on the function doc.
%%%
%%% For some functions
%%% Therefore, the call to the ETS control table, held by the shards
%%% supervisor, is skipped.
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
  shard/3,
  pool_size/1,
  list/1
]).

%% Default pool size
-define(DEFAULT_POOL_SIZE, 2).

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
start(_StartType, _StartArgs) -> shards_pool_sup:start_link().

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
  ok = shards_pool_sup:terminate_child(shards_pool_sup, whereis(Tab)),
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

file2tab(_Filename) ->
  %% @TODO: Implement this function.
  throw(unsupported_operation).

file2tab(_Filename, _Options) ->
  %% @TODO: Implement this function.
  throw(unsupported_operation).

first(_Tab) ->
  %% @TODO: Implement this function.
  throw(unsupported_operation).

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

from_dets(_Tab, _DetsTab) ->
  %% @TODO: Implement this function.
  throw(unsupported_operation).

%% @doc
%% <p><font color="red"><b>WARNING:</b> Please use `ets:fun2ms/1'
%% instead</font></p>
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

give_away(_Tab, _Pid, _GiftData) ->
  %% @TODO: Implement this function.
  throw(unsupported_operation).

%% @doc
%% Equivalent to `ets:i/0'. You can also call `ets:i/0' directly
%% from your code, since `shards' here only works as a wrapper.
%%
%% @see ets:i/0.
%% @end
i() -> ets:i().

i(_Tab) ->
  %% @TODO: Implement this function.
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
  InfoList :: [term()].
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
  Value    :: term().
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
  Result   :: [term()].
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
  Result   :: [term()].
info_shard(Tab, Shard, Item) ->
  ShardName = shard_name(Tab, Shard),
  ets:info(ShardName, Item).

init_table(_Tab, _InitFun) ->
  %% @TODO: Implement this function.
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

is_compiled_ms(_Term) ->
  %% @TODO: Implement this function.
  throw(unsupported_operation).

last(_Tab) ->
  %% @TODO: Implement this function.
  throw(unsupported_operation).

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
%% This operation behaves like `ets:match/2'.
%%
%% The `PoolSize' is obtained internally by `shards'.
%%
%% @see ets:match/2.
%% @end
match(Tab, Pattern) ->
  lists:append(pmap(Tab, pool_size(Tab), fun ets:match/2, [Pattern])).

match(_Tab, _Pattern, _Limit) ->
  %% @TODO: Implement this function.
  throw(unsupported_operation).

match(_Continuation) ->
  %% @TODO: Implement this function.
  throw(unsupported_operation).

%% @doc
%% This operation behaves like `ets:match_delete/2'.
%%
%% The `PoolSize' is obtained internally by `shards'.
%%
%% @see ets:match_delete/2.
%% @end
match_delete(Tab, Pattern) ->
  Results = pmap(Tab, pool_size(Tab), fun ets:match_delete/2, [Pattern]),
  lists:foldl(fun(Res, Acc) -> Acc and Res end, true, Results).


%% @doc
%% This operation behaves like `ets:match_object/2'.
%%
%% The `PoolSize' is obtained internally by `shards'.
%%
%% @see ets:match_object/2.
%% @end
match_object(Tab, Pattern) ->
  lists:append(pmap(Tab, pool_size(Tab), fun ets:match_object/2, [Pattern])).

match_object(_Tab, _Pattern, _Limit) ->
  %% @TODO: Implement this function.
  throw(unsupported_operation).

match_object(_Continuation) ->
  %% @TODO: Implement this function.
  throw(unsupported_operation).

match_spec_compile(_MatchSpec) ->
  %% @TODO: Implement this function.
  throw(unsupported_operation).

match_spec_run(_List, _CompiledMatchSpec) ->
  %% @TODO: Implement this function.
  throw(unsupported_operation).

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
%% table, a new supervision tree is created and added to
%% `shards_pool_sup'.
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
  case shards_pool_sup:start_child([Name, Options, PoolSize]) of
    {ok, _} -> Name;
    _       -> throw(badarg)
  end.

next(_Tab, _Key1) ->
  %% @TODO: Implement this function.
  throw(unsupported_operation).

prev(_Tab, _Key1) ->
  %% @TODO: Implement this function.
  throw(unsupported_operation).

rename(_Tab, _Name) ->
  %% @TODO: Implement this function.
  throw(unsupported_operation).

repair_continuation(_Continuation, _MatchSpec) ->
  %% @TODO: Implement this function.
  throw(unsupported_operation).

safe_fixtable(_Tab, _Fix) ->
  %% @TODO: Implement this function.
  throw(unsupported_operation).

%% @doc
%% This operation behaves like `ets:select/2'.
%%
%% The `PoolSize' is obtained internally by `shards'.
%%
%% @see ets:select/2.
%% @end
select(Tab, MatchSpec) ->
  %% @TODO: Enhancement: Validate behavior when table type is an ordered_set
  lists:append(pmap(Tab, pool_size(Tab), fun ets:select/2, [MatchSpec])).

select(_Tab, _MatchSpec, _Limit) ->
  %% @TODO: Implement this function.
  throw(unsupported_operation).

select(_Continuation) ->
  %% @TODO: Implement this function.
  throw(unsupported_operation).

%% @doc
%% This operation behaves like `ets:select_count/2'.
%%
%% The `PoolSize' is obtained internally by `shards'.
%%
%% @see ets:select_count/2.
%% @end
select_count(Tab, MatchSpec) ->
  Results = pmap(Tab, pool_size(Tab), fun ets:select_count/2, [MatchSpec]),
  lists:foldl(fun(Res, Acc) -> Acc + Res end, 0, Results).

%% @doc
%% This operation behaves like `ets:select_delete/2'.
%%
%% The `PoolSize' is obtained internally by `shards'.
%%
%% @see ets:select_delete/2.
%% @end
select_delete(Tab, MatchSpec) ->
  Results = pmap(Tab, pool_size(Tab), fun ets:select_delete/2, [MatchSpec]),
  lists:foldl(fun(Res, Acc) -> Acc + Res end, 0, Results).

%% @doc
%% This operation behaves like `ets:select_reverse/2'.
%%
%% The `PoolSize' is obtained internally by `shards'.
%%
%% @see ets:select_reverse/2.
%% @end
select_reverse(Tab, MatchSpec) ->
  %% @TODO: Enhancement: Validate behavior when table type is an ordered_set
  lists:append(pmap(Tab, pool_size(Tab), fun ets:select_reverse/2, [MatchSpec])).

select_reverse(_Tab, _MatchSpec, _Limit) ->
  %% @TODO: Implement this function.
  throw(unsupported_operation).

select_reverse(_Continuation) ->
  %% @TODO: Implement this function.
  throw(unsupported_operation).

setopts(_Tab, _Opts) ->
  %% @TODO: Implement this function.
  throw(unsupported_operation).

slot(_Tab, _I) ->
  %% @TODO: Implement this function.
  throw(unsupported_operation).

tab2file(_Tab, _Filename) ->
  %% @TODO: Implement this function.
  throw(unsupported_operation).

tab2file(_Tab, _Filename, _Options) ->
  %% @TODO: Implement this function.
  throw(unsupported_operation).

tab2list(_Tab) ->
  %% @TODO: Implement this function.
  throw(unsupported_operation).

tabfile_info(_Filename) ->
  %% @TODO: Implement this function.
  throw(unsupported_operation).

table(_Tab) ->
  %% @TODO: Implement this function.
  throw(unsupported_operation).

table(_Tab, _Options) ->
  %% @TODO: Implement this function.
  throw(unsupported_operation).

test_ms(_Tuple, _MatchSpec) ->
  %% @TODO: Implement this function.
  throw(unsupported_operation).

take(_Tab, _Key) ->
  %% @TODO: Implement this function.
  throw(unsupported_operation).

to_dets(_Tab, _DetsTab) ->
  %% @TODO: Implement this function.
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

-spec shard(atom(), term(), pos_integer()) -> atom().
shard(Tab, Key, PoolSize) ->
  Shard = erlang:phash2(Key) rem PoolSize,
  shard_name(Tab, Shard).

-spec pool_size(atom()) -> pos_integer().
pool_size(Tab) ->
  [{_, PoolSize}] = ets:lookup(Tab, pool_size),
  PoolSize.

-spec list(atom()) -> [atom()].
list(Tab) ->
  [shard_name(Tab, Shard) || Shard <- lists:seq(0, pool_size(Tab) - 1)].

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
call(Tab, Key, PoolSize, Fun, Args) ->
  Shard = shard(Tab, Key, PoolSize),
  apply(erlang, apply, [Fun, [Shard | Args]]).

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
