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
  init_table/2,
  insert/2, insert/3,
  insert_new/2,
  is_compiled_ms/1,
  last/1,
  lookup/2, lookup/3,
  lookup_element/3, lookup_element/4,
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

%% @see ets:all().
all() -> ets:all().

delete(Tab) ->
  ok = shards_pool_sup:terminate_child(shards_pool_sup, whereis(Tab)),
  true.

%% @see ets:delete/2.
delete(Tab, Key) ->
  delete(Tab, Key, pool_size(Tab)).

delete(Tab, Key, PoolSize) ->
  call(Tab, Key, PoolSize, fun ets:delete/2, [Key]).

delete_all_objects(Tab) ->
  delete_all_objects(Tab, pool_size(Tab)).

delete_all_objects(Tab, PoolSize) ->
  pmap(Tab, PoolSize, fun ets:delete_all_objects/1),
  true.

%% @see ets:delete_object/2.
delete_object(Tab, Object) ->
  delete_object(Tab, Object, pool_size(Tab)).

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

foldl(_Function, _Acc0, _Tab) ->
  %% @TODO: Implement this function.
  throw(unsupported_operation).

foldr(_Function, _Acc0, _Tab) ->
  %% @TODO: Implement this function.
  throw(unsupported_operation).

from_dets(_Tab, _DetsTab) ->
  %% @TODO: Implement this function.
  throw(unsupported_operation).

fun2ms(_LiteralFun) ->
  %% @TODO: Implement this function.
  throw(unsupported_operation).

give_away(_Tab, _Pid, _GiftData) ->
  %% @TODO: Implement this function.
  throw(unsupported_operation).

i() ->
  %% @TODO: Implement this function.
  throw(unsupported_operation).

i(_Tab) ->
  %% @TODO: Implement this function.
  throw(unsupported_operation).

info(_Tab) ->
  %% @TODO: Implement this function.
  throw(unsupported_operation).

info(_Tab, _Item) ->
  %% @TODO: Implement this function.
  throw(unsupported_operation).

init_table(_Tab, _InitFun) ->
  %% @TODO: Implement this function.
  throw(unsupported_operation).

%% @see ets:insert/2.
insert(Tab, ObjectOrObjects) ->
  insert(Tab, ObjectOrObjects, pool_size(Tab)).

insert(Tab, ObjectOrObjects, PoolSize) when is_list(ObjectOrObjects) ->
  lists:foreach(fun(Object) ->
    true = insert(Tab, Object, PoolSize)
                end, ObjectOrObjects), true;
insert(Tab, ObjectOrObjects, PoolSize) when is_tuple(ObjectOrObjects) ->
  [Key | _] = tuple_to_list(ObjectOrObjects),
  call(Tab, Key, PoolSize, fun ets:insert/2, [ObjectOrObjects]).

insert_new(_Tab, _ObjectOrObjects) ->
  %% @TODO: Implement this function.
  throw(unsupported_operation).

is_compiled_ms(_Term) ->
  %% @TODO: Implement this function.
  throw(unsupported_operation).

last(_Tab) ->
  %% @TODO: Implement this function.
  throw(unsupported_operation).

%% @see ets:lookup/2.
lookup(Tab, Key) ->
  lookup(Tab, Key, pool_size(Tab)).

lookup(Tab, Key, PoolSize) ->
  call(Tab, Key, PoolSize, fun ets:lookup/2, [Key]).

%% @see ets:lookup_element/3.
lookup_element(Tab, Key, Pos) ->
  lookup_element(Tab, Key, Pos, pool_size(Tab)).

lookup_element(Tab, Key, Pos, PoolSize) ->
  call(Tab, Key, PoolSize, fun ets:lookup_element/3, [Key, Pos]).

%% @see ets:match/2.
match(Tab, Pattern) ->
  lists:append(pmap(Tab, pool_size(Tab), fun ets:match/2, [Pattern])).

match(_Tab, _Pattern, _Limit) ->
  %% @TODO: Implement this function.
  throw(unsupported_operation).

match(_Continuation) ->
  %% @TODO: Implement this function.
  throw(unsupported_operation).

match_delete(_Tab, _Pattern) ->
  %% @TODO: Implement this function.
  throw(unsupported_operation).

match_object(_Tab, _Pattern) ->
  %% @TODO: Implement this function.
  throw(unsupported_operation).

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

member(_Tab, _Key) ->
  %% @TODO: Implement this function.
  throw(unsupported_operation).

%% @see ets:new/2.
new(Name, Options) ->
  new(Name, Options, ?DEFAULT_POOL_SIZE).

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

%% @see ets:select/2.
select(Tab, MatchSpec) ->
  lists:append(pmap(Tab, pool_size(Tab), fun ets:select/2, [MatchSpec])).

select(_Tab, _MatchSpec, _Limit) ->
  %% @TODO: Implement this function.
  throw(unsupported_operation).

select(_Continuation) ->
  %% @TODO: Implement this function.
  throw(unsupported_operation).

select_count(_Tab, _MatchSpec) ->
  %% @TODO: Implement this function.
  throw(unsupported_operation).

select_delete(_Tab, _MatchSpec) ->
  %% @TODO: Implement this function.
  throw(unsupported_operation).

select_reverse(_Tab, _MatchSpec) ->
  %% @TODO: Implement this function.
  throw(unsupported_operation).

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

update_counter(_Tab, _Key, _UpdateOp) ->
  %% @TODO: Implement this function.
  throw(unsupported_operation).

update_counter(_Tab, _Key, _UpdateOp, _Default) ->
  %% @TODO: Implement this function.
  throw(unsupported_operation).

update_element(_Tab, _Key, _ElementSpec) ->
  %% @TODO: Implement this function.
  throw(unsupported_operation).

%%%===================================================================
%%% Extended API
%%%===================================================================

-spec shard_name(atom(), pos_integer()) -> atom().
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
