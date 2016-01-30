-module(shards).

%% ETS API
-export([
  all/0,
  new/2, new/3,
  insert/2, insert/3,
  lookup/2, lookup/3,
  lookup_element/3, lookup_element/4,
  delete/1, delete/2, delete/3,
  delete_object/2, delete_object/3,
  delete_all_objects/1, delete_all_objects/2,
  select/2, select/3,
  match/2, match/3
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
%%% ETS API
%%%===================================================================

%% @see ets:all().
all() -> ets:all().

%% @see ets:new/2.
new(Name, Options) ->
  new(Name, Options, ?DEFAULT_POOL_SIZE).

%% @doc
%% @end
new(Name, Options, PoolSize) ->
  case shards_sup:start_link(Name, Options, PoolSize) of
    {ok, _} -> Name;
    _       -> throw(badarg)
  end.

%% @see ets:insert/2.
insert(Tab, ObjectOrObjects) ->
  insert(Tab, ObjectOrObjects, pool_size(Tab)).

%% @doc
%% @end
insert(Tab, ObjectOrObjects, PoolSize) when is_list(ObjectOrObjects) ->
  lists:foreach(fun(Object) ->
    true = insert(Tab, Object, PoolSize)
  end, ObjectOrObjects), true;
insert(Tab, ObjectOrObjects, PoolSize) when is_tuple(ObjectOrObjects) ->
  [Key | _] = tuple_to_list(ObjectOrObjects),
  call(Tab, Key, PoolSize, fun ets:insert/2, [ObjectOrObjects]).

%% @see ets:lookup/2.
lookup(Tab, Key) ->
  lookup(Tab, Key, pool_size(Tab)).

%% @doc
%% @end
lookup(Tab, Key, PoolSize) ->
  call(Tab, Key, PoolSize, fun ets:lookup/2, [Key]).

lookup_element(Tab, Key, Pos) ->
  lookup_element(Tab, Key, Pos, pool_size(Tab)).

lookup_element(Tab, Key, Pos, PoolSize) ->
  call(Tab, Key, PoolSize, fun ets:lookup_element/3, [Key, Pos]).

delete(Tab) ->
  %% @TODO: Find a better way to stop the shards supervisor
  exit(whereis(Tab), normal).

%% @see ets:delete/2.
delete(Tab, Key) ->
  delete(Tab, Key, pool_size(Tab)).

%% @doc
%% @end
delete(Tab, Key, PoolSize) ->
  call(Tab, Key, PoolSize, fun ets:delete/2, [Key]).

%% @see ets:delete_object/2.
delete_object(Tab, Object) ->
  delete_object(Tab, Object, pool_size(Tab)).

%% @doc
%% @end
delete_object(Tab, Object, PoolSize) when is_tuple(Object) ->
  [Key | _] = tuple_to_list(Object),
  call(Tab, Key, PoolSize, fun ets:delete_object/2, [Object]).

delete_all_objects(Tab) ->
  delete_all_objects(Tab, pool_size(Tab)).

delete_all_objects(Tab, PoolSize) ->
  pmap(Tab, PoolSize, fun ets:delete_all_objects/1),
  true.

%% @see ets:select/2.
select(Tab, MatchSpec) ->
  select(Tab, MatchSpec, pool_size(Tab)).

%% @doc
%% @end
select(Tab, MatchSpec, PoolSize) ->
  lists:append(pmap(Tab, PoolSize, fun ets:select/2, [MatchSpec])).

match(Tab, Pattern) ->
  match(Tab, Pattern, pool_size(Tab)).

match(Tab, Pattern, PoolSize) ->
  lists:append(pmap(Tab, PoolSize, fun ets:match/2, [Pattern])).

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
