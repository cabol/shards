%%%-------------------------------------------------------------------
%%% @doc
%%% This is the main module, which contains all Shards/API functions.
%%% This is a wrapper on top of `shards_local' and `shards_dist'.
%%%
%%% @see shards_local.
%%% @see shards_dist.
%%% @end
%%%-------------------------------------------------------------------
-module(shards).

-behaviour(application).

%% Application callbacks and functions
-export([
  start/0, start/2,
  stop/0, stop/1
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

-export([
  shards_state/1,
  list/1
]).

%% Macro to get the module to use: 'shards_local' (default) | 'shards_dist'.
-define(SHARDS,
  case application:get_env(shards, module) of
    {ok, shards_dist} -> shards_dist;
    _                 -> shards_local
  end
).

%%%===================================================================
%%% Application callbacks and functions
%%%===================================================================

%% @doc Starts `shards' application.
-spec start() -> {ok, _} | {error, term()}.
start() ->
  application:ensure_all_started(shards).

%% @doc Stops `shards' application.
-spec stop() -> ok | {error, term()}.
stop() ->
  application:stop(shards).

%% @hidden
start(_StartType, _StartArgs) ->
  shards_sup:start_link().

%% @hidden
stop(_State) ->
  ok.

%%%===================================================================
%%% Shards/ETS API
%%%===================================================================

all() ->
  (?SHARDS):all().

delete(Tab) ->
  (?SHARDS):delete(Tab).

delete(Tab, Key) ->
  (?SHARDS):delete(Tab, Key).

delete_all_objects(Tab) ->
  (?SHARDS):delete_all_objects(Tab).

delete_object(Tab, Object) ->
  (?SHARDS):delete_object(Tab, Object).

file2tab(Filenames) ->
  (?SHARDS):file2tab(Filenames).

file2tab(Filenames, Options) ->
  (?SHARDS):file2tab(Filenames, Options).

first(Tab) ->
  (?SHARDS):first(Tab).

foldl(Function, Acc0, Tab) ->
  (?SHARDS):foldl(Function, Acc0, Tab).

foldr(Function, Acc0, Tab) ->
  (?SHARDS):foldr(Function, Acc0, Tab).

from_dets(_Tab, _DetsTab) ->
  throw(unsupported_operation).

fun2ms(_LiteralFun) ->
  throw(unsupported_operation).

give_away(Tab, Pid, GiftData) ->
  (?SHARDS):give_away(Tab, Pid, GiftData).

i() ->
  (?SHARDS):i().

i(_Tab) ->
  throw(unsupported_operation).

info(Tab) ->
  (?SHARDS):info(Tab).

info(Tab, Item) ->
  (?SHARDS):info(Tab, Item).

info_shard(Tab, Shard) ->
  (?SHARDS):info_shard(Tab, Shard).

info_shard(Tab, Shard, Item) ->
  (?SHARDS):info_shard(Tab, Shard, Item).

init_table(_Tab, _InitFun) ->
  throw(unsupported_operation).

insert(Tab, ObjectOrObjects) ->
  (?SHARDS):insert(Tab, ObjectOrObjects).

insert_new(Tab, ObjectOrObjects) ->
  (?SHARDS):insert_new(Tab, ObjectOrObjects).

is_compiled_ms(Term) ->
  (?SHARDS):is_compiled_ms(Term).

last(Tab) ->
  (?SHARDS):last(Tab).

lookup(Tab, Key) ->
  (?SHARDS):lookup(Tab, Key).

lookup_element(Tab, Key, Pos) ->
  (?SHARDS):lookup_element(Tab, Key, Pos).

match(Tab, Pattern) ->
  (?SHARDS):match(Tab, Pattern).

match(Tab, Pattern, Limit) ->
  (?SHARDS):match(Tab, Pattern, Limit).

match(Continuation) ->
  (?SHARDS):match(Continuation).

match_delete(Tab, Pattern) ->
  (?SHARDS):match_delete(Tab, Pattern).

match_object(Tab, Pattern) ->
  (?SHARDS):match_object(Tab, Pattern).

match_object(Tab, Pattern, Limit) ->
  (?SHARDS):match_object(Tab, Pattern, Limit).

match_object(Continuation) ->
  (?SHARDS):match_object(Continuation).

match_spec_compile(MatchSpec) ->
  (?SHARDS):match_spec_compile(MatchSpec).

match_spec_run(List, CompiledMatchSpec) ->
  (?SHARDS):match_spec_run(List, CompiledMatchSpec).

member(Tab, Key) ->
  (?SHARDS):member(Tab, Key).

new(Name, Options) ->
  (?SHARDS):new(Name, Options).

new(Name, Options, PoolSize) ->
  (?SHARDS):new(Name, Options, PoolSize).

next(Tab, Key1) ->
  (?SHARDS):next(Tab, Key1).

prev(Tab, Key1) ->
  (?SHARDS):prev(Tab, Key1).

rename(_Tab, _Name) ->
  throw(unsupported_operation).

repair_continuation(_Continuation, _MatchSpec) ->
  throw(unsupported_operation).

safe_fixtable(_Tab, _Fix) ->
  throw(unsupported_operation).

select(Tab, MatchSpec) ->
  (?SHARDS):select(Tab, MatchSpec).

select(Tab, MatchSpec, Limit) ->
  (?SHARDS):select(Tab, MatchSpec, Limit).

select(Continuation) ->
  (?SHARDS):select(Continuation).

select_count(Tab, MatchSpec) ->
  (?SHARDS):select_count(Tab, MatchSpec).

select_delete(Tab, MatchSpec) ->
  (?SHARDS):select_delete(Tab, MatchSpec).

select_reverse(Tab, MatchSpec) ->
  (?SHARDS):select_reverse(Tab, MatchSpec).

select_reverse(Tab, MatchSpec, Limit) ->
  (?SHARDS):select_reverse(Tab, MatchSpec, Limit).

select_reverse(Continuation) ->
  (?SHARDS):select_reverse(Continuation).

setopts(Tab, Opts) ->
  (?SHARDS):setopts(Tab, Opts).

slot(_Tab, _I) ->
  throw(unsupported_operation).

tab2file(Tab, Filenames) ->
  (?SHARDS):tab2file(Tab, Filenames).

tab2file(Tab, Filenames, Options) ->
  (?SHARDS):tab2file(Tab, Filenames, Options).

tab2list(Tab) ->
  (?SHARDS):tab2list(Tab).

tabfile_info(Filename) ->
  (?SHARDS):tabfile_info(Filename).

table(Tab) ->
  (?SHARDS):table(Tab).

table(Tab, Options) ->
  (?SHARDS):table(Tab, Options).

test_ms(Tuple, MatchSpec) ->
  (?SHARDS):test_ms(Tuple, MatchSpec).

take(Tab, Key) ->
  (?SHARDS):take(Tab, Key).

to_dets(_Tab, _DetsTab) ->
  throw(unsupported_operation).

update_counter(Tab, Key, UpdateOp) ->
  (?SHARDS):update_counter(Tab, Key, UpdateOp).

update_counter(Tab, Key, UpdateOp, Default) ->
  (?SHARDS):update_counter(Tab, Key, UpdateOp, Default).

update_element(Tab, Key, ElementSpec) ->
  (?SHARDS):update_element(Tab, Key, ElementSpec).

%%%===================================================================
%%% Extended API
%%%===================================================================

shards_state(Tab) ->
  (?SHARDS):shards_state(Tab).

list(Tab) ->
  (?SHARDS):list(Tab).

%%%===================================================================
%%% Internal functions
%%%===================================================================

