-module(shards_lib_SUITE).

%% Common Test
-export([
  all/0
]).

%% Test Cases
-export([
  t_partition_name/1,
  t_object_key/1,
  t_keyfind/1,
  t_keyupdate/1,
  t_keypop/1,
  t_to_string/1,
  t_read_write_tabfile/1
]).

-define(EXCLUDED_FUNS, [
  module_info,
  all
]).

%%%===================================================================
%%% Common Test
%%%===================================================================

-spec all() -> [atom()].
all() ->
  Exports = ?MODULE:module_info(exports),
  [F || {F, _} <- Exports, not lists:member(F, ?EXCLUDED_FUNS)].

%%%===================================================================
%%% Test Cases
%%%===================================================================

-spec t_partition_name(shards_ct:config()) -> any().
t_partition_name(_Config) ->
  't.ptn0' = shards_lib:partition_name(t, 0),
  't.ptn1' = shards_lib:partition_name(t, 1).

-spec t_object_key(shards_ct:config()) -> any().
t_object_key(_Config) ->
  1 = shards_lib:object_key([{1, 1}, {2, 2}, {3, 3}], shards_meta:new()),

  State = shards_meta:from_map(#{tab_pid => self(), keypos => 2}),
  "foo" = shards_lib:object_key({abc, "foo", "bar"}, State).

-spec t_keyfind(shards_ct:config()) -> any().
t_keyfind(_Config) ->
  TupleList = new_tuple_list(10),
  undefined = shards_lib:keyfind(11, TupleList),
  nil = shards_lib:keyfind(11, TupleList, nil),
  10 = shards_lib:keyfind(10, TupleList).

-spec t_keyupdate(shards_ct:config()) -> any().
t_keyupdate(_Config) ->
  TL1 = new_tuple_list(10),
  TL11 = shards_lib:keyupdate(fun(_, V) -> V * 2 end, [2, 4], TL1),
  4 = shards_lib:keyfind(2, TL11),
  8 = shards_lib:keyfind(4, TL11),
  [{2, 4}, {4, 8}] = TL11 -- TL1,
  TL12 = shards_lib:keyupdate(fun(_, V) -> V * 2 end, [11], TL11),
  undefined = shards_lib:keyfind(11, TL12).

-spec t_keypop(shards_ct:config()) -> any().
t_keypop(_Config) ->
  {1, [{2, 2}, {3, 3}]} = shards_lib:keypop(1, new_tuple_list(3)),
  {undefined, [{1, 1}, {2, 2}, {3, 3}]} = shards_lib:keypop(11, new_tuple_list(3)),
  {11, [{1, 1}, {2, 2}, {3, 3}]} = shards_lib:keypop(11, new_tuple_list(3), 11),
  ok.

-spec t_to_string(shards_ct:config()) -> any().
t_to_string(_Config) ->
  "hello" = shards_lib:to_string("hello"),
  "hello" = shards_lib:to_string(<<"hello">>),
  "hello" = shards_lib:to_string(hello),
  "123" = shards_lib:to_string(123),
  "123.4" = shards_lib:to_string(123.4),

  shards_ct:assert_error(fun() ->
    shards_lib:to_string([1, 2, 3])
  end, badarg),

  shards_ct:assert_error(fun() ->
    shards_lib:to_string(self())
  end, badarg).

-spec t_read_write_tabfile(shards_ct:config()) -> any().
t_read_write_tabfile(_Config) ->
  {error, _} = shards_lib:write_tabfile("dir/myfile", [{body, "Hello"}]),
  ok = shards_lib:write_tabfile("myfile", [{body, "Hello"}]),

  shards_ct:assert_error(fun() ->
    shards_lib:read_tabfile("dir/myfile")
  end, {error, enoent}),

  [{body, "Hello"}] = shards_lib:read_tabfile("myfile").

%%%===================================================================
%%% Internal Functions
%%%===================================================================

%% @private
new_tuple_list(Max) ->
  new_tuple_list(1, Max).

%% @private
new_tuple_list(Min, Max) ->
  It = lists:seq(Min, Max),
  lists:zip(It, It).
