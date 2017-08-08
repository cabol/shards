-module(shards_lib_SUITE).

-include_lib("common_test/include/ct.hrl").

%% Common Test
-export([
  all/0
]).

%% Test Cases
-export([
  t_shard_name/1,
  t_list_shards/1,
  t_iterator/1,
  t_get_pid/1,
  t_pick/1,
  t_keyfind/1,
  t_keyupdate/1,
  t_reduce_while/1,
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

all() ->
  Exports = ?MODULE:module_info(exports),
  [F || {F, _} <- Exports, not lists:member(F, ?EXCLUDED_FUNS)].

%%%===================================================================
%%% Test Cases
%%%===================================================================

t_shard_name(_Config) ->
  't.0' = shards_lib:shard_name(t, 0),
  't.1' = shards_lib:shard_name(t, 1),
  ok.

t_list_shards(_Config) ->
  ['t.0', 't.1', 't.2', 't.3'] = shards_lib:list_shards(t, 4),
  ok.

t_iterator(_Config) ->
  R = [0, 1, 2, 3] = shards_lib:iterator(4),
  R = shards_lib:iterator(shards_state:new(4)),
  ok.

t_get_pid(_Config) ->
  _ = register(test, self()),
  true = is_pid(shards_lib:get_pid(test)),
  ok.

t_pick(_Config) ->
  lists:foreach(fun(K) ->
    Res = shards_lib:pick(K, 4, nil),
    true = Res >= 0 andalso Res =< 3
  end, lists:seq(1, 100)).

t_keyfind(_Config) ->
  TupleList = new_tuple_list(10),
  undefined = shards_lib:keyfind(11, TupleList),
  nil = shards_lib:keyfind(11, TupleList, nil),
  10 = shards_lib:keyfind(10, TupleList),
  ok.

t_keyupdate(_Config) ->
  TL1 = new_tuple_list(10),
  TL11 = shards_lib:keyupdate(fun(_, V) -> V * 2 end, [2, 4], TL1),
  4 = shards_lib:keyfind(2, TL11),
  8 = shards_lib:keyfind(4, TL11),
  [{2, 4}, {4, 8}] = TL11 -- TL1,
  TL12 = shards_lib:keyupdate(fun(_, V) -> V * 2 end, [11], TL11),
  undefined = shards_lib:keyfind(11, TL12),
  ok.

t_reduce_while(_Config) ->
  Fun =
    fun
      ({_, V}, Acc) when V < 4 -> {halt, [V | Acc]};
      ({_, V}, Acc)            -> {cont, [V | Acc]}
    end,
  R = shards_lib:reduce_while(Fun, [], new_tuple_list(10)),
  R = shards_lib:reduce_while(Fun, [], new_tuple_list(3)),

  try
    shards_lib:reduce_while(fun({K, V}, Acc) ->
      K = V + Acc
    end, 0, new_tuple_list(10))
  catch
    _:_ -> ok
  end.

t_to_string(_Config) ->
  "hello" = shards_lib:to_string("hello"),
  "hello" = shards_lib:to_string(<<"hello">>),
  "hello" = shards_lib:to_string(hello),
  "123" = shards_lib:to_string(123),
  "123.4" = shards_lib:to_string(123.4),

  ok =
    try
      shards_lib:to_string([1, 2, 3])
    catch
      _:{badarg, _} -> ok
    end,

  try
    shards_lib:to_string(self())
  catch
    _:{badarg, _} -> ok
  end.

t_read_write_tabfile(_Config) ->
  {error, _} = shards_lib:write_tabfile("dir/myfile", [{body, "Hello"}]),
  ok = shards_lib:write_tabfile("myfile", [{body, "Hello"}]),

  ok =
    try
      shards_lib:read_tabfile("dir/myfile")
    catch
      _:_ -> ok
    end,
  [{body, "Hello"}] = shards_lib:read_tabfile("myfile"),
  ok.

%%%===================================================================
%%% Internal Functions
%%%===================================================================

new_tuple_list(Max) ->
  new_tuple_list(1, Max).

new_tuple_list(Min, Max) ->
  It = lists:seq(Min, Max),
  lists:zip(It, It).
