-module(shards_enum_SUITE).

-include_lib("common_test/include/ct.hrl").

%% Common Test
-export([
  all/0
]).

%% Test Cases
-export([
  t_map/1,
  t_pmap/1,
  t_reduce/1,
  t_reduce_with_maps/1,
  t_reduce_with_count/1,
  t_reduce_while/1,
  t_reduce_while_with_maps/1,
  t_reduce_while_with_count/1
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

-spec t_map(shards_ct:config()) -> any().
t_map(_Config) ->
  [2, 4, 6, 8] = shards_enum:map(fun(X) -> X * 2 end, [1, 2, 3, 4]),
  [1, 4, 9, 16] = shards_enum:map(fun({K, V}) -> K * V end, new_tuple_list(4)),
  [18, 8, 2] = shards_enum:map(fun({K, V}) -> K * V end, #{1 => 2, 2 => 4, 3 => 6}),
  [3, 2, 1, 0] = shards_enum:map(fun(X) -> X end, 4).

-spec t_pmap(shards_ct:config()) -> any().
t_pmap(_Config) ->
  [8, 6, 4, 2] = shards_enum:pmap(fun(X) -> X * 2 end, [1, 2, 3, 4]),

  shards_ct:assert_error(fun() ->
    shards_enum:pmap(fun
      (1) -> error({badmatch, 1});
      (X) -> X * 2
    end, lists:seq(1, 10))
  end, {badmatch, 1}),

  shards_ct:assert_error(fun() ->
    shards_enum:pmap(fun
      (1) -> exit(shutdown);
      (X) -> X * 2
    end, lists:seq(1, 10))
  end, shutdown),

  shards_ct:assert_error(fun() ->
    shards_enum:pmap(fun(_) -> timer:sleep(1000) end, 10, lists:seq(1, 10))
  end, pmap_timeout).

-spec t_reduce(shards_ct:config()) -> any().
t_reduce(_Config) ->
  15 = shards_enum:reduce(fun(E, Acc) -> E + Acc end, 0, [1, 2, 3, 4, 5]),
  [3, 2, 1] = shards_enum:reduce(fun(E, Acc) -> [E | Acc] end, [], [1, 2, 3]).

-spec t_reduce_with_maps(shards_ct:config()) -> any().
t_reduce_with_maps(_Config) ->
  11 = shards_enum:reduce(fun({_K, V}, Acc) -> V + Acc end, 0, #{a => 1, b => 10}),

  [{b, 10}, {a, 1}] =
    shards_enum:reduce(fun({K, V}, Acc) -> [{K, V} | Acc] end, [], #{a => 1, b => 10}),

  shards_ct:assert_error(fun() ->
    shards_enum:reduce(fun(K, V, Acc) ->
      K + V + Acc
    end, 0, new_tuple_list(10))
  end, function_clause).

-spec t_reduce_with_count(shards_ct:config()) -> any().
t_reduce_with_count(_Config) ->
  15 = shards_enum:reduce(fun(E, Acc) -> E + Acc end, 0, 6),
  [2, 1, 0] = shards_enum:reduce(fun(E, Acc) -> [E | Acc] end, [], 3),

  shards_ct:assert_error(fun() ->
    shards_enum:reduce(fun({K, V}, Acc) ->
      K = V + Acc * 2
    end, 0, new_tuple_list(10))
  end, badmatch).

-spec t_reduce_while(shards_ct:config()) -> any().
t_reduce_while(_Config) ->
  Fun = fun
    ({_, V}, Acc) when V > 4 -> {halt, [V | Acc]};
    ({_, V}, Acc)            -> {cont, [V | Acc]}
  end,

  R = shards_enum:reduce_while(Fun, [], new_tuple_list(10)),
  R = shards_enum:reduce_while(Fun, [], new_tuple_list(5)),

  shards_ct:assert_error(fun() ->
    shards_enum:reduce_while(fun(K, V, Acc) ->
      K + V + Acc
    end, 0, new_tuple_list(10))
  end, function_clause).

-spec t_reduce_while_with_maps(shards_ct:config()) -> any().
t_reduce_while_with_maps(_Config) ->
  Fun = fun
    ({K, V}, Acc) when V < 4 -> {cont, Acc#{K => V}};
    ({_, _}, Acc)            -> {halt, Acc}
  end,

  R = shards_enum:reduce_while(Fun, #{}, maps:from_list(new_tuple_list(10))),
  R = shards_enum:reduce_while(Fun, #{}, maps:from_list(new_tuple_list(3))),

  shards_ct:assert_error(fun() ->
    shards_enum:reduce_while(fun({K, V}, Acc) ->
      {ok, K + V + Acc}
    end, 0, maps:from_list(new_tuple_list(10)))
  end, case_clause).

-spec t_reduce_while_with_count(shards_ct:config()) -> any().
t_reduce_while_with_count(_Config) ->
  Fun = fun
    (V, Acc) when V > 3 -> {halt, Acc};
    (V, Acc)            -> {cont, [V | Acc]}
  end,

  R = shards_enum:reduce_while(Fun, [], 10),
  R = shards_enum:reduce_while(fun(V, Acc) -> {cont, [V | Acc]} end, [], 4).

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
