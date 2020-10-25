%%%-------------------------------------------------------------------
%%% @doc
%%% Provides a set of utilities to work with enumerables.
%%% @end
%%%-------------------------------------------------------------------
-module(shards_enum).

%% API
-export([
  map/2,
  reduce/3,
  reduce_while/3,
  pmap/2,
  pmap/3
]).

%%%===================================================================
%%% API
%%%===================================================================

%% @doc
%% Returns a list where each element is the result of invoking `Fun' on each
%% corresponding element of `Enumerable'.
%%
%% For maps, the function expects a key-value tuple.
%% @end
-spec map(Fun, Enumerable) -> [Result] when
      Fun        :: fun((Elem) -> Result),
      Elem       :: term(),
      Result     :: term(),
      Enumerable :: [term()] | map() | non_neg_integer().
map(Fun, Enumerable) when is_function(Fun, 1) ->
  do_map(Fun, Enumerable).

%% @private
do_map(Fun, Enum) when is_list(Enum) ->
  [Fun(Elem) || Elem <- Enum];
do_map(Fun, Enum) when is_map(Enum) ->
  maps:fold(fun(Key, Value, Acc) ->
    [Fun({Key, Value}) | Acc]
  end, [], Enum);
do_map(Fun, Enum) when is_integer(Enum) ->
  do_map(Fun, [], Enum, 0).

%% @private
do_map(Fun, Acc, N, Count) when Count < N ->
  do_map(Fun, [Fun(Count) | Acc], N, Count + 1);
do_map(_Fun, Acc, _N, _Count) ->
  Acc.

%% @doc
%% Invokes `Fun' for each element in the `Enumerable' with the
%% accumulator.
%%
%% The initial value of the accumulator is `Acc0'. The function is
%% invoked for each element in the enumerable with the accumulator.
%% The result returned by the function is used as the accumulator
%% for the next iteration. The function returns the last accumulator.
%% @end
-spec reduce(Fun, Acc0, Enumerable) -> Acc1 when
      Fun        :: fun((Elem, AccIn) -> AccOut),
      Elem       :: term(),
      AccIn      :: term(),
      AccOut     :: term(),
      Acc0       :: term(),
      Acc1       :: term(),
      Enumerable :: [term()] | map() | non_neg_integer().
reduce(Fun, Acc0, Enumerable) when is_function(Fun, 2) ->
  do_reduce(Fun, Acc0, Enumerable).

%% @private
do_reduce(Fun, AccIn, Enum) when is_list(Enum) ->
  lists:foldl(Fun, AccIn, Enum);
do_reduce(Fun, AccIn, Enum) when is_map(Enum) ->
  maps:fold(fun(Key, Value, Acc) ->
    Fun({Key, Value}, Acc)
  end, AccIn, Enum);
do_reduce(Fun, AccIn, Enum) when is_integer(Enum) ->
  do_reduce(Fun, AccIn, Enum, 0).

%% @private
do_reduce(Fun, Acc, N, Count) when Count < N ->
  do_reduce(Fun, Fun(Count, Acc), N, Count + 1);
do_reduce(_Fun, Acc, _N, _Count) ->
  Acc.

%% @doc
%% Reduces enumerable until `Fun' returns `{halt, AccOut}'.
%%
%% The return value for `Fun' is expected to be
%%
%% <ul>
%% <li>
%% `{cont, AccOut}' to continue the reduction with `AccOut' as the new accumulator or
%% </li>
%% <li>
%% `{halt, AccOut}' to halt the reduction
%% </li>
%% </ul>
%%
%% If fun returns `{halt, AccOut}' the reduction is halted and the function
%% returns `AccOut'. Otherwise, if the enumerable is exhausted, the function
%% returns the accumulator of the last `{cont, AccOut}'.
%% @end
-spec reduce_while(Fun, Acc0, Enumerable) -> Acc1 when
      Fun        :: fun((Elem, AccIn) -> FunRes),
      FunRes     :: {cont | halt, AccOut},
      Elem       :: term(),
      AccIn      :: term(),
      AccOut     :: term(),
      Acc0       :: term(),
      Acc1       :: term(),
      Enumerable :: [term()] | map() | non_neg_integer().
reduce_while(Fun, AccIn, Enumerable) when is_function(Fun, 2) ->
  try
    do_reduce_while(Fun, AccIn, Enumerable)
  catch
    throw:{halt, AccOut} -> AccOut
  end.

%% @private
do_reduce_while(Fun, AccIn, Enum) when is_list(Enum) ->
  lists:foldl(fun(Elem, Acc) ->
    case Fun(Elem, Acc) of
      {cont, AccOut} -> AccOut;
      {halt, AccOut} -> throw({halt, AccOut})
    end
  end, AccIn, Enum);
do_reduce_while(Fun, AccIn, Enum) when is_map(Enum) ->
  maps:fold(fun(Key, Value, Acc) ->
    case Fun({Key, Value}, Acc) of
      {cont, AccOut} -> AccOut;
      {halt, AccOut} -> throw({halt, AccOut})
    end
  end, AccIn, Enum);
do_reduce_while(Fun, AccIn, Enum) when is_integer(Enum) ->
  do_reduce_while(Fun, AccIn, Enum, 0).

%% @private
do_reduce_while(Fun, Acc, N, Count) when Count < N ->
  case Fun(Count, Acc) of
    {cont, AccOut} -> do_reduce_while(Fun, AccOut, N, Count + 1);
    {halt, AccOut} -> throw({halt, AccOut})
  end;
do_reduce_while(_Fun, Acc, _N, _Count) ->
  Acc.

%% @equiv pmap(Fun, infinity, Enumerable)
pmap(Fun, Enumerable) ->
  pmap(Fun, infinity, Enumerable).

%% @doc
%% Similar to `shards_enum:map/2' but it runs in parallel.
%% @end
-spec pmap(Fun, Timeout, Enumerable) -> [Result] when
      Fun        :: fun((Elem) -> Result),
      Elem       :: term(),
      Timeout    :: timeout(),
      Result     :: term(),
      Enumerable :: [term()] | map() | non_neg_integer().
pmap(Fun, Timeout, Enumerable) when is_function(Fun, 1), is_list(Enumerable) ->
  Parent = self(),
  Running = [spawn_monitor(fun() -> Parent ! {self(), Fun(E)} end) || E <- Enumerable],
  pmap_collect(Running, Timeout, {[], undefined}).

%% @private
pmap_collect([], _Timeout, {Acc, undefined}) ->
  Acc;
pmap_collect([], _Timeout, {_Acc, {error, {Reason, Stacktrace}}}) ->
  erlang:raise(error, Reason, Stacktrace);
pmap_collect([], _Timeout, {_Acc, {error, Reason}}) ->
  error(Reason);
pmap_collect([{Pid, MRef} | Next], Timeout, {Acc, Err}) ->
  receive
    {Pid, Res} ->
      erlang:demonitor(MRef, [flush]),
      pmap_collect(Next, Timeout, {[Res | Acc], Err});

    {'DOWN', MRef, process, _Pid, Reason} ->
      pmap_collect(Next, Timeout, {Acc, {error, Reason}})
  after Timeout ->
    exit(pmap_timeout)
  end.
