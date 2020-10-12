-module(shards_ct).

-export([
  assert_error/2,
  assert_values/3,
  wait_for_msg/1,
  with_table/3
]).

-type config() :: proplists:proplist().

-export_type([config/0]).

%%%===================================================================
%%% API
%%%===================================================================

-spec assert_error(fun(), term()) -> any().
assert_error(Fun, Error) ->
  try
    Res = Fun(),
    Res = Error
  catch
    _:{Error, _} -> ok;
    _:Error      -> ok
  end.

-spec assert_values(shards:tab(), [term()], [term()]) -> ok.
assert_values(Tab, Keys, Expected) ->
  lists:foreach(fun({K, ExpectedV}) ->
    case shards:lookup(Tab, K) of
      []       -> ExpectedV = nil;
      [{K, V}] -> ExpectedV = V
    end
  end, lists:zip(Keys, Expected)).

-spec wait_for_msg(timeout()) -> any() | {error, timeout}.
wait_for_msg(Timeout) ->
  receive
    Msg -> Msg
  after
    Timeout -> {error, timeout}
  end.

-spec with_table(fun(), atom(), [term()]) -> term().
with_table(Fun, Name, Opts) ->
  Tab = shards:new(Name, Opts),

  try
    Fun(Tab)
  after
    shards:delete(Tab)
  end.
