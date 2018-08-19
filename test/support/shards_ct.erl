-module(shards_ct).

-export([
  assert_error/2,
  wait_for_msg/1
]).

-type config() :: proplists:proplist().

-export_type([config/0]).

%%%===================================================================
%%% API
%%%===================================================================

-spec assert_error(fun(), term()) -> any().
assert_error(Fun, Error) ->
  try
    Fun()
  catch
    _:{Error, _} -> ok;
    _:Error      -> ok
  end.

-spec wait_for_msg(timeout()) -> any() | {error, timeout}.
wait_for_msg(Timeout) ->
  receive
    Msg -> Msg
  after
    Timeout -> {error, timeout}
  end.
