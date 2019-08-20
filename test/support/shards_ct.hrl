%%%-------------------------------------------------------------------
%%% @doc
%%% Commons.
%%% @end
%%%-------------------------------------------------------------------

-define(SET, test_set).
-define(DUPLICATE_BAG, test_duplicate_bag).
-define(ORDERED_SET, test_ordered_set).
-define(SHARDED_DUPLICATE_BAG, test_sharded_duplicate_bag).

-define(ETS_SET, ets_test_set).
-define(ETS_DUPLICATE_BAG, ets_test_duplicate_bag).
-define(ETS_ORDERED_SET, ets_test_ordered_set).
-define(ETS_SHARDED_DUPLICATE_BAG, ets_test_sharded_duplicate_bag).

-define(SHARDS_TABS, [
  ?SET,
  ?DUPLICATE_BAG,
  ?ORDERED_SET,
  ?SHARDED_DUPLICATE_BAG
]).

-define(ETS_TABS, [
  ?ETS_SET,
  ?ETS_DUPLICATE_BAG,
  ?ETS_ORDERED_SET,
  ?ETS_SHARDED_DUPLICATE_BAG
]).

-define(is_tab_sharded(T_),
  T_ =:= ?DUPLICATE_BAG; T_ =:= ?SHARDED_DUPLICATE_BAG
).

-define(N_SHARDS, erlang:system_info(schedulers_online)).

-define(fn(FN_, S_),
  case S_ of
    g -> shards_lib:to_string(node()) ++ "." ++ shards_lib:to_string(FN_);
    _ -> FN_
  end
).

-record(test_rec, {name :: atom(), age :: integer()}).

-type config() :: proplists:proplist().
