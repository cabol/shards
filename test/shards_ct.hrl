%%%-------------------------------------------------------------------
%%% @doc
%%% Commons.
%%% @end
%%%-------------------------------------------------------------------

-define(SET, test_set).
-define(DUPLICATE_BAG, test_duplicate_bag).
-define(ORDERED_SET, test_ordered_set).

-define(ETS_SET, ets_test_set).
-define(ETS_DUPLICATE_BAG, ets_test_duplicate_bag).
-define(ETS_ORDERED_SET, ets_test_ordered_set).

-define(SHARDS_TABS, [
  ?SET,
  ?DUPLICATE_BAG,
  ?ORDERED_SET
]).

-define(ETS_TABS, [
  ?ETS_SET,
  ?ETS_DUPLICATE_BAG,
  ?ETS_ORDERED_SET
]).

-define(PARTITIONS, erlang:system_info(schedulers_online)).

-record(test_rec, {name :: atom(), age :: integer()}).

-type config() :: proplists:proplist().
