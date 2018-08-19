-module(shards_meta_SUITE).

-include_lib("mixer/include/mixer.hrl").
-mixin([ktn_meta_SUITE]).

-export([init_per_suite/1, end_per_suite/1]).

-spec init_per_suite(xdb_ct:config()) -> xdb_ct:config().
init_per_suite(Config) ->
  Dialyzer = {dialyzer_warnings, [
    no_return,
    unmatched_returns,
    error_handling,
    unknown
  ]},
  [{application, shards}, {dirs, ["ebin"]}, Dialyzer | Config].

-spec end_per_suite(xdb_ct:config()) -> ok.
end_per_suite(_) ->
  ok.
