%%%-------------------------------------------------------------------
%%% @doc
%%% This module encapsulates the partitioned table metadata.
%%%
%%% Different properties must be stored somewhere so `shards' can work
%%% properly. Shards performs its logic on top of ETS tables, for
%%% example, computing the partition based on the `Key' where the
%%% action will be applied. To do so, it needs the number of
%%% partitions, the function to select the partition, and also
%%% the partition identifier to perform the ETS action.
%%% @end
%%%-------------------------------------------------------------------
-module(shards_meta).

%% API
-export([
  new/0,
  from_map/1,
  to_map/1,
  is_metadata/1,
  init/2,
  rename/2,
  lookup/2,
  put/2,
  put/3,
  get/1,
  get/2,
  get/3,
  get_owner/1,
  get_ets_opts/1,
  fetch/2,
  get_partition_tables/1,
  get_partition_pids/1
]).

%% API – Getters
-export([
  keypos/1,
  partitions/1,
  keyslot_fun/1,
  parallel/1,
  parallel_timeout/1,
  cache/1
]).

%% Inline common instructions
-compile({inline, [
  lookup/2,
  put/3,
  get/1,
  get_owner/1,
  get_ets_opts/1,
  get_partition_tables/1,
  get_partition_pids/1
]}).

%%%===================================================================
%%% Types & Macros
%%%===================================================================

%% Default number of partitions
-define(PARTITIONS, erlang:system_info(schedulers_online)).

%% Defines a tuple-list with the partition number and the table reference.
-type partition_tables() :: [{non_neg_integer(), shards:tab()}].

%% Defines a tuple-list with the partition number and the partition owner PID.
-type partition_pids() :: [{non_neg_integer(), pid()}].

%% Defines spec function to pick or compute the partition and/or node.
%% The function returns a value for `Key' within the range `0..Range-1'.
-type keyslot_fun() :: fun((Key :: term(), Range :: pos_integer()) -> non_neg_integer()).

%% Metadata definition
-record(meta, {
  keypos           = 1                   :: pos_integer(),
  partitions       = ?PARTITIONS         :: pos_integer(),
  keyslot_fun      = fun erlang:phash2/2 :: keyslot_fun(),
  parallel         = false               :: boolean(),
  parallel_timeout = infinity            :: timeout(),
  cache            = false               :: boolean()
}).

%% Defines `shards' metadata.
-type t() :: #meta{}.

%% Defines the map representation for the metadata data type.
-type meta_map() :: #{
        keypos           => pos_integer(),
        partitions       => pos_integer(),
        keyslot_fun      => keyslot_fun(),
        parallel         => boolean(),
        parallel_timeout => timeout(),
        cache            => boolean()
      }.

%% Exported types
-export_type([
  t/0,
  keyslot_fun/0,
  meta_map/0
]).

%%%===================================================================
%%% API
%%%===================================================================

%% @doc
%% Returns a metadata data type with the default values.
%% @end
-spec new() -> t().
new() -> #meta{}.

%% @doc
%% Builds a new `meta' from the given `Map'.
%% @end
-spec from_map(Map :: #{atom() => term()}) -> t().
from_map(Map) ->
  #meta{
    keypos           = maps:get(keypos, Map, 1),
    partitions       = maps:get(partitions, Map, ?PARTITIONS),
    keyslot_fun      = maps:get(keyslot_fun, Map, fun erlang:phash2/2),
    parallel         = maps:get(parallel, Map, false),
    parallel_timeout = maps:get(parallel_timeout, Map, infinity),
    cache            = maps:get(cache, Map, false)
  }.

%% @doc
%% Converts the given `Meta' into a `map'.
%% @end
-spec to_map(t()) -> meta_map().
to_map(Meta) ->
  #{
    keypos           => Meta#meta.keypos,
    partitions       => Meta#meta.partitions,
    keyslot_fun      => Meta#meta.keyslot_fun,
    parallel         => Meta#meta.parallel,
    parallel_timeout => Meta#meta.parallel_timeout,
    cache            => Meta#meta.cache
  }.

%% @doc
%% Returns `true' if `Meta' is a metadata data type, otherwise,
%% `false' is returned.
%% @end
-spec is_metadata(Meta :: term()) -> boolean().
is_metadata(#meta{}) -> true;
is_metadata(_)       -> false.

%% @doc
%% Initializes the metadata ETS table.
%% @end
-spec init(Name, Opts) -> Tab when
      Name :: atom(),
      Opts :: [shards:option()],
      Tab  :: shards:tab().
init(Tab, Opts) ->
  ExtraOpts =
    case lists:member(named_table, Opts) of
      true  -> [named_table];
      false -> []
    end,

  ets:new(Tab, [set, public, {read_concurrency, true} | ExtraOpts]).

%% @doc
%% Renames the metadata ETS table.
%% @end
-spec rename(Tab, Name) -> Name when
      Tab  :: shards:tab(),
      Name :: atom().
rename(Tab, Name) ->
  ets:rename(Tab, Name).

%% @doc
%% Returns the value associated to the key `Key' in the metadata table `Tab'.
%% If `Key' is not found, the error `{unknown_table, Tab}' is raised.
%% @end
-spec lookup(Tab, Key) -> Val when
      Tab :: shards:tab(),
      Key :: term(),
      Val :: term().
lookup(Tab, Key) ->
  try
    ets:lookup_element(Tab, Key, 2)
  catch
    error:badarg -> error({unknown_table, Tab})
  end.

%% @doc
%% Inserts the given entries `TupleList' into the metadata table `Tab'.
%% @end
-spec put(Tab, TupleList) -> ok when
      Tab       :: shards:tab(),
      TupleList :: [tuple()].
put(Tab, TupleList) ->
  true = ets:insert(Tab, TupleList),
  ok.

%% @doc
%% Stores the value `Val' under the given key `Key' into the metadata table
%% `Tab'.
%% @end
-spec put(Tab, Key, Val) -> ok when
      Tab :: shards:tab(),
      Key :: term(),
      Val :: term().
put(Tab, Key, Val) ->
  true = ets:insert(Tab, {Key, Val}),
  ok.

%% @doc
%% Returns the table metadata.
%% @end
-spec get(Tab :: shards:tab()) -> t() | no_return().
get(Tab) ->
  case shards_meta_cache:get_meta(Tab) of
    undefined ->
      case lookup(Tab, '$tab_meta') of
        #meta{cache = true} = Meta ->
          % Caching is enabled, hence, cache the metadata
          ok = shards_meta_cache:put_meta(Tab, Meta),

          % Return the metadata
          Meta;

        Meta ->
          Meta
      end;

    Meta ->
      Meta
  end.

%% @equiv get(Tab, Key, undefined)
get(Tab, Key) ->
  get(Tab, Key, undefined).

%% @doc
%% Returns the value for the given `Key' in the metadata,
%% or `Def' if `Key' is not set.
%% @end
-spec get(Tab, Key, Def) -> Val when
      Tab :: shards:tab(),
      Key :: term(),
      Def :: term(),
      Val :: term().
get(Tab, Key, Def) ->
  case fetch(Tab, Key) of
    {ok, Val} ->
      Val;

    {error, not_found} ->
      Def;

    {error, unknown_table} ->
      error({unknown_table, Tab})
  end.

%% @doc
%% Fetches the value for a specific `Key' in the metadata.
%%
%% If the metadata contains the given `Key', its value is returned in
%% the shape of `{ok, Value}'. If the metadata doesn't contain `Key',
%% `{error, Reason}' is returned.
%% @end
-spec fetch(Tab, Key) -> {ok, Value} | {error, Reason} when
      Tab    :: shards:tab(),
      Key    :: term(),
      Value  :: term(),
      Reason :: not_found | unknown_table.
fetch(Tab, Key) ->
  try
    case ets:lookup(Tab, Key) of
      [{Key, Val}] -> {ok, Val};
      []           -> {error, not_found}
    end
  catch
    error:badarg -> {error, unknown_table}
  end.

%% @doc Returns the owner PID for the table `Tab'.
%% @equiv lookup(Tab, '$tab_owner')
-spec get_owner(Tab :: shards:tab()) -> pid().
get_owner(Tab) ->
  lookup(Tab, '$tab_owner').

%% @doc Returns the ETS options for the table `Tab'.
%% @equiv lookup(Tab, '$ets_opts')
-spec get_ets_opts(Tab :: shards:tab()) -> [term()].
get_ets_opts(Tab) ->
  lookup(Tab, '$ets_opts').

%% @doc
%% Returns a list with the partition TIDs.
%% @end
-spec get_partition_tables(Tab :: shards:tab()) -> partition_tables().
get_partition_tables(Tab) ->
  partitions_info(Tab, table).

%% @doc
%% Returns a list with the partition PIDs.
%% @end
-spec get_partition_pids(Tab :: shards:tab()) -> partition_pids().
get_partition_pids(Tab) ->
  partitions_info(Tab, pid).

%% @private
partitions_info(Tab, KeyPrefix) ->
  try
    ets:select(Tab, [{{{KeyPrefix, '$1'}, '$2'}, [], [{{'$1', '$2'}}]}])
  catch
    error:badarg -> error({unknown_table, Tab})
  end.

%%%===================================================================
%%% Getters
%%%===================================================================

-spec keypos(t() | shards:tab()) -> pos_integer().
keypos(#meta{keypos = Value}) ->
  Value;
keypos(Tab) when is_atom(Tab); is_reference(Tab) ->
  keypos(?MODULE:get(Tab)).

-spec partitions(t() | shards:tab()) -> pos_integer().
partitions(#meta{partitions = Value}) ->
  Value;
partitions(Tab) when is_atom(Tab); is_reference(Tab) ->
  partitions(?MODULE:get(Tab)).

-spec keyslot_fun(t() | shards:tab()) -> keyslot_fun().
keyslot_fun(#meta{keyslot_fun = Value}) ->
  Value;
keyslot_fun(Tab) when is_atom(Tab); is_reference(Tab) ->
  keyslot_fun(?MODULE:get(Tab)).

-spec parallel(t() | shards:tab()) -> boolean().
parallel(#meta{parallel = Value}) ->
  Value;
parallel(Tab) when is_atom(Tab); is_reference(Tab) ->
  parallel(?MODULE:get(Tab)).

-spec parallel_timeout(t() | shards:tab()) -> timeout().
parallel_timeout(#meta{parallel_timeout = Value}) ->
  Value;
parallel_timeout(Tab) when is_atom(Tab); is_reference(Tab) ->
  parallel_timeout(?MODULE:get(Tab)).

-spec cache(t() | shards:tab()) -> boolean().
cache(#meta{cache = Value}) ->
  Value;
cache(Tab) when is_atom(Tab); is_reference(Tab) ->
  cache(?MODULE:get(Tab)).
