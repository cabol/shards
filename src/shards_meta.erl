%%%-------------------------------------------------------------------
%%% @doc
%%% This module encapsulates the partitioned table metadata.
%%%
%%% Different properties must be stored somewhere so  `shards'
%%% can work properly. Shards perform logic on top of ETS tables,
%%% for example, compute the partition based on the `Key' where
%%% the action will be applied. To do so, it needs the number of
%%% partitions, the function to select the partition, and also the
%%% partition identifier to perform the ETS action.
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
  put/3,
  get/1,
  get_partition_tids/1,
  get_partition_pids/1
]).

%% API – Getters
-export([
  tab_pid/1,
  keypos/1,
  partitions/1,
  keyslot_fun/1,
  parallel/1,
  parallel_timeout/1,
  ets_opts/1
]).

%%%===================================================================
%%% Types & Macros
%%%===================================================================

%% Default number of partitions
-define(PARTITIONS, erlang:system_info(schedulers_online)).

%% @type partition_tids() = [{non_neg_integer(), ets:tid()}].
%%
%% Defines a tuple-list with the partition number and the ETS TID.
-type partition_tids() :: [{non_neg_integer(), ets:tid()}].

%% @type partition_pids() = [{non_neg_integer(), pid()}].
%%
%% Defines a tuple-list with the partition number and the partition owner PID.
-type partition_pids() :: [{non_neg_integer(), pid()}].

%% @type keyslot_fun() = fun((Key :: term(), Range :: pos_integer()) -> non_neg_integer()).
%%
%% Defines spec function to pick or compute the partition and/or node.
%% The function returns a value for `Key' within the range `0..Range-1'.
-type keyslot_fun() :: fun((Key :: term(), Range :: pos_integer()) -> non_neg_integer()).

%% Metadata definition
-record(meta, {
  tab_pid          = undefined           :: pid() | undefined,
  keypos           = 1                   :: pos_integer(),
  partitions       = ?PARTITIONS         :: pos_integer(),
  keyslot_fun      = fun erlang:phash2/2 :: keyslot_fun(),
  parallel         = false               :: boolean(),
  parallel_timeout = infinity            :: timeout(),
  ets_opts         = []                  :: [term()]
}).

%% @type t() = #meta{}.
%%
%% Defines `shards' metadata.
-type t() :: #meta{}.

%% @type meta_map() = #{
%%         tab_pid          => pid(),
%%         keypos           => pos_integer(),
%%         partitions       => pos_integer(),
%%         keyslot_fun      => keyslot_fun(),
%%         parallel         => boolean(),
%%         parallel_timeout => timeout(),
%%         ets_opts         => [term()]
%%       }.
%%
%% Defines the map representation for the metadata data type.
-type meta_map() :: #{
        tab_pid          => pid(),
        keypos           => pos_integer(),
        partitions       => pos_integer(),
        keyslot_fun      => keyslot_fun(),
        parallel         => boolean(),
        parallel_timeout => timeout(),
        ets_opts         => [term()]
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
    tab_pid          = maps:get(tab_pid, Map, self()),
    keypos           = maps:get(keypos, Map, 1),
    partitions       = maps:get(partitions, Map, ?PARTITIONS),
    keyslot_fun      = maps:get(keyslot_fun, Map, fun erlang:phash2/2),
    parallel         = maps:get(parallel, Map, false),
    parallel_timeout = maps:get(parallel_timeout, Map, infinity),
    ets_opts         = maps:get(ets_opts, Map, [])
  }.

%% @doc
%% Converts the given `Meta' into a `map'.
%% @end
-spec to_map(t()) -> meta_map().
to_map(Meta) ->
  #{
    tab_pid          => Meta#meta.tab_pid,
    keypos           => Meta#meta.keypos,
    partitions       => Meta#meta.partitions,
    keyslot_fun      => Meta#meta.keyslot_fun,
    parallel         => Meta#meta.parallel,
    parallel_timeout => Meta#meta.parallel_timeout,
    ets_opts         => Meta#meta.ets_opts
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

  ets:new(Tab, [set, public, {read_concurrency, true}] ++ ExtraOpts).

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
-spec lookup(Tab, Key) -> term() when
      Tab :: shards:tab(),
      Key :: term().
lookup(Tab, Key) ->
  try
    ets:lookup_element(Tab, Key, 2)
  catch
    error:badarg -> error({unknown_table, Tab})
  end.

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
%% Returns the metadata.
%% @end
-spec get(Tab :: shards:tab()) -> t() | no_return().
get(Tab) -> lookup(Tab, meta).

%% @doc
%% Returns a list with the partition TIDs.
%% @end
-spec get_partition_tids(Tab :: shards:tab()) -> partition_tids().
get_partition_tids(Tab) -> partitions_info(Tab, tid).

%% @doc
%% Returns a list with the partition PIDs.
%% @end
-spec get_partition_pids(Tab :: shards:tab()) -> partition_pids().
get_partition_pids(Tab) -> partitions_info(Tab, pid).

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

-spec tab_pid(t() | shards:tab()) -> pid().
tab_pid(#meta{tab_pid = Value}) ->
  Value;
tab_pid(Tab) when is_atom(Tab); is_reference(Tab) ->
  tab_pid(?MODULE:get(Tab)).

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

-spec ets_opts(t() | shards:tab()) -> [term()].
ets_opts(#meta{ets_opts = Value}) ->
  Value;
ets_opts(Tab) when is_atom(Tab); is_reference(Tab) ->
  ets_opts(?MODULE:get(Tab)).
