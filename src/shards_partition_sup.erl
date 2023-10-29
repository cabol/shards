%%%-------------------------------------------------------------------
%%% @doc
%%% This supervisor represents a partitioned table, which is
%%% composed by a group of ETS tables, each of them owned by
%%% `shards_partition'.
%%%
%%% This supervisor holds and handles the supervision tree for a
%%% partitioned table. Every time a new partioned table is created,
%%% a new supervision tree is created to handle its lifecycle, and
%%% this module is the main supervisor for that tree.
%%% @end
%%%-------------------------------------------------------------------
-module(shards_partition_sup).

-behaviour(supervisor).

%% API
-export([
  start_link/2,
  stop/1,
  stop/2
]).

%% Supervisor callbacks
-export([init/1]).

%% Default number of shards
-define(PARTITIONS, erlang:system_info(schedulers_online)).

%% Partitioned table options
-type opts() :: #{atom() => term()}.

%%%===================================================================
%%% API functions
%%%===================================================================

-spec start_link(Name, Opts) -> OnStart when
        Name    :: atom(),
        Opts    :: opts(),
        OnStart :: {ok, pid()} | ignore | {error, term()}.
start_link(Name, Opts) ->
  supervisor:start_link(?MODULE, {self(), Name, Opts}).

%% @equiv stop(Pid, 5000)
stop(SupRef) ->
  stop(SupRef, 5000).

-spec stop(SupRef, Timeout) -> ok when
      SupRef  :: atom() | pid(),
      Timeout :: timeout().
stop(SupRef, Timeout) ->
  gen_server:stop(SupRef, normal, Timeout).

%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================

%% @hidden
init({ParentPid, Name, Opts}) ->
  EtsOpts = maps:get(ets_opts, Opts, []),
  Partitions = maps:get(partitions, Opts, ?PARTITIONS),

  % Init metadata
  Tab = init_meta(Name, Opts, EtsOpts, Partitions),

  % Send a reply with the table reference to the client that called `new/2`
  _ = ParentPid ! {reply, self(), Tab},

  % Build the parrtition owner specs
  PartitionOwners =
    shards_enum:map(fun(PartitionIdx) ->
      shards_partition:child_spec(Name, Tab, self(), PartitionIdx, EtsOpts)
    end, Partitions),

  % The children list contains the partition owners plus the metadata cache
  Children = [
    shards_meta_cache:child_spec(Tab, self())
    | PartitionOwners
  ],

  {ok, {{one_for_all, 1, 5}, Children}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
init_meta(Name, Opts, EtsOpts, Partitions) ->
  try
    Tab = shards_meta:init(Name, EtsOpts),
    Meta = shards_meta:from_map(Opts#{partitions => Partitions}),

    % Store the metadata
    ok =
      shards_meta:put(Tab, [
        {'$tab_meta', Meta},
        {'$tab_owner', self()},
        {'$ets_opts', EtsOpts}
      ]),

    Tab
  catch
    error:badarg -> error({conflict, Name})
  end.
