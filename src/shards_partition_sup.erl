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
  supervisor:start_link(?MODULE, {Name, Opts}).

%% @equiv stop(Pid, 5000)
stop(Pid) ->
  stop(Pid, 5000).

-spec stop(SupRef, Timeout) -> ok when
      SupRef  :: atom() | pid(),
      Timeout :: timeout().
stop(Pid, Timeout) ->
  gen_server:stop(Pid, normal, Timeout).

%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================

%% @hidden
init({Name, Opts}) ->
  EtsOpts = maps:get(ets_opts, Opts, []),
  Partitions = maps:get(partitions, Opts, ?PARTITIONS),

  % init metadata
  Tab = init_meta(Name, Opts, EtsOpts, Partitions),

  Children =
    shards_enum:map(fun(Partition) ->
      child(shards_partition, [Tab, self(), Partition, EtsOpts], #{id => Partition})
    end, Partitions),

  {ok, {{one_for_all, 1, 5}, Children}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
child(Module, Args, Spec) ->
  Spec#{start => {Module, start_link, Args}}.

%% @private
init_meta(Name, Opts, EtsOpts, Partitions) ->
  try
    Tab = shards_meta:init(Name, EtsOpts),
    Meta = shards_meta:from_map(Opts#{tab_pid => self(), partitions => Partitions}),
    true = ets:insert(Tab, {meta, Meta}),
    Tab
  catch
    error:badarg -> error({conflict, Name})
  end.
