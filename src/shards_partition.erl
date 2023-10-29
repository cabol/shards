%%%-------------------------------------------------------------------
%%% @doc
%%% Partition Owner.
%%%
%%% The partition owner is a `gen_server' that creates and holds the
%%% ETS table associated with the partition.
%%% @end
%%%-------------------------------------------------------------------
-module(shards_partition).

-behaviour(gen_server).

%% API
-export([
  start_link/5,
  child_spec/5,
  apply_ets_fun/3,
  stop/1,
  stop/2,
  table/2,
  table/3,
  pid/2,
  compute/2
]).

%% gen_server callbacks
-export([
  init/1,
  handle_call/3,
  handle_cast/2,
  handle_info/2,
  terminate/2
]).

%% Inline-compiled functions
-compile({inline, [table/2, table/3, pid/2]}).

%% State
-record(state, {
  tab           :: shards:tab(),
  tab_pid       :: pid() | undefined,
  tab_name      :: atom() | undefined,
  partition     :: non_neg_integer() | undefined,
  partition_tab :: shards:tab() | undefined,
  named_table   :: boolean() | undefined
}).

%% Key for the partition table reference
-define(ptn_key(Ptn_), {table, Ptn_}).
-define(ptn_key(Tab_, Ptn_), {table, Tab_, Ptn_}).

%% Key for the partition pid
-define(pid_key(Ptn_), {pid, Ptn_}).

%%%===================================================================
%%% API
%%%===================================================================

-spec start_link(
        Name                :: atom(),
        Tab                 :: shards:tab(),
        PartitionedTablePid :: pid(),
        PartitionIndex      :: non_neg_integer(),
        Options             :: [term()]
      ) -> gen_server:start_ret().
start_link(Name, Tab, PartitionedTablePid, PartitionIndex, Options) ->
  gen_server:start_link(?MODULE, {Name, Tab, PartitionedTablePid, PartitionIndex, Options}, []).

-spec child_spec(
        Name                :: atom(),
        Tab                 :: shards:tab(),
        PartitionedTablePid :: pid(),
        PartitionIndex      :: non_neg_integer(),
        Options             :: [term()]
      ) -> supervisor:child_spec().
child_spec(Name, Tab, PartitionedTablePid, PartitionIndex, Options) ->
  #{
    id    => {?MODULE, PartitionIndex},
    start => {?MODULE, start_link, [Name, Tab, PartitionedTablePid, PartitionIndex, Options]}
  }.

-spec apply_ets_fun(Pid :: pid(), EtsFun :: atom(), Args :: [term()]) -> term().
apply_ets_fun(Pid, Fun, Args) ->
  gen_server:call(Pid, {ets, Fun, Args}).

%% @equiv stop(Server, 5000)
stop(Pid) ->
  stop(Pid, 5000).

-spec stop(Pid :: pid(), Timeout :: timeout()) -> ok.
stop(Pid, Timeout) ->
  gen_server:stop(Pid, normal, Timeout).

-spec table(Tab :: shards:tab(), Partition :: non_neg_integer()) -> shards:tab().
table(Tab, Partition) ->
  shards_meta:lookup(Tab, ?ptn_key(Partition)).
  % MetaKey = ?ptn_key(Tab, Partition),

  % case persistent_term:get(MetaKey, undefined) of
  %   undefined ->
  %     % Retrieve the partition table ref from the ETS meta table
  %     PtnTab = shards_meta:lookup(Tab, ?ptn_key(Partition)),

  %     % Caching is enabled, hence, cache the partition table ref
  %     ok = persistent_term:put(MetaKey, PtnTab),

  %     % Return the partition table ref
  %     PtnTab;

  %     % case shards_meta:cache(Meta) of
  %     %   true ->
  %     %     % Caching is enabled, hence, cache the partition table ref
  %     %     ok = persistent_term:put(MetaKey, PtnTab),

  %     %     % Return the partition table ref
  %     %     PtnTab;

  %     %   false ->
  %     %     PtnTab
  %     % end;

  %   PtnTab ->
  %     PtnTab
  % end.

-spec table(Tab :: shards:tab(), Key :: term(), Meta :: shards_meta:t()) -> shards:tab().
table(Tab, Key, Meta) ->
  table(Tab, compute(Key, Meta)).

-spec pid(Tab :: shards:tab(), Partition :: non_neg_integer()) -> pid().
pid(Tab, Partition) ->
  shards_meta:lookup(Tab, ?pid_key(Partition)).

-spec compute(Key :: term(), Meta :: shards_meta:t()) -> non_neg_integer().
compute(Key, Meta) ->
  N = shards_meta:partitions(Meta),
  KeyslotFun = shards_meta:keyslot_fun(Meta),
  KeyslotFun(Key, N).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%% @hidden
init({Name, Tab, TabPid, Partition, Opts}) ->
  _ = process_flag(trap_exit, true),

  NewOpts =
    case lists:keyfind(restore, 1, Opts) of
      {restore, _, _} = Val -> Val;
      false                 -> Opts
    end,

  InitState = #state{
    tab       = Tab,
    tab_pid   = TabPid,
    tab_name  = Name,
    partition = Partition
  },

  init(NewOpts, InitState).

%% @private
init({restore, PartitionFilenames, Opts}, #state{partition = Partition} = State) ->
  Filename = maps:get(Partition, PartitionFilenames),

  case ets:file2tab(Filename, Opts) of
    {ok, PartitionTab} ->
      {ok, register(State#state{partition_tab = PartitionTab})};

    Error ->
      {stop, {restore_error, Error}}
  end;
init(Opts, #state{tab_name = Name, partition = Partition} = State) ->
  PartitionName = shards_lib:partition_name(Name, Partition),
  PartitionTab = ets:new(PartitionName, lists:usort([public | Opts])),

  NewState = State#state{
    partition_tab = PartitionTab,
    named_table   = lists:member(named_table, Opts)
  },

  {ok, register(NewState)}.

%% @hidden
handle_call({ets, Fun, Args}, _From, #state{partition_tab = PartitionTab} = State) ->
  {reply, apply(ets, Fun, [PartitionTab | Args]), State};
handle_call(_Request, _From, State) ->
  {reply, ok, State}.

%% @hidden
handle_cast(_Request, State) ->
  {noreply, State}.

%% @hidden
handle_info({'EXIT', _Pid, _Reason}, #state{} = State) ->
  {stop, normal, State};
handle_info(_Reason, State) ->
  {noreply, State}.

%% @hidden
terminate(_Reason, #state{tab = Tab, partition = Partition, named_table = true}) ->
  persistent_term:erase(?ptn_key(Tab, Partition));
terminate(_Reason, State) ->
  State.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
register(#state{tab = Tab, partition = Ptn, partition_tab = PartTab} = State) ->
  ok = shards_meta:put(Tab, ?ptn_key(Ptn), PartTab),
  ok = shards_meta:put(Tab, ?pid_key(Ptn), self()),
  State.
