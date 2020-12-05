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
  start_link/4,
  apply_ets_fun/3,
  retrieve_tab/1,
  stop/1,
  stop/2,
  tid/2,
  tid/3,
  pid/2,
  compute/2
]).

%% gen_server callbacks
-export([
  init/1,
  handle_call/3,
  handle_cast/2,
  handle_info/2
]).

%% Inline-compiled functions
-compile({inline, [tid/2, tid/3, pid/2]}).

%% State
-record(state, {
  tab           :: atom() | ets:tid(),
  tab_pid       :: pid() | undefined,
  partition     :: non_neg_integer() | undefined,
  partition_tid :: ets:tid() | undefined
}).

%%%===================================================================
%%% API
%%%===================================================================

-spec start_link(
        Tab                 :: atom() | ets:tid(),
        PartitionedTablePid :: pid(),
        PartitionIndex      :: non_neg_integer(),
        Options             :: [term()]
      ) -> {ok, pid()} | ignore | {error, term()}.
start_link(Tab, TabPid, Partition, Options) ->
  gen_server:start_link(?MODULE, {Tab, TabPid, Partition, Options}, []).

-spec apply_ets_fun(
        Pid    :: pid(),
        EtsFun :: atom(),
        Args   :: [term()]
      ) -> term().
apply_ets_fun(Pid, Fun, Args) ->
  gen_server:call(Pid, {ets, Fun, Args}).

-spec retrieve_tab(Pid :: pid()) -> atom() | ets:tid().
retrieve_tab(Pid) ->
  gen_server:call(Pid, retrieve_tab).

%% @equiv stop(Server, 5000)
stop(Pid) ->
  stop(Pid, 5000).

-spec stop(Pid :: pid(), Timeout :: timeout()) -> ok.
stop(Pid, Timeout) ->
  gen_server:stop(Pid, normal, Timeout).

-spec tid(Tab :: shards:tab(), Partition :: non_neg_integer()) -> ets:tid().
tid(Tab, Partition) ->
  shards_meta:lookup(Tab, {tid, Partition}).

-spec tid(Tab :: shards:tab(), Key :: term(), Meta :: shards_meta:t()) -> ets:tid().
tid(Tab, Key, Meta) ->
  shards_meta:lookup(Tab, {tid, compute(Key, Meta)}).

-spec pid(Tab :: shards:tab(), Partition :: non_neg_integer()) -> pid().
pid(Tab, Partition) ->
  shards_meta:lookup(Tab, {pid, Partition}).

-spec compute(Key :: term(), Meta :: shards_meta:t()) -> non_neg_integer().
compute(Key, Meta) ->
  N = shards_meta:partitions(Meta),
  KeyslotFun = shards_meta:keyslot_fun(Meta),
  KeyslotFun(Key, N).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%% @hidden
init({Tab, TabPid, Partition, Opts}) ->
  _ = process_flag(trap_exit, true),

  NewOpts =
    case lists:keyfind(restore, 1, Opts) of
      {restore, _, _} = Val -> Val;
      false                 -> Opts
    end,

  init(NewOpts, #state{tab = Tab, tab_pid = TabPid, partition = Partition}).

%% @private
init({restore, PartitionFilenames, Opts}, #state{partition = Partition} = State) ->
  Filename = maps:get(Partition, PartitionFilenames),

  case ets:file2tab(Filename, Opts) of
    {ok, Tid} ->
      NewState = register(State#state{partition_tid = Tid}),
      {ok, NewState};

    Error ->
      {stop, {restore_error, Error}}
  end;
init(Opts, State) ->
  NewOpts = lists:delete(named_table, [public | Opts]),
  Tid = ets:new(?MODULE, NewOpts),
  NewState = register(State#state{partition_tid = Tid}),
  {ok, NewState}.

%% @hidden
handle_call(retrieve_tab, _From, #state{tab = Tab} = State) ->
  {reply, Tab, State};
handle_call({ets, Fun, Args}, _From, #state{partition_tid = PartTid} = State) ->
  Response = apply(ets, Fun, [PartTid | Args]),
  {reply, Response, State};
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

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
register(#state{tab = Tab, partition = Part, partition_tid = PartTid} = State) ->
  ok = shards_meta:put(Tab, {tid, Part}, PartTid),
  ok = shards_meta:put(Tab, {pid, Part}, self()),
  State.
