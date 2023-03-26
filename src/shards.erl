%%%-------------------------------------------------------------------
%%% @doc
%%% Partitioned/Sharded ETS tables.
%%%
%%% <h2>Features</h2>
%%%
%%% <ul>
%%% <li>
%%% `shards' implements the ETS API to keep compatibility and make the usage
%%% straightforward; exactly the same if you were using `ets'.
%%% </li>
%%% <li>
%%% Sharded or partitioned tables under-the-hood. This feature is managed
%%% entirely by `shards' and it is 100% transparent for the client. Provides
%%% a logical view of a single ETS table, but internally, it is composed by
%%% `N' number of `partitions' and each partition has associated an ETS table.
%%% </li>
%%% <li>
%%% High performance and scalability. `shards' keeps the lock contention under
%%% control enabling ETS tables to scale out and support higher levels of
%%% concurrency without lock issues; specially write-locks, which most of the
%%% cases might cause significant performance degradation.
%%% </li>
%%% </ul>
%%%
%%% <h2>Partitioned Table</h2>
%%%
%%% When a table is created with `shards:new/2', a new supervision tree is
%%% created to represent the partitioned table. There is a main supervisor
%%% `shards_partition_sup' that create an ETS table to store the metadata
%%% and also starts the children which are the partitions to create. Each
%%% partition is owned by `shards_partition' (it is a `gen_server') and it
%%% creates an ETS table for storing data mapped to that partition. The
%%% supervision tree looks like:
%%%
%%% <br></br>
%%% ```
%%%                           [shards_partition]--><ETS-Table>
%%%                           /
%%% [shards_partition_sup]--<-[shards_partition]--><ETS-Table>
%%%           |               \
%%%  <Metadata-ETS-Table>     [shards_partition]--><ETS-Table>
%%% '''
%%% <br></br>
%%%
%%% The returned value by `shards:new/2' may be an atom if it is a named
%%% table or a reference otherwise, and in the second case the returned
%%% reference is the one of the metadata table, which is the main entry
%%% point and it is owned by the main supervisor. See `shards:new/2' for
%%% more information.
%%%
%%% <h2>Usage</h2>
%%%
%%% ```
%%% > Tab = shards:new(tab, []).
%%% #Ref<0.1541908042.2337144842.31535>
%%% > shards:insert(Tab, [{a, 1}, {b, 2}, {c, 3}]).
%%% true
%%% > shards:lookup(Tab, a).
%%% [{a,1}]
%%% > shards:lookup(Tab, b).
%%% [{b,2}]
%%% > shards:lookup(Tab, c).
%%% [{c,3}]
%%% > shards:lookup_element(Tab, c, 2).
%%% 3
%%% > shards:lookup(Tab, d).
%%% []
%%% > shards:delete(Tab, c).
%%% true
%%% > shards:lookup(Tab, c).
%%% []
%%% '''
%%%
%%% As you can see, the usage is exactly the same if you were using `ets',
%%% you can try the rest of the ETS API but with `shards' module.
%%%
%%% <h2>Important</h2>
%%%
%%% Despite `shards' aims to keep 100% compatibility with current ETS API,
%%% the semantic for some of the functions may be a bit different due to
%%% the nature of sharding; it is not the same having all the entries in
%%% a single table than distributed across multiple ones. For example,
%%% for query-based functions like `select', `match', etc., the returned
%%% entries are the same but not necessary the same order than `ets'.
%%% For `first', `next', and `last' they behave the similar in the sense
%%% by means of them a partitioned table is traversed, so the final result
%%% is the same, but the order in which the entries are traversed may be
%%% different. Therefore, it is highly recommended to read the documentation
%%% of the functions.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(shards).

%% ETS API
-export([
  all/0,
  delete/1, delete/2, delete/3,
  delete_all_objects/1, delete_all_objects/2,
  delete_object/2, delete_object/3,
  file2tab/1, file2tab/2,
  first/1, first/2,
  foldl/3, foldl/4,
  foldr/3, foldr/4,
  i/0,
  info/1, info/2,
  insert/2, insert/3,
  insert_new/2, insert_new/3,
  is_compiled_ms/1,
  last/1,
  lookup/2, lookup/3,
  lookup_element/3, lookup_element/4,
  match/1, match/2, match/3, match/4,
  match_delete/2, match_delete/3,
  match_object/1, match_object/2, match_object/3, match_object/4,
  match_spec_compile/1,
  match_spec_run/2,
  member/2, member/3,
  new/2,
  next/2, next/3,
  prev/2,
  rename/2, rename/3,
  safe_fixtable/2,
  select/1, select/2, select/3, select/4,
  select_count/2, select_count/3,
  select_delete/2, select_delete/3,
  select_replace/2, select_replace/3,
  select_reverse/1, select_reverse/2, select_reverse/3, select_reverse/4,
  setopts/2, setopts/3,
  tab2file/2, tab2file/3,
  tab2list/1, tab2list/2,
  tabfile_info/1,
  table/1, table/2, table/3,
  test_ms/2,
  take/2, take/3,
  update_counter/3, update_counter/4, update_counter/5,
  update_element/3, update_element/4
]).

%% Helpers
-export([
  table_meta/1,
  get_meta/2,
  get_meta/3,
  put_meta/3,
  partition_owners/1
]).

%% Inline-compiled functions
-compile({inline, [
  table_meta/1,
  get_meta/2,
  get_meta/3,
  put_meta/3
]}).

%%%===================================================================
%%% Types & Macros
%%%===================================================================

%% Table parameter
-type tab() :: atom() | ets:tid().

%% ETS Types
-type access() :: public | protected | private.
-type type() :: set | ordered_set | bag | duplicate_bag.
-type ets_continuation() ::
        '$end_of_table'
        | {tab(), integer(), integer(), ets:comp_match_spec(), list(), integer()}
        | {tab(), _, _, integer(), ets:comp_match_spec(), list(), integer(), integer()}.

%% ETS tweaks option
-type tweaks() ::
        {write_concurrency, boolean()}
        | {read_concurrency, boolean()}
        | compressed.

%% Shards extended options.
-type shards_opt() ::
        {partitions, pos_integer()}
        | {keyslot_fun, shards_meta:keyslot_fun()}
        | {restore, term(), term()}.

%% Create table options – used by `new/2'.
-type option() ::
        type()
        | access()
        | named_table
        | {keypos, pos_integer()}
        | {heir, pid(), HeirData :: term()}
        | {heir, none}
        | tweaks()
        | shards_opt().

%% ETS Info Tuple
-type info_tuple() ::
        {compressed, boolean()}
        | {heir, pid() | none}
        | {keypos, pos_integer()}
        | {memory, non_neg_integer()}
        | {name, atom()}
        | {named_table, boolean()}
        | {node, node()}
        | {owner, pid()}
        | {protection, access()}
        | {size, non_neg_integer()}
        | {type, type()}
        | {write_concurrency, boolean()}
        | {read_concurrency, boolean()}
        | {shards, [atom()]}.

%% ETS Info Item
-type info_item() ::
        compressed | fixed | heir | keypos | memory
        | name | named_table | node | owner | protection
        | safe_fixed | size | stats | type
        | write_concurrency | read_concurrency
        | shards.

%% ETS TabInfo Item
-type tabinfo_item() ::
        {name, atom()}
        | {type, type()}
        | {protection, access()}
        | {named_table, boolean()}
        | {keypos, non_neg_integer()}
        | {size, non_neg_integer()}
        | {extended_info, [md5sum | object_count]}
        | {version, {Major :: non_neg_integer(), Minor :: non_neg_integer()}}
        | {shards, [atom()]}.

%% Defines the convention for the query functions:
%% <ul>
%% <li>`Tab': Table reference.</li>
%% <li>`MatchSpec': The `ets:match_spec()'.</li>
%% <li>`Limit': Results limit.</li>
%% <li>`Partition': Partition index.</li>
%% <li>`Continuation': The `ets:continuation()'.</li>
%% </ul>
-type continuation() :: {
        Tab          :: tab(),
        MatchSpec    :: ets:match_spec(),
        Limit        :: pos_integer(),
        Partition    :: non_neg_integer(),
        Continuation :: ets_continuation()
      }.

%% Filename type.
-type filename() :: string() | binary() | atom().

% Exported Types
-export_type([
  tab/0,
  option/0,
  info_tuple/0,
  info_item/0,
  tabinfo_item/0,
  continuation/0,
  filename/0
]).

%% Macro to check if the given Filename has the right type
-define(is_filename(FN_), is_list(FN_); is_binary(FN_); is_atom(FN_)).

%%%===================================================================
%%% Helpers
%%%===================================================================

%% @doc Returns the metadata associated with the given table `Tab'.
-spec table_meta(Tab :: tab()) -> shards_meta:t().
table_meta(Tab) -> shards_meta:get(Tab).

%% @equiv get_meta(Tab, Key, undefined)
get_meta(Tab, Key) ->
  get_meta(Tab, Key, undefined).

%% @doc Wrapper for `shards_meta:get/3'.
-spec get_meta(Tab, Key, Def) -> Val when
      Tab :: tab(),
      Key :: term(),
      Def :: term(),
      Val :: term().
get_meta(Tab, Key, Def) ->
  shards_meta:get(Tab, Key, Def).

%% @doc Wrapper for `shards_meta:put/3'.
-spec put_meta(Tab, Key, Val) -> ok when
      Tab :: shards:tab(),
      Key :: term(),
      Val :: term().
put_meta(Tab, Key, Val) ->
  shards_meta:put(Tab, Key, Val).

%% @doc Returns the partition PIDs associated with the given table `TabOrPid'.
-spec partition_owners(TabOrPid) -> [OwnerPid] when
      TabOrPid :: pid() | tab(),
      OwnerPid :: pid().
partition_owners(TabOrPid) when is_pid(TabOrPid) ->
  [Child || {_, Child, _, _} <- supervisor:which_children(TabOrPid)];
partition_owners(TabOrPid) when is_atom(TabOrPid); is_reference(TabOrPid) ->
  partition_owners(shards_meta:tab_pid(TabOrPid)).

%%%===================================================================
%%% ETS API
%%%===================================================================

%% @equiv ets:all()
all() ->
  ets:all().

%% @doc
%% Equivalent to `ets:delete/1'.
%%
%% @see ets:delete/1.
%% @end
-spec delete(Tab :: tab()) -> true.
delete(Tab) ->
  Meta = shards_meta:get(Tab),
  TabPid = shards_meta:tab_pid(Meta),
  ok = shards_partition_sup:stop(TabPid),
  true.

%% @equiv delete(Tab, Key, shards_meta:get(Tab))
delete(Tab, Key) ->
  delete(Tab, Key, shards_meta:get(Tab)).

%% @doc
%% Equivalent to `ets:delete/2'.
%%
%% @see ets:delete/2.
%% @end
-spec delete(Tab, Key, Meta) -> true when
      Tab  :: tab(),
      Key  :: term(),
      Meta :: shards_meta:t().
delete(Tab, Key, Meta) ->
  PartTid = shards_partition:tid(Tab, Key, Meta),
  ets:delete(PartTid, Key).

%% @equiv delete_all_objects(Tab, shards_meta:get(Tab))
delete_all_objects(Tab) ->
  delete_all_objects(Tab, shards_meta:get(Tab)).

%% @doc
%% Equivalent to `ets:delete_all_objects/1'.
%%
%% @see ets:delete_all_objects/1.
%% @end
-spec delete_all_objects(Tab, Meta) -> true when
      Tab  :: tab(),
      Meta :: shards_meta:t().
delete_all_objects(Tab, Meta) ->
  _ = mapred(Tab, fun ets:delete_all_objects/1, Meta),
  true.

%% @equiv delete_object(Tab, Object, shards_meta:get(Tab))
delete_object(Tab, Object) ->
  delete_object(Tab, Object, shards_meta:get(Tab)).

%% @doc
%% Equivalent to `ets:delete_object/2'.
%%
%% @see ets:delete_object/2.
%% @end
-spec delete_object(Tab, Object, Meta) -> true when
      Tab    :: tab(),
      Object :: tuple(),
      Meta   :: shards_meta:t().
delete_object(Tab, Object, Meta) when is_tuple(Object) ->
  Key = shards_lib:object_key(Object, Meta),
  PartTid = shards_partition:tid(Tab, Key, Meta),
  ets:delete_object(PartTid, Object).

%% @equiv file2tab(Filename, [])
file2tab(Filename) ->
  file2tab(Filename, []).

%% @doc
%% Equivalent to `shards:file2tab/2'. Moreover, it restores the
%% supervision tree for the `shards' corresponding to the given
%% file, such as if they had been created using `shards:new/2'.
%%
%% @see ets:file2tab/2.
%% @end
-spec file2tab(Filename, Options) -> {ok, Tab} | {error, Reason} when
      Filename :: filename(),
      Tab      :: tab(),
      Options  :: [Option],
      Option   :: {verify, boolean()},
      Reason   :: term().
file2tab(Filename, Options) when ?is_filename(Filename) ->
  try
    StrFilename = shards_lib:to_string(Filename),
    Header = shards_lib:read_tabfile(StrFilename),

    TabName = maps:get(name, Header),
    Meta = maps:get(metadata, Header),
    Partitions = maps:get(partitions, Header),

    TabOpts = [
      {partitions, shards_meta:partitions(Meta)},
      {keyslot_fun, shards_meta:keyslot_fun(Meta)}
      | shards_meta:ets_opts(Meta)
    ],

    Return = new(TabName, [{restore, Partitions, Options} | TabOpts]),
    {ok, Return}
  catch
    throw:Error ->
      Error;

    error:{error, _} = Error ->
      Error;

    error:{badarg, Arg} ->
      {error, {read_error, {file_error, Arg, enoent}}}
  end.

%% @equiv first(Tab, shards_meta:get(Tab))
first(Tab) ->
  first(Tab, shards_meta:get(Tab)).

%% @doc
%% Equivalent to `ets:first/1'.
%%
%% However, the order in which results are returned might be not the same as
%% the original ETS function, since it is a sharded table.
%%
%% @see ets:first/1.
%% @end
-spec first(Tab, Meta) -> Key | '$end_of_table' when
      Tab  :: tab(),
      Key  :: term(),
      Meta :: shards_meta:t().
first(Tab, Meta) ->
  N = shards_meta:partitions(Meta),
  Partition = N - 1,
  first(Tab, ets:first(shards_partition:tid(Tab, Partition)), Partition).

%% @private
first(Tab, '$end_of_table', Partition) when Partition > 0 ->
  NextPartition = Partition - 1,
  first(Tab, ets:first(shards_partition:tid(Tab, NextPartition)), NextPartition);
first(_, '$end_of_table', _) ->
  '$end_of_table';
first(_, Key, _) ->
  Key.

%% @equiv foldl(Fun, Acc, Tab, shards_meta:get(Tab))
foldl(Fun, Acc, Tab) ->
  foldl(Fun, Acc, Tab, shards_meta:get(Tab)).

%% @doc
%% Equivalent to `ets:foldl/3'.
%%
%% However, the order in which the entries are traversed may be different
%% since they are distributed across multiple partitions.
%%
%% @see ets:foldl/3.
%% @end
-spec foldl(Fun, Acc0, Tab, Meta) -> Acc1 when
      Fun    :: fun((Element :: term(), AccIn) -> AccOut),
      Tab    :: tab(),
      Meta   :: shards_meta:t(),
      Acc0   :: term(),
      Acc1   :: term(),
      AccIn  :: term(),
      AccOut :: term().
foldl(Fun, Acc, Tab, Meta) ->
  fold(foldl, Fun, Acc, Tab, shards_meta:partitions(Meta) - 1).

%% @equiv foldr(Fun, Acc, Tab, shards_meta:get(Tab))
foldr(Fun, Acc, Tab) ->
  foldr(Fun, Acc, Tab, shards_meta:get(Tab)).

%% @doc
%% Equivalent to `ets:foldr/3'.
%%
%% However, the order in which the entries are traversed may be different
%% since they are distributed across multiple partitions.
%%
%% @see ets:foldr/3.
%% @end
-spec foldr(Fun, Acc0, Tab, Meta) -> Acc1 when
      Fun    :: fun((Element :: term(), AccIn) -> AccOut),
      Tab    :: tab(),
      Meta   :: shards_meta:t(),
      Acc0   :: term(),
      Acc1   :: term(),
      AccIn  :: term(),
      AccOut :: term().
foldr(Fun, Acc, Tab, Meta) ->
  fold(foldr, Fun, Acc, Tab, shards_meta:partitions(Meta) - 1).

%% @private
fold(Fold, Fun, Acc, Tab, Partition) when Partition >= 0 ->
  NewAcc = ets:Fold(Fun, Acc, shards_partition:tid(Tab, Partition)),
  fold(Fold, Fun, NewAcc, Tab, Partition - 1);
fold(_Fold, _Fun, Acc, _Tab, _Partition) ->
  Acc.

%% @equiv ets:i()
i() -> ets:i().

%% @doc
%% Similar to ets:info/1' but extra information about the partitioned
%% table is added.
%%
%% <h2>Extra Info:</h2>
%%
%% <ul>
%% <li>
%% `{partitions, post_integer()}' - Number of partitions.
%% </li>
%% <li>
%% `{keyslot_fun, shards_meta:keyslot_fun()}' - Functions used for compute
%% the keyslot.
%% </li>
%% <li>
%% `{parallel, boolean()}' - Whether the parallel mode is enabled or not.
%% </li>
%% </ul>
%%
%% @see ets:info/1.
%% @end
-spec info(Tab) -> InfoList | undefined when
      Tab       :: tab(),
      InfoList  :: [InfoTuple],
      InfoTuple :: info_tuple().
info(Tab) ->
  with_meta(Tab, fun(Meta) ->
    do_info(Tab, Meta)
  end).

%% @doc
%% Equivalent to `ets:info/2'.
%%
%% See the added items by `shards:info/1'.
%%
%% @see ets:info/2.
%% @end
-spec info(Tab, Item) -> Value | undefined when
      Tab   :: tab(),
      Item  :: info_item(),
      Value :: term().
info(Tab, Item) when is_atom(Item) ->
  with_meta(Tab, fun(Meta) ->
    shards_lib:keyfind(Item, do_info(Tab, Meta))
  end).

%% @private
do_info(Tab, Meta) ->
  InfoLists = mapred(Tab, fun ets:info/1, Meta),

  [
    {partitions, shards_meta:partitions(Meta)},
    {keyslot_fun, shards_meta:keyslot_fun(Meta)},
    {parallel, shards_meta:parallel(Meta)}
    | parts_info(Tab, InfoLists, [memory])
  ].

%% @equiv insert(Tab, ObjOrObjs, shards_meta:get(Tab))
insert(Tab, ObjOrObjs) ->
  insert(Tab, ObjOrObjs, shards_meta:get(Tab)).

%% @doc
%% Equivalent to `ets:insert/2'.
%%
%% Despite this functions behaves exactly the same as `ets:insert/2'
%% and produces the same result, there is a big difference due to the
%% nature of the sharding distribution model, <b>IT IS NOT ATOMIC</b>.
%% Therefore, if it fails by inserting an object at some partition,
%% previous inserts execution on other partitions are not rolled back,
%% but an error is raised instead.
%%
%% @see ets:insert/2.
%% @end
-spec insert(Tab, ObjOrObjs, Meta) -> true | no_return() when
      Tab       :: tab(),
      ObjOrObjs :: tuple() | [tuple()],
      Meta      :: shards_meta:t().
insert(Tab, ObjOrObjs, Meta) when is_list(ObjOrObjs) ->
  maps:fold(fun(Partition, Group, Acc) ->
    Acc = ets:insert(Partition, Group)
  end, true, group_keys_by_partition(Tab, ObjOrObjs, Meta));
insert(Tab, ObjOrObjs, Meta) when is_tuple(ObjOrObjs) ->
  ets:insert(get_part_tid(Tab, ObjOrObjs, Meta), ObjOrObjs).

%% @equiv insert_new(Tab, ObjOrObjs, shards_meta:get(Tab))
insert_new(Tab, ObjOrObjs) ->
  insert_new(Tab, ObjOrObjs, shards_meta:get(Tab)).

%% @doc
%% Equivalent to `ets:insert_new/2'.
%%
%% Despite this functions behaves exactly the same as `ets:insert_new/2'
%% and produces the same result, there is a big difference due to the
%% nature of the sharding distribution model, <b>IT IS NOT ATOMIC</b>.
%% Opposite to `shards:insert/2', this function tries to roll-back
%% previous inserts execution on other partitions if it fails by
%% inserting an object at some partition, but there might be race
%% conditions during roll-back execution.
%%
%% <b>Example:</b>
%%
%% ```
%% > shards:insert_new(mytab, {k1, 1}).
%% true
%%
%% > shards:insert_new(mytab, {k1, 1}).
%% false
%%
%% > shards:insert_new(mytab, [{k1, 1}, {k2, 2}]).
%% false
%% '''
%%
%% @see ets:insert_new/2.
%% @end
-spec insert_new(Tab, ObjOrObjs, Meta) -> boolean() when
      Tab       :: tab(),
      ObjOrObjs :: tuple() | [tuple()],
      Meta      :: shards_meta:t().
insert_new(Tab, ObjOrObjs, Meta) when is_list(ObjOrObjs) ->
  Result =
    shards_enum:reduce_while(fun({Partition, Group}, Acc) ->
      case ets:insert_new(Partition, Group) of
        true ->
          {cont, Group ++ Acc};

        false ->
          ok = rollback_insert(Tab, Acc, Meta),
          {halt, false}
      end
    end, [], group_keys_by_partition(Tab, ObjOrObjs, Meta)),

  case Result of
    false -> false;
    _     -> true
  end;
insert_new(Tab, ObjOrObjs, Meta) when is_tuple(ObjOrObjs) ->
  ets:insert_new(get_part_tid(Tab, ObjOrObjs, Meta), ObjOrObjs).

%% @private
rollback_insert(Tab, Entries, Meta) ->
  lists:foreach(fun(Entry) ->
    Key = element(shards_meta:keypos(Meta), Entry),
    ?MODULE:delete(Tab, Key)
  end, Entries).

%% @equiv ets:is_compiled_ms(Term)
is_compiled_ms(Term) ->
  ets:is_compiled_ms(Term).

%% @doc
%% Equivalent to `ets:last/1'.
%%
%% However, the order in which results are returned might be not the same as
%% the original ETS function, since it is a sharded table.
%%
%% @see ets:last/1.
%% @end
-spec last(Tab) -> Key | '$end_of_table' when
      Tab :: tab(),
      Key :: term().
last(Tab) ->
  Partition0 = shards_partition:tid(Tab, 0),

  case ets:info(Partition0, type) of
    ordered_set -> ets:last(Partition0);
    _TableType  -> first(Tab)
  end.

%% @equiv lookup(Tab, Key, shards_meta:get(Tab))
lookup(Tab, Key) ->
  lookup(Tab, Key, shards_meta:get(Tab)).

%% @doc
%% Equivalent to `ets:lookup/2'.
%%
%% @see ets:lookup/2.
%% @end
-spec lookup(Tab, Key, Meta) -> [Object] when
      Tab    :: tab(),
      Key    :: term(),
      Meta   :: shards_meta:t(),
      Object :: tuple().
lookup(Tab, Key, Meta) ->
  PartTid = shards_partition:tid(Tab, Key, Meta),
  ets:lookup(PartTid, Key).

%% @equiv lookup_element(Tab, Key, Pos, shards_meta:get(Tab))
lookup_element(Tab, Key, Pos) ->
  lookup_element(Tab, Key, Pos, shards_meta:get(Tab)).

%% @doc
%% Equivalent to `ets:lookup_element/3'.
%%
%% @see ets:lookup_element/3.
%% @end
-spec lookup_element(Tab, Key, Pos, Meta) -> Elem when
      Tab  :: tab(),
      Key  :: term(),
      Pos  :: pos_integer(),
      Meta :: shards_meta:t(),
      Elem :: term() | [term()].
lookup_element(Tab, Key, Pos, Meta) ->
  PartTid = shards_partition:tid(Tab, Key, Meta),
  ets:lookup_element(PartTid, Key, Pos).

%% @equiv match(Tab, Pattern, shards_meta:get(Tab))
match(Tab, Pattern) ->
  match(Tab, Pattern, shards_meta:get(Tab)).

%% @doc
%% If 3rd argument is `pos_integer()' this function behaves
%% like `ets:match/3', otherwise, the 3rd argument is
%% assumed as `shards_meta:t()` and it behaves like
%% `ets:match/2'.
%%
%% The order in which results are returned might be not the same
%% as the original ETS function.
%%
%% @see ets:match/2.
%% @see ets:match/3.
%% @end
-spec match(Tab, Pattern, LimitOrMeta) -> {[Match], Cont} | '$end_of_table' | [Match] when
      Tab         :: tab(),
      Pattern     :: ets:match_pattern(),
      LimitOrMeta :: pos_integer() | shards_meta:t(),
      Match       :: [term()],
      Cont        :: continuation().
match(Tab, Pattern, Limit) when is_integer(Limit), Limit > 0 ->
  match(Tab, Pattern, Limit, shards_meta:get(Tab));
match(Tab, Pattern, Meta) ->
  Map = {fun ets:match/2, [Pattern]},
  Reduce = fun erlang:'++'/2,
  mapred(Tab, Map, Reduce, Meta).

%% @doc
%% Equivalent to `ets:match/3'.
%%
%% The order in which results are returned might be not the same
%% as the original ETS function.
%%
%% @see ets:match/3.
%% @end
-spec match(Tab, Pattern, Limit, Meta) -> {[Match], Cont} | '$end_of_table' when
      Tab     :: tab(),
      Pattern :: ets:match_pattern(),
      Limit   :: pos_integer(),
      Meta    :: shards_meta:t(),
      Match   :: [term()],
      Cont    :: continuation().
match(Tab, Pattern, Limit, Meta) when is_integer(Limit), Limit > 0 ->
  N = shards_meta:partitions(Meta),
  q(match, Tab, Pattern, Limit, q_fun(), Limit, N - 1, {[], nil}).

%% @doc
%% Equivalent to `ets:match/1'.
%%
%% The order in which results are returned might be not the same
%% as the original ETS function.
%%
%% @see ets:match/1.
%% @end
-spec match(Continuation) -> {[Match], continuation()} | '$end_of_table' when
      Match        :: [term()],
      Continuation :: continuation().
match({_, _, Limit, _, _} = Continuation) ->
  q(match, Continuation, q_fun(), Limit, []).

%% @equiv match_delete(Tab, Pattern, shards_meta:get(Tab))
match_delete(Tab, Pattern) ->
  match_delete(Tab, Pattern, shards_meta:get(Tab)).

%% @doc
%% Equivalent to `ets:match_delete/2'.
%%
%% @see ets:match_delete/2.
%% @end
-spec match_delete(Tab, Pattern, Meta) -> true when
      Tab     :: tab(),
      Pattern :: ets:match_pattern(),
      Meta    :: shards_meta:t().
match_delete(Tab, Pattern, Meta) ->
  Map = {fun ets:match_delete/2, [Pattern]},
  Reduce = {fun erlang:'and'/2, true},
  mapred(Tab, Map, Reduce, Meta).

%% @equiv match_object(Tab, Pattern, shards_meta:get(Tab))
match_object(Tab, Pattern) ->
  match_object(Tab, Pattern, shards_meta:get(Tab)).

%% @doc
%% If 3rd argument is `pos_integer()' this function behaves
%% like `ets:match_object/3', otherwise, the 3rd argument is
%% assumed as `shards_meta:t()` and it behaves like
%% `ets:match_object/2'.
%%
%% The order in which results are returned might be not the same
%% as the original ETS function.
%%
%% @see ets:match_object/3.
%% @end
-spec match_object(Tab, Pattern, LimitOrMeta) -> {[Object], Cont} | '$end_of_table' | [Object] when
      Tab         :: tab(),
      Pattern     :: ets:match_pattern(),
      LimitOrMeta :: pos_integer() | shards_meta:t(),
      Object      :: tuple(),
      Cont        :: continuation().
match_object(Tab, Pattern, Limit) when is_integer(Limit), Limit > 0 ->
  match_object(Tab, Pattern, Limit, shards_meta:get(Tab));
match_object(Tab, Pattern, Meta) ->
  Map = {fun ets:match_object/2, [Pattern]},
  Reduce = fun erlang:'++'/2,
  mapred(Tab, Map, Reduce, Meta).

%% @doc
%% Equivalent to `ets:match_object/3'.
%%
%% The order in which results are returned might be not the same
%% as the original ETS function.
%%
%% @see ets:match_object/3.
%% @end
-spec match_object(Tab, Pattern, Limit, Meta) -> {[Object], Cont} | '$end_of_table' when
      Tab     :: tab(),
      Pattern :: ets:match_pattern(),
      Limit   :: pos_integer(),
      Meta    :: shards_meta:t(),
      Object  :: tuple(),
      Cont    :: continuation().
match_object(Tab, Pattern, Limit, Meta) when is_integer(Limit), Limit > 0 ->
  N = shards_meta:partitions(Meta),
  q(match_object, Tab, Pattern, Limit, q_fun(), Limit, N - 1, {[], nil}).

%% @doc
%% Equivalent to `ets:match_object/1'.
%%
%% The order in which results are returned might be not the same
%% as the original ETS function.
%%
%% @see ets:match_object/1.
%% @end
-spec match_object(Cont) -> {[Object], Cont} | '$end_of_table' when
      Object :: tuple(),
      Cont   :: continuation().
match_object({_, _, Limit, _, _} = Continuation) ->
  q(match_object, Continuation, q_fun(), Limit, []).

%% @equiv ets:match_spec_compile(MatchSpec)
match_spec_compile(MatchSpec) ->
  ets:match_spec_compile(MatchSpec).

%% @equiv ets:match_spec_run(List, CompiledMatchSpec)
match_spec_run(List, CompiledMatchSpec) ->
  ets:match_spec_run(List, CompiledMatchSpec).

%% @equiv member(Tab, Key, shards_meta:get(Tab))
member(Tab, Key) ->
  member(Tab, Key, shards_meta:get(Tab)).

%% @doc
%% Equivalent to `ets:member/2'.
%%
%% @see ets:member/2.
%% @end
-spec member(Tab, Key, Meta) -> boolean() when
      Tab  :: tab(),
      Key  :: term(),
      Meta :: shards_meta:t().
member(Tab, Key, Meta) ->
  PartTid = shards_partition:tid(Tab, Key, Meta),
  ets:member(PartTid, Key).

%% @doc
%% This operation is equivalent to `ets:new/2', but when is called,
%% instead of create a single ETS table, it creates a new supervision
%% tree for the partitioned table.
%%
%% The supervision tree is composed by a main supervisor `shards_partition_sup'
%%  and `N' number of workers or partitions handled by `shards_partition'
%% (partition owner). Each worker creates an ETS table to handle the partition.
%% Also, the main supervisor `shards_partition_sup' creates an ETS table to
%% keep the metadata for the partitioned table.
%%
%% Returns an atom if the created table is a named table, otherwise,
%% a reference is returned. In the last case, the returned reference
%% is the one of the metadata table, which is the main entry-point
%% and it is owned by the main supervisor `shards_partition_sup'.
%%
%% <h3>Options:</h3>
%% In addition to the options given by `ets:new/2', this functions provides
%% the next options:
%% <ul>
%% <li>
%% `{partitions, N}' - Specifies the number of partitions for the sharded
%% table. By default, `N = erlang:system_info(schedulers_online)'.
%% </li>
%% <li>
%% `{keyslot_fun, F}' - Specifies the function used to compute the partition
%% where the action will be evaluated. Defaults to `erlang:phash2/2'.
%% </li>
%% <li>
%% `{parallel, P}' - Specifies whether `shards' should work in parallel mode
%% or not, for the applicable functions, e.g.: `select', `match', etc. By
%% default is set to `false'.
%% </li>
%% <li>
%% `{parallel_timeout, T}' - When `parallel' is set to `true', it specifies
%% the max timeout for a parallel execution. Defaults to `infinity'.
%% </li>
%% </ul>
%%
%% <h3>Access:</h3>
%% Currently, only `public' access is supported by `shards:new/2'. Since a
%% partitioned table is started with its own supervision tree when created,
%% it is very tricky to provide `private' or `protected' access since there
%% are multiple partitions (or ETS tables) and they are owned by the
%% supervisor's children, and the supervisor along with their children
%% (or partitions) are managed by `shards' under-the-hood; it is completely
%% transparent for the client.
%%
%% <h3>Examples:</h3>
%%
%% ```
%% > Tab = shards:new(tab1, []).
%% #Ref<0.1541908042.2337144842.31535>
%%
%% > shards:new(tab2, [named_table]).
%% tab2
%% '''
%%
%% See also the <b>"Partitioned Table"</b> section at the module documentation
%% for more information.
%%
%% @see ets:new/2.
%% @end
-spec new(Name, Options) -> Tab when
      Name    :: atom(),
      Options :: [option()],
      Tab     :: tab().
new(Name, Options) ->
  with_trap_exit(fun() ->
    ParsedOpts = shards_opts:parse(Options),
    StartResult = shards_partition_sup:start_link(Name, ParsedOpts),
    do_new(StartResult, Name, ParsedOpts)
  end).

%% @private
do_new({ok, Pid}, Name, Options) ->
  ok = maybe_register(Name, Pid, maps:get(ets_opts, Options)),
  shards_partition:retrieve_tab(shards_lib:get_sup_child(Pid, 0));
do_new({error, {shutdown, {_, _, {restore_error, Error}}}}, _Name, _Options) ->
  ok = wrap_exit(),
  error(Error);
do_new({error, {Reason, _}}, _Name, _Options) ->
  ok = wrap_exit(),
  error(Reason).

%% @private
maybe_register(Name, Pid, Options) ->
  case lists:member(named_table, Options) of
    true ->
      true = register(Name, Pid),
      ok;

    false ->
      ok
  end.

%% @private
wrap_exit() ->
  % We wait for the 'EXIT' signal from the partition supervisor knowing
  % that it will reply at some point, either after roughly timeout or
  % when an 'EXIT' signal response is ready.
  receive
    {'EXIT', _Pid, _Reason} -> ok
  end.

%% @equiv next(Tab, Key1, shards_meta:get(Tab))
next(Tab, Key1) ->
  next(Tab, Key1, shards_meta:get(Tab)).

%% @doc
%% Equivalent to `ets:next/2'.
%%
%% However, the order in which results are returned might be not the same as
%% the original ETS function, since it is a sharded table.
%%
%% @see ets:next/2.
%% @end
-spec next(Tab, Key1, Meta) -> Key2 | '$end_of_table' when
      Tab  :: tab(),
      Key1 :: term(),
      Key2 :: term(),
      Meta :: shards_meta:t().
next(Tab, Key1, Meta) ->
  KeyslotFun = shards_meta:keyslot_fun(Meta),
  Partitions = shards_meta:partitions(Meta),
  Idx = KeyslotFun(Key1, Partitions),
  PartTid = shards_partition:tid(Tab, Idx),
  next_(Tab, ets:next(PartTid, Key1), Idx).

%% @private
next_(Tab, '$end_of_table', Partition) when Partition > 0 ->
  NextPartition = Partition - 1,
  next_(Tab, ets:first(shards_partition:tid(Tab, NextPartition)), NextPartition);
next_(_, '$end_of_table', _) ->
  '$end_of_table';
next_(_, Key2, _) ->
  Key2.

%% @doc
%% Equivalent to `ets:next/2'.
%%
%% However, the order in which results are returned might be not the same as
%% the original ETS function, since it is a sharded table.
%%
%% @see ets:prev/2.
%% @end
-spec prev(Tab, Key1) -> Key2 | '$end_of_table' when
      Tab  :: tab(),
      Key1 :: term(),
      Key2 :: term().
prev(Tab, Key1) ->
  Partition0 = shards_partition:tid(Tab, 0),

  case ets:info(Partition0, type) of
    ordered_set -> ets:prev(Partition0, Key1);
    _TableType  -> next(Tab, Key1)
  end.

%% @equiv rename(Tab, Name, shards_meta:get(Tab))
rename(Tab, Name) ->
  rename(Tab, Name, shards_meta:get(Tab)).

%% @doc
%% Equivalent to `ets:rename/2'.
%%
%% Renames the table name and all its associated shard tables.
%% If something unexpected occurs during the process, an exception
%% will be raised.
%%
%% @see ets:rename/2.
%% @end
-spec rename(Tab, Name, Meta) -> Name when
      Tab  :: tab(),
      Name :: atom(),
      Meta :: shards_meta:t().
rename(Tab, Name, Meta) ->
  Pid = shards_meta:tab_pid(Meta),
  true = unregister(Tab),
  true = register(Name, Pid),
  Name = shards_meta:rename(Tab, Name).

%% @equiv safe_fixtable(Tab, Fix, shards_meta:get(Tab))
safe_fixtable(Tab, Fix) ->
  safe_fixtable(Tab, Fix, shards_meta:get(Tab)).

%% @doc
%% Equivalent to `ets:safe_fixtable/2'.
%%
%% Returns `true' if the function was applied successfully
%% on each partition, otherwise `false' is returned.
%%
%% @see ets:safe_fixtable/2.
%% @end
-spec safe_fixtable(Tab, Fix, Meta) -> true when
      Tab  :: tab(),
      Fix  :: boolean(),
      Meta :: shards_meta:t().
safe_fixtable(Tab, Fix, Meta) ->
  Map = {fun ets:safe_fixtable/2, [Fix]},
  Reduce = {fun erlang:'and'/2, true},
  mapred(Tab, Map, Reduce, Meta).

%% @equiv select(Tab, MatchSpec, shards_meta:get(Tab))
select(Tab, MatchSpec) ->
  select(Tab, MatchSpec, shards_meta:get(Tab)).

%% @doc
%% If 3rd argument is `pos_integer()' this function behaves
%% like `ets:select/3', otherwise, the 3rd argument is
%% assumed as `shards_meta:t()` and it behaves like
%% `ets:select/2'.
%%
%% The order in which results are returned might be not the same
%% as the original ETS function.
%%
%% @see ets:select/3.
%% @end
-spec select(Tab, MatchSpec, LimitOrMeta) -> {[Match], Cont} | '$end_of_table' | [Match] when
      Tab         :: tab(),
      MatchSpec   :: ets:match_spec(),
      LimitOrMeta :: pos_integer() | shards_meta:t(),
      Match       :: term(),
      Cont        :: continuation().
select(Tab, MatchSpec, Limit) when is_integer(Limit) ->
  select(Tab, MatchSpec, Limit, shards_meta:get(Tab));
select(Tab, MatchSpec, Meta) ->
  Map = {fun ets:select/2, [MatchSpec]},
  Reduce = fun erlang:'++'/2,
  mapred(Tab, Map, Reduce, Meta).

%% @doc
%% Equivalent to `ets:select/3'.
%%
%% The order in which results are returned might be not the same
%% as the original ETS function.
%%
%% @see ets:select/3.
%% @end
-spec select(Tab, MatchSpec, Limit, Meta) -> {[Match], Cont} | '$end_of_table' when
      Tab       :: tab(),
      MatchSpec :: ets:match_spec(),
      Limit     :: pos_integer(),
      Meta      :: shards_meta:t(),
      Match     :: term(),
      Cont      :: continuation().
select(Tab, MatchSpec, Limit, Meta) ->
  N = shards_meta:partitions(Meta),
  q(select, Tab, MatchSpec, Limit, q_fun(), Limit, N - 1, {[], nil}).

%% @doc
%% Equivalent to `ets:select/1'.
%%
%% The order in which results are returned might be not the same
%% as the original ETS function.
%%
%% @see ets:select/1.
%% @end
-spec select(Cont) -> {[Match], Cont} | '$end_of_table' when
      Match :: term(),
      Cont  :: continuation().
select({_, _, Limit, _, _} = Continuation) ->
  q(select, Continuation, q_fun(), Limit, []).

%% @equiv select_count(Tab, MatchSpec, shards_meta:get(Tab))
select_count(Tab, MatchSpec) ->
  select_count(Tab, MatchSpec, shards_meta:get(Tab)).

%% @doc
%% Equivalent to `ets:select_count/2'.
%%
%% @see ets:select_count/2.
%% @end
-spec select_count(Tab, MatchSpec, Meta) -> NumMatched when
      Tab        :: tab(),
      MatchSpec  :: ets:match_spec(),
      Meta       :: shards_meta:t(),
      NumMatched :: non_neg_integer().
select_count(Tab, MatchSpec, Meta) ->
  Map = {fun ets:select_count/2, [MatchSpec]},
  Reduce = {fun erlang:'+'/2, 0},
  mapred(Tab, Map, Reduce, Meta).

%% @equiv select_delete(Tab, MatchSpec, shards_meta:get(Tab))
select_delete(Tab, MatchSpec) ->
  select_delete(Tab, MatchSpec, shards_meta:get(Tab)).

%% @doc
%% Equivalent to `ets:select_delete/2'.
%%
%% @see ets:select_delete/2.
%% @end
-spec select_delete(Tab, MatchSpec, Meta) -> NumDeleted when
      Tab        :: tab(),
      MatchSpec  :: ets:match_spec(),
      Meta       :: shards_meta:t(),
      NumDeleted :: non_neg_integer().
select_delete(Tab, MatchSpec, Meta) ->
  Map = {fun ets:select_delete/2, [MatchSpec]},
  Reduce = {fun erlang:'+'/2, 0},
  mapred(Tab, Map, Reduce, Meta).

%% @equiv select_replace(Tab, MatchSpec, shards_meta:get(Tab))
select_replace(Tab, MatchSpec) ->
  select_replace(Tab, MatchSpec, shards_meta:get(Tab)).

%% @doc
%% Equivalent to `ets:select_replace/2'.
%%
%% @see ets:select_replace/2.
%% @end
-spec select_replace(Tab, MatchSpec, Meta) -> NumReplaced when
      Tab         :: tab(),
      MatchSpec   :: ets:match_spec(),
      Meta        :: shards_meta:t(),
      NumReplaced :: non_neg_integer().
select_replace(Tab, MatchSpec, Meta) ->
  Map = {fun ets:select_replace/2, [MatchSpec]},
  Reduce = {fun erlang:'+'/2, 0},
  mapred(Tab, Map, Reduce, Meta).

%% @equiv select_reverse(Tab, MatchSpec, shards_meta:get(Tab))
select_reverse(Tab, MatchSpec) ->
  select_reverse(Tab, MatchSpec, shards_meta:get(Tab)).

%% @doc
%% If 3rd argument is `pos_integer()' this function behaves
%% like `ets:select_reverse/3', otherwise, the 3rd argument is
%% assumed as `shards_meta:t()` and it behaves like
%% `ets:select_reverse/2'.
%%
%% The order in which results are returned might be not the same
%% as the original ETS function.
%%
%% @see ets:select_reverse/3.
%% @end
-spec select_reverse(Tab, MatchSpec, LimitOrMeta) -> {[Match], Cont} | '$end_of_table' | [Match] when
      Tab         :: tab(),
      MatchSpec   :: ets:match_spec(),
      LimitOrMeta :: pos_integer() | shards_meta:t(),
      Match       :: term(),
      Cont        :: continuation().
select_reverse(Tab, MatchSpec, Limit) when is_integer(Limit) ->
  select_reverse(Tab, MatchSpec, Limit, shards_meta:get(Tab));
select_reverse(Tab, MatchSpec, Meta) ->
  Map = {fun ets:select_reverse/2, [MatchSpec]},
  Reduce = fun erlang:'++'/2,
  mapred(Tab, Map, Reduce, Meta).

%% @doc
%% Equivalent to `ets:select_reverse/3'.
%%
%% The order in which results are returned might be not the same
%% as the original ETS function.
%%
%% @see ets:select_reverse/3.
%% @end
-spec select_reverse(Tab, MatchSpec, Limit, Meta) -> {[Match], Cont} | '$end_of_table' when
      Tab       :: tab(),
      MatchSpec :: ets:match_spec(),
      Limit     :: pos_integer(),
      Meta      :: shards_meta:t(),
      Match     :: term(),
      Cont      :: continuation().
select_reverse(Tab, MatchSpec, Limit, Meta) ->
  N = shards_meta:partitions(Meta),
  q(select_reverse, Tab, MatchSpec, Limit, q_fun(), Limit, N - 1, {[], nil}).

%% @doc
%% Equivalent to `ets:select_reverse/1'.
%%
%% The order in which results are returned might be not the same
%% as the original ETS function.
%%
%% @see ets:select_reverse/1.
%% @end
-spec select_reverse(Cont) -> {[Match], Cont} | '$end_of_table' when
      Cont  :: continuation(),
      Match :: term().
select_reverse({_, _, Limit, _, _} = Continuation) ->
  q(select_reverse, Continuation, q_fun(), Limit, []).

%% @equiv setopts(Tab, Opts, shards_meta:get(Tab))
setopts(Tab, Opts) ->
  setopts(Tab, Opts, shards_meta:get(Tab)).

%% @doc
%% Equivalent to `ets:setopts/2'.
%%
%% Returns `true' if the function was applied successfully on each partition,
%% otherwise, `false' is returned.
%%
%% @see ets:setopts/2.
%% @end
-spec setopts(Tab, Opts, Meta) -> true when
      Tab      :: tab(),
      Opts     :: Opt | [Opt],
      Opt      :: {heir, pid(), HeirData} | {heir, none},
      Meta     :: shards_meta:t(),
      HeirData :: term().
setopts(Tab, Opts, Meta) ->
  Map = {fun shards_partition:apply_ets_fun/3, [setopts, [Opts]]},
  Reduce = {fun erlang:'and'/2, true},
  mapred(Tab, Map, Reduce, {pid, Meta}).

%% @equiv tab2file(Tab, Filename, [])
tab2file(Tab, Filename) ->
  tab2file(Tab, Filename, []).

%% @doc
%% Equivalent to `ets:tab2file/3'.
%%
%% This function generates one file per partition using `ets:tab2file/3',
%% and also generates a master file with the given `Filename' that holds
%% the information of the created partition files so that they can be
%% recovered by calling `ets:file2tab/1,2'.
%%
%% @see ets:tab2file/3.
%% @end
-spec tab2file(Tab, Filename, Options) -> ok | {error, Reason} when
      Tab      :: tab(),
      Filename :: filename(),
      Options  :: [Option],
      Option   :: {extended_info, [md5sum | object_count]} | {sync, boolean()},
      Reason   :: term().
tab2file(Tab, Filename, Options) when ?is_filename(Filename) ->
  StrFilename = shards_lib:to_string(Filename),
  Metadata = shards_meta:get(Tab),

  PartitionFilenamePairs =
    shards_enum:reduce_while(fun({Idx, Partition}, Acc) ->
      PartitionFilename = StrFilename ++ "." ++ integer_to_list(Idx),

      case ets:tab2file(Partition, PartitionFilename, Options) of
        ok ->
          {cont, Acc#{Idx => PartitionFilename}};

        {error, _} = Error ->
          {halt, Error}
      end
    end, #{}, shards_meta:get_partition_tids(Tab)),

  case PartitionFilenamePairs of
    {error, _} = Error ->
      Error;

    _ ->
      TabInfo = maps:from_list(shards:info(Tab)),

      Header = #{
        name          => maps:get(name, TabInfo),
        type          => maps:get(type, TabInfo),
        protection    => maps:get(protection, TabInfo),
        keypos        => maps:get(keypos, TabInfo),
        size          => maps:get(size, TabInfo),
        named_table   => maps:get(named_table, TabInfo),
        extended_info => maps:get(extended_info, TabInfo, []),
        metadata      => Metadata,
        partitions    => PartitionFilenamePairs
      },

      shards_lib:write_tabfile(StrFilename, Header)
  end.

%% @equiv tab2list(Tab, shards_meta:get(Tab))
tab2list(Tab) ->
  tab2list(Tab, shards_meta:get(Tab)).

%% @doc
%% Equivalent to `ets:tab2list/1'.
%%
%% @see ets:tab2list/1.
%% @end
-spec tab2list(Tab, Meta) -> [Object] when
      Tab    :: tab(),
      Meta   :: shards_meta:t(),
      Object :: tuple().
tab2list(Tab, Meta) ->
  mapred(Tab, fun ets:tab2list/1, fun erlang:'++'/2, Meta).

%% @doc
%% Equivalent to `ets:tabfile_info/1'.
%%
%% Adds extra information about the partitions.
%%
%% @see ets:tabfile_info/1.
%% @end
-spec tabfile_info(Filename) -> {ok, TableInfo} | {error, Reason} when
      Filename  :: filename(),
      TableInfo :: [tabinfo_item()],
      Reason    :: term().
tabfile_info(Filename) when ?is_filename(Filename) ->
  try
    StrFilename = shards_lib:to_string(Filename),
    Header = shards_lib:read_tabfile(StrFilename),

    TabName = maps:get(name, Header),
    Metadata = maps:get(metadata, Header),
    NamedTable = lists:member(named_table, shards_meta:ets_opts(Metadata)),
    Partitions = maps:get(partitions, Header),

    ShardsTabInfo =
      maps:fold(fun(_, PartitionFN, Acc) ->
        case ets:tabfile_info(PartitionFN) of
          {ok, TabInfo} -> [TabInfo | Acc];
          Error         -> throw(Error)
        end
      end, [], Partitions),

    PartitionedTabInfo =
      lists:keystore(
        named_table,
        1,
        [
          {partitions, map_size(Partitions)}
          | parts_info(TabName, ShardsTabInfo)
        ],
        {named_table, NamedTable}
      ),

    {ok, PartitionedTabInfo}
  catch
    throw:Error -> Error
  end.

%% @equiv table(Tab, [])
table(Tab) ->
  table(Tab, []).

%% @equiv table(Tab, Options, shards_meta:get(Tab))
table(Tab, Options) ->
  table(Tab, Options, shards_meta:get(Tab)).

%% @doc
%% Similar to `ets:table/2', but it returns a list of `qlc:query_handle()';
%% one per partition.
%%
%% @see ets:table/2.
%% @end
-spec table(Tab, Options, Meta) -> QueryHandle when
      Tab            :: tab(),
      QueryHandle    :: qlc:query_handle(),
      Options        :: [Option] | Option,
      Meta           :: shards_meta:t(),
      Option         :: {n_objects, NObjects} | {traverse, TraverseMethod},
      NObjects       :: 'default' | pos_integer(),
      MatchSpec      :: ets:match_spec(),
      TraverseMethod :: first_next | last_prev | select | {select, MatchSpec}.
table(Tab, Options, Meta) ->
  mapred(Tab, {fun ets:table/2, [Options]}, Meta).

%% @equiv ets:test_ms(Tuple, MatchSpec)
test_ms(Tuple, MatchSpec) ->
  ets:test_ms(Tuple, MatchSpec).

%% @equiv take(Tab, Key, shards_meta:get(Tab))
take(Tab, Key) ->
  take(Tab, Key, shards_meta:get(Tab)).

%% @doc
%% Equivalent to `ets:take/2'.
%%
%% @see ets:take/2.
%% @end
-spec take(Tab, Key, Meta) -> [Object] when
      Tab    :: tab(),
      Key    :: term(),
      Meta   :: shards_meta:t(),
      Object :: tuple().
take(Tab, Key, Meta) ->
  PartTid = shards_partition:tid(Tab, Key, Meta),
  ets:take(PartTid, Key).

%% @equiv update_counter(Tab, Key, UpdateOp, shards_meta:get(Tab))
update_counter(Tab, Key, UpdateOp) ->
  update_counter(Tab, Key, UpdateOp, shards_meta:get(Tab)).

%% @doc
%% Equivalent to `ets:update_counter/4'.
%%
%% If the 4th argument is `shards_meta:t()', it behaves like
%% `ets:update_counter/3'.
%%
%% @see ets:update_counter/4.
%% @end
-spec update_counter(Tab, Key, UpdateOp, DefaultOrMeta) -> Result | [Result] when
      Tab           :: tab(),
      Key           :: term(),
      UpdateOp      :: term(),
      DefaultOrMeta :: tuple() | shards_meta:t(),
      Result        :: integer().
update_counter(Tab, Key, UpdateOp, DefaultOrMeta) ->
  case shards_meta:is_metadata(DefaultOrMeta) of
    true ->
      PartTid = shards_partition:tid(Tab, Key, DefaultOrMeta),
      ets:update_counter(PartTid, Key, UpdateOp);

    false ->
      update_counter(Tab, Key, UpdateOp, DefaultOrMeta, shards_meta:get(Tab))
  end.

%% @doc
%% Equivalent to `ets:update_counter/4'.
%%
%% @see ets:update_counter/4.
%% @end
-spec update_counter(Tab, Key, UpdateOp, Default, Meta) -> Result | [Result] when
      Tab      :: tab(),
      Key      :: term(),
      UpdateOp :: term(),
      Default  :: tuple(),
      Meta     :: shards_meta:t(),
      Result   :: integer().
update_counter(Tab, Key, UpdateOp, Default, Meta) ->
  PartTid = shards_partition:tid(Tab, Key, Meta),
  ets:update_counter(PartTid, Key, UpdateOp, Default).

%% @equiv update_element(Tab, Key, ElementSpec, shards_meta:get(Tab))
update_element(Tab, Key, ElementSpec) ->
  update_element(Tab, Key, ElementSpec, shards_meta:get(Tab)).

%% @doc
%% Equivalent to `ets:update_element/3'.
%%
%% @see ets:update_element/3.
%% @end
-spec update_element(Tab, Key, ElementSpec, Meta) -> boolean() when
      Tab         :: tab(),
      Key         :: term(),
      ElementSpec :: {Pos, Value} | [{Pos, Value}],
      Pos         :: pos_integer(),
      Value       :: term(),
      Meta        :: shards_meta:t().
update_element(Tab, Key, ElementSpec, Meta) ->
  PartTid = shards_partition:tid(Tab, Key, Meta),
  ets:update_element(PartTid, Key, ElementSpec).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
with_trap_exit(Fun) ->
  Flag = process_flag(trap_exit, true),

  try
    Fun()
  after
    process_flag(trap_exit, Flag)
  end.

%% @private
with_meta(Tab, Fun) ->
  try shards_meta:get(Tab) of
    Meta -> Fun(Meta)
  catch
    error:{unknown_table, Tab} -> undefined
  end.

%% @private
get_part_tid(Tab, Object, Meta) ->
  Key = shards_lib:object_key(Object, Meta),
  shards_partition:tid(Tab, Key, Meta).

%% @private
group_keys_by_partition(Tab, Objects, Meta) ->
  lists:foldr(fun(Object, Acc) ->
    PartTid = get_part_tid(Tab, Object, Meta),
    Acc#{PartTid => [Object | maps:get(PartTid, Acc, [])]}
  end, #{}, Objects).

%% @private
parts_info(Tab, InfoLists) ->
  parts_info(Tab, InfoLists, []).

%% @private
parts_info(Tab, [FirstInfo | RestInfoLists], ExtraAttrs) ->
  FirstInfo1 =
    shards_lib:keyupdate(fun(K, _V) ->
      ets:info(Tab, K)
    end, [id, name, named_table, owner], FirstInfo),

  Keys = [size | ExtraAttrs],

  lists:foldl(fun(InfoList, InfoListAcc) ->
    shards_lib:keyupdate(fun(K, V) ->
      {K, V1} = lists:keyfind(K, 1, InfoList),
      V + V1
    end, Keys, InfoListAcc)
  end, FirstInfo1, RestInfoLists).

%% @private
mapred(Tab, Map, Meta) ->
  mapred(Tab, Map, nil, Meta).

%% @private
mapred(Tab, Map, nil, Meta) ->
  mapred(Tab, Map, fun(E, Acc) -> [E | Acc] end, Meta);
mapred(Tab, Map, Reduce, {PartitionFun, Meta}) ->
  do_mapred(Tab, Map, Reduce, PartitionFun, Meta);
mapred(Tab, Map, Reduce, Meta) ->
  do_mapred(Tab, Map, Reduce, tid, Meta).

%% @private
do_mapred(Tab, Map, Reduce, PartFun, Meta) ->
  case {shards_meta:partitions(Meta), shards_meta:parallel(Meta)} of
    {Partitions, true} when Partitions > 1 ->
      ParallelTimeout = shards_meta:parallel_timeout(Meta),
      p_mapred(Tab, Map, Reduce, PartFun, Partitions, ParallelTimeout);

    {Partitions, false} ->
      s_mapred(Tab, Map, Reduce, PartFun, Partitions)
  end.

%% @private
s_mapred(Tab, {MapFun, Args}, {ReduceFun, AccIn}, PartFun, Partitions) ->
  shards_enum:reduce(fun(Part, Acc) ->
    PartitionId = shards_partition:PartFun(Tab, Part),
    MapRes = apply(MapFun, [PartitionId | Args]),
    ReduceFun(MapRes, Acc)
  end, AccIn, Partitions);
s_mapred(Tab, MapFun, ReduceFun, PartFun, Partitions) ->
  {Map, Reduce} = mapred_funs(MapFun, ReduceFun),
  s_mapred(Tab, Map, Reduce, PartFun, Partitions).

%% @private
p_mapred(Tab, {MapFun, Args}, {ReduceFun, AccIn}, PartFun, Partitions, ParallelTimeout) ->
  MapResults =
    shards_enum:pmap(fun(Idx) ->
      PartitionId = shards_partition:PartFun(Tab, Idx),
      apply(MapFun, [PartitionId | Args])
    end, ParallelTimeout, lists:seq(0, Partitions - 1)),

  lists:foldl(ReduceFun, AccIn, MapResults);
p_mapred(Tab, MapFun, ReduceFun, PartFun, Partitions, ParallelTimeout) ->
  {Map, Reduce} = mapred_funs(MapFun, ReduceFun),
  p_mapred(Tab, Map, Reduce, PartFun, Partitions, ParallelTimeout).

%% @private
mapred_funs(MapFun, ReduceFun) ->
  Map =
    case is_function(MapFun) of
      true -> {MapFun, []};
      _    -> MapFun
    end,

  {Map, {ReduceFun, []}}.

%% @private
q(_, Tab, MatchSpec, Limit, _, I, Shard, {Acc, Continuation}) when I =< 0 ->
  {Acc, {Tab, MatchSpec, Limit, Shard, Continuation}};
q(_, _, _, _, _, _, Shard, {[], _}) when Shard < 0 ->
  '$end_of_table';
q(_, Tab, MatchSpec, Limit, _, _, Shard, {Acc, _}) when Shard < 0 ->
  {Acc, {Tab, MatchSpec, Limit, Shard, '$end_of_table'}};
q(F, Tab, MatchSpec, Limit, QFun, I, Shard, {Acc, '$end_of_table'}) ->
  q(F, Tab, MatchSpec, Limit, QFun, I, Shard - 1, {Acc, nil});
q(F, Tab, MatchSpec, Limit, QFun, I, Shard, {Acc, _}) ->
  case ets:F(shards_partition:tid(Tab, Shard), MatchSpec, I) of
    {L, Cont} ->
      NewAcc = {QFun(L, Acc), Cont},
      q(F, Tab, MatchSpec, Limit, QFun, I - length(L), Shard, NewAcc);

    '$end_of_table' ->
      q(F, Tab, MatchSpec, Limit, QFun, I, Shard, {Acc, '$end_of_table'})
  end.

%% @private
q(_, {Tab, MatchSpec, Limit, Shard, Continuation}, _, I, Acc) when I =< 0 ->
  {Acc, {Tab, MatchSpec, Limit, Shard, Continuation}};
q(F, {Tab, MatchSpec, Limit, Shard, '$end_of_table'}, QFun, _I, Acc) ->
  q(F, Tab, MatchSpec, Limit, QFun, Limit, Shard - 1, {Acc, nil});
q(F, {Tab, MatchSpec, Limit, Shard, Continuation}, QFun, I, Acc) ->
  case ets:F(Continuation) of
    {L, Cont} ->
      NewAcc = QFun(L, Acc),
      q(F, {Tab, MatchSpec, Limit, Shard, Cont}, QFun, I - length(L), NewAcc);

    '$end_of_table' ->
      q(F, {Tab, MatchSpec, Limit, Shard, '$end_of_table'}, QFun, I, Acc)
  end.

%% @private
q_fun() ->
  fun(L1, L0) -> L1 ++ L0 end.
