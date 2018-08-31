%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements local or single-node Sharding.
%%%
%%% <b>Shards</b> is compatible with ETS API, most of the functions
%%% preserves the same contract and semantic, but there are some
%%% exceptions (check functions documentation).
%%%
%%% Shards provides a logical view of a single ETS table, but
%%% internally, that logical table is partitioned in multiple
%%% physical ETS tables or <b>Shards</b>, where `Shards = [0 .. N-1]',
%%% and `N' is the number of shards you want to partition the table.
%%%
%%% The K/V pairs are distributed across these shards, therefore,
%%% some of the functions don't follow the same semantics as the
%%% original ones (analogous ETS functions).
%%%
%%% A good example of those are the query-based functions, which
%%% return multiple results; in case of `ordered_set', with a
%%% particular order. For example: `select/1,2,3`,
%%% `select_reverse/1,2,3', `match/1,2,3', `match_object/1,2,3', etc.
%%%
%%% For those cases, the order which results are returned is not
%%% guaranteed to be the same as the original ETS functions.
%%%
%%% Additionally to the ETS functions, `shards_local' module allows
%%% to pass an optional argument, the `State'. When `shards' is
%%% called without the `State', it must fetch the `state' first,
%%% and it is recovered doing an extra call to an ETS control table
%%% owned by `shards_owner_sup'. If any microsecond matters, you can
%%% skip it call by calling `shards_local' directly and passing
%%% the `State'. E.g.:
%%%
%%% ```
%%% % create a table
%%% tab_name = shards:new(tab_name, [{n_shards, 4}]).
%%%
%%% % you can get the state at any time by calling:
%%% State = shards_state:get(tab_name).
%%%
%%% % normal way
%%% shards:lookup(table, key1).
%%%
%%% % calling shards_local directly
%%% shards_local:lookup(table, key1, State).
%%%
%%% % if you have created the table with default options,
%%% % you can skip the sate
%%% shards_local:lookup(table, key1).
%%% '''
%%%
%%% Pools of shards can be added/removed dynamically. For example,
%%% using `shards:new/2' you can add more pools, and `shards:delete/1'
%%% to remove the pool you wish.
%%% @end
%%%-------------------------------------------------------------------
-module(shards_local).

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
  give_away/3, give_away/4,
  i/0,
  info/1, info/2, info/3,
  info_shard/1, info_shard/2,
  insert/2, insert/3,
  insert_new/2, insert_new/3,
  is_compiled_ms/1,
  last/1, last/2,
  lookup/2, lookup/3,
  lookup_element/3, lookup_element/4,
  match/2, match/3, match/4, match/1,
  match_delete/2, match_delete/3,
  match_object/2, match_object/3, match_object/4, match_object/1,
  match_spec_compile/1,
  match_spec_run/2,
  member/2, member/3,
  new/2,
  next/2, next/3,
  prev/2, prev/3,
  rename/2, rename/3,
  safe_fixtable/2, safe_fixtable/3,
  select/2, select/3, select/4, select/1,
  select_count/2, select_count/3,
  select_delete/2, select_delete/3,
  select_reverse/2, select_reverse/3, select_reverse/4, select_reverse/1,
  setopts/2, setopts/3,
  tab2file/2, tab2file/3, tab2file/4,
  tab2list/1, tab2list/2,
  tabfile_info/1,
  table/1, table/2, table/3,
  test_ms/2,
  take/2, take/3,
  update_counter/3, update_counter/4, update_counter/5,
  update_element/3, update_element/4
]).

%%%===================================================================
%%% Types & Macros
%%%===================================================================

%% ETS Types
-type access() :: public | protected | private.
-type tab() :: atom().
-type type() :: set | ordered_set | bag | duplicate_bag.
-type cont() ::
        '$end_of_table'
        | {tab(), integer(), integer(), ets:comp_match_spec(), list(), integer()}
        | {tab(), _, _, integer(), ets:comp_match_spec(), list(), integer(), integer()}.

%% @type tweaks() =
%%         {write_concurrency, boolean()}
%%         | {read_concurrency, boolean()}
%%         | compressed.
%%
%% ETS tweaks option
-type tweaks() ::
        {write_concurrency, boolean()}
        | {read_concurrency, boolean()}
        | compressed.

%% @type shards_opt() =
%%         {scope, l | g}
%%         | {n_shards, pos_integer()}
%%         | {pick_shard_fun, shards_state:pick_fun()}
%%         | {pick_node_fun, shards_state:pick_fun()}
%%         | {restart_strategy, one_for_one | one_for_all}.
%%
%% Shards extended options.
-type shards_opt() ::
        {scope, l | g}
        | {n_shards, pos_integer()}
        | {pick_shard_fun, shards_state:pick_fun()}
        | {pick_node_fun, shards_state:pick_fun()}
        | {restart_strategy, one_for_one | one_for_all}
        | {sup_name, module()}.

%% @type option() =
%%         type() | access() | named_table
%%         | {keypos, pos_integer()}
%%         | {heir, pid(), HeirData :: term()}
%%         | {heir, none} | tweaks()
%%         | shards_opt().
%%
%% Create table options – used by `new/2'.
-type option() ::
        type() | access() | named_table
        | {keypos, pos_integer()}
        | {heir, pid(), HeirData :: term()}
        | {heir, none} | tweaks()
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

%% @type continuation() = {
%%         Tab          :: atom(),
%%         MatchSpec    :: ets:match_spec(),
%%         Limit        :: pos_integer(),
%%         Shard        :: non_neg_integer(),
%%         Continuation :: cont()
%%       }.
%%
%% Defines the convention to `ets:select/1,3' continuation:
%% <ul>
%% <li>`Tab': Table name.</li>
%% <li>`MatchSpec': The `ets:match_spec()'.</li>
%% <li>`Limit': Results limit.</li>
%% <li>`Shard': Shards number.</li>
%% <li>`Continuation': The `ets:continuation()'.</li>
%% </ul>
-type continuation() :: {
        Tab          :: atom(),
        MatchSpec    :: ets:match_spec(),
        Limit        :: pos_integer(),
        Shard        :: non_neg_integer(),
        Continuation :: cont()
      }.

%% @type filename() = string() | binary() | atom().
-type filename() :: string() | binary() | atom().

% Exported Types
-export_type([
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
%%% ETS API
%%%===================================================================

%% @equiv ets:all()
all() ->
  ets:all().

%% @doc
%% This operation behaves like `ets:delete/1'.
%%
%% @see ets:delete/1.
%% @end
-spec delete(Tab :: atom()) -> true.
delete(Tab) ->
  SupName = shards_state:sup_name(Tab),
  ok = shards_sup:terminate_child(SupName, Tab),
  true.

%% @equiv delete(Tab, Key, shards_state:new())
delete(Tab, Key) ->
  delete(Tab, Key, shards_state:new()).

%% @doc
%% This operation behaves like `ets:delete/2'.
%%
%% @see ets:delete/2.
%% @end
-spec delete(
        Tab   :: atom(),
        Key   :: term(),
        State :: shards_state:state()
      ) -> true.
delete(Tab, Key, State) ->
  _ = mapred(Tab, Key, {fun ets:delete/2, [Key]}, nil, State, d),
  true.

%% @equiv delete_all_objects(Tab, shards_state:new())
delete_all_objects(Tab) ->
  delete_all_objects(Tab, shards_state:new()).

%% @doc
%% This operation behaves like `ets:delete_all_objects/1'.
%%
%% @see ets:delete_all_objects/1.
%% @end
-spec delete_all_objects(
        Tab   :: atom(),
        State :: shards_state:state()
      ) -> true.
delete_all_objects(Tab, State) ->
  _ = mapred(Tab, fun ets:delete_all_objects/1, State),
  true.

%% @equiv delete_object(Tab, Object, shards_state:new())
delete_object(Tab, Object) ->
  delete_object(Tab, Object, shards_state:new()).

%% @doc
%% This operation behaves like `ets:delete_object/2'.
%%
%% @see ets:delete_object/2.
%% @end
-spec delete_object(
        Tab    :: atom(),
        Object :: tuple(),
        State  :: shards_state:state()
      ) -> true.
delete_object(Tab, Object, State) when is_tuple(Object) ->
  Key = element(1, Object),
  _ = mapred(Tab, Key, {fun ets:delete_object/2, [Object]}, nil, State, d),
  true.

%% @equiv file2tab(Filename, [])
file2tab(Filename) ->
  file2tab(Filename, []).

%% @doc
%% Similar to `shards:file2tab/2'. Moreover, it restores the
%% supervision tree for the `shards' corresponding to the given
%% file, such as if they had been created using `shards:new/2,3'.
%%
%% @see ets:file2tab/2.
%% @end
-spec file2tab(
        Filename :: filename(),
        Options  :: [{verify, boolean()}]
      ) -> {ok, Tab :: atom()} | {error, Reason :: term()}.
file2tab(Filename, Options) when ?is_filename(Filename) ->
  try
    StrFilename = shards_lib:to_string(Filename),
    Metadata = shards_lib:read_tabfile(StrFilename),

    {name, Tab} = lists:keyfind(name, 1, Metadata),
    {state, State} = lists:keyfind(state, 1, Metadata),
    {shards, ShardTabs} = lists:keyfind(shards, 1, Metadata),

    Tab = new(Tab, [{restore, ShardTabs, Options} | state_to_tab_opts(State)]),
    {ok, Tab}
  catch
    throw:Error ->
      Error;

    error:{error, _} = Error ->
      Error;

    error:{badarg, Arg} ->
      {error, {read_error, {file_error, Arg, enoent}}}
  end.

%% private
state_to_tab_opts(State) ->
  Scope = shards_state:scope(State),
  NewState = maps:remove(module, shards_state:to_map(State)),
  maps:to_list(NewState#{scope => Scope}).

%% @equiv first(Tab, shards_state:new())
first(Tab) ->
  first(Tab, shards_state:new()).

%% @doc
%% This operation behaves similar to `ets:first/1'.
%% The order in which results are returned, might be not the same
%% as the original ETS function. Remember shards architecture
%% described at the beginning.
%%
%% @see ets:first/1.
%% @end
-spec first(Tab :: atom(), State :: shards_state:state()) ->
        Key :: term() | '$end_of_table'.
first(Tab, State) ->
  N = shards_state:n_shards(State),
  Shard = N - 1,
  first(Tab, ets:first(shards_lib:shard_name(Tab, Shard)), Shard).

%% @private
first(Tab, '$end_of_table', Shard) when Shard > 0 ->
  NextShard = Shard - 1,
  first(Tab, ets:first(shards_lib:shard_name(Tab, NextShard)), NextShard);
first(_, '$end_of_table', _) ->
  '$end_of_table';
first(_, Key, _) ->
  Key.

%% @equiv foldl(Function, Acc0, Tab, shards_state:new())
foldl(Fun, Acc0, Tab) ->
  foldl(Fun, Acc0, Tab, shards_state:new()).

%% @doc
%% This operation behaves like `ets:foldl/3'.
%%
%% @see ets:foldl/3.
%% @end
%-spec foldl(Function, Acc0, Tab, State) -> Acc1 when
-spec foldl(
        Fun   :: fun((Element :: term(), Acc) -> Acc),
        Acc   :: term(),
        Tab   :: atom(),
        State :: shards_state:state()
      ) -> Acc
      when Acc :: term().
foldl(Fun, Acc, Tab, State) ->
  N = shards_state:n_shards(State),
  fold(Tab, N, foldl, [Fun, Acc]).

%% @equiv foldr(Function, Acc0, Tab, shards_state:new())
foldr(Fun, Acc0, Tab) ->
  foldr(Fun, Acc0, Tab, shards_state:new()).

%% @doc
%% This operation behaves like `ets:foldr/3'.
%%
%% @see ets:foldr/3.
%% @end
-spec foldr(
        Fun   :: fun((Element :: term(), Acc) -> Acc),
        Acc   :: term(),
        Tab   :: atom(),
        State :: shards_state:state()
      ) -> Acc
      when Acc :: term().
foldr(Fun, Acc, Tab, State) ->
  N = shards_state:n_shards(State),
  fold(Tab, N, foldr, [Fun, Acc]).

%% @equiv give_away(Tab, Pid, GiftData, shards_state:new())
give_away(Tab, Pid, GiftData) ->
  give_away(Tab, Pid, GiftData, shards_state:new()).

%% @doc
%% Equivalent to `ets:give_away/3' for each shard table. It returns
%% a `boolean()' instead that just `true'. Returns `true' if the
%% function was applied successfully on each shard, otherwise
%% `false' is returned.
%%
%% <p><font color="red"><b>WARNING: It is not recommended execute
%% this function, since it might cause an unexpected behavior.
%% Once this function is executed, `shards' doesn't control/manage
%% the ETS shards anymore. So from this point, you should use
%% ETS API instead. Also it is recommended to run `shards:delete/1'
%% after run this function.
%% </b></font></p>
%%
%% @see ets:give_away/3.
%% @end
-spec give_away(
        Tab      :: atom(),
        Pid      :: pid(),
        GiftData :: term(),
        State    :: shards_state:state()
      ) -> true.
give_away(Tab, Pid, GiftData, State) ->
  Map = {fun shards_owner:apply_ets_fun/3, [give_away, [Pid, GiftData]]},
  Reduce = {fun(_, Acc) -> Acc end, true},
  mapred(Tab, Map, Reduce, State).

%% @equiv ets:i()
i() ->
  ets:i().

%% @equiv info(Tab, shards_state:new())
info(Tab) ->
  info(Tab, shards_state:new()).

%% @doc
%% If 2nd argument is `info_item()' this function behaves like
%% `ets:info/2', but if it is the `shards_state:state()',
%% it behaves like `ets:info/1'.
%%
%% This function also adds info about the shards `{shards, [atom()]}'.
%%
%% @see ets:info/1.
%% @see ets:info/2.
%% @end
-spec info(
        Tab         :: atom(),
        StateOrItem :: shards_state:state() | info_item()
      ) -> Value | [info_tuple()] | undefined
      when Value :: any().
info(Tab, Item) when is_atom(Item) ->
  info(Tab, Item, shards_state:new());
info(Tab, State) ->
  case whereis(Tab) of
    undefined ->
      undefined;

    _Pid ->
      InfoLists = mapred(Tab, fun ets:info/1, State),
      shards_info(Tab, InfoLists, [memory])
  end.

%% @doc
%% This operation is analogous to `ets:info/2'.
%%
%% @see ets:info/2.
%% @end
-spec info(
        Tab   :: atom(),
        Item  :: info_item(),
        State :: shards_state:state()
      ) -> any() | undefined.
info(Tab, Item, State) ->
  case info(Tab, State) of
    undefined -> undefined;
    TabInfo   -> shards_lib:keyfind(Item, TabInfo)
  end.

%% @equiv ets:info(ShardTab)
info_shard(ShardTab) ->
  ets:info(ShardTab).

%% @equiv ets:info(ShardTab, Item)
info_shard(ShardTab, Item) ->
  ets:info(ShardTab, Item).

%% @equiv insert(Tab, ObjOrObjs, shards_state:new())
insert(Tab, ObjOrObjs) ->
  insert(Tab, ObjOrObjs, shards_state:new()).

%% @doc
%% This operation behaves similar to `ets:insert_new/2', BUT with a big
%% difference, <b>IT IS NOT ATOMIC</b>, which means, if it fails inserting
%% some object, previous inserted objects are not rolled back, in that
%% case an error is raised.
%%
%% @see ets:insert/2.
%% @end
-spec insert(
        Tab       :: atom(),
        ObjOrObjs :: tuple() | [tuple()],
        State     :: shards_state:state()
      ) -> true | no_return().
insert(Tab, ObjOrObjs, State) when is_list(ObjOrObjs) ->
  maps:fold(fun(Shard, Group, Acc) ->
    Acc = ets:insert(Shard, Group)
  end, true, group_keys_by_shard(Tab, ObjOrObjs, State));
insert(Tab, ObjOrObjs, State) when is_tuple(ObjOrObjs) ->
  ets:insert(get_shard(Tab, ObjOrObjs, State), ObjOrObjs).

%% @equiv insert_new(Tab, ObjOrObjs, shards_state:new())
insert_new(Tab, ObjOrObjs) ->
  insert_new(Tab, ObjOrObjs, shards_state:new()).

%% @doc
%% This operation behaves similar to `ets:insert_new/2', BUT with a big
%% difference, <b>IT IS NOT ATOMIC</b>, which means, if it fails inserting
%% some object, previous inserted objects are not rolled back, in that case
%% only that object is affected, the rest may be successfully inserted.
%%
%% This function returns `true' if all entries were successfully inserted.
%% If one of the given entries within `ObjOrObjs' fails, the tuple
%% `{false, FailedObjs}' is returned, where `FailedObjs' contains the list
%% of the failed objects. If only one entry/object is passed to this function
%% and it fails, only `false' is returned.
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
%% {false,[{k1,1}]}
%% '''
%%
%% @see ets:insert_new/2.
%% @end
-spec insert_new(Tab :: atom(), ObjOrObjs, State :: shards_state:state()) ->
        boolean() | {false, ObjOrObjs}
      when ObjOrObjs :: tuple() | [tuple()].
insert_new(Tab, ObjOrObjs, State) when is_list(ObjOrObjs) ->
  Result =
    maps:fold(fun(Shard, Group, Acc) ->
      case do_insert_new(Tab, Shard, Group, State) of
        true  -> Acc;
        false -> Group ++ Acc
      end
    end, [], group_keys_by_shard(Tab, ObjOrObjs, State)),

  case Result of
    [] -> true;
    _  -> {false, Result}
  end;
insert_new(Tab, ObjOrObjs, State) when is_tuple(ObjOrObjs) ->
  do_insert_new(Tab, get_shard(Tab, ObjOrObjs, State), ObjOrObjs, State).

%% @private
do_insert_new(Tab, Shard, Objs, State) ->
  Key = shards_lib:key_from_object(Objs),

  case shards_state:eval_pick_shard(Key, r, State) of
    any ->
      Map = {fun ets:lookup/2, [Key]},
      Reduce = fun erlang:'++'/2,

      case mapred(Tab, Map, Reduce, State) of
        [] -> ets:insert_new(Shard, Objs);
        _  -> false
      end;

    _ ->
      ets:insert_new(Shard, Objs)
  end.

%% @equiv ets:is_compiled_ms(Term)
is_compiled_ms(Term) ->
  ets:is_compiled_ms(Term).

%% @equiv last(Tab, shards_state:new())
last(Tab) ->
  last(Tab, shards_state:new()).

%% @doc
%% This operation behaves similar to `ets:last/1'.
%% The order in which results are returned, might be not the same
%% as the original ETS function. Remember shards architecture
%% described at the beginning.
%%
%% @see ets:last/1.
%% @end
-spec last(Tab :: atom(), State :: shards_state:state()) ->
        Key :: term() | '$end_of_table'.
last(Tab, State) ->
  case ets:info(shards_lib:shard_name(Tab, 0), type) of
    ordered_set ->
      ets:last(shards_lib:shard_name(Tab, 0));

    _ ->
      first(Tab, State)
  end.

%% @equiv lookup(Tab, Key, shards_state:new())
lookup(Tab, Key) ->
  lookup(Tab, Key, shards_state:new()).

%% @doc
%% This operation behaves like `ets:lookup/2'.
%%
%% @see ets:lookup/2.
%% @end
-spec lookup(
        Tab :: atom(),
        Key :: term(),
        State :: shards_state:state()
      ) -> [Object :: tuple()].
lookup(Tab, Key, State) ->
  Map = {fun ets:lookup/2, [Key]},
  Reduce = fun erlang:'++'/2,
  mapred(Tab, Key, Map, Reduce, State, r).

%% @equiv lookup_element(Tab, Key, Pos, shards_state:new())
lookup_element(Tab, Key, Pos) ->
  lookup_element(Tab, Key, Pos, shards_state:new()).

%% @doc
%% This operation behaves like `ets:lookup_element/3'.
%%
%% @see ets:lookup_element/3.
%% @end
-spec lookup_element(
        Tab   :: atom(),
        Key   :: term(),
        Pos   :: pos_integer(),
        State :: shards_state:state()
      ) -> Element :: term() | [term()].
lookup_element(Tab, Key, Pos, State) ->
  Shard = shards_state:eval_pick_shard(Key, r, State),
  lookup_element(Tab, Shard, Key, Pos, State).

%% @private
lookup_element(Tab, any, Key, Pos, State) ->
  LookupElem = fun(Tx, Kx, Px) ->
    try
      ets:lookup_element(Tx, Kx, Px)
    catch
      error:badarg -> {error, notfound}
    end
  end,

  Filter =
    lists:filter(fun
      ({error, notfound}) -> false;
      (_)                 -> true
    end, mapred(Tab, {LookupElem, [Key, Pos]}, State)),

  case Filter of
    [] -> error(badarg);
    _  -> lists:append(Filter)
  end;
lookup_element(Tab, Shard, Key, Pos, _State) ->
  ShardName = shards_lib:shard_name(Tab, Shard),
  ets:lookup_element(ShardName, Key, Pos).

match(Tab, Pattern) ->
  match(Tab, Pattern, shards_state:new()).

%% @doc
%% If 3rd argument is `pos_integer()' this function behaves
%% like `ets:match/3', but if it is the `shards_state:state()',
%% it behaves like `ets:match/2'.
%%
%% The order in which results are returned, might be not the same
%% as the original ETS function. Remember shards architecture
%% described at the beginning.
%%
%% @see ets:match/2.
%% @see ets:match/3.
%% @end
-spec match(
        Tab          :: atom(),
        Pattern      :: ets:match_pattern(),
        StateOrLimit :: shards_state:state() | pos_integer()
      ) -> [Match] | {[Match], continuation()} | '$end_of_table'
      when Match :: [term()].
match(Tab, Pattern, Limit) when is_integer(Limit), Limit > 0 ->
  match(Tab, Pattern, Limit, shards_state:new());
match(Tab, Pattern, State) ->
  Map = {fun ets:match/2, [Pattern]},
  Reduce = fun erlang:'++'/2,
  mapred(Tab, Map, Reduce, State).

%% @doc
%% This operation behaves similar to `ets:match/3'.
%% The order in which results are returned, might be not the same
%% as the original ETS function. Remember shards architecture
%% described at the beginning.
%%
%% @see ets:match/3.
%% @end
-spec match(
        Tab     :: atom(),
        Pattern :: ets:match_pattern(),
        Limit   :: pos_integer(),
        State   :: shards_state:state()
      ) -> {[Match :: term()], continuation()} | '$end_of_table'.
match(Tab, Pattern, Limit, State) ->
  N = shards_state:n_shards(State),
  q(match, Tab, Pattern, Limit, q_fun(), Limit, N - 1, {[], nil}).

%% @doc
%% This operation behaves similar to `ets:match/1'.
%% The order in which results are returned, might be not the same
%% as the original ETS function. Remember shards architecture
%% described at the beginning.
%%
%% @see ets:match/1.
%% @end
-spec match(continuation()) ->
        {[Match :: term()], continuation()} | '$end_of_table'.
match({_, _, Limit, _, _} = Continuation) ->
  q(match, Continuation, q_fun(), Limit, []).

%% @equiv match_delete(Tab, Pattern, shards_state:new())
match_delete(Tab, Pattern) ->
  match_delete(Tab, Pattern, shards_state:new()).

%% @doc
%% This operation behaves like `ets:match_delete/2'.
%%
%% @see ets:match_delete/2.
%% @end
-spec match_delete(
        Tab     :: atom(),
        Pattern :: ets:match_pattern(),
        State   :: shards_state:state()
      ) -> true.
match_delete(Tab, Pattern, State) ->
  Map = {fun ets:match_delete/2, [Pattern]},
  Reduce = {fun erlang:'and'/2, true},
  mapred(Tab, Map, Reduce, State).

%% @equiv match_object(Tab, Pattern, shards_state:new())
match_object(Tab, Pattern) ->
  match_object(Tab, Pattern, shards_state:new()).

%% @doc
%% If 3rd argument is `pos_integer()' this function behaves like
%% `ets:match_object/3', but if it is the `shards_state:state()',
%% it behaves like `ets:match_object/2'.
%%
%% The order in which results are returned, might be not the same
%% as the original ETS function. Remember shards architecture
%% described at the beginning.
%%
%% @see ets:match_object/2.
%% @see ets:match_object/3.
%% @end
-spec match_object(
        Tab          :: atom(),
        Pattern      :: ets:match_pattern(),
        StateOrLimit :: shards_state:state() | pos_integer()
      ) -> [Object :: tuple()] | {[term()], continuation()} | '$end_of_table'.
match_object(Tab, Pattern, Limit) when is_integer(Limit), Limit > 0 ->
  match_object(Tab, Pattern, Limit, shards_state:new());
match_object(Tab, Pattern, State) ->
  Map = {fun ets:match_object/2, [Pattern]},
  Reduce = fun erlang:'++'/2,
  mapred(Tab, Map, Reduce, State).

%% @doc
%% This operation behaves similar to `ets:match_object/3'.
%% The order in which results are returned, might be not the same
%% as the original ETS function. Remember shards architecture
%% described at the beginning.
%%
%% @see ets:match_object/3.
%% @end
-spec match_object(
        Tab     :: atom(),
        Pattern :: ets:match_pattern(),
        Limit   :: pos_integer(),
        State   :: shards_state:state()
      ) -> {[Match :: term()], continuation()} | '$end_of_table'.
match_object(Tab, Pattern, Limit, State) ->
  N = shards_state:n_shards(State),
  q(match_object, Tab, Pattern, Limit, q_fun(), Limit, N - 1, {[], nil}).

%% @doc
%% This operation behaves similar to `ets:match_object/1'.
%% The order in which results are returned, might be not the same
%% as the original ETS function. Remember shards architecture
%% described at the beginning.
%%
%% @see ets:match_object/1.
%% @end
-spec match_object(continuation()) ->
        {[Match :: term()], continuation()} | '$end_of_table'.
match_object({_, _, Limit, _, _} = Continuation) ->
  q(match_object, Continuation, q_fun(), Limit, []).

%% @equiv ets:match_spec_compile(MatchSpec)
match_spec_compile(MatchSpec) ->
  ets:match_spec_compile(MatchSpec).

%% @equiv ets:match_spec_run(List, CompiledMatchSpec)
match_spec_run(List, CompiledMatchSpec) ->
  ets:match_spec_run(List, CompiledMatchSpec).

%% @equiv member(Tab, Key, shards_state:new())
member(Tab, Key) ->
  member(Tab, Key, shards_state:new()).

%% @doc
%% This operation behaves like `ets:member/2'.
%%
%% @see ets:member/2.
%% @end
-spec member(
        Tab   :: atom(),
        Key   :: term(),
        State :: shards_state:state()
      ) -> boolean().
member(Tab, Key, State) ->
  case mapred(Tab, Key, {fun ets:member/2, [Key]}, nil, State, r) of
    Result when is_list(Result) ->
      lists:member(true, Result);

    Result ->
      Result
  end.

%% @doc
%% This operation is analogous to `ets:new/2', BUT it behaves totally
%% different. When this function is called, instead of create a single
%% table, a new supervision tree is created and added to `shards_sup'.
%%
%% This supervision tree has a main supervisor `shards_sup' which
%% creates a control ETS table and also creates `N' number of
%% `shards_owner' (being `N' the number of shards). Each `shards_owner'
%% creates an ETS table to represent each shard, so this `gen_server'
%% acts as the table owner.
%%
%% Finally, when you create a table, internally `N' physical tables
%% are created (one per shard), but `shards' encapsulates all this
%% and you see only one logical table (similar to how a distributed
%% storage works).
%%
%% <b>IMPORTANT: By default, `NumShards = number of schedulers'.</b>
%%
%% @see ets:new/2.
%% @end
-spec new(Name, Options :: [option()]) -> Name when Name :: atom().
new(Name, Options) ->
  SupName = shards_lib:keyfind(sup_name, Options, shards_sup),
  do_new(SupName, Name, Options).

%% @private
do_new(SupName, Name, Options) ->
  case shards_sup:start_child(SupName, Name, Options) of
    {ok, Pid} ->
      true = register(Name, Pid),
      Name;

    {error, {shutdown, {_, _, {restore_error, Error}}}} ->
      error(Error);

    {error, Reason} ->
      error({badarg, Reason})
  end.

%% @equiv next(Tab, Key1, shards_state:new())
next(Tab, Key1) ->
  next(Tab, Key1, shards_state:new()).

%% @doc
%% This operation behaves similar to `ets:next/2'.
%% The order in which results are returned, might be not the same
%% as the original ETS function. Remember shards architecture
%% described at the beginning. It raises a `bad_pick_fun_ret'
%% exception in case of pick fun returns `any'.
%%
%% @see ets:next/2.
%% @end
-spec next(Tab :: atom(), Key, State :: shards_state:state()) ->
        Key | '$end_of_table'
      when Key :: term().
next(Tab, Key1, State) ->
  case shards_state:eval_pick_shard(Key1, r, State) of
    any ->
      error(bad_pick_fun_ret);

    Shard ->
      ShardName = shards_lib:shard_name(Tab, Shard),
      next_(Tab, ets:next(ShardName, Key1), Shard)
  end.

%% @private
next_(Tab, '$end_of_table', Shard) when Shard > 0 ->
  NextShard = Shard - 1,
  next_(Tab, ets:first(shards_lib:shard_name(Tab, NextShard)), NextShard);
next_(_, '$end_of_table', _) ->
  '$end_of_table';
next_(_, Key2, _) ->
  Key2.

%% @equiv prev(Tab, Key1, shards_state:new())
prev(Tab, Key1) ->
  prev(Tab, Key1, shards_state:new()).

%% @doc
%% This operation behaves similar to `ets:prev/2'.
%% The order in which results are returned, might be not the same
%% as the original ETS function. Remember shards architecture
%% described at the beginning.
%%
%% @see ets:prev/2.
%% @end
-spec prev(Tab :: atom(), Key, State :: shards_state:state()) ->
        Key | '$end_of_table'
      when Key :: term().
prev(Tab, Key1, State) ->
  case ets:info(shards_lib:shard_name(Tab, 0), type) of
    ordered_set ->
      ets:prev(shards_lib:shard_name(Tab, 0), Key1);

    _ ->
      next(Tab, Key1, State)
  end.

%% @equiv rename(Tab, Name, shards_state:new())
rename(Tab, Name) ->
  rename(Tab, Name, shards_state:new()).

%% @doc
%% Equivalent to `ets:rename/2'.
%%
%% Renames the table name and all its associated shard tables.
%% If something unexpected occurs during the process, an exception
%% will be thrown.
%%
%% @see ets:rename/2.
%% @end
-spec rename(Tab :: atom(), Name, State :: shards_state:state()) ->
        Name | no_return()
      when Name :: atom().
rename(Tab, Name, State) ->
  ok =
    lists:foreach(fun(Shard) ->
      ShardName = shards_lib:shard_name(Tab, Shard),
      NewShardName = shards_lib:shard_name(Name, Shard),
      NewShardName = do_rename(ShardName, NewShardName)
    end, shards_lib:iterator(State)),

  do_rename(Tab, Name).

%% @private
do_rename(OldName, NewName) ->
  NewName = ets:rename(OldName, NewName),
  Pid = shards_lib:get_pid(OldName),
  true = unregister(OldName),
  true = register(NewName, Pid),
  NewName.

%% @equiv safe_fixtable(Tab, Fix, shards_state:new())
safe_fixtable(Tab, Fix) ->
  safe_fixtable(Tab, Fix, shards_state:new()).

%% @doc
%% Equivalent to `ets:safe_fixtable/2' for each shard table.
%% It returns a `boolean()' instead that just `true'.
%% Returns `true' if the function was applied successfully
%% on each shard, otherwise `false' is returned.
%%
%% @see ets:safe_fixtable/2.
%% @end
-spec safe_fixtable(
        Tab   :: atom(),
        Fix   :: boolean(),
        State :: shards_state:state()
      ) -> boolean().
safe_fixtable(Tab, Fix, State) ->
  Map = {fun ets:safe_fixtable/2, [Fix]},
  Reduce = {fun erlang:'and'/2, true},
  mapred(Tab, Map, Reduce, State).

%% @equiv select(Tab, MatchSpec, shards_state:new())
select(Tab, MatchSpec) ->
  select(Tab, MatchSpec, shards_state:new()).

%% @doc
%% If 3rd argument is `pos_integer()' this function behaves like
%% `ets:select/3', but if it is the `shards_state:state()',
%% it behaves like `ets:select/2'.
%%
%% The order in which results are returned, might be not the same
%% as the original ETS function. Remember shards architecture
%% described at the beginning.
%%
%% @see ets:select/2.
%% @see ets:select/3.
%% @end
-spec select(
        Tab          :: atom(),
        MatchSpec    :: ets:match_spec(),
        StateOrLimit :: shards_state:state() | pos_integer()
      ) -> [Match] | {[Match], continuation()} | '$end_of_table'
      when Match :: term().
select(Tab, MatchSpec, Limit) when is_integer(Limit), Limit > 0 ->
  select(Tab, MatchSpec, Limit, shards_state:new());
select(Tab, MatchSpec, State) ->
  Map = {fun ets:select/2, [MatchSpec]},
  Reduce = fun erlang:'++'/2,
  mapred(Tab, Map, Reduce, State).

%% @doc
%% This operation behaves similar to `ets:select/3'.
%% The order in which results are returned, might be not the same
%% as the original ETS function. Remember shards architecture
%% described at the beginning.
%%
%% @see ets:select/3.
%% @end
-spec select(
        Tab       :: atom(),
        MatchSpec :: ets:match_spec(),
        Limit     :: pos_integer(),
        State     :: shards_state:state()
      ) -> {[Match :: term()], continuation()} | '$end_of_table'.
select(Tab, MatchSpec, Limit, State) ->
  N = shards_state:n_shards(State),
  q(select, Tab, MatchSpec, Limit, q_fun(), Limit, N - 1, {[], nil}).

%% @doc
%% This operation behaves similar to `ets:select/1'.
%% The order in which results are returned, might be not the same
%% as the original ETS function. Remember shards architecture
%% described at the beginning.
%%
%% @see ets:select/1.
%% @end
-spec select(continuation()) ->
        {[Match :: term()], continuation()} | '$end_of_table'.
select({_, _, Limit, _, _} = Continuation) ->
  q(select, Continuation, q_fun(), Limit, []).

%% @equiv select_count(Tab, MatchSpec, shards_state:new())
select_count(Tab, MatchSpec) ->
  select_count(Tab, MatchSpec, shards_state:new()).

%% @doc
%% This operation behaves like `ets:select_count/2'.
%%
%% @see ets:select_count/2.
%% @end
-spec select_count(
        Tab       :: atom(),
        MatchSpec :: ets:match_spec(),
        State     :: shards_state:state()
      ) -> non_neg_integer().
select_count(Tab, MatchSpec, State) ->
  Map = {fun ets:select_count/2, [MatchSpec]},
  Reduce = {fun erlang:'+'/2, 0},
  mapred(Tab, Map, Reduce, State).

%% @equiv select_delete(Tab, MatchSpec, shards_state:new())
select_delete(Tab, MatchSpec) ->
  select_delete(Tab, MatchSpec, shards_state:new()).

%% @doc
%% This operation behaves like `ets:select_delete/2'.
%%
%% @see ets:select_delete/2.
%% @end
-spec select_delete(
        Tab       :: atom(),
        MatchSpec :: ets:match_spec(),
        State     :: shards_state:state()
      ) -> non_neg_integer().
select_delete(Tab, MatchSpec, State) ->
  Map = {fun ets:select_delete/2, [MatchSpec]},
  Reduce = {fun erlang:'+'/2, 0},
  mapred(Tab, Map, Reduce, State).

%% @equiv select_reverse(Tab, MatchSpec, shards_state:new())
select_reverse(Tab, MatchSpec) ->
  select_reverse(Tab, MatchSpec, shards_state:new()).

%% @doc
%% If 3rd argument is `pos_integer()' this function behaves like
%% `ets:select_reverse/3', but if it is the `shards_state:state()',
%% it behaves like `ets:select_reverse/2'.
%%
%% The order in which results are returned, might be not the same
%% as the original ETS function. Remember shards architecture
%% described at the beginning.
%%
%% @see ets:select_reverse/2.
%% @see ets:select_reverse/3.
%% @end
-spec select_reverse(
        Tab          :: atom(),
        MatchSpec    :: ets:match_spec(),
        StateOrLimit :: shards_state:state() | pos_integer()
      ) -> [Match] | {[Match], continuation()} | '$end_of_table'
      when Match :: term().
select_reverse(Tab, MatchSpec, Limit) when is_integer(Limit), Limit > 0 ->
  select_reverse(Tab, MatchSpec, Limit, shards_state:new());
select_reverse(Tab, MatchSpec, State) ->
  Map = {fun ets:select_reverse/2, [MatchSpec]},
  Reduce = fun erlang:'++'/2,
  mapred(Tab, Map, Reduce, State).

%% @doc
%% This operation behaves similar to `ets:select_reverse/3'.
%% The order in which results are returned, might be not the same
%% as the original ETS function. Remember shards architecture
%% described at the beginning.
%%
%% @see ets:select_reverse/3.
%% @end
-spec select_reverse(
        Tab       :: atom(),
        MatchSpec :: ets:match_spec(),
        Limit     :: pos_integer(),
        State     :: shards_state:state()
      ) -> {[Match :: term()], continuation()} | '$end_of_table'.
select_reverse(Tab, MatchSpec, Limit, State) ->
  N = shards_state:n_shards(State),
  q(select_reverse, Tab, MatchSpec, Limit, q_fun(), Limit, N - 1, {[], nil}).

%% @doc
%% This operation behaves similar to `ets:select_reverse/1'.
%% The order in which results are returned, might be not the same
%% as the original ETS function. Remember shards architecture
%% described at the beginning.
%%
%% @see ets:select_reverse/1.
%% @end
-spec select_reverse(continuation()) ->
        {[Match :: term()], continuation()} | '$end_of_table'.
select_reverse({_, _, Limit, _, _} = Continuation) ->
  q(select_reverse, Continuation, q_fun(), Limit, []).

%% @equiv setopts(Tab, Opts, shards_state:new())
setopts(Tab, Opts) ->
  setopts(Tab, Opts, shards_state:new()).

%% @doc
%% Equivalent to `ets:setopts/2' for each shard table. It returns
%% a `boolean()' instead that just `true'. Returns `true' if the
%% function was applied successfully on each shard, otherwise
%% `false' is returned.
%%
%% @see ets:setopts/2.
%% @end
-spec setopts(Tab :: atom(), Opt | [Opt], State :: shards_state:state()) ->
        boolean()
      when Opt :: {heir, pid(), HeirData :: term()} | {heir, none}.
setopts(Tab, Opts, State) ->
  Map = {fun shards_owner:apply_ets_fun/3, [setopts, [Opts]]},
  Reduce = {fun erlang:'and'/2, true},
  mapred(Tab, Map, Reduce, State).

%% @equiv tab2file(Tab, Filenames, shards_state:new())
tab2file(Tab, Filename) ->
  tab2file(Tab, Filename, shards_state:new()).

%% @equiv tab2file/4
tab2file(Tab, Filename, Options) when is_list(Options) ->
  tab2file(Tab, Filename, Options, shards_state:new());
tab2file(Tab, Filename, State) ->
  tab2file(Tab, Filename, [], State).

%% @doc
%% Similar to `ets:tab2file/3', but it behaves different.
%% This function generates one file per shard using `ets:tab2file/3',
%% and also generate a master file with the given `Filename' that
%% holds the information about the other shards files in order to
%% be able to recover them using `ets:file2tab/1,2'.
%%
%% @see ets:tab2file/3.
%% @end
-spec tab2file(
        Tab      :: atom(),
        Filename :: filename(),
        Options  :: [Option],
        State    :: shards_state:state()
      ) -> ok | {error, Reason :: term()}
      when Option ::
             {extended_info, [md5sum | object_count]}
             | {sync, boolean()}
             | {nodes, [node()]}.
tab2file(Tab, Filename, Options, State) when ?is_filename(Filename) ->
  StrFilename = shards_lib:to_string(Filename),

  {Nodes, NewOpts} =
    case lists:keytake(nodes, 1, Options) of
      {value, {nodes, Val}, Opts1} ->
        {Val, Opts1};

      _ ->
        {[node()], Options}
    end,

  ShardFilenamePairs =
    shards_lib:reduce_while(fun(Shard, Acc) ->
      ShardName = shards_lib:shard_name(Tab, Shard),
      ShardFilename = StrFilename ++ "." ++ integer_to_list(Shard),

      case ets:tab2file(ShardName, ShardFilename, NewOpts) of
        ok ->
          {cont, [{ShardName, ShardFilename} | Acc]};

        {error, _} = Error ->
          {halt, Error}
      end
    end, [], shards_lib:iterator(State)),

  case ShardFilenamePairs of
    {error, _} = Error ->
      Error;

    _ ->
      FileContent = [
        {name, Tab},
        {state, State},
        {shards, ShardFilenamePairs},
        {nodes, Nodes}
      ],

      shards_lib:write_tabfile(StrFilename, FileContent)
  end.

%% @equiv tab2list(Tab, shards_state:new())
tab2list(Tab) ->
  tab2list(Tab, shards_state:new()).

%% @doc
%% This operation behaves like `ets:tab2list/1'.
%%
%% @see ets:tab2list/1.
%% @end
-spec tab2list(Tab :: atom(),  State :: shards_state:state()) ->
        [Object :: tuple()].
tab2list(Tab, State) ->
  mapred(Tab, fun ets:tab2list/1, fun erlang:'++'/2, State).

%% @doc
%% This operation is analogous to `ets:tabfile_info/1', but it
%% adds info about the shards `{shards, [atom()]}'.
%%
%% @see ets:tabfile_info/1.
%% @end
-spec tabfile_info(filename()) ->
        {ok, [tabinfo_item()]} | {error, Reason :: term()}.
tabfile_info(Filename) when ?is_filename(Filename) ->
  try
    StrFilename = shards_lib:to_string(Filename),
    Metadata = shards_lib:read_tabfile(StrFilename),

    {name, Tab} = lists:keyfind(name, 1, Metadata),
    {shards, ShardTabs} = lists:keyfind(shards, 1, Metadata),

    ShardsTabInfo =
      lists:foldl(fun({_, ShardFN}, Acc) ->
        case ets:tabfile_info(ShardFN) of
          {ok, TabInfo} -> [TabInfo | Acc];
          Error         -> throw(Error)
        end
      end, [], ShardTabs),

    {ok, shards_info(Tab, ShardsTabInfo)}
  catch
    throw:Error -> Error
  end.

%% @equiv table(Tab, shards_state:new())
table(Tab) ->
  table(Tab, shards_state:new()).

%% @equiv table/3
table(Tab, Options) when is_list(Options) ->
  table(Tab, Options, shards_state:new());
table(Tab, State) ->
  table(Tab, [], State).

%% @doc
%% Similar to `ets:table/2', but it returns a list of `ets:table/2'
%% responses, one for each shard table.
%%
%% @see ets:table/2.
%% @end
-spec table(
        Tab     :: atom(),
        Options :: [Option] | Option,
        State   :: shards_state:state()
      ) -> [qlc:query_handle()]
      when NObjs  :: default | pos_integer(),
           MS     :: ets:match_spec(),
           Option ::
             {n_objects, NObjs}
             | {traverse, first_next | last_prev | select | {select, MS}}.
table(Tab, Options, State) ->
  mapred(Tab, {fun ets:table/2, [Options]}, State).

%% @equiv ets:test_ms(Tuple, MatchSpec)
test_ms(Tuple, MatchSpec) ->
  ets:test_ms(Tuple, MatchSpec).

%% @equiv take(Tab, Key, shards_state:new())
take(Tab, Key) ->
  take(Tab, Key, shards_state:new()).

%% @doc
%% This operation behaves like `ets:take/2'.
%%
%% @see ets:take/2.
%% @end
-spec take(Tab :: atom(), Key :: term(), State :: shards_state:state()) ->
        [Object :: tuple()].
take(Tab, Key, State) ->
  Map = {fun ets:take/2, [Key]},
  Reduce = fun erlang:'++'/2,
  mapred(Tab, Key, Map, Reduce, State, r).

%% @equiv update_counter(Tab, Key, UpdateOp, shards_state:new())
update_counter(Tab, Key, UpdateOp) ->
  update_counter(Tab, Key, UpdateOp, shards_state:new()).

%% @doc
%% This operation behaves like `ets:update_counter/3'.
%%
%% @see ets:update_counter/3.
%% @end
-spec update_counter(
        Tab      :: atom(),
        Key      :: term(),
        UpdateOp :: term(),
        State    :: shards_state:state()
      ) -> Result :: integer().
update_counter(Tab, Key, UpdateOp, State) ->
  Map = {fun ets:update_counter/3, [Key, UpdateOp]},
  mapred(Tab, Key, Map, nil, State, w).

%% @doc
%% This operation behaves like `ets:update_counter/4'.
%%
%% @see ets:update_counter/4.
%% @end
-spec update_counter(
        Tab      :: atom(),
        Key      :: term(),
        UpdateOp :: term(),
        Default  :: tuple(),
        State    :: shards_state:state()
      ) -> Result :: integer().
update_counter(Tab, Key, UpdateOp, Default, State) ->
  Map = {fun ets:update_counter/4, [Key, UpdateOp, Default]},
  mapred(Tab, Key, Map, nil, State, w).

%% @equiv update_element(Tab, Key, ElementSpec, shards_state:new())
update_element(Tab, Key, ElementSpec) ->
  update_element(Tab, Key, ElementSpec, shards_state:new()).

%% @doc
%% This operation behaves like `ets:update_element/3'.
%%
%% @see ets:update_element/3.
%% @end
-spec update_element(
        Tab         :: atom(),
        Key         :: term(),
        ElementSpec :: {Pos, Value} | [{Pos, Value}],
        State       :: shards_state:state()
      ) -> boolean()
      when Pos :: pos_integer(), Value :: term().
update_element(Tab, Key, ElementSpec, State) ->
  Map = {fun ets:update_element/3, [Key, ElementSpec]},
  mapred(Tab, Key, Map, nil, State, w).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
mapred(Tab, Map, State) ->
  mapred(Tab, Map, nil, State).

%% @private
mapred(Tab, Map, Reduce, State) ->
  mapred(Tab, nil, Map, Reduce, State, r).

%% @private
mapred(Tab, Key, Map, nil, State, Op) ->
  mapred(Tab, Key, Map, fun(E, Acc) -> [E | Acc] end, State, Op);
mapred(Tab, nil, Map, Reduce, State, _) ->
  case shards_state:n_shards(State) of
    N when N =< 1 ->
      s_mapred(Tab, N, Map, Reduce);

    N ->
      p_mapred(Tab, N, Map, Reduce)
  end;
mapred(Tab, Key, {MapFun, Args} = Map, Reduce, State, Op) ->
  N = shards_state:n_shards(State),
  PickShardFun = shards_state:pick_shard_fun(State),

  case PickShardFun(Key, N, Op) of
    any ->
      s_mapred(Tab, N, Map, Reduce);

    Shard ->
      apply(MapFun, [shards_lib:shard_name(Tab, Shard) | Args])
  end.

%% @private
s_mapred(Tab, NumShards, {MapFun, Args}, {ReduceFun, AccIn}) ->
  lists:foldl(fun(Shard, Acc) ->
    MapRes = apply(MapFun, [shards_lib:shard_name(Tab, Shard) | Args]),
    ReduceFun(MapRes, Acc)
  end, AccIn, shards_lib:iterator(NumShards));
s_mapred(Tab, NumShards, MapFun, ReduceFun) ->
  {Map, Reduce} = mapred_funs(MapFun, ReduceFun),
  s_mapred(Tab, NumShards, Map, Reduce).

%% @private
p_mapred(Tab, NumShards, {MapFun, Args}, {ReduceFun, AccIn}) ->
  Tasks =
    lists:foldl(fun(Shard, Acc) ->
      AsyncTask =
        shards_task:async(fun() ->
          apply(MapFun, [shards_lib:shard_name(Tab, Shard) | Args])
        end),

      [AsyncTask | Acc]
    end, [], shards_lib:iterator(NumShards)),

  lists:foldl(fun(Task, Acc) ->
    MapRes = shards_task:await(Task),
    ReduceFun(MapRes, Acc)
  end, AccIn, Tasks);
p_mapred(Tab, NumShards, MapFun, ReduceFun) ->
  {Map, Reduce} = mapred_funs(MapFun, ReduceFun),
  p_mapred(Tab, NumShards, Map, Reduce).

%% @private
mapred_funs(MapFun, ReduceFun) ->
  Map =
    case is_function(MapFun) of
      true -> {MapFun, []};
      _    -> MapFun
    end,

  {Map, {ReduceFun, []}}.

%% @private
get_shard(Tab, Object, State) ->
  get_shard(Tab, Object, w, State) .

%% @private
get_shard(Tab, Object, Op, State) ->
  Key = element(1, Object),
  shards_lib:shard_name(Tab, shards_state:eval_pick_shard(Key, Op, State)).

%% @private
group_keys_by_shard(Tab, Objects, State) ->
  lists:foldr(fun(Object, Acc) ->
    Shard = get_shard(Tab, Object, State),
    Acc#{Shard => [Object | maps:get(Shard, Acc, [])]}
  end, #{}, Objects).

%% @private
fold(Tab, NumShards, Fold, [Fun, Acc]) ->
  lists:foldl(fun(Shard, FoldAcc) ->
    ShardName = shards_lib:shard_name(Tab, Shard),
    apply(ets, Fold, [Fun, FoldAcc, ShardName])
  end, Acc, shards_lib:iterator(NumShards)).

%% @private
shards_info(Tab, InfoLists) ->
  shards_info(Tab, InfoLists, []).

%% @private
shards_info(Tab, [FirstInfo | RestInfoLists], ExtraAttrs) ->
  {name, InitShard} = lists:keyfind(name, 1, FirstInfo),
  FirstInfo1 = lists:keystore(name, 1, FirstInfo, {name, Tab}),

  lists:foldl(fun(InfoList, InfoListAcc) ->
    shards_lib:keyupdate(fun
      (shards, Shards) ->
        {name, ShardName} = lists:keyfind(name, 1, InfoList),
        [ShardName | Shards];

      (K, V) ->
        {K, V1} = lists:keyfind(K, 1, InfoList),
        V + V1
    end, [size, shards] ++ ExtraAttrs, InfoListAcc)
  end, [{shards, [InitShard]} | FirstInfo1], RestInfoLists).

%% @private
q(_, Tab, MatchSpec, Limit, _, 0, Shard, {Acc, Continuation}) ->
  {Acc, {Tab, MatchSpec, Limit, Shard, Continuation}};
q(_, _, _, _, _, _, Shard, {[], _}) when Shard < 0 ->
  '$end_of_table';
q(_, Tab, MatchSpec, Limit, _, _, Shard, {Acc, _}) when Shard < 0 ->
  {Acc, {Tab, MatchSpec, Limit, Shard, '$end_of_table'}};
q(F, Tab, MatchSpec, Limit, QFun, I, Shard, {Acc, '$end_of_table'}) ->
  q(F, Tab, MatchSpec, Limit, QFun, I, Shard - 1, {Acc, nil});
q(F, Tab, MatchSpec, Limit, QFun, I, Shard, {Acc, _}) ->
  case ets:F(shards_lib:shard_name(Tab, Shard), MatchSpec, I) of
    {L, Cont} ->
      NewAcc = {QFun(L, Acc), Cont},
      q(F, Tab, MatchSpec, Limit, QFun, I - length(L), Shard, NewAcc);

    '$end_of_table' ->
      q(F, Tab, MatchSpec, Limit, QFun, I, Shard, {Acc, '$end_of_table'})
  end.

%% @private
q(_, {Tab, MatchSpec, Limit, Shard, Continuation}, _, 0, Acc) ->
  {Acc, {Tab, MatchSpec, Limit, Shard, Continuation}};
q(F, {Tab, MatchSpec, Limit, Shard, '$end_of_table'}, QFun, I, Acc) ->
  q(F, Tab, MatchSpec, Limit, QFun, I, Shard - 1, {Acc, nil});
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
