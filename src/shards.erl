%%%-------------------------------------------------------------------
%%% @doc
%%% Shards is split into 4 main components:
%%%
%%% <ul>
%%% <li>
%%% `shards_local' - Implements Sharding but locally, on a single
%%% Erlang node.
%%% </li>
%%% <li>
%%% `shards_dist' - Implements Sharding but globally, running on top of
%%% multiple distributed Erlang nodes; `shards_dist' uses `shards_local'
%%% internally.
%%% </li>
%%% <li>
%%% `shards' - This is the main module, and it is the wrapper for
%%% `shards_local' and `shards_dist'.
%%% </li>
%%% <li>
%%% `shards_state' - This module encapsulates the `state', it is where
%%% the metadata about the partitioned table is stored, such as: number
%%% of shards or partitions, type of the table, the scope (if it is local
%%% or global), etc.
%%% </li>
%%% </ul>
%%%
%%% @see shards_local.
%%% @see shards_dist.
%%% @end
%%%-------------------------------------------------------------------
-module(shards).

%% Application Utilities
-export([
  start/0,
  stop/0
]).

%% ETS API
-export([
  all/0,
  delete/1, delete/2,
  delete_object/2,
  delete_all_objects/1,
  file2tab/1, file2tab/2,
  first/1,
  foldl/3,
  foldr/3,
  give_away/3,
  i/0,
  info/1, info/2,
  info_shard/1, info_shard/2,
  insert/2,
  insert_new/2,
  is_compiled_ms/1,
  last/1,
  lookup/2,
  lookup_element/3,
  match/2, match/3, match/1,
  match_delete/2,
  match_object/2, match_object/3, match_object/1,
  match_spec_compile/1,
  match_spec_run/2,
  member/2,
  new/2,
  next/2,
  prev/2,
  rename/2,
  safe_fixtable/2,
  select/2, select/3, select/1,
  select_count/2,
  select_delete/2,
  select_reverse/2, select_reverse/3, select_reverse/1,
  setopts/2,
  tab2file/2, tab2file/3,
  tab2list/1,
  tabfile_info/1,
  table/1, table/2,
  test_ms/2,
  take/2,
  update_counter/3, update_counter/4,
  update_element/3
]).

%% Distributed API
-export([
  join/2,
  leave/2,
  get_nodes/1
]).

%% Extras
-export([
  list/1,
  state/1
]).

%%%===================================================================
%%% Types & Macros
%%%===================================================================

%% @type continuation() = shards_local:continuation().
-type continuation() :: shards_local:continuation().

-export_type([
  continuation/0
]).

%% Macro to get the default module to use: `shards_local'.
-define(SHARDS, shards_local).

%%%===================================================================
%%% Application Utilities
%%%===================================================================

%% @doc Starts `shards' application.
-spec start() -> ok | {error, term()}.
start() ->
  application:start(shards).

%% @doc Stops `shards' application.
-spec stop() -> ok | {error, term()}.
stop() ->
  application:stop(shards).

%%%===================================================================
%%% Shards/ETS API
%%%===================================================================

%% @equiv shards_local:all/0
all() ->
  ?SHARDS:all().

%% @doc
%% Wrapper to `shards_local:delete/1' and `shards_dist:delete/1'.
%%
%% @see shards_local:delete/1.
%% @see shards_dist:delete/1.
%% @end
-spec delete(Tab :: atom()) -> true.
delete(Tab) ->
  Module = shards_state:module(Tab),
  Module:delete(Tab).

%% @doc
%% Wrapper to `shards_local:delete/3' and `shards_dist:delete/3'.
%%
%% @see shards_local:delete/3.
%% @see shards_dist:delete/3.
%% @end
-spec delete(Tab :: atom(), Key :: term()) -> true.
delete(Tab, Key) ->
  call(Tab, delete, [Tab, Key]).

%% @doc
%% Wrapper to `shards_local:delete_all_objects/2' and
%% `shards_dist:delete_all_objects/2'.
%%
%% @see shards_local:delete_all_objects/2.
%% @see shards_dist:delete_all_objects/2
%% @end
-spec delete_all_objects(Tab :: atom()) -> true.
delete_all_objects(Tab) ->
  call(Tab, delete_all_objects, [Tab]).

%% @doc
%% Wrapper to `shards_local:delete_object/3' and `shards_dist:delete_object/3'.
%%
%% @see shards_local:delete_object/3.
%% @see shards_dist:delete_object/3.
%% @end
-spec delete_object(Tab :: atom(), Object :: tuple()) -> true.
delete_object(Tab, Object) ->
  call(Tab, delete_object, [Tab, Object]).

%% @equiv file2tab(Filename, [])
file2tab(Filename) ->
  call_from_file(Filename, file2tab, [Filename]).

%% @doc
%% Wrapper to `shards_local:file2tab/2' and `shards_dist:file2tab/2'.
%%
%% @see shards_local:file2tab/2.
%% @see shards_dist:file2tab/2.
%% @end
-spec file2tab(
        Filename :: shards_local:filename(),
        Options  :: [Option]
      ) -> {ok, Tab :: atom()} | {error, Reason :: term()}
      when Option :: {verify, boolean()}.
file2tab(Filename, Options) ->
  call_from_file(Filename, file2tab, [Filename, Options]).

%% @doc
%% Wrapper to `shards_local:first/2' and `shards_dist:first/2'.
%%
%% @see shards_local:first/2.
%% @see shards_dist:first/2.
%% @end
-spec first(Tab :: atom()) -> Key :: term() | '$end_of_table'.
first(Tab) ->
  call(Tab, first, [Tab]).

%% @doc
%% Wrapper to `shards_local:foldl/4' and `shards_dist:foldl/4'.
%%
%% @see shards_local:foldl/4.
%% @see shards_dist:foldl/4.
%% @end
-spec foldl(
        Fun :: fun((Element :: term(), Acc) -> Acc),
        Acc :: term(),
        Tab :: atom()
      ) -> Acc
      when Acc :: term().
foldl(Function, Acc0, Tab) ->
  call(Tab, foldl, [Function, Acc0, Tab]).

%% @doc
%% Wrapper to `shards_local:foldr/4' and `shards_dist:foldr/4'.
%%
%% @see shards_local:foldr/4.
%% @see shards_dist:foldr/4.
%% @end
-spec foldr(
        Fun :: fun((Element :: term(), Acc) -> Acc),
        Acc :: term(),
        Tab :: atom()
      ) -> Acc
      when Acc :: term().
foldr(Function, Acc0, Tab) ->
  call(Tab, foldr, [Function, Acc0, Tab]).

%% @doc
%% Wrapper to `shards_local:give_away/4' and `shards_dist:give_away/4'.
%%
%% @see shards_local:give_away/4.
%% @see shards_dist:give_away/4.
%% @end
-spec give_away(Tab :: atom(), Pid :: pid(), GiftData :: term()) -> true.
give_away(Tab, Pid, GiftData) ->
  call(Tab, give_away, [Tab, Pid, GiftData]).

%% @equiv shards_local:i/0
i() ->
  ?SHARDS:i().

%% @doc
%% Wrapper to `shards_local:info/2' and `shards_dist:info/2'.
%%
%% @see shards_local:info/2.
%% @see shards_dist:info/2.
%% @end
-spec info(Tab :: atom()) ->
        InfoList | undefined
      when InfoList :: [shards_local:info_tuple() | {nodes, [node()]}].
info(Tab) ->
  case whereis(Tab) of
    undefined -> undefined;
    _         -> call(Tab, info, [Tab])
  end.

%% @doc
%% Wrapper to `shards_local:info/3' and `shards_dist:info/3'.
%%
%% @see shards_local:info/3.
%% @see shards_dist:info/3.
%% @end
-spec info(
        Tab   :: atom(),
        Item  :: shards_local:info_item() | nodes
      ) -> any() | undefined.
info(Tab, Item) ->
  case whereis(Tab) of
    undefined -> undefined;
    _         -> call(Tab, info, [Tab, Item])
  end.

%% @equiv shards_local:info_shard/1
info_shard(ShardTab) ->
  ?SHARDS:info_shard(ShardTab).

%% @equiv shards_local:info_shard/2
info_shard(ShardTab, Item) ->
  ?SHARDS:info_shard(ShardTab, Item).

%% @doc
%% Wrapper to `shards_local:insert/3' and `shards_dist:insert/3'.
%%
%% @see shards_local:insert/3.
%% @see shards_dist:insert/3.
%% @end
-spec insert(Tab :: atom(), ObjOrObjL :: tuple() | [tuple()]) -> true.
insert(Tab, ObjectOrObjects) ->
  call(Tab, insert, [Tab, ObjectOrObjects]).

%% @doc
%% Wrapper to `shards_local:insert_new/3' and `shards_dist:insert_new/3'.
%%
%% @see shards_local:insert_new/3.
%% @see shards_dist:insert_new/3.
%% @end
-spec insert_new(Tab :: atom(), ObjOrObjs) ->
        boolean() | {false, ObjOrObjs}
      when ObjOrObjs :: tuple() | [tuple()].
insert_new(Tab, ObjectOrObjects) ->
  call(Tab, insert_new, [Tab, ObjectOrObjects]).

%% @equiv shards_local:is_compiled_ms/1
is_compiled_ms(Term) ->
  ?SHARDS:is_compiled_ms(Term).

%% @doc
%% Wrapper to `shards_local:last/2' and `shards_dist:last/2'.
%%
%% @see shards_local:last/2.
%% @see shards_dist:last/2.
%% @end
-spec last(Tab :: atom()) -> Key :: term() | '$end_of_table'.
last(Tab) ->
  call(Tab, last, [Tab]).

%% @doc
%% Wrapper to `shards_local:lookup/3' and `shards_dist:lookup/3'.
%%
%% @see shards_local:lookup/3.
%% @see shards_dist:lookup/3.
%% @end
-spec lookup(Tab :: atom(), Key :: term()) -> Result :: [tuple()].
lookup(Tab, Key) ->
  call(Tab, lookup, [Tab, Key]).

%% @doc
%% Wrapper to `shards_local:lookup_element/4' and
%% `shards_dist:lookup_element/4'.
%%
%% @see shards_local:lookup_element/4.
%% @see shards_dist:lookup_element/4.
%% @end
-spec lookup_element(
        Tab :: atom(),
        Key :: term(),
        Pos :: pos_integer()
      ) -> Elem  :: term() | [term()].
lookup_element(Tab, Key, Pos) ->
  call(Tab, lookup_element, [Tab, Key, Pos]).

%% @doc
%% Wrapper to `shards_local:match/3' and `shards_dist:match/3'.
%%
%% @see shards_local:match/3.
%% @see shards_dist:match/3.
%% @end
-spec match(Tab :: atom(), Pattern :: ets:match_pattern()) ->
        [Match :: [term()]].
match(Tab, Pattern) ->
  call(Tab, match, [Tab, Pattern]).

%% @doc
%% Wrapper to `shards_local:match/4' and `shards_dist:match/4'.
%%
%% @see shards_local:match/4.
%% @see shards_dist:match/4.
%% @end
-spec match(
        Tab     :: atom(),
        Pattern :: ets:match_pattern(),
        Limit   :: pos_integer()
      ) -> {[Match :: term()], continuation()} | '$end_of_table'.
match(Tab, Pattern, Limit) ->
  call(Tab, match, [Tab, Pattern, Limit]).

%% @doc
%% Wrapper to `shards_local:match/2' and `shards_dist:match/2'.
%%
%% @see shards_local:match/2.
%% @see shards_dist:match/2.
%% @end
-spec match(Cont) ->
        {[Match :: term()], Cont} | '$end_of_table'
      when Cont :: continuation().
match(Continuation) ->
  Tab = hd(tuple_to_list(Continuation)),
  Module = shards_state:module(Tab),
  Module:match(Continuation).

%% @doc
%% Wrapper to `shards_local:match_delete/3' and `shards_dist:match_delete/3'.
%%
%% @see shards_local:match_delete/3.
%% @see shards_dist:match_delete/3.
%% @end
-spec match_delete(Tab :: atom(), Pattern :: ets:match_pattern()) -> true.
match_delete(Tab, Pattern) ->
  call(Tab, match_delete, [Tab, Pattern]).

%% @doc
%% Wrapper to `shards_local:match_object/3' and `shards_dist:match_object/3'.
%%
%% @see shards_local:match_object/3.
%% @see shards_dist:match_object/3.
%% @end
-spec match_object(Tab :: atom(), Pattern :: ets:match_pattern()) ->
        [Object :: tuple()].
match_object(Tab, Pattern) ->
  call(Tab, match_object, [Tab, Pattern]).

%% @doc
%% Wrapper to `shards_local:match_object/4' and `shards_dist:match_object/4'.
%%
%% @see shards_local:match_object/4.
%% @see shards_dist:match_object/4.
%% @end
-spec match_object(
        Tab     :: atom(),
        Pattern :: ets:match_pattern(),
        Limit   :: pos_integer()
      ) -> {[Match :: term()], continuation()} | '$end_of_table'.
match_object(Tab, Pattern, Limit) ->
  call(Tab, match_object, [Tab, Pattern, Limit]).

%% @doc
%% Wrapper to `shards_local:match_object/2' and `shards_dist:match_object/2'.
%%
%% @see shards_local:match_object/2.
%% @see shards_dist:match_object/2.
%% @end
-spec match_object(Cont) ->
        {[Match :: term()], Cont} | '$end_of_table'
      when Cont :: continuation().
match_object(Continuation) ->
  Tab = hd(tuple_to_list(Continuation)),
  Module = shards_state:module(Tab),
  Module:match_object(Continuation).

%% @equiv shards_local:match_spec_compile/1
match_spec_compile(MatchSpec) ->
  ?SHARDS:match_spec_compile(MatchSpec).

%% @equiv shards_local:match_spec_run/2
match_spec_run(List, CompiledMatchSpec) ->
  ?SHARDS:match_spec_run(List, CompiledMatchSpec).

%% @doc
%% Wrapper to `shards_local:member/3' and `shards_dist:member/3'.
%%
%% @see shards_local:member/3.
%% @see shards_dist:member/3.
%% @end
-spec member(Tab :: atom(), Key :: term()) -> boolean().
member(Tab, Key) ->
  call(Tab, member, [Tab, Key]).

%% @doc
%% Wrapper to `shards_local:new/2' and `shards_dist:new/2'.
%%
%% @see shards_local:new/2.
%% @see shards_dist:new/2.
%% @end
-spec new(Name, Options :: [shards_dist:option()]) -> Name when Name :: atom().
new(Name, Options) ->
  case lists:keyfind(scope, 1, Options) of
    {scope, g} -> shards_dist:new(Name, Options);
    _          -> shards_local:new(Name, Options)
  end.

%% @doc
%% Wrapper to `shards_local:next/3' and `shards_dist:next/3'.
%%
%% @see shards_local:next/3.
%% @see shards_dist:next/3.
%% @end
-spec next(Tab :: atom(), Key) -> Key | '$end_of_table' when Key :: term().
next(Tab, Key1) ->
  call(Tab, next, [Tab, Key1]).

%% @doc
%% Wrapper to `shards_local:prev/3' and `shards_dist:prev/3'.
%%
%% @see shards_local:prev/3.
%% @see shards_dist:prev/3.
%% @end
-spec prev(Tab :: atom(), Key) -> Key | '$end_of_table' when Key :: term().
prev(Tab, Key1) ->
  call(Tab, prev, [Tab, Key1]).

%% @doc
%% Wrapper to `shards_local:rename/3' and `shards_dist:rename/3'.
%%
%% @see shards_local:rename/3.
%% @see shards_dist:rename/3.
%% @end
-spec rename(Tab :: atom(), Name) -> Name | no_return() when Name :: atom().
rename(Tab, Name) ->
  call(Tab, rename, [Tab, Name]).

%% @doc
%% Wrapper to `shards_local:safe_fixtable/2' and `shards_dist:safe_fixtable/2'.
%%
%% @see safe_fixtable:select/2.
%% @see safe_fixtable:select/2.
%% @end
-spec safe_fixtable(Tab :: atom(), Fix :: boolean()) -> boolean().
safe_fixtable(Tab, Fix) ->
  call(Tab, safe_fixtable, [Tab, Fix]).

%% @doc
%% Wrapper to `shards_local:select/3' and `shards_dist:select/3'.
%%
%% @see shards_local:select/3.
%% @see shards_dist:select/3.
%% @end
-spec select(Tab :: atom(), MatchSpec :: ets:match_spec()) -> [Match :: term()].
select(Tab, MatchSpec) ->
  call(Tab, select, [Tab, MatchSpec]).

%% @doc
%% Wrapper to `shards_local:select/4' and `shards_dist:select/4'.
%%
%% @see shards_local:select/4.
%% @see shards_dist:select/4.
%% @end
-spec select(
        Tab       :: atom(),
        MatchSpec :: ets:match_spec(),
        Limit     :: pos_integer()
      ) -> {[Match :: term()], continuation()} | '$end_of_table'.
select(Tab, MatchSpec, Limit) ->
  call(Tab, select, [Tab, MatchSpec, Limit]).

%% @doc
%% Wrapper to `shards_local:select/2' and `shards_dist:select/2'.
%%
%% @see shards_local:select/2.
%% @see shards_dist:select/2.
%% @end
-spec select(Cont) ->
        {[Match :: term()], Cont} | '$end_of_table'
      when Cont :: continuation().
select(Continuation) ->
  Tab = hd(tuple_to_list(Continuation)),
  Module = shards_state:module(Tab),
  Module:select(Continuation).

%% @doc
%% Wrapper to `shards_local:select_count/3' and `shards_dist:select_count/3'.
%%
%% @see shards_local:select_count/3.
%% @see shards_dist:select_count/3.
%% @end
-spec select_count(Tab :: atom(), MatchSpec :: ets:match_spec()) ->
        NumMatched :: non_neg_integer().
select_count(Tab, MatchSpec) ->
  call(Tab, select_count, [Tab, MatchSpec]).

%% @doc
%% Wrapper to `shards_local:select_delete/3' and `shards_dist:select_delete/3'.
%%
%% @see shards_local:select_delete/3.
%% @see shards_dist:select_delete/3.
%% @end
-spec select_delete(Tab :: atom(), MatchSpec :: ets:match_spec()) ->
        NumDeleted :: non_neg_integer().
select_delete(Tab, MatchSpec) ->
  call(Tab, select_delete, [Tab, MatchSpec]).

%% @doc
%% Wrapper to `shards_local:select_reverse/3' and
%% `shards_dist:select_reverse/3'.
%%
%% @see shards_local:select_reverse/3.
%% @see shards_dist:select_reverse/3.
%% @end
-spec select_reverse(Tab :: atom(), MatchSpec :: ets:match_spec()) ->
        [Match :: term()].
select_reverse(Tab, MatchSpec) ->
  call(Tab, select_reverse, [Tab, MatchSpec]).

%% @doc
%% Wrapper to `shards_local:select_reverse/4' and
%% `shards_dist:select_reverse/4'.
%%
%% @see shards_local:select_reverse/4.
%% @see shards_dist:select_reverse/4.
%% @end
-spec select_reverse(
        Tab       :: atom(),
        MatchSpec :: ets:match_spec(),
        Limit     :: pos_integer()
      ) -> {[Match :: term()], continuation()} | '$end_of_table'.
select_reverse(Tab, MatchSpec, Limit) ->
  call(Tab, select_reverse, [Tab, MatchSpec, Limit]).

%% @doc
%% Wrapper to `shards_local:select_reverse/2' and
%% `shards_dist:select_reverse/2'.
%%
%% @see shards_local:select_reverse/2.
%% @see shards_dist:select_reverse/2.
%% @end
-spec select_reverse(Cont) ->
        {[Match :: term()], Cont} | '$end_of_table'
      when Cont :: continuation().
select_reverse(Continuation) ->
  Tab = hd(tuple_to_list(Continuation)),
  Module = shards_state:module(Tab),
  Module:select_reverse(Continuation).

%% @doc
%% Wrapper to `shards_local:setopts/3' and `shards_dist:setopts/3'.
%%
%% @see shards_local:setopts/3.
%% @see shards_dist:setopts/3.
%% @end
-spec setopts(Tab :: atom(), Opt | [Opt]) ->
        boolean()
      when Opt :: {heir, pid(), HeirData :: term()} | {heir, none}.
setopts(Tab, Opts) ->
  call(Tab, setopts, [Tab, Opts]).

%% @equiv tab2file(Tab, Filename, [])
tab2file(Tab, Filename) ->
  call(Tab, tab2file, [Tab, Filename]).

%% @doc
%% Wrapper to `shards_local:tab2file/4' and `shards_dist:tab2file/4'.
%%
%% @see shards_local:tab2file/4.
%% @see shards_dist:tab2file/4.
%% @end
-spec tab2file(
        Tab      :: atom(),
        Filename :: shards_local:filename(),
        Options  :: [Option]
      ) -> ok | {error, Reason :: term()}
      when Option  :: {extended_info, [ExtInfo]} | {sync, boolean()},
           ExtInfo :: md5sum | object_count.
tab2file(Tab, Filename, Options) ->
  call(Tab, tab2file, [Tab, Filename, Options]).

%% @doc
%% Wrapper to `shards_local:tab2list/2' and `shards_dist:tab2list/2'.
%%
%% @see shards_local:tab2list/2.
%% @see shards_dist:tab2list/2.
%% @end
-spec tab2list(Tab :: atom()) -> [Object :: tuple()].
tab2list(Tab) ->
  call(Tab, tab2list, [Tab]).

%% @doc
%% Wrapper to `shards_local:tabfile_info/1' and `shards_dist:tabfile_info/1'.
%%
%% @see shards_local:tabfile_info/1.
%% @see shards_dist:tabfile_info/1.
%% @end
-spec tabfile_info(Filename :: shards_local:filename()) ->
        {ok, TableInfo} | {error, Reason :: term()}
      when TableInfo :: [shards_local:tabinfo_item() | {nodes, [node()]}].
tabfile_info(Filename) ->
  call_from_file(Filename, tabfile_info, [Filename]).

%% @doc
%% Wrapper to `shards_local:table/2' and `shards_dist:table/2'.
%%
%% @see shards_local:table/2.
%% @see shards_dist:table/2.
%% @end
-spec table(Tab :: atom()) -> [QueryHandle :: qlc:query_handle()].
table(Tab) ->
  call(Tab, table, [Tab]).

%% @doc
%% Wrapper to `shards_local:table/3' and `shards_dist:table/3'.
%%
%% @see shards_local:table/3.
%% @see shards_dist:table/3.
%% @end
-spec table(Tab :: atom(), Options :: [Option] | Option) ->
        [qlc:query_handle()]
      when Option         :: {n_objects, NumObjs} | {traverse, TraverseMethod},
           NumObjs        :: default | pos_integer(),
           MS             :: ets:match_spec(),
           TraverseMethod :: first_next | last_prev | select | {select, MS}.
table(Tab, Options) ->
  call(Tab, table, [Tab, Options]).

%% @equiv shards_local:test_ms/2
test_ms(Tuple, MatchSpec) ->
  ?SHARDS:test_ms(Tuple, MatchSpec).

%% @doc
%% Wrapper to `shards_local:take/3' and `shards_dist:take/3'.
%%
%% @see shards_local:take/3.
%% @see shards_dist:take/3.
%% @end
-spec take(Tab :: atom(), Key :: term()) -> [Object :: tuple()].
take(Tab, Key) ->
  call(Tab, take, [Tab, Key]).

%% @doc
%% Wrapper to `shards_local:update_counter/4' and
%% `shards_dist:update_counter/4'.
%%
%% @see shards_local:update_counter/4.
%% @see shards_dist:update_counter/4.
%% @end
-spec update_counter(
        Tab      :: atom(),
        Key      :: term(),
        UpdateOp :: term()
      ) -> integer().
update_counter(Tab, Key, UpdateOp) ->
  call(Tab, update_counter, [Tab, Key, UpdateOp]).

%% @doc
%% Wrapper to `shards_local:update_counter/5' and
%% `shards_dist:update_counter/5'.
%%
%% @see shards_local:update_counter/5.
%% @see shards_dist:update_counter/5.
%% @end
-spec update_counter(
        Tab      :: atom(),
        Key      :: term(),
        UpdateOp :: term(),
        Default  :: tuple()
      ) -> integer().
update_counter(Tab, Key, UpdateOp, Default) ->
  call(Tab, update_counter, [Tab, Key, UpdateOp, Default]).

%% @doc
%% Wrapper to `shards_local:update_element/4' and
%% `shards_dist:update_element/4'.
%%
%% @see shards_local:update_element/4.
%% @see shards_dist:update_element/4.
%% @end
-spec update_element(
        Tab      :: atom(),
        Key      :: term(),
        ElemSpec :: {Pos, Value} | [{Pos, Value}]
      ) -> boolean()
      when Pos :: pos_integer(), Value :: term().
update_element(Tab, Key, ElemSpec) ->
  call(Tab, update_element, [Tab, Key, ElemSpec]).

%%%===================================================================
%%% Distributed API
%%%===================================================================

-spec join(Tab :: atom(), Nodes :: [node()]) -> CurrentNodes :: [node()].
join(Tab, Nodes) ->
  shards_dist:join(Tab, Nodes).

-spec leave(Tab :: atom(), Nodes :: [node()]) -> CurrentNodes :: [node()].
leave(Tab, Nodes) ->
  shards_dist:leave(Tab, Nodes).

-spec get_nodes(Tab :: atom()) -> Nodes :: [node()].
get_nodes(Tab) ->
  shards_dist:get_nodes(Tab).

%%%===================================================================
%%% Extended API
%%%===================================================================

%% @doc
%% Returns the list of shard names associated to the given `TabName'.
%% The shard names that were created in the `shards:new/2,3' fun.
%% <ul>
%% <li>`Tab': Table name.</li>
%% </ul>
%% @end
-spec list(Tab :: atom()) -> Result :: [atom()].
list(Tab) ->
  shards_lib:list_shards(Tab, shards_state:n_shards(Tab)).

%% @doc
%% Utility to get the `state' for the given table `Tab'.
%% @end
-spec state(Tab :: atom()) -> shards_state:state().
state(Tab) ->
  shards_state:get(Tab).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
call(Tab, Fun, Args) ->
  State = shards_state:get(Tab),
  Module = shards_state:module(State),
  apply(Module, Fun, Args ++ [State]).

%% @private
call_from_file(Filename, Fun, Args) ->
  try
    StrFilename = shards_lib:to_string(Filename),
    Metadata = tabfile_metadata(StrFilename),
    {state, State} = lists:keyfind(state, 1, Metadata),

    case shards_state:scope(State) of
      g -> apply(shards_dist, Fun, Args);
      _ -> apply(shards_local, Fun, Args)
    end
  catch
    throw:Error ->
      Error;

    error:{badarg, Arg} ->
      {error, {read_error, {file_error, Arg, enoent}}}
  end.

%% @private
tabfile_metadata(Filename) ->
  try
    shards_lib:read_tabfile(Filename)
  catch
    throw:{error, enoent} ->
      NodeFilename = shards_lib:to_string(node()) ++ "." ++ Filename,
      shards_lib:read_tabfile(NodeFilename)
  end.
