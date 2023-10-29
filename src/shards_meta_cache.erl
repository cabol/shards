%%%-------------------------------------------------------------------
%%% @doc
%%% Metadata cache storage using the `persistent_term'.
%%% @end
%%%-------------------------------------------------------------------
-module(shards_meta_cache).

-behaviour(gen_server).

%% API
-export([
  start_link/2,
  child_spec/2,
  get_meta/1,
  put_meta/2,
  del_meta/1
]).

%% gen_server callbacks
-export([
  init/1,
  handle_call/3,
  handle_cast/2,
  handle_info/2,
  terminate/2
]).

%% Inline common instructions
-compile({inline, [
  get_meta/1,
  put_meta/2
]}).

%% State
-record(state, {
  tab     :: shards:tab(),
  tab_pid :: pid()
}).

%% Macro to build the metadata cache key
-define(meta_key(Tab_), {?MODULE, Tab_}).

%%%===================================================================
%%% API
%%%===================================================================

-spec start_link(Tab :: shards:tab(), TabPid :: pid()) -> gen_server:start_ret().
start_link(Tab, TabPid) ->
  gen_server:start_link(?MODULE, {Tab, TabPid}, []).

-spec child_spec(Tab :: shards:tab(), TabPid :: pid()) -> supervisor:child_spec().
child_spec(Tab, TabPid) ->
  #{
    id => {?MODULE, TabPid},
    start => {?MODULE, start_link, [Tab, TabPid]},
    type => supervisor
  }.

-spec get_meta(Tab :: shards:tab()) -> shards_meta:t() | undefined.
get_meta(Tab) ->
  persistent_term:get(?meta_key(Tab), undefined).

-spec put_meta(Tab :: shards:tab(), Meta :: shards_meta:t()) -> ok.
put_meta(Tab, Meta) ->
  persistent_term:put(?meta_key(Tab), Meta).

-spec del_meta(Tab :: shards:tab()) -> ok.
del_meta(Tab) ->
  _ = persistent_term:erase(?meta_key(Tab)),
  ok.

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%% @hidden
init({Tab, TabPid}) ->
  _ = process_flag(trap_exit, true),

  {ok, #state{tab = Tab, tab_pid = TabPid}}.

%% @hidden
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
terminate(_Reason, #state{tab = Tab}) ->
  del_meta(Tab).
