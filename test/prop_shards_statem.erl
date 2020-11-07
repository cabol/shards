-module(prop_shards_statem).
-behaviour(proper_statem).
-compile([debug_info]).

%% proper_statem
-export([
  initial_state/0,
  command/1,
  precondition/2,
  postcondition/3,
  next_state/3
]).

-include_lib("common_test/include/ct_property_test.hrl").
-include_lib("proper/include/proper.hrl").

%%%===================================================================
%%% Types & Macros
%%%===================================================================

-type object()     :: tuple().
-type table_type() :: 'set' | 'ordered_set' | 'bag' | 'duplicate_bag'.

-record(state, {
  tabs   = []  :: [shards:tab()],
  stored = []  :: [object()],      %% list of objects stored in shards table
  type   = set :: table_type()     %% type of shards table
}).

-define(INT_KEYS, lists:seq(0, 2)).
-define(FLOAT_KEYS, [float(Key) || Key <- ?INT_KEYS]).
-define(BIN_KEYS, [integer_to_binary(Key) || Key <- ?INT_KEYS]).
-define(ATOM_KEYS, [binary_to_atom(<<"a", Key/binary>>, utf8) || Key <- ?BIN_KEYS]).

%%%===================================================================
%%% Generators
%%%===================================================================

key() ->
  frequency([
    {2, elements(?INT_KEYS)},
    {1, elements(?FLOAT_KEYS)},
    {3, elements(?BIN_KEYS)},
    {5, elements(?ATOM_KEYS)}
  ]).

value() ->
  frequency([
    {5, int()},
    {1, elements([a, b, c, d])},
    {5, term()},
    {5, list()}
  ]).

object() ->
  {key(), value()}.

object(S) ->
  elements(S#state.stored).

key(S) ->
  ?LET(Object, object(S), element(1, Object)).

tab(S) ->
  elements(S#state.tabs).

%%%===================================================================
%%% Abstract state machine for shards table
%%%===================================================================

%% @hidden
initial_state() ->
  #state{type = set}.

initial_state(Type) ->
  #state{type = Type}.

initial_state(Type, parallel) ->
  #state{tabs = [tab], type = Type}.

%% @hidden
command(#state{tabs = [], type = Type}) ->
  {call, shards, new, [tab, [Type]]};
command(S) ->
  oneof(
    [
      {call, shards, insert, [tab(S), object()]},
      {call, shards, insert_new, [tab(S), object()]},
      {call, shards, delete, [tab(S), key()]}
    ] ++
    [
      {call, shards, lookup, [tab(S), key(S)]}
      || S#state.stored =/= []
    ] ++
    [
      {call, shards, member, [tab(S), key(S)]}
      || S#state.stored =/= []
    ] ++
    [
      {call, shards, lookup_element, [tab(S), key(S), range(1, 2)]}
      || S#state.stored =/= []
    ] ++
    [
      {call, shards, take, [tab(S), key(S)]}
      || S#state.stored =/= []
    ] ++
    [
      {call, shards, update_counter, [tab(S), key(S), int()]}
      || S#state.stored =/= [], S#state.type =:= set orelse S#state.type =:= ordered_set
    ] ++
    [
      {call, shards, update_element, [tab(S), key(S), {2, int()}]}
      || S#state.stored =/= [], S#state.type =:= set orelse S#state.type =:= ordered_set
    ]
  ).

%% @hidden
precondition(S, {call, _, lookup, [_, Key]}) ->
  proplists:is_defined(Key, S#state.stored);
precondition(S, {call, _, lookup_element, [_, Key, _]}) ->
  proplists:is_defined(Key, S#state.stored);
precondition(S, {call, _, take, [_, Key]}) ->
  proplists:is_defined(Key, S#state.stored);
precondition(S, {call, _, update_counter, [_, Key, _Incr]}) ->
  proplists:is_defined(Key, S#state.stored) andalso case S#state.type of
    set ->
      Obj = proplists:lookup(Key, S#state.stored),
      is_integer(element(2, Obj));

    ordered_set ->
      Obj = lists:keyfind(Key, 1, S#state.stored),
      is_integer(element(2, Obj))
  end;
precondition(_S, {call, _, _, _}) ->
  true.

%% @hidden
postcondition(_S, {call, _, new, [_Tab, _Opts]}, _Res) ->
  true;
postcondition(S, {call, _, update_counter, [_Tab, Key, Incr]}, Res) ->
  Object =
    case S#state.type of
      set         -> proplists:lookup(Key, S#state.stored);
      ordered_set -> lists:keyfind(Key, 1, S#state.stored)
    end,

  Value = element(2, Object),
  Res =:= Value + Incr;
postcondition(_S, {call, _, update_element, [_Tab, _Key, _]}, false) ->
  true;
postcondition(S, {call, _, update_element, [_Tab, Key, _]}, true) ->
  Object =
    case S#state.type of
      set         -> proplists:lookup(Key, S#state.stored);
      ordered_set -> lists:keyfind(Key, 1, S#state.stored)
    end,

  is_tuple(Object);
postcondition(_S, {call, _, delete, [_Tab, _Key]}, Res) ->
  Res =:= true;
postcondition(_S, {call, _, insert, [_Tab, _Object]}, Res) ->
  Res =:= true;
postcondition(_S, {call, _, insert_new, [_Tab, _Object]}, false) ->
  true;
postcondition(S, {call, _, insert_new, [_Tab, Object]}, true) ->
  with_value(element(1, Object), S) =:= nil;
postcondition(S, {call, _, member, [_Tab, Key]}, Res) ->
  with_value(Key, S, fun(nil) -> false; (_) -> true end) =:= Res;
postcondition(S, {call, _, Cmd, [_Tab, Key]}, Res) when Cmd =:= lookup; Cmd =:= take ->
  Val = with_value(Key, S),
  Res =:= Val orelse Res =:= [Val];
postcondition(S, {call, _, lookup_element, [_Tab, Key, Pos]}, Res) ->
  case {S#state.type, with_value(Key, S, fun(V) -> V end)} of
    {ordered_set, Val} -> Res =:= element(Pos, Val);
    {set, Val}         -> Res =:= element(Pos, Val);
    {_, Val}           -> Res =:= [element(Pos, Tuple) || Tuple <- Val]
  end.

%% @hidden
next_state(S, Res, {call, _, new, [_Tab, _Opts]}) ->
  S#state{tabs = [Res | S#state.tabs]};
next_state(S, _Res, {call, _, update_counter, [_Tab, Key, Incr]}) ->
  case S#state.type of
    set ->
      Object = proplists:lookup(Key, S#state.stored),
      Value = element(2, Object),
      NewObj = setelement(2, Object, Value + Incr),
      S#state{stored = keyreplace(Key, 1, S#state.stored, NewObj)};

    ordered_set ->
      Object = lists:keyfind(Key, 1, S#state.stored),
      Value = element(2, Object),
      NewObj = setelement(2, Object, Value + Incr),
      S#state{stored = lists:keyreplace(Key, 1, S#state.stored, NewObj)}
  end;
next_state(S, false, {call, _, update_element, [_Tab, _Key, _]}) ->
  S;
next_state(S, true, {call, _, update_element, [_Tab, Key, {_Pos, Value}]}) ->
  maybe_update_store(Key, {Key, Value}, S);
next_state(S, _Res, {call, _, insert, [_Tab, Object]}) ->
  maybe_update_store(element(1, Object), Object, S);
next_state(S, true, {call, _, insert_new, [_Tab, Object]}) ->
  S#state{stored = S#state.stored ++ [Object]};
next_state(S, false, {call, _, insert_new, [_Tab, _Object]}) ->
  S;
next_state(S, _Res, {call, _, Cmd, [_Tab, Key]}) when Cmd =:= delete; Cmd =:= take ->
  del_key(Key, S);
next_state(S, _Res, {call, _, _, _}) ->
  S.

%%%===================================================================
%%% Properties
%%%===================================================================

prop_shards() ->
  ?FORALL(Type, noshrink(table_type()),
    ?FORALL(Cmds, commands(?MODULE, initial_state(Type)), begin
      {H,S,Res} = run_commands(?MODULE, Cmds),
      [shards:delete(Tab) || Tab <- S#state.tabs],

      ?WHENFAIL(
        io:format("History: ~p~nState: ~p~nRes: ~p~n", [H, S, Res]),
        collect(Type, Res =:= ok)
      )
    end)
  ).

prop_parallel_shards() ->
  ?FORALL(Type, noshrink(table_type()),
    ?FORALL(Cmds, commands(?MODULE, initial_state(Type, parallel)), begin
      Tab = shards:new(tab, [named_table, public, Type]),
      {Seq, P, Res} = run_commands(?MODULE, Cmds),
      shards:delete(Tab),

      ?WHENFAIL(
        io:format("Sequential: ~p~nParallel: ~p~nRes: ~p~n", [Seq, P, Res]),
        collect(Type, Res =:= ok)
      )
    end)
  ).

%%%===================================================================
%%% Helpers
%%%===================================================================

keyreplace(Key, Pos, List, NewTuple) ->
  keyreplace(Key, Pos, List, NewTuple, []).

keyreplace(_Key, _Pos, [], _NewTuple, Acc) ->
  lists:reverse(Acc);
keyreplace(Key, Pos, [Tuple | Rest], NewTuple, Acc) ->
  case element(Pos, Tuple) =:= Key of
    true  -> lists:reverse(Acc) ++ [NewTuple | Rest];
    false -> keyreplace(Key, Pos, Rest, NewTuple, [Tuple | Acc])
  end.

maybe_update_store(Key, Object, #state{type = set} = S) ->
  case proplists:is_defined(Key, S#state.stored) of
    false -> S#state{stored = S#state.stored ++ [Object]};
    true  -> S#state{stored = keyreplace(Key, 1, S#state.stored, Object)}
  end;
maybe_update_store(Key, Object, #state{type = ordered_set} = S) ->
  case lists:keymember(Key, 1, S#state.stored) of
    false -> S#state{stored = S#state.stored ++ [Object]};
    true  -> S#state{stored = lists:keyreplace(Key, 1, S#state.stored, Object)}
  end;
maybe_update_store(_Key, Object, #state{type = bag} = S) ->
  case lists:member(Object, S#state.stored) of
    false -> S#state{stored = S#state.stored ++ [Object]};
    true  -> S
  end;
maybe_update_store(_Key, Object, #state{type = duplicate_bag} = S) ->
  S#state{stored = S#state.stored ++ [Object]}.

with_value(Key, S) ->
  with_value(Key, S, fun
    (false) -> nil;
    (none)  -> nil;
    ([])    -> nil;
    (V)     -> V
  end).

with_value(Key, #state{type = ordered_set} = S, Fun) ->
  Fun(lists:keyfind(Key, 1, S#state.stored));
with_value(Key, #state{type = set} = S, Fun) ->
  Fun(proplists:lookup(Key, S#state.stored));
with_value(Key, #state{type = _} = S, Fun) ->
  Fun(proplists:lookup_all(Key, S#state.stored)).

del_key(Key, #state{type = ordered_set} = S) ->
  S#state{stored = lists:keydelete(Key, 1, S#state.stored)};
del_key(Key, #state{} = S) ->
  S#state{stored = proplists:delete(Key, S#state.stored)}.
