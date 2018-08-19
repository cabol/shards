%%%-------------------------------------------------------------------
%%% @doc
%%% Common Shards Utilities.
%%% @end
%%%-------------------------------------------------------------------
-module(shards_lib).

%% API
-export([
  shard_name/2,
  list_shards/2,
  key_from_object/1,
  iterator/1,
  get_pid/1,
  pick/3,
  keyfind/2,
  keyfind/3,
  keyupdate/3,
  keyupdate/4,
  reduce_while/3,
  to_string/1,
  read_tabfile/1,
  write_tabfile/2
]).

-type kv_list() :: [{any(), any()}].

-export_type([kv_list/0]).

%%%===================================================================
%%% API
%%%===================================================================

%% @doc
%% Builds a shard name `ShardName'.
%% <ul>
%% <li>`TabName': Table name from which the shard name is generated.</li>
%% <li>`Shard': Shard number – from `0' to `(NumShards - 1)'</li>
%% </ul>
%% @end
-spec shard_name(Tab :: atom(), Shard :: non_neg_integer()) ->
        ShardName :: atom().
shard_name(Tab, Shard) ->
  Bin = <<(atom_to_binary(Tab, utf8))/binary, ".", (integer_to_binary(Shard))/binary>>,
  binary_to_atom(Bin, utf8).

%% @doc
%% Returns the list of shard names associated to the given `TabName'.
%% The shard names that were created in the `shards:new/2,3' fun.
%% <ul>
%% <li>`TabName': Table name.</li>
%% <li>`NumShards': Number of shards.</li>
%% </ul>
%% @end
-spec list_shards(Tab :: atom(), NumShards :: pos_integer()) ->
        ShardNames :: [atom()].
list_shards(Tab, NumShards) ->
  [shard_name(Tab, Shard) || Shard <- iterator(NumShards)].

%% @doc
%% Returns the key for the given object or list of objects.
%% <ul>
%% <li>`ObjOrObjs': Object or list of objects.</li>
%% </ul>
%% @end
-spec key_from_object(ObjOrObjs :: tuple() | [tuple()]) -> Key :: any().
key_from_object(ObjOrObjs) when is_list(ObjOrObjs) ->
  element(1, hd(ObjOrObjs));
key_from_object(ObjOrObjs) when is_tuple(ObjOrObjs) ->
  element(1, ObjOrObjs).

%% @doc
%% Returns a sequence of integers that starts with `0' and contains the
%% successive results of adding `+1' to the previous element, until
%% `NumShards' is reached or passed (in the latter case, `NumShards'
%% is not an element of the sequence).
%% <ul>
%% <li>`StateOrNumShards': A valid shards state or number of shards.</li>
%% </ul>
%% @end
-spec iterator(StateOrNumShards :: shards_state:state() | pos_integer()) ->
        Iterator :: [integer()] .
iterator(StateOrNumShards) ->
  N =
    case shards_state:is_state(StateOrNumShards) of
      true ->
        shards_state:n_shards(StateOrNumShards);

      false when is_integer(StateOrNumShards) ->
        StateOrNumShards
    end,

  lists:seq(0, N - 1).

%% @doc
%% Returns the PID associated to the `Name'.
%% <ul>
%% <li>`Name': Process name.</li>
%% </ul>
%% @end
-spec get_pid(Name :: atom()) -> pid() | no_return().
get_pid(Name) ->
  case whereis(Name) of
    undefined -> error(badarg);
    Pid       -> Pid
  end.

%% @doc
%% Pick/computes the shard where the `Key' will be handled.
%% <ul>
%% <li>`Key': The key to be hashed to calculate the shard.</li>
%% <li>`Range': Range/set – number of shards/nodes.</li>
%% <li>`Op': Operation type: `r | w | d'.</li>
%% <li>`Result': Returns a number between `0..Range-1'.</li>
%% </ul>
%% @end
-spec pick(
        Key   :: shards_state:key(),
        Range :: shards_state:range(),
        Op    :: atom()
      ) -> Result :: non_neg_integer().
pick(Key, NumShards, _) ->
  erlang:phash2(Key, NumShards).

%% @equiv keyfind(Key, KVList, undefined)
keyfind(Key, KVList) ->
  keyfind(Key, KVList, undefined).

%% @doc
%% Returns the value to the given `Key' or `Default' if it doesn't exist.
%% @end
-spec keyfind(term(), kv_list(), term()) -> term().
keyfind(Key, KVList, Default) ->
  case lists:keyfind(Key, 1, KVList) of
    {Key, Value} -> Value;
    _            -> Default
  end.

%% @equiv keyupdate(Fun, Keys, undefined, TupleList)
keyupdate(Fun, Keys, TupleList) ->
  keyupdate(Fun, Keys, undefined, TupleList).

%% @doc
%% Updates the given `Keys' by the result of calling `Fun(OldValue)'.
%% If `Key' doesn't exist, then `Init' is set.
%% @end
-spec keyupdate(
        Fun     :: fun((Key :: any(), Value :: any()) -> any()),
        Keys    :: [any()],
        Init    :: any(),
        KVList1 :: kv_list()
      ) -> KVList2 :: kv_list().
keyupdate(Fun, Keys, Init, KVList1) when is_function(Fun, 2) ->
  lists:foldl(fun(Key, Acc) ->
    NewKV =
      case lists:keyfind(Key, 1, Acc) of
        {Key, Value} -> {Key, Fun(Key, Value)};
        false        -> {Key, Init}
      end,

    lists:keystore(Key, 1, Acc, NewKV)
  end, KVList1, Keys).

%% @doc
%% Reduces the `List' until fun returns `{halt, any()}''.
%% <ul>
%% <li>`Fun': Function invoked for each element in `List'.</li>
%% <li>`AccIn': Initial accumulator.</li>
%% <li>`List': List (enumerable) to be reduced.</li>
%% <li>`Result': Accumulator.</li>
%% </ul>
%% @end
-spec reduce_while(
        Fun       :: fun((Elem :: any(), Acc :: any()) -> FunRes),
        AccIn     :: any(),
        ListOrMap :: [any()] | map()
      ) -> Result :: any()
      when FunRes :: {cont | halt, AccOut :: any()}.
reduce_while(Fun, AccIn, List) when is_function(Fun, 2) ->
  try
    do_fold(Fun, AccIn, List)
  catch
    throw:{halt, AccOut} -> AccOut
  end.

%% @private
do_fold(Fun, AccIn, List) when is_list(List) ->
  lists:foldl(fun(Elem, Acc) ->
    case Fun(Elem, Acc) of
      {cont, AccOut} -> AccOut;
      {halt, AccOut} -> throw({halt, AccOut})
    end
  end, AccIn, List);
do_fold(Fun, AccIn, Map) when is_map(Map) ->
  maps:fold(fun(Key, Value, Acc) ->
    case Fun({Key, Value}, Acc) of
      {cont, AccOut} -> AccOut;
      {halt, AccOut} -> throw({halt, AccOut})
    end
  end, AccIn, Map).

%% @doc
%% Converts the input data to a string.
%% @end
-spec to_string(Data :: any()) -> string() | no_return().
to_string(Data) when is_binary(Data) ->
  binary_to_list(Data);
to_string(Data) when is_integer(Data) ->
  integer_to_list(Data);
to_string(Data) when is_float(Data) ->
  lists:flatten(io_lib:format("~.1f", [Data]));
to_string(Data) when is_atom(Data) ->
  atom_to_list(Data);
to_string(Data) when is_list(Data) ->
  case io_lib:printable_list(Data) of
    true  -> Data;
    false -> error({badarg, Data})
  end;
to_string(Data) ->
  error({badarg, Data}).

%% @doc
%% Reads the file info related to a tabfile saved previously.
%% @end
-spec read_tabfile(shards_local:filename()) -> term() | no_return().
read_tabfile(Filename) ->
  case file:read_file(Filename) of
    {ok, Binary} -> binary_to_term(Binary);
    Error        -> throw(Error)
  end.

%% @doc
%% Writes to a file a content related to a table.
%% @end
-spec write_tabfile(shards_local:filename(), term()) -> ok | {error, term()}.
write_tabfile(Filename, Content) ->
  file:write_file(Filename, term_to_binary(Content)).
