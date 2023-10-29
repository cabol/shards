%%%-------------------------------------------------------------------
%%% @doc
%%% Common Shards Utilities.
%%% @end
%%%-------------------------------------------------------------------
-module(shards_lib).

%% API
-export([
  partition_name/2,
  object_key/2,
  keyfind/2,
  keyfind/3,
  keyupdate/3,
  keyupdate/4,
  keypop/2,
  keypop/3,
  to_string/1,
  read_tabfile/1,
  write_tabfile/2
]).

%% Defines a key/value tuple list.
-type kv_list() :: [{term(), term()}].

-export_type([kv_list/0]).

%%%===================================================================
%%% API
%%%===================================================================

%% @doc
%% Builds the partition name for the given table `Tab'
%% and partition `Partition'.
%% <ul>
%% <li>`TabName': Table name from which the shard name is generated.</li>
%% <li>`Partition': Partition number – from `0' to `(NumShards - 1)'</li>
%% </ul>
%% @end
-spec partition_name(Tab, Partition) -> PartitionName when
      Tab           :: atom(),
      Partition     :: non_neg_integer(),
      PartitionName :: atom().
partition_name(Tab, Partition) ->
  Bin = <<(atom_to_binary(Tab, utf8))/binary, ".ptn", (integer_to_binary(Partition))/binary>>,
  binary_to_atom(Bin, utf8).

%% @doc
%% Returns the key for the given object or list of objects.
%% @end
-spec object_key(ObjOrObjs, Meta) -> term() when
      ObjOrObjs :: tuple() | [tuple()],
      Meta      :: shards_meta:t().
object_key(ObjOrObjs, Meta) when is_tuple(ObjOrObjs) ->
  element(shards_meta:keypos(Meta), ObjOrObjs);
object_key(ObjOrObjs, Meta) when is_list(ObjOrObjs) ->
  element(shards_meta:keypos(Meta), hd(ObjOrObjs)).

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
    false        -> Default
  end.

%% @equiv keyupdate(Fun, Keys, undefined, TupleList)
keyupdate(Fun, Keys, TupleList) ->
  keyupdate(Fun, Keys, undefined, TupleList).

%% @doc
%% Updates the given `Keys' by the result of calling `Fun(OldValue)'.
%% If `Key' doesn't exist, then `Init' is set.
%% @end
-spec keyupdate(Fun, Keys, Init, KVList1) -> KVList2 when
      Fun     :: fun((Key :: term(), Value :: term()) -> term()),
      Keys    :: [term()],
      Init    :: term(),
      KVList1 :: kv_list(),
      KVList2 :: kv_list().
keyupdate(Fun, Keys, Init, KVList1) when is_function(Fun, 2) ->
  lists:foldl(fun(Key, Acc) ->
    NewKV =
      case lists:keyfind(Key, 1, Acc) of
        {Key, Value} -> {Key, Fun(Key, Value)};
        false        -> {Key, Init}
      end,

    lists:keystore(Key, 1, Acc, NewKV)
  end, KVList1, Keys).

%% @equiv keypop(Key, KVList1, undefined)
keypop(Key, KVList1) ->
  keypop(Key, KVList1, undefined).

%% @doc
%% Searches the list of tuples `KVList1' for a tuple whose Nth element
%% compares equal to `Key'. Returns `{Value, KVList2}' if such a tuple
%% is found, otherwise `{Default, KVList1}'. `KVList2' is a copy of
%% `KVList1' where the first occurrence of Tuple has been removed.
%% @end
-spec keypop(Key, KVList1, Default) -> {Value, KVList2} when
      Key     :: term(),
      KVList1 :: kv_list(),
      Default :: term(),
      Value   :: term(),
      KVList2 :: kv_list().
keypop(Key, KVList1, Default) ->
  case lists:keytake(Key, 1, KVList1) of
    {value, {Key, Value}, KVList2} ->
      {Value, KVList2};

    false ->
      {Default, KVList1}
  end.

%% @doc
%% Converts the input data to a string.
%% @end
-spec to_string(Data :: term()) -> string() | no_return().
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
-spec read_tabfile(shards:filename()) -> term() | no_return().
read_tabfile(Filename) ->
  case file:read_file(Filename) of
    {ok, Binary} -> binary_to_term(Binary);
    Error        -> throw(Error)
  end.

%% @doc
%% Writes to a file a content related to a table.
%% @end
-spec write_tabfile(shards:filename(), term()) -> ok | {error, term()}.
write_tabfile(Filename, Content) ->
  file:write_file(Filename, term_to_binary(Content)).
