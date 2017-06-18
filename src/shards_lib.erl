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
  get_pid/1,
  pick/3,
  reduce_while/3,
  to_string/1
]).

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
-spec shard_name(Tab, Shard) -> ShardName when
  Tab       :: atom(),
  Shard     :: non_neg_integer(),
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
-spec list_shards(Tab, NumShards) -> ShardNames when
  Tab        :: atom(),
  NumShards  :: pos_integer(),
  ShardNames :: [atom()].
list_shards(Tab, NumShards) ->
  [shard_name(Tab, Shard) || Shard <- lists:seq(0, NumShards - 1)].

%% @doc
%% Returns the PID associated to the table `Tab'.
%% <ul>
%% <li>`TabName': Table name.</li>
%% </ul>
%% @end
-spec get_pid(Tab :: atom()) -> pid() | no_return().
get_pid(Tab) ->
  case whereis(Tab) of
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
-spec pick(Key, Range, Op) -> Result when
  Key    :: shards_state:key(),
  Range  :: shards_state:range(),
  Op     :: shards_state:op(),
  Result :: non_neg_integer().
pick(Key, NumShards, _) ->
  erlang:phash2(Key, NumShards).

%% @doc
%% Reduces the `List' until fun returns `{halt, any()}''.
%% <ul>
%% <li>`Fun': Function invoked for each element in `List'.</li>
%% <li>`AccIn': Initial accumulator.</li>
%% <li>`List': List (enumerable) to be reduced.</li>
%% <li>`Result': Accumulator.</li>
%% </ul>
%% @end
-spec reduce_while(Fun, AccIn, List) -> Result when
  Fun    :: fun((Elem :: any(), Acc :: any()) -> any()),
  AccIn  :: any(),
  List   :: [any()],
  Result :: any().
reduce_while(Fun, AccIn, List) ->
  try
    lists:foldl(fun(Elem, Acc) ->
      case Fun(Elem, Acc) of
        {cont, AccOut} -> AccOut;
        {halt, AccOut} -> throw({halt, AccOut})
      end
    end, AccIn, List)
  catch
    throw:{halt, AccOut} -> AccOut
  end.

%% @doc
%% Converts the input data to a string.
%% @end
-spec to_string(Data) -> Result when
  Data   :: binary() | number() | atom() | list(),
  Result :: string() | no_return().
to_string(Data) when is_binary(Data) ->
  binary_to_list(Data);
to_string(Data) when is_integer(Data) ->
  integer_to_list(Data);
to_string(Data) when is_float(Data) ->
  float_to_list(Data);
to_string(Data) when is_atom(Data) ->
  atom_to_list(Data);
to_string(Data) when is_list(Data) ->
  case io_lib:printable_list(Data) of
    true  -> Data;
    false -> error({badarg, Data})
  end;
to_string(Data) ->
  error({badarg, Data}).
