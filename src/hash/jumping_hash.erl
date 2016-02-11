-module(jumping_hash).

-export([calculate/2]).
-on_load(init/0).

-define(APPNAME, shards).
-define(LIBNAME, jumping_hash).

%%==============================================================================
%% API
%%==============================================================================

%% @hidden
calculate(_, _) ->
  not_loaded(?LINE).

%% @private
not_loaded(Line) ->
  erlang:nif_error({not_loaded, [{module, ?MODULE}, {line, Line}]}).

%%==============================================================================
%% NIF init
%%==============================================================================

%% @hidden
init() ->
  SoName = case code:priv_dir(?APPNAME) of
    {error, bad_name} ->
      case filelib:is_dir(filename:join(["..", priv])) of
        true ->
          filename:join(["..", priv, ?LIBNAME]);
        _ ->
          filename:join([priv, ?LIBNAME])
      end;
    Dir ->
      filename:join(Dir, ?LIBNAME)
  end,
  erlang:load_nif(SoName, 0).
