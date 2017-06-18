%%%-------------------------------------------------------------------
%%% @doc
%%% Shards Application.
%%% @end
%%%-------------------------------------------------------------------
-module(shards_app).

-behaviour(application).

%% Application callbacks
-export([
  start/2,
  stop/1
]).

%%%===================================================================
%%% Application callbacks
%%%===================================================================

%% @hidden
start(_StartType, _StartArgs) ->
  shards_sup:start_link().

%% @hidden
stop(_State) ->
  ok.
