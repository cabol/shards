%%%-------------------------------------------------------------------
%%% @doc
%%% Utilities for parsing the given shards/ets options..
%%% @end
%%%-------------------------------------------------------------------
-module(shards_opts).

-export([parse/1]).

%% Macro to check if option is a valid ETS type
-define(is_ets_type(T_),
  T_ == set;
  T_ == ordered_set;
  T_ == bag;
  T_ == duplicate_bag
).

%% Macro to check if option is valid ETS option
-define(is_ets_opt(V_),
  V_ == public;
  V_ == protected;
  V_ == private;
  V_ == named_table;
  V_ == compressed
).

%%%===================================================================
%%% API
%%%===================================================================

-spec parse([shards:option()]) -> map().
parse(Opts) ->
  MetaMap = shards_meta:to_map(shards_meta:new()),
  ParsedOpts = parse_opts(Opts, MetaMap#{ets_opts => []}),

  %% @FIXME: workaround to support ordered_set
  case maps:take(type, ParsedOpts) of
    {ordered_set, ParsedOpts1} ->
      ParsedOpts1#{partitions := 1};

    {_Type, ParsedOpts1} ->
      ParsedOpts1;

    error ->
      ParsedOpts
  end.

%% @private
parse_opts([], #{ets_opts := EtsOpts} = Acc) ->
  Acc#{ets_opts := lists:reverse(EtsOpts)};
parse_opts([{keypos, Pos} | Opts], #{ets_opts := NOpts} = Acc) when is_integer(Pos), Pos > 0 ->
  parse_opts(Opts, Acc#{keypos := Pos, ets_opts := [{keypos, Pos} | NOpts]});
parse_opts([{partitions, N} | Opts], Acc) when is_integer(N), N > 0 ->
  parse_opts(Opts, Acc#{partitions := N});
parse_opts([{keyslot_fun, Val} | Opts], Acc) when is_function(Val, 2) ->
  parse_opts(Opts, Acc#{keyslot_fun := Val});
parse_opts([{parallel, Val} | Opts], Acc) when is_boolean(Val) ->
  parse_opts(Opts, Acc#{parallel := Val});
parse_opts([{parallel_timeout, Val} | Opts], Acc)
    when (is_integer(Val) andalso Val >= 0) orelse Val == infinity ->
  parse_opts(Opts, Acc#{parallel_timeout := Val});
parse_opts([{cache, Val} | Opts], Acc) when is_boolean(Val) ->
  parse_opts(Opts, Acc#{cache := Val});
parse_opts([{restore, _, _} = Opt | Opts], #{ets_opts := NOpts} = Acc) ->
  parse_opts(Opts, Acc#{ets_opts := [Opt | NOpts]});
parse_opts([{heir, _, _} = Opt | Opts], #{ets_opts := NOpts} = Acc) ->
  parse_opts(Opts, Acc#{ets_opts := [Opt | NOpts]});
parse_opts([{heir, none} = Opt | Opts], #{ets_opts := NOpts} = Acc) ->
  parse_opts(Opts, Acc#{ets_opts := [Opt | NOpts]});
parse_opts([{write_concurrency, _} = Opt | Opts], #{ets_opts := NOpts} = Acc) ->
  parse_opts(Opts, Acc#{ets_opts := [Opt | NOpts]});
parse_opts([{read_concurrency, _} = Opt | Opts], #{ets_opts := NOpts} = Acc) ->
  parse_opts(Opts, Acc#{ets_opts := [Opt | NOpts]});
parse_opts([{decentralized_counters, _} = Opt | Opts], #{ets_opts := NOpts} = Acc) ->
  parse_opts(Opts, Acc#{ets_opts := [Opt | NOpts]});
parse_opts([Opt | Opts], #{ets_opts := NOpts} = Acc) when ?is_ets_type(Opt) ->
  parse_opts(Opts, Acc#{type => Opt, ets_opts := [Opt | NOpts]});
parse_opts([Opt | Opts], #{ets_opts := NOpts} = Acc) when ?is_ets_opt(Opt) ->
  parse_opts(Opts, Acc#{ets_opts := [Opt | NOpts]});
parse_opts([Opt | _], _Acc) ->
  error({badoption, Opt}).
