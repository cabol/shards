<h1>
  <img align="left" height="60px" src="guides/images/shards.png">
  Shards
</h1>

> ### ETS tables on steroids!
> Sharding for ETS tables out-of-box.

[![Build Status](https://github.com/cabol/shards/workflows/CI/badge.svg)](https://github.com/cabol/shards/actions)
[![Hex Version](https://img.shields.io/hexpm/v/shards.svg)](https://hex.pm/packages/shards)

Why might we need **Sharding** on ETS tables? Well, the main reason is to keep
the lock contention under control, in order to scale-out ETS tables and support
higher levels of concurrency without lock issues; specially write-locks, which
most of the cases might cause significant performance degradation.

Therefore, one of the most common and proven strategies to deal with these
problems is [Sharding][sharding] or [Partitioning][partitioning]; the principle
is pretty similar to [DHTs][dht].

This is where **Shards** comes in. **Shards** is an **Erlang/Elixir** library
compatible with the current [ETS API][ets_api], which implements Sharding or
Partitioning on top of ETS tables, completely transparent and out-of-box.

See the [getting started][getting_started] guide
and the [online documentation](https://hexdocs.pm/shards/).

[ets_api]: http://erlang.org/doc/man/ets.html
[sharding]: https://en.wikipedia.org/wiki/Shard_(database_architecture)
[partitioning]: https://en.wikipedia.org/wiki/Partition_(database)
[dht]: https://en.wikipedia.org/wiki/Distributed_hash_table
[getting_started]: https://github.com/cabol/shards/blob/master/guides/getting-started.md

## Installation

### Erlang

In your `rebar.config`:

```erlang
{deps, [
  {shards, "0.7.0"}
]}.
```

### Elixir

In your `mix.exs`:

```elixir
def deps do
  [{:shards, "~> 0.7.0"}]
end
```

> For more information and examples, see the [getting started][getting_started]
  guide.

## Important links

 * [Documentation](https://hexdocs.pm/shards) - Hex Docs.

 * [Blog Post](http://cabol.github.io/posts/2016/04/14/sharding-support-for-ets.html) -
   Transparent and out-of-box sharding support for ETS tables in Erlang/Elixir.

 * [ExShards](https://github.com/cabol/ex_shards) – Elixir wrapper for
   `shards`; with extra and nicer functions.

 * [Nebulex](https://github.com/cabol/nebulex) – Distributed Caching
   framework for Elixir.

 * [KVX](https://github.com/cabol/kvx) – Simple Elixir in-memory Key/Value
   Store using `shards` (default adapter).

 * [Cacherl](https://github.com/ferigis/cacherl) Distributed Cache
   using `shards`.

## Testing

```
$ make test
```

You can find tests results in `_build/test/logs`, and coverage in
`_build/test/cover`.

> **NOTE:** `shards` comes with a helper `Makefile`, but it is just a simple
  wrapper on top of `rebar3`, therefore, you can do everything using `rebar3`
  directly as well (e.g.: `rebar3 do ct, cover`).

## Generating Edoc

```
$ make docs
```

> **NOTE:** Once you run the previous command, a new folder `doc` is created,
  and you'll have a pretty nice HTML documentation.

## Copyright and License

Copyright (c) 2016 Carlos Andres Bolaños R.A.

**Shards** source code is licensed under the [MIT License](LICENSE.md).
