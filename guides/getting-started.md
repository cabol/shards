# Getting Started

In this guide, we're going to learn some basics about `shards`, how to create
partitioned tables and how to use its API; which is the easiest part, since
it is the same ETS API.

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

## Creating partitioned tables

Exactly as ETS, `shards:new/2` function receives 2 arguments, the name of the
table and the options. But in addition to the options given by `ets:new/2`,
`shards` provides the next ones:

  * `{partitions, pos_integer()}` - Allows to set the desired number of
    partitions. By default, the number of partitions is the total of online
    schedulers (`erlang:system_info(schedulers_online)`).

  * `{keyslot_fun, shards_meta:keyslot_fun()}` - Function used to compute the
    partition where the action will be evaluated based on the key. Defaults to
    `erlang:phash2/2`.

  * `{parallel, boolean()}` - Specifies whether `shards` should work in parallel
    mode or not, for the applicable functions, e.g.: `select`, `match`, etc. By
    default is set to `false`.

Wen a new table is created, the [metadata][shards_meta] is created for that
table as well. The purpose of the **metadata** is to store information related
to that table, such as: number of partitions, keyslot function, etc. To learn
more about it, check out [shards_meta][shards_meta].

[shards_meta]: https://github.com/cabol/shards/blob/master/src/shards_meta.erl

**Examples:**

```erlang
% let's create a named table with 4 shards
> shards:new(tab1, [named_table, {partitions, 4}]).
tab1

% create another one with default options
> shards:new(tab2, []).
#Ref<0.893853601.1266286595.53859>
```

## Inserting entries

The contract and the logic for insertion functions (`insert/2`, `insert_new/2`)
is a bit different in `shards`, specially for `insert_new/2`. The main
difference is that in `shards` these functions are not atomic. Due to
we cannot insert a list of objects in a single table, because in `shards`
a table is composed by several ones internally, so `shards` has to compute
the partition based on the key before to insert the object, so each object
could be inserted in a different partition; but again, this logic is hadled
by `shards` internally.

Let's insert some entries:

```erlang
% for insert is pretty much the same, the contract is the same
> shards:insert(tab1, {k1, 1}).
true
> shards:insert(tab1, [{k1, 1}, {k2, 2}]).
true

% for insert_new
> shards:insert_new(tab1, {k3, 3}).
true
> shards:insert_new(tab1, {k3, 3}).
false
> shards:insert_new(tab1, [{k3, 3}, {k4, 4}]).
false
```

## Playing with shards

Let's use some write/read operations against the partitioned tables we just
created previously:

```erlang
% inserting some objects
> shards:insert(tab1, [{k1, 1}, {k2, 2}, {k3, 3}]).
true

% let's check those objects
> shards:lookup(tab1, k1).
[{k1,1}]
> shards:lookup(tab1, k2).
[{k2,2}]
> shards:lookup(tab1, k3).
[{k3,3}]
> shards:lookup(tab1, k4).
[]

% delete an object and then check
> shards:delete(tab1, k3).
true
> shards:lookup(tab1, k3).
[]

% now let's find all stored objects using select
> MatchSpec = ets:fun2ms(fun({K, V}) -> {K, V} end).
[{{'$1','$2'},[],[{{'$1','$2'}}]}]
> shards:select(tab1, MatchSpec).
[{k2,2},{k1,1}]
```

As you may have noticed using `shards` is extremely easy, it's only matters of
using the same ETS API but with `shards` module.

You can try the rest of the ETS API but using `shards`.

## Deleting partitioned tables

```erlang
> shards:delete(tab1).
true
```

## Cachig the metadta
