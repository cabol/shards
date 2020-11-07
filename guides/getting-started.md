# Getting Started

## Table of Contents

* __[Prelude](#prelude)__
* __[Installation](#installation)__
  * [Erlang](#erlang)
  * [Elixir](#elixir)
* __[Creating partitioned tables](#creating-partitioned-tables)__
* __[Inserting entries](#inserting-entries)__
* __[Retrieving entries](#retrieving-entries)__
* __[Making partitioned tables part of an application's supervision tree](#making-partitioned-tables-part-of-an-applications-supervision-tree)__
  * [Erlang example](#erlang-example)
  * [Elixir example](#elixir-example)
* __[Advanced topics](#advanced-topics)__
  * __[Caching the metadata](#caching-the-metadata)__

## Prelude

In this guide, we're going to learn some basics about `shards`, how to create
partitioned tables and how to use them; which is the easiest part, since
it is the same ETS API.

## Installation

### Erlang

In your `rebar.config`:

```erlang
{deps, [
  {shards, "1.0.1"}
]}.
```

### Elixir

In your `mix.exs`:

```elixir
def deps do
  [{:shards, "~> 1.0"}]
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

  * `{parallel_timeout, timeout()}` - When `parallel` is set to `true`, it
    specifies the max timeout for a parallel execution. Defaults to `infinity`.

Wen a new table is created, the [metadata][shards_meta] is created for that
table as well. The purpose of the **metadata** is to store information related
to that table, such as: number of partitions, keyslot function, etc. To learn
more about it, check out [shards_meta][shards_meta].

[shards_meta]: https://github.com/cabol/shards/blob/master/src/shards_meta.erl

**Erlang:**

```erlang
% create a named table with 4 shards
> shards:new(tab1, [named_table, {partitions, 4}]).
tab1

% create another one with default options
> shards:new(tab2, []).
#Ref<0.893853601.1266286595.53859>
```

**Elixir:**

```elixir
# create a named table with 4 shards
iex> :shards.new(:tab1, [:named_table, partitions: 4])
:tab1

# create another one with default options
iex> :shards.new(:tab2, [])
#Reference<0.1257361217.1909325839.17166>
```

> **NOTE:** You can also start the observer by calling `observer:start()`
  to see how a partitioned table looks like.

## Inserting entries

**Erlang:**

```erlang
> shards:insert(tab1, {k1, 1}).
true
> shards:insert(tab1, [{k1, 1}, {k2, 2}]).
true

> shards:insert_new(tab1, {k3, 3}).
true
> shards:insert_new(tab1, {k3, 3}).
false
> shards:insert_new(tab1, [{k3, 3}, {k4, 4}]).
false
```

**Elixir:**

```elixir
iex> :shards.insert(:tab1, k1: 1)
true
iex> :shards.insert(:tab1, k1: 1, k2: 2)
true

iex> :shards.insert_new(:tab1, k3: 3)
true
iex> :shards.insert_new(:tab1, k3: 3)
false
iex> :shards.insert_new(:tab1, k3: 3, k4: 4)
false
```

## Retrieving entries

**Erlang:**

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

> shards:lookup_element(tab1, k3, 2).
3

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

**Elixir:**

```elixir
iex> :shards.insert(:tab1, k1: 1, k2: 2, k3: 3)
true

iex> :shards.lookup(:tab1, :k1)
[k1: 1]
iex> :shards.lookup(:tab1, :k2)
[k2: 2]
iex> :shards.lookup(:tab1, :k3)
[k3: 3]
iex> :shards.lookup(:tab1, :k4)
[]

iex> :shards.lookup_element(:tab1, :k3, 2)
3

iex> :shards.delete(:tab1, :k3)
true
iex> :shards.lookup(:tab1, :k3)
[]

iex> ms = :ets.fun2ms(& &1)
[{:"$1", [], [:"$1"]}]
iex> :shards.select(:tab1, ms)
[k2: 2, k1: 1]
```

As you may have noticed using `shards` is extremely easy, it's only matters of
using the same ETS API but with `shards` module.

You can try the rest of the ETS API but using `shards`.

## Making partitioned tables part of an application's supervision tree

There might be some cases we may want to start the tables as part of an
existing supervision tree. To do so, we can create a dynamic supervisor
for taking care of creating/deleting the tables and add the dynamic
supervisor as part of our app supervision tree.

Shards provides the module `shards_group`, which is a dynamic supervisor that
can bee added to an existing application and/or supervision tree. Besides,
`shards_group` brings with the function to create tables making them part of
the dynamic supervisor and also with the function to delete them and remove
them from the dynamic supervisor. But let's see how it works!

### Erlang example

Supposing you have an application `myapp` and the main supervisor `myapp_sup`,
the only piece of configuration is to setup the `shards_group` as a supervisor
within the application's supervision tree, like so:

```erlang
-module(myapp_sup).

-export([start_link/1, init/1]).

start_link() ->
  supervisor:start_link({name, ?MODULE}, ?MODULE, []).

init(_) ->
  Children = [
    % Dynamic supervisor with default name shards_group
    % See shards_group:child_spec/1
    shards_group:child_spec()
  ],

  {ok, {{one_for_one, 10, 10}, Children}}.
```

> The call `shards_group:child_spec()` will return the spec using the default
  name for the dynamic supervisor `shards_group`, but can pass the desired name
  by calling `shards_group:child_spec(desired_name)`.

Once the app is started, you can use `shards_group` to create/delete tables
and `shards` for the rest of the API functions, like so:

```erlang
% create a table as part of the app supervision tree
> shards_group:new_table(myapp_dynamic_sup, tab1, [named_table]).
{ok,<0.194.0>,#Ref<0.3052443831.2753691659.260835>}

> shards:insert(tab1, [{a, 1}, {b, 2}]).
true
> shards:lookup_element(tab1, a, 2).
1

% let's create another table
> {ok, _Pid, Tab} = shards_group:new_table(myapp_dynamic_sup, tab2, []).
{ok,<0.195.0>,#Ref<0.3052443831.2753691659.260836>}

> shards:insert(tab2, [{c, 3}, {d, 4}]).
true
> shards:lookup_element(tab1, c, 2).
3

% you can open the observer to see hoe the tables look like
> observer:start().
ok

% deleting a table
> shards_group:del_table(myapp_dynamic_sup, tab1).
true
```

### Elixir example

In Elixir is much easier it provides the `DynamicSupervisor` module out-of-box,
so we have two options, either use `:shards_group` like before in the Erlang
example, or use `DynamicSupervisor` and create a simple module to encapsulate
the logic of creating/deleting tables. Since we already know how `:shards_group`
works in the previous example, let's take the second option.

First of all, let's create the module to encapsulate the logic of creating and
deleting the tables:

```elixir
defmodule MyApp.DynamicShards do
  @moduledoc false

  # creates a table with shards as part of the DynamicSupervisor which at
  # the same time is part of the app supervision tree
  def new(name, opts) do
    {:ok, _pid, tab} =
      DynamicSupervisor.start_child(__MODULE__, table_spec(name, opts))

    tab
  end

  # deletes the table and also removes the table supervisor from the
  # DynamicSupervisor
  def delete(tab) do
    DynamicSupervisor.terminate_child(__MODULE__, :shards_meta.tab_pid(tab))
  end

  # this functions encapsulates the logic of creating the sharded table
  # as child of the DynamicSupervisor
  def start_table(name, opts) do
    tab = :shards.new(name, opts)
    pid = :shards_meta.tab_pid(tab)
    {:ok, pid, tab}
  end

  # DynamicSupervisor child spec
  defp table_spec(name, opts) do
    %{
      id: name,
      start: {__MODULE__, :start_table, [name, opts]},
      type: :supervisor
    }
  end
end
```

The final piece of configuration is to setup a `DynamicSupervisor` as a
supervisor within the application's supervision tree, which we can do in
`lib/my_app/application.ex`, inside the `start/2` function:

```elixir
def start(_type, _args) do
  children = [
    {DynamicSupervisor, strategy: :one_for_one, name: MyApp.DynamicShards}
  ]

  ...
```

Now we can use `:shards`:

```elixir
iex> MyApp.DynamicShards.new(:t1, [:named_table])
:t1
iex> t2 = MyApp.DynamicShards.new(:t2, [])
#Reference<0.2506606486.3592028173.65798>

iex> :shards.insert(:t1, a: 1, b: 2)
true
iex> :shards.insert(t2, c: 3, d: 4)
true

iex> :shards.lookup_element(:t1, :a, 2)
1
iex> :shards.lookup_element(t2, :c, 2)
3

# open the observer
iex> :observer.start()
ok

# delete a table
iex> MyApp.DynamicShards.delete(:t1)
:ok
```

## Advanced topics

### Caching the metadata

Like it is explained in [shards module](https://hexdocs.pm/shards/shards.html),
when a partitioned table is created there is a metadata associated to it, to
resolve the number of partitions and other needed attributes. Hence, every time
a function is executed, `shards` has to resolve the metadata first; but this is
done internally by `shards`. Nevertheless, `shards` allows to pass the metadata
as last argument for most of the functions (check the docs to see what functions
allow it), in case you are able to cache it and use it later for further
operations. This is possible because the metadata is something doesn't change,
so it can be easily cached and avoid `shards` the extra step to retrieve it
from the meta table. But be aware this is just an ETS lookup, so the improvement
is terms of performance may be insignificant, for that reason it is
recommendable to evaluate very carefully each scenario and and see if its really
worth it.

> The overall recommendation is to let `shards` take care of retrieving the
  metadata and everything else, just use the base `shards` API (ETS API).

In case you want to cache the metadata:

**Erlang:**

```erlang
> Tab = shards:new(mytab, [named_table, {partitions, 4}]).
named_table

> Meta = shards:meta(Tab).
{meta,<0.174.0>,1,4,fun erlang:phash2/2,false,[named_table]}
```

**Elixir:**

```elixir
iex> tab = :shards.new(:mytab, [:named_table, partitions: 4])
:mytab

iex> meta = :shards.meta(tab)
{:meta, #PID<0.163.0>, 1, 4, &:erlang.phash2/2, false, [:named_table]}
```

Then, you can use it for most of the functions (check the docs):

**Erlang:**

```erlang
> shards:insert(Tab, [{a, 1}, {b, 2}], Meta).
true

> shards:lookup(Tab, a, Meta).
[{a,1}]

> shards:lookup_element(Tab, a, 2, Meta).
1
```

**Elixir:**

```elixir
iex> :shards.insert(tab, [a: 1, b: 2], meta)
true

iex> :shards.lookup(tab, :a, meta)
[a: 1]

iex> :shards.lookup_element(tab, :a, 2, meta)
1
```

> **NOTE:** See [shards](https://hexdocs.pm/shards/shards.html) docs for more
  information about the functions that support receiving the metadata; most of
  them allow the metadata parameter, but there are few exceptions you should
  know.
