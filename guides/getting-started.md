# Getting Started

In this guide, we're going to learn some basics about `shards`, how to create
partitioned tables and how to use its API; which is the easiest part, since
it is the same ETS API.

## Installation

### Erlang

In your `rebar.config`:

```erlang
{deps, [
  {shards, "0.6.1"}
]}.
```

### Elixir

In your `mix.exs`:

```elixir
def deps do
  [{:shards, "~> 0.6"}]
end
```

### Adding shards as part of your supervision tree

For this part of configuration, we have to setup `shards_sup` as a supervisor
within the application's supervision tree.

<u>**Erlang**</u>

In your main supervisor module, inside the `init/1` callback:

```erlang
init({YourShardsSupName}) ->
  Children = [
    #{
      id    => YourShardsSupName,
      start => {shards_sup, start_link, [YourShardsSupName]},
      type  => supervisor
    }
  ],

  {ok, {{one_for_one, 10, 10}, Children}}.
```

<u>**Elixir**</u>

In `lib/YOUR_APP_NAME/application.ex`, inside the `start/2` function:

```elixir
def start(_type, _args) do
  children = [
    %{
        id: your_shards_sup_name,
        start: {:shards_sup, :start_link, [your_shards_sup_name]},
        type: :supervisor
      }
  ]

  ...
```

### Running shards as an independent app

In this case `shards` will run as an independent app on its own supervision
tree.

<u>**Erlang**</u>

We have to add `shards` to your list of applications in the `*.app.src` file:

```erlang
{application, your_app, [
  {description, "Your App"},
  {vsn, "0.1.0"},
  {registered, []},
  {mod, {your_app, []}},
  {applications, [
    kernel,
    stdlib,
    shards
  ]},
  {env,[]},
  {modules, []}
]}.
```

Then in your `rebar.config`:

```erlang
{shell, [{apps, [your_app]}]}.
```

Finally run an Erlang console:

```
$ rebar3 shell
```

<u>**Elixir**</u>

We have to add `shards` to your list of applications in the `mix.exs` file:

```elixir
def application do
  [
    mod: {YourApp.Application, []},
    extra_applications: [:logger, :shards]
  ]
end
```

Then run an Elixir console:

```
$ iex -S mix
```

## Creating partitioned tables

Exactly as ETS, `shards:new/2` function receives 2 arguments, the name of the
table and the options. `shards` adds more options:

 * `{n_shards, pos_integer()}` - Allows to set the desired number of shards or
   partitions. By default, the number of shards is the total of online
   schedulers (`erlang:system_info(schedulers_online)`).

  * `{scope, l | g}` - Defines the scope, in other words, if sharding will be
    applied locally (`l`) or global/distributed (`g`). By default is set to `l`.

  * `{restart_strategy, one_for_one | one_for_all}` - Allows to configure the
    restart strategy for `shards_owner_sup`. By default is set to `one_for_one`.

  * `{pick_shard_fun, pick_fun()}` - Function to pick the shard or partition
    based on the `Key` where the function will be applied locally; used by
    `shards_local`. Defaults to `shards_state:pick_fun()`.
    See [shards_state][shards_state].

  * `{pick_node_fun, pick_fun()}` - Function to pick the node based on the `Key`
    where the function will be applied globally/distributed; used by
    `shards_dist`. Defaults to `shards_state:pick_fun()`.
    See [shards_state][shards_state].

  * `{nodes, [node()]}` - A list of nodes to auto setup a distributed table.
    The table is created in all given nodes and then all nodes are joined.
    This option only has effect if the option `{scope, g}` has been set.

  * `{sup_name, atom()}` - Allows to define a custom name for `shards_sup`.
    By default is set to `shards_sup`. This option might be useful in the
    case you want to start `shards` as part of your app supervision tree;
    not as an independent app.

Wen a new table is created, the [State][shards_state] is created for that table
as well. The purpose of the **State** is to store information related to that
table, such as: number of shards, scope, ty pe, pick functions, etc. To learn
more about it, check out [shards_state module][shards_state].

[shards_state]: https://github.com/cabol/shards/blob/master/src/shards_state.erl

**Examples:**

```erlang
% let's create a table, such as you would create it with ETS, with 4 shards
> shards:new(tab1, [{n_shards, 4}]).
tab1

% create another one with default options
> shards:new(tab2, []).
tab2

% now open the observer so you can see what happened
> observer:start().
ok
```

You will see the supervision trees that `shards` has created. When you create a
new "table," what happens behind scenes is, `shards` creates a supervision tree
dedicated only for that group of shards, which represent your logical table
partitioned on multiple ETS tables, and everything is handled transparently
by`shards`, you only have to use the API like if you were working with a common
ETS table and that's it, `shards` will take care of everything for you.

## Inserting entries

The contract and the logic for insertion functions (`insert/2`, `insert_new/2`)
is a bit different in `shards`, specially for `insert_new/2`. The first big
difference is in `shards` these functions are not atomic. Due to we cannot
insert a list of objects in a single table, since for `shards` a table is
represented by several ones internally, `shards` has to compute the
shard/partition based on the key before to insert the object, so each object
may be inserted in a different partition; but again, this logic is hadled by
`shards`.

Let's insert some entries:

```erlang
% for insert is pretty much the same, the contract is the same
> shards:insert(mytab, {k1, 1}).
true
> shards:insert(mytab, [{k1, 1}, {k2, 2}]).
true

% for insert_new
> shards:insert_new(mytab, {k3, 3}).
true
> shards:insert_new(mytab, {k3, 3}).
false
> shards:insert_new(mytab, [{k3, 3}, {k4, 4}]).
false
```

## Playing with shards

Let's use some write/read operations against the partitioned tables we just
created previously:

```erlang
% inserting some objects
> shards:insert(mytab1, [{k1, 1}, {k2, 2}, {k3, 3}]).
true

% let's check those objects
> shards:lookup(mytab1, k1).
[{k1,1}]
> shards:lookup(mytab1, k2).
[{k2,2}]
> shards:lookup(mytab1, k3).
[{k3,3}]
> shards:lookup(mytab1, k4).
[]

% delete an object and then check
> shards:delete(mytab1, k3).
true
> shards:lookup(mytab1, k3).
[]

% now let's find all stored objects using select
> MatchSpec = ets:fun2ms(fun({K, V}) -> {K, V} end).
[{{'$1','$2'},[],[{{'$1','$2'}}]}]
> shards:select(mytab1, MatchSpec).
[{k1,1},{k2,2}]
```

As you may have noticed, it's extremely easy, because almost all ETS functions
are implemented by shards, it's only matters of replacing `ets` module by
`shards`.

You can try the rest of the ETS API but using `shards`.

## Deleting partitioned tables

**Shards** behaves in an elastic way, as you saw, more shards can be
added/removed dynamically:

```erlang
> shards:delete(mytab1).
true

> observer:start().
ok
```

See how `shards` gets shrinks.

## Using shards_local directly

The module `shards` is a wrapper on top of two main modules:

 * [shards_local](../src/shards_local.erl): Implements Sharding but locally,
   on a single Erlang node.

 * [shards_dist](../src/shards_dist.erl): Implements Sharding but globally,
   running on top of multiple distributed Erlang nodes; `shards_dist` uses
   `shards_local` internally.

When you use `shards` on top of `shards_local`, a call to the control ETS table
owned by `shards_owner_sup` must be done, in order to recover the
[State][shards_state], mentioned previously.

Most of the `shards_local` functions allow to pass the `State` as a parameter,
therefore, it must be fetched before to call it. To learn more about it,
check out how [shards](../src/shards.erl) module is implemented.

So, if in your case any microsecond matters, you can skip the call to the
control table by calling `shards_local` directly. There are two options
to do so.

### Option 1

The first option is getting the `State`, and passing it as an argument. Now the
question is: how to get the `State`? Well, it's extremely easy, you can get
the `State` when you call `shards:new/2` for the first time, or you can call
`shards:state/1`, `shards_state:get/1` or `shards_state:new/0,1,2,3,4` at any
time you want, and then it might be stored within the calling process, or
wherever you want. For example:

```erlang
% take a look at the 2nd element of the returned tuple, that is the state
> shards:new(mytab, [{n_shards, 4}]).
mytab

% remember you can get the state at any time you want
> State = shards:state(mytab).
{state,shards_local,shards_sup,4,#Fun<shards_local.pick.3>,
       #Fun<shards_local.pick.3>}

% or
> State = shards_state:get(mytab).
{state,shards_local,shards_sup,4,#Fun<shards_local.pick.3>,
       #Fun<shards_local.pick.3>}

% now you can call shards_local directly
> shards_local:insert(mytab, {1, 1}, State).
true
> shards_local:lookup(mytab, 1, State).
[{1,1}]

% in this case, only n_shards option is different from the default one,
% so you can do this:
> shards_local:lookup(mytab, 1, shards_state:new(4)).
[{1,1}]
```

### Option 2

The second option is to call `shards_local` directly without the `state`, but
this is only possible if you have created a table with default options, such as
`n_shards`, `pick_shard_fun` and `pick_node_fun`. If you can take this option,
it might be significantly better, since in this case no additional calls are
needed, not even to recover the `State` (like in the previous option), because
a new `State` is created with default values every time you call a
`shards_local` function. Therefore, the call is mapped directly to an ETS
function. For example:

```erlang
% create a table without set n_shards, pick_shard_fun or pick_node_fun
> shards:new(mytab, []).
mytab

% call shards_local without the state
> shards_local:insert(mytab, {1, 1}).
true
> shards_local:lookup(mytab, 1).
[{1,1}]
```

Most of the cases this is not necessary, `shards` wrapper is more than enough,
it adds only a few microseconds of latency. In conclusion, **Shards** gives you
the flexibility to do so, but it's your call!

## Distributed Shards

So far, we've seen how `shards` works locally, now let's see how `shards`
works but distributed.

**1.** Let's start 3 Erlang consoles running shards:

Node `a`:

```
$ rebar3 shell --name a@127.0.0.1
```

Node `b`:

```
$ rebar3 shell --name b@127.0.0.1
```

Node `c`:

```
$ rebar3 shell --name c@127.0.0.1
```

**2.** Create a table with global scope `{scope, g}` on each node and then
join them.

There are two ways to achieve this:

 * Manually create the table on each node and then from any of them join
   the rest.

    Create the table on each node:

    ```erlang
    % when a table is created with {scope, g}, the module shards_dist is used
    % internally by shards
    > shards:new(mytab, [{scope, g}]).
    mytab
    ```

    Join them. From node `a`, join `b` and `c` nodes:

    ```erlang
    > shards:join(mytab, ['b@127.0.0.1', 'c@127.0.0.1']).
    ['a@127.0.0.1','b@127.0.0.1','c@127.0.0.1']
    ```

    Let's check all nodes have the same list of joined nodes by running next
    function on each node:

    ```erlang
    > shards:get_nodes(mytab).
    ['a@127.0.0.1','b@127.0.0.1','c@127.0.0.1']
    ```

 * The easiest way is calling `shards:new/3` but passing the option
   `{nodes, Nodes}`, where `Nodes` is the list of nodes you want to join.

    From Node `a`:

    ```erlang
    > shards:new(mytab, [{scope, g}, {nodes, ['b@127.0.0.1', 'c@127.0.0.1']}]).
    mytab
    ```

    Let's check again on all nodes:

    ```erlang
    > shards:get_nodes(mytab).
    ['a@127.0.0.1','b@127.0.0.1','c@127.0.0.1']
    ```

**3.** Now **Shards** cluster is ready, let's do some basic operations:

From node `a`:

```erlang
> shards:insert(mytab, [{k1, 1}, {k2, 2}]).
true
```

From node `b`:

```erlang
> shards:insert(mytab, [{k3, 3}, {k4, 4}]).
true
```

From node `c`:

```erlang
> shards:insert(mytab, [{k5, 5}, {k6, 6}]).
true
```

Now, from any of previous nodes:

```erlang
> [shards:lookup_element(mytab, Key, 2) || Key <- [k1, k2, k3, k4, k5, k6]].
[1,2,3,4,5,6]
```

All nodes should return the same result.

Let's do some deletions, from any node:

```erlang
> shards:delete(mytab, k6).
true
```

And again, let's check it out from any node:

```erlang
% as you can see 'k6' was deleted
> shards:lookup(mytab, k6).
[]

% check remaining values
> [shards:lookup_element(mytab, Key, 2) || Key <- [k1, k2, k3, k4, k5]].
[1,2,3,4,5]
```
