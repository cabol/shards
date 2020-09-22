-module(shards_cluster).

-export([
  create/1,
  delete/1,
  join/2,
  leave/2,
  get_members/1
]).

-ifndef(OTP_RELEASE).
  %% Because OTP_RELEASE macro was introduced since OTP 21, so ensure it is defined
  -define(OTP_RELEASE, 20). %=> I think it can be any value less than 21
-endif.

-if(?OTP_RELEASE >= 23).
  create(_Group) -> ok.

  delete(Group) ->
    %% In order to delete a group in pg, We need to make all members leave!
    GrouppedPids = group_by_node(get_members(Group)),
    pg_action_on_nodes(leave, [Group], GrouppedPids).

  join(Group, Pids) when is_list(Pids) ->
    GrouppedPids = group_by_node(Pids),
    pg_action_on_nodes(join, [Group], GrouppedPids);

  join(Group, Pid) ->
    %% HACK: Maybe implement apply_on_target?
    Self = node(),
    case node(Pid) of
      Self ->
        pg:join(Group, Pid);
      OwnerNode ->
        erpc:call(OwnerNode, pg, join, [Group, Pid])
    end.

  leave(Group, Pids) when is_list(Pids) ->
    GrouppedPids = group_by_node(Pids),
    pg_action_on_nodes(leave, [Group], GrouppedPids);

  leave(Group, Pid) ->
    Self = node(),
    case node(Pid) of
      Self ->
        pg:leave(Group, Pid);
      OwnerNode ->
        erpc:call(OwnerNode, pg, leave, [Group, Pid])
    end.

  get_members(Group) -> pg:get_members(Group).

  group_by_node(Pids) -> group_by_node(Pids, #{}).

  group_by_node([Pid | Tail], Acc) ->
    Node = node(Pid),
    NodeMembers = maps:get(Node, Acc, []),
    group_by_node(Tail, maps:put(Node, [Pid | NodeMembers], Acc));

  group_by_node([], Acc) -> Acc.

  pg_action_on_node(Node, Action, Params, [Pid | Tail]) ->
    _ = erpc:call(Node, pg, Action, Params ++ [Pid]),
    pg_action_on_node(Node, Action, Params, Tail);

  pg_action_on_node(_Node, _Action, _Params, []) -> ok.

  pg_action_on_nodes(Action, Params, GrouppedPids) ->
    lists:foreach(fun(Node) ->
                    Pids = maps:get(Node, GrouppedPids), 
                    pg_action_on_node(Node, Action, Params, Pids)
                  end, maps:keys(GrouppedPids)).

-else.
  %% In OTP versions under 23, pg module is not available.
  create(Group) -> pg2:create(Group).

  delete(Group) -> pg2:delete(Group).

  join(Group, PidOrPids) -> pg2:join(Group, PidOrPids).

  leave(Group, PidOrPids) -> pg2:leave(Group, PidOrPids).

  get_members(Group) -> pg2:get_members(Group).
-endif.
