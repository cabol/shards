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

  delete(_Group) -> ok.

  join(Group, Pids) when is_list(Pids) ->
    pg:join(Group, Pids);

  join(Group, Pid) ->
    %% HACK: Maybe implement apply_on_target?
    Self = node(),
    case node(Pid) of
      Self ->
        pg:join(Group, Pid);
      OwnerNode ->
        spawn(OwnerNode, pg, join, [Group, Pid]),
        ok
    end.

  leave(Group, Pids) when is_list(Pids) ->
    pg:leave(Group, Pids);

  leave(Group, Pid) ->
    Self = node(),
    case node(Pid) of
      Self ->
        pg:leave(Group, Pid);
      OwnerNode ->
        spawn(OwnerNode, pg, leave, [Group, Pid]),
        ok
    end.

  get_members(Group) -> pg:get_members(Group).

-else.
  %% In OTP versions under 23, pg module is not available.
  create(Group) -> pg2:create(Group).

  delete(Group) -> pg2:delete(Group).

  join(Group, PidOrPids) -> pg2:join(Group, PidOrPids).

  leave(Group, PidOrPids) -> pg2:leave(Group, PidOrPids).

  get_members(Group) -> pg2:get_members(Group).
-endif.
