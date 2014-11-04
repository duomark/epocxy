%%%------------------------------------------------------------------------------
%%% @copyright (c) 2014, DuoMark International, Inc.
%%% @author Jay Nelson <jay@duomark.com> [http://duomark.com/]
%%% @reference 2013 Development sponsored by TigerText, Inc. [http://tigertext.com/]
%%% @reference The license is based on the template for Modified BSD from
%%%   <a href="http://opensource.org/licenses/BSD-3-Clause">OSI</a>
%%% @doc
%%%   Synchronization barriers force multiple processes to pause until all
%%%   participants reach the same point. They then may proceed independently.
%%%
%%% @since 0.9.8
%%% @end
%%%------------------------------------------------------------------------------
-module(cxy_synch).
-author('Jay Nelson <jay@duomark.com>').

%% External API
-export([
         before_task/2,
         before_task/4,
         cleanup_star_pattern/2
        ]).

-include("tracing_levels.hrl").

-type pid_count()   :: non_neg_integer().
-type timeout_ms()  :: pos_integer().
-type bare_mfa()    :: {module(), atom(), list()}.
-type bare_fun()    :: fun(() -> any()).
-type barrier_fun() :: fun(() -> any()).
-type synch_error() :: {error, no_start}
                     | {error, {not_synched, {pid_count(), pid_count()}}}.

-export_type([bare_mfa/0, bare_fun/0, pid_count/0, timeout_ms/0, synch_error/0]).


%%%------------------------------------------------------------------------------
%%% External API
%%%------------------------------------------------------------------------------

-spec before_task(pid_count(), bare_mfa() | bare_fun()) -> ok | synch_error().
-spec before_task(pid_count(), timeout_ms(), timeout_ms(), bare_mfa() | bare_fun()) -> ok | synch_error().

before_task(Num_Pids_To_Synch, Task_Fun)
  when is_integer(Num_Pids_To_Synch), Num_Pids_To_Synch > 0 ->
    before_task(Num_Pids_To_Synch, 1000, 1000, Task_Fun).

before_task(Num_Pids_To_Synch, Spawn_Timeout, Synch_Timeout, Task_Fun) ->
    {Synch_Result, Task_Pids}
        = case Task_Fun of
              Task_Fun when is_function(Task_Fun, 0) ->
                  synch_pids(Num_Pids_To_Synch, Spawn_Timeout, Synch_Timeout);
              {_Module, _Task_Fun, _Args}
                when is_atom(_Module), is_atom(_Task_Fun), is_list(_Args) ->
                  synch_pids(Num_Pids_To_Synch, Spawn_Timeout, Synch_Timeout)
          end,
    open_barrier_gate(Synch_Result, Task_Pids, Task_Fun).


%%%------------------------------------------------------------------------------
%%% Synchronization support functions
%%%------------------------------------------------------------------------------

synch_pids(Num_Pids, Spawn_Timeout, Synch_Timeout) ->

    %% A barrier function is used to allow all spawned pids to wait at the same point...
    {Self,        Coord_Ref} = {self(), make_ref()},
    {Barrier_Fun, Synch_Ref} = create_barrier_fun(Num_Pids, Spawn_Timeout),
    Spawn_Workers_Fun = fun() -> spawn_link_wait_pids(Self, Coord_Ref, Synch_Ref,
                                                      Num_Pids, Synch_Timeout, Barrier_Fun) end,

    %% The synchronization messages are collected by the coordinator...
    trace(coord_start, {Coord_Ref}),
    {Coordinator_Pid, Coord_Monitor_Ref} = spawn_monitor(Spawn_Workers_Fun),
    wait_for_synch(Coord_Ref, Coord_Monitor_Ref, Coordinator_Pid, Spawn_Timeout, Synch_Timeout, []).

wait_for_synch(Coord_Ref, Coord_Mref, Coord_Pid, Spawn_Timeout, Synch_Timeout, Spawned_Pids) ->

    {Timeout_Trace_Type, Receive_Timeout}
        = case Spawned_Pids of
              [] -> {spawn_timeout, Spawn_Timeout};
              _  -> {synch_timeout, Synch_Timeout}
          end,

    receive
        %% First message is spawned to allow for spawn timeout and tracing...
        {Coord_Ref, spawned, New_Pids} ->
            wait_for_synch(Coord_Ref, Coord_Mref, Coord_Pid, Spawn_Timeout, Synch_Timeout, New_Pids);

        %% The second message determines the result of synching.
        {Coord_Ref, {error, _} = Err}   -> {Err,     Spawned_Pids};
        {Coord_Ref, synched, _Num_Pids} -> {synched, Spawned_Pids};

        %% But the Coordinator may go down before synching is complete.
        {'DOWN', Coord_Mref, process, Coord_Pid, Error} ->
            trace(coordinator_dead, {Coord_Ref, Error}),
            {error, {synchronization_coordinator_failure, Error}}

    after Receive_Timeout ->
            trace(Timeout_Trace_Type, {Receive_Timeout}),
            {error, {Timeout_Trace_Type, Receive_Timeout}}
    end.

spawn_link_wait_pids(Caller, Coord_Ref, Synch_Ref, Num_Pids, Synch_Timeout, Barrier_Fun) ->
    process_flag(trap_exit, true),
    Self = self(),
    Pids = spawn_link_times(fun() -> coordinate(Self, Synch_Ref, Synch_Timeout) end, Num_Pids, []),
    trace(pids_spawned, {Coord_Ref, Num_Pids}),
    Caller ! {Coord_Ref, spawned, Pids},
    trace(wait_at_barrier, {}),
    case Barrier_Fun() of
        {error, Err} -> trace(synch_error,  {Coord_Ref, Err}), Caller ! {Coord_Ref, {error, Err}};
        synched      -> trace(synched, {Coord_Ref, Num_Pids}), Caller ! {Coord_Ref, synched, Num_Pids}
    end.

%% List Comprehension requires lists:seq(1,N) when N is large.
spawn_link_times(_Fun, 0, Pids) -> Pids;
spawn_link_times( Fun, N, Pids) ->
    New_Pid = spawn_link(Fun),
    trace(spawn, {New_Pid}),
    spawn_link_times(Fun, N-1, [New_Pid | Pids]).


%% All spawned workers are linked to the coordinator,
%% so killing it takes all of them with the coordinator,
%% and we make sure to flush the message queue of the
%% downed coordinator message.
cleanup_star_pattern(Coordinator_Pid, Coord_Monitor_Ref) ->
    exit(Coordinator_Pid, kill),
    erlang:yield(),
    demonitor(Coord_Monitor_Ref, [flush]).

open_barrier_gate({error, _} = Error, _Pids, _Task_Fun) -> Error;
open_barrier_gate(synched,             Pids,  Task_Fun) ->
    _ = [Pid ! {start, Task_Fun} || Pid <- Pids],
    ok.


%%%-----------------------------------------------------------------------
%%% Coordination among workers, barrier function and tracing utilities.
%%%-----------------------------------------------------------------------

-spec coordinate(pid(), reference(), timeout_ms())
                -> synched
                       | {error, no_start}
                       | {error, {not_synched, {pos_integer(), pos_integer()}}}.

-spec create_barrier_fun(pid_count(), timeout_ms()) -> {barrier_fun(), reference()}.

coordinate(Coordinator, Synch_Ref, Synch_Timeout) ->
    trace(ready, {Synch_Ref, self()}),
    Coordinator ! {Synch_Ref, ready, self()},
    receive {start, Task_Fun} ->
            trace(start, {self()}),
            Result = case Task_Fun of
                         {Mod, Fun, Args} -> Mod:Fun(Args);
                         Task_Fun         -> Task_Fun()
                     end,
            trace(completed, {self(), Result}),
            Result
    after Synch_Timeout ->
            trace(no_start, {Synch_Timeout, self()}),
            {error, no_start}
    end.

create_barrier_fun(Num_Pids_To_Synch, Spawn_Timeout)
  when is_integer(Num_Pids_To_Synch), Num_Pids_To_Synch > 0,
       is_integer(Spawn_Timeout),     Spawn_Timeout     > 0 ->

    %% Create a recursive function with a receive barrier reachable within Timeout milliseconds.
    %% Using anonymous fun() to be compatible with R16 and prior VMs.
    %% TODO: Total Time Elapsed should be less than Timeout, not just the last message rcvd.
    Synchronization_Ref = make_ref(),
    {fun() -> Barrier_Fun = fun(_, _Start_Time, _Timeout, 0, Num_Pids) ->
                                    trace(synched, {Num_Pids}),
                                    synched;
                               (F,  Start_Time, Timeout, Remaining, Num_Pids) ->
                                    case remaining_timeout(Start_Time, Timeout) of
                                        Expired when Expired =< 0 ->
                                            trace(expired, {Remaining, Num_Pids}),
                                            {error, {not_synched, {Remaining, Num_Pids}}};
                                        Remaining_Time ->
                                            trace(remaining_time, {Remaining_Time}),
                                            receive
                                                {Synchronization_Ref, ready, Pid} ->
                                                    Remaining_Pid_Count = Remaining - 1,
                                                    trace(pid_synch, {Pid, Remaining_Pid_Count}),
                                                    F(F, Start_Time, Timeout, Remaining_Pid_Count, Num_Pids)
                                            after Remaining_Time ->
                                                    trace(barrier_timeout, {Timeout}),
                                                    {error, {not_synched, {Remaining, Num_Pids}}}
                                            end
                                    end
                            end,
              Start_Time = os:timestamp(),
              Barrier_Fun(Barrier_Fun, Start_Time, Spawn_Timeout, Num_Pids_To_Synch, Num_Pids_To_Synch)
     end, Synchronization_Ref}.

remaining_timeout(Start_Time, Original_Timeout) ->
    (Original_Timeout - timer:now_diff(os:timestamp(), Start_Time) div 1000).

%% Tracing of messages back to application...
trace(coord_start,               {Ref}) -> et:trace_me(?TRACE_TIMINGS, app, coord, coord_start,   [Ref]);
trace(pids_spawned,      {Ref,  Count}) -> et:trace_me(?TRACE_TIMINGS, coord, app, pids_spawned,  [Ref, Count]);
trace(synched,           {Ref,  Count}) -> et:trace_me(?TRACE_TIMINGS, coord, app, synched,       [Ref, Count]);

trace(start,                     {Pid}) -> et:trace_me(?TRACE_TIMINGS, coord, Pid, start,         [Pid]);
trace(no_start,          {Time,   Pid}) -> et:trace_me(?TRACE_TIMINGS, Pid, coord, no_start,      [Time]);

trace(coordinator_dead,  {Ref, Reason}) -> et:trace_me(?TRACE_TIMINGS, coord, app, coord_dead,    [Ref, Reason]);
trace(spawn_timeout,            {Time}) -> et:trace_me(?TRACE_TIMINGS, coord, app, spawn_timeout, [Time]);
trace(synch_timeout,            {Time}) -> et:trace_me(?TRACE_TIMINGS, coord, app, synch_timeout, [Time]);
trace(synch_error,       {Ref,  Error}) -> et:trace_me(?TRACE_TIMINGS, coord, app, synch_error,   [Ref, Error]);

%% Tracing of messages within the barrier coordinator...
trace(barrier_timeout,   {Time}) -> et:trace_me(?TRACE_TIMINGS, barrier, coord,   barrier_timeout, [Time]);
trace(expired,     {Unack, All}) -> et:trace_me(?TRACE_TIMINGS, barrier, coord,   expired,   [Unack, All]);
trace(synched,            {All}) -> et:trace_me(?TRACE_TIMINGS, barrier, coord,   synched,          [All]);
trace(wait_at_barrier,       {}) -> et:trace_me(?TRACE_TIMINGS, coord, barrier,   wait,                []);
trace(ready,         {Ref, Pid}) -> et:trace_me(?TRACE_TIMINGS, Pid,   barrier,   ready,       [Ref, Pid]);

%% Tracing of individual pid synchronization, only used for debugging.
trace(spawn,              {Pid}) -> et:trace_me(?TRACE_DEBUG,   coord,   Pid,     spawn,                []);
trace(pid_synch,   {Pid, Count}) -> et:trace_me(?TRACE_DEBUG,   Pid,     coord,   pid_synch,  [Pid, Count]);
trace(remaining_time,      {RT}) -> et:trace_me(?TRACE_DEBUG,   barrier, barrier, remaining_time,     [RT]);
trace(completed,  {Pid, Result}) -> et:trace_me(?TRACE_DEBUG,   Pid,     app,     completed, [Pid, Result]).
