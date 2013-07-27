-module(cxy_ctl_SUITE).
-auth('jay@duomark.com').
-vsn('').

-export([all/0, init_per_suite/1, end_per_suite/1]).
-export([
         check_no_timer_limits/1, check_with_timer_limits/1,
         check_atom_limits/1, check_limit_errors/1,
         check_concurrency_types/1,
         check_execute_task/1, check_execute_pid/1
        ]).

%% Spawned functions
-export([put_pdict/2]).

-include_lib("common_test/include/ct.hrl").

-spec all() -> [atom()].

all() -> [
          check_no_timer_limits, check_with_timer_limits,
          check_atom_limits, check_limit_errors,
          check_concurrency_types,
          check_execute_task, check_execute_pid
         ].

-type config() :: proplists:proplist().
-spec init_per_suite(config()) -> config().
-spec end_per_suite(config()) -> config().

init_per_suite(Config) -> Config.
end_per_suite(Config)  -> Config.

%% Test Modules is ?TM
-define(TM, cxy_ctl).


-spec check_no_timer_limits(proplists:proplist()) -> ok.
check_no_timer_limits(_Config) ->
    Limits = [{a, 15, 0}, {b, 35, 0}],
    ok = ?TM:init(Limits),
    All_Entries = ets:tab2list(?TM),
    2 = length(All_Entries),
    true = lists:member({a, 15, 0, 0}, All_Entries),
    true = lists:member({b, 35, 0, 0}, All_Entries),
    ok.

-spec check_with_timer_limits(proplists:proplist()) -> ok.
check_with_timer_limits(_Config) ->
    Limits = [{a, 15, 5}, {b, 35, 0}, {c, 17, 4}],
    ok = ?TM:init(Limits),
    All_Entries = ets:tab2list(?TM),
    3 = length(All_Entries),
    true = lists:member({a, 15, 0, 5}, All_Entries),
    true = lists:member({b, 35, 0, 0}, All_Entries),
    true = lists:member({c, 17, 0, 4}, All_Entries),
    ok.

-spec check_atom_limits(proplists:proplist()) -> ok.
check_atom_limits(_Config) ->
    Limits = [{a, unlimited, 0},   {b, unlimited, 5},
              {c, inline_only, 0}, {d, inline_only, 7}],
    ok = ?TM:init(Limits),
    All_Entries = ets:tab2list(?TM),
    4 = length(All_Entries),
    true = lists:member({a, -1, 0, 0}, All_Entries),
    true = lists:member({b, -1, 0, 5}, All_Entries),
    true = lists:member({c,  0, 0, 0}, All_Entries),
    true = lists:member({d,  0, 0, 7}, All_Entries),
    ok.

-spec check_limit_errors(proplists:proplist()) -> ok.
check_limit_errors(_Config) ->
    Limits1 = [{a, unlimited, -1}, {b, 5, 0}, {c, unlimited, 0}],
    {error, {invalid_init_args, [{a, unlimited, -1}]}} = ?TM:init(Limits1),
    Limits2 = [{a, unlimited, -1}, {b, foo, 0}, {c, 0, bar}],
    {error, {invalid_init_args, Limits2}} = ?TM:init(Limits2),
    ok.

-spec check_concurrency_types(proplists:proplist()) -> ok.
check_concurrency_types(_Config) ->
    Limits = [{a, unlimited, 0}, {b, 17, 5}, {c, 8, 0}, {d, inline_only, 7}],
    ok = ?TM:init(Limits),
    Types = ?TM:concurrency_types(),
    [[a, -1, 0, 0], [b, 17, 0, 5], [c, 8, 0, 0], [d, 0, 0, 7]]
        = [[proplists:get_value(P, This_Type_Props)
            || P <- [task_type, max_procs, active_procs, max_history]]
           || This_Type_Props <- Types],
    ok.

%% execute_task runs a background task without feedback.
-spec check_execute_task(proplists:proplist()) -> ok.
check_execute_task(_Config) ->
    Limits = [{ets_inline, 0, 0}, {ets_spawn, 3, 5}],
    ok = ?TM:init(Limits),
    check_execute_task = ets:new(check_execute_task, [public, named_table]),

    try
        %% Inline update the shared ets table...
        ok = ?TM:execute_task(ets_inline, ets, insert_new, [check_execute_task, {joe, 5}]),
        [{joe, 5}] = ets:lookup(check_execute_task, joe),
        ok = ?TM:execute_task(ets_inline, ets, insert, [check_execute_task, {joe, 7}]),
        [{joe, 7}] = ets:lookup(check_execute_task, joe),
        true = ets:delete(check_execute_task, joe),

        %% Spawn update the shared ets table.
        ok = ?TM:execute_task(ets_spawn, ets, insert_new, [check_execute_task, {joe, 4}]),
        erlang:yield(),
        [{joe, 4}] = ets:lookup(check_execute_task, joe),
        ok = ?TM:execute_task(ets_spawn, ets, insert, [check_execute_task, {joe, 6}]),
        erlang:yield(),
        [{joe, 6}] = ets:lookup(check_execute_task, joe),
        true = ets:delete(check_execute_task, joe)
    after true = ets:delete(check_execute_task)
    end,

    ok.

%% execute_pid runs a task with a return value of the Pid or {inline, Result}.
-spec check_execute_pid(proplists:proplist()) -> ok.
check_execute_pid(_Config) ->
    Limits = [{pdict_inline, 0, 0}, {pdict_spawn, 3, 5}],
    ok = ?TM:init(Limits),
    
    %% When inline, update our process dictionary...
    Old_Joe = erase(joe),
    try
        {inline, undefined} = ?TM:execute_pid(pdict_inline, erlang, put, [joe, 5]),
        5 = get(joe),
        {inline, 5} = ?TM:execute_pid(pdict_inline, erlang, put, [joe, 7]),
        7 = get(joe)
    after put(joe, Old_Joe)
    end,

    %% When spawned, it affects a new process dictionary, not ours.
    Old_Joe = erase(joe),
    try
        undefined = get(joe),
        New_Pid = ?TM:execute_pid(pdict_spawn, ?MODULE, put_pdict, [joe, 5]),
        false = New_Pid =:= self(),
        New_Pid ! {self(), get_pdict, joe},
        undefined = get(joe),
        ok = receive {get_pdict, New_Pid, 5} -> ok
             after 100 -> timeout
             end
    after put(joe, Old_Joe)
    end,

    ok.

-spec put_pdict(any(), any()) -> {get_pdict, pid(), get(any())}.
put_pdict(Key, Value) ->
    put(Key, Value),
    receive {From, get_pdict, Key} -> From ! {get_pdict, self(), get(Key)} end.
            
