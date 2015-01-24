%%%------------------------------------------------------------------------------
%%% @copyright (c) 2013-2015, DuoMark International, Inc.
%%% @author Jay Nelson <jay@duomark.com>
%%% @reference 2013-2015 Development sponsored by TigerText, Inc. [http://tigertext.com/]
%%% @reference The license is based on the template for Modified BSD from
%%%   <a href="http://opensource.org/licenses/BSD-3-Clause">OSI</a>
%%% @doc
%%%   Tests for cxy_ctl using common test.
%%%
%%% @since 0.9.6
%%% @end
%%%------------------------------------------------------------------------------
-module(cxy_ctl_SUITE).
-auth('jay@duomark.com').
-vsn('').

-export([all/0, init_per_suite/1, end_per_suite/1]).
-export([
         check_proc_dict_helper/1,
         check_no_timer_limits/1,     check_with_timer_limits/1,
         check_atom_limits/1,         check_limit_errors/1,
         check_concurrency_types/1,
         check_execute_task/1,        check_maybe_execute_task/1,
         check_execute_pid_link/1,    check_maybe_execute_pid_link/1,
         check_execute_pid_monitor/1, check_maybe_execute_pid_monitor/1,
         check_multiple_init_calls/1, check_copying_dict/1
        ]).

%% Spawned functions
-export([put_pdict/2, fetch_ages/0, fetch_ets_ages/1]).

-include_lib("common_test/include/ct.hrl").

-spec all() -> [atom()].

all() -> [
          check_proc_dict_helper,
          check_no_timer_limits,     check_with_timer_limits,
          check_atom_limits,         check_limit_errors,
          check_concurrency_types,
          check_execute_task,        check_maybe_execute_task,
          check_execute_pid_link,    check_maybe_execute_pid_link,
          check_execute_pid_monitor, check_maybe_execute_pid_monitor,
          check_multiple_init_calls, check_copying_dict
         ].

-type config() :: proplists:proplist().
-spec init_per_suite(config()) -> config().
-spec end_per_suite(config()) -> config().

init_per_suite(Config) -> Config.
end_per_suite(Config)  -> Config.

%% Test Modules is ?TM
-define(TM, cxy_ctl).


-spec check_proc_dict_helper(config()) -> ok.
check_proc_dict_helper(_Config) ->
    {'$$dict_prop', {boo, 22}} = ?TM:make_process_dictionary_default_value(boo, 22),
    {'$$dict_prop', {{k,v}, {key,value}}} = ?TM:make_process_dictionary_default_value({k,v}, {key,value}),
    ok.

-spec check_no_timer_limits(config()) -> ok.
check_no_timer_limits(_Config) ->
    Limits = [{a, 15, 0}, {b, 35, 0}],
    true = ?TM:init(Limits),
    All_Entries = ets:tab2list(?TM),
    2 = length(All_Entries),
    true = lists:member({a, 15, 0, 0}, All_Entries),
    true = lists:member({b, 35, 0, 0}, All_Entries),
    ok.

-spec check_with_timer_limits(config()) -> ok.
check_with_timer_limits(_Config) ->
    Limits = [{a, 15, 5}, {b, 35, 0}, {c, 17, 4}],
    true = ?TM:init(Limits),
    All_Entries = ets:tab2list(?TM),
    3 = length(All_Entries),
    true = lists:member({a, 15, 0, 5}, All_Entries),
    true = lists:member({b, 35, 0, 0}, All_Entries),
    true = lists:member({c, 17, 0, 4}, All_Entries),
    ok.

-spec check_atom_limits(config()) -> ok.
check_atom_limits(_Config) ->
    Limits = [{a, unlimited, 0},   {b, unlimited, 5},
              {c, inline_only, 0}, {d, inline_only, 7}],
    true = ?TM:init(Limits),
    All_Entries = ets:tab2list(?TM),
    4 = length(All_Entries),
    true = lists:member({a, -1, 0, 0}, All_Entries),
    true = lists:member({b, -1, 0, 5}, All_Entries),
    true = lists:member({c,  0, 0, 0}, All_Entries),
    true = lists:member({d,  0, 0, 7}, All_Entries),
    ok.

-spec check_limit_errors(config()) -> ok.
check_limit_errors(_Config) ->
    Limits1 = [{a, unlimited, -1}, {b, 5, 0}, {c, unlimited, 0}],
    {error, {invalid_init_args, [{a, unlimited, -1}]}} = ?TM:init(Limits1),
    Limits2 = [{a, unlimited, -1}, {b, foo, 0}, {c, 0, bar}],
    {error, {invalid_init_args, Limits2}} = ?TM:init(Limits2),

    %% Call init with good values to test add/remove/adjust...
    Limits = [{a, unlimited, 0}, {b, 17, 5}, {c, 8, 0}, {d, inline_only, 7}],
    true = ?TM:init(Limits),

    {error, {add_duplicate_task_types, Limits}} = ?TM:add_task_types(Limits),
    Limits3 = [{g, foo, 1}, {h, 17, -1}],
    {error, {invalid_add_args, Limits3}} = ?TM:add_task_types(Limits3),

    Limits3a = [{TT, L} || {TT, L, _H} <- Limits3],
    {error, {missing_task_types,  [{g, foo}, {h, 17}]}} = ?TM:adjust_task_limits(Limits3a),
    Limits4 = [{a, foo}, {b, -1}],
    {error, {invalid_task_limits, Limits4}} = ?TM:adjust_task_limits(Limits4),
    ok.

-spec check_concurrency_types(config()) -> ok.
check_concurrency_types(_Config) ->
    Limits = [{a, unlimited, 0}, {b, 17, 5}, {c, 8, 0}, {d, inline_only, 7}],
    true = ?TM:init(Limits),
    Types = ?TM:concurrency_types(),
    [[a, unlimited, 0, 0], [b, 17, 0, 5], [c, 8, 0, 0], [d, inline_only, 0, 7]]
        = [[proplists:get_value(P, This_Type_Props)
            || P <- [task_type, max_procs, active_procs, max_history]]
           || This_Type_Props <- Types],
    ok.

%% execute_task runs a background task without feedback.
-spec check_execute_task(config()) -> ok.
check_execute_task(_Config) ->
    {Inline_Type, Spawn_Type} = {ets_inline, ets_spawn},
    Limits = [{Inline_Type, inline_only, 2}, {Spawn_Type, 3, 5}],
    true = ?TM:init(Limits),
    Ets_Table = ets:new(check_execute_task, [public, named_table]),

    _ = try
        %% Inline update the shared ets table...
        ok = ?TM:execute_task(Inline_Type, ets, insert_new, [Ets_Table, {joe, 5}]),
        [{joe, 5}] = ets:lookup(Ets_Table, joe),
        ok = ?TM:execute_task(Inline_Type, ets, insert, [Ets_Table, {joe, 7}]),
        [{joe, 7}] = ets:lookup(Ets_Table, joe),
        true = ets:delete(Ets_Table, joe),

        %% Spawn update the shared ets table.
        ok = ?TM:execute_task(Spawn_Type, ets, insert_new, [Ets_Table, {joe, 4}]),
        erlang:yield(),
        [{joe, 4}] = ets:lookup(Ets_Table, joe),
        ok = ?TM:execute_task(Spawn_Type, ets, insert, [Ets_Table, {joe, 6}]),
        erlang:yield(),
        [{joe, 6}] = ets:lookup(Ets_Table, joe),
        true = ets:delete(Ets_Table, joe)
    after true = ets:delete(Ets_Table)
    end,

    ok.

%% maybe_execute_task runs a background task without feedback but not more than limit.
-spec check_maybe_execute_task(config()) -> ok.
check_maybe_execute_task(_Config) ->
    {Overmax_Type, Spawn_Type} = {ets_overmax, ets_spawn},
    Limits = [{Overmax_Type, inline_only, 0}, {Spawn_Type, 3, 5}],
    true = ?TM:init(Limits),
    Ets_Table = ets:new(check_maybe_execute_task, [public, named_table]),

    _ = try
        %% Over max should refuse to run...
        {max_pids, 0} = ?TM:maybe_execute_task(Overmax_Type, ets, insert_new, [Ets_Table, {joe, 5}]),
        erlang:yield(),
        [] = ets:lookup(Ets_Table, joe),
        {max_pids, 0} = ?TM:maybe_execute_task(Overmax_Type, ets, insert_new, [Ets_Table, {joe, 7}]),
        erlang:yield(),
        [] = ets:lookup(Ets_Table, joe),
        true = ets:delete(Ets_Table, joe),
        [0] = [proplists:get_value(active_procs, Props)
               || [{task_type, Type} | _] = Props <- cxy_ctl:concurrency_types(),
                  Type =:= Overmax_Type],

        %% Spawn update the shared ets table.
        ok = ?TM:maybe_execute_task(Spawn_Type, ets, insert_new, [Ets_Table, {joe, 4}]),
        erlang:yield(),
        [{joe, 4}] = ets:lookup(Ets_Table, joe),
        ok = ?TM:maybe_execute_task(Spawn_Type, ets, insert, [Ets_Table, {joe, 6}]),
        erlang:yield(),
        [{joe, 6}] = ets:lookup(Ets_Table, joe),
        true = ets:delete(Ets_Table, joe),
        [0] = [proplists:get_value(active_procs, Props)
               || [{task_type, Type} | _] = Props <- cxy_ctl:concurrency_types(),
                  Type =:= Overmax_Type]

    after true = ets:delete(Ets_Table)
    end,

    ok.

%% execute_pid_link runs a task with a return value of Pid or {inline, Result}.
-spec check_execute_pid_link(config()) -> ok.
check_execute_pid_link(_Config) ->
    {Inline_Type, Spawn_Type} = {pdict_inline, pdict_spawn},
    Limits = [{Inline_Type, inline_only, 2}, {Spawn_Type, 3, 5}],
    true = ?TM:init(Limits),
    
    %% When inline, update our process dictionary...
    Old_Joe = erase(joe),
    _ = try
        {inline, undefined} = ?TM:execute_pid_link(Inline_Type, erlang, put, [joe, 5]),
        5 = get(joe),
        {inline, 5} = ?TM:execute_pid_link(Inline_Type, erlang, put, [joe, 7]),
        7 = get(joe)
    after put(joe, Old_Joe)
    end,

    %% When spawned, it affects a new process dictionary, not ours.
    Self = self(),
    Old_Joe = erase(joe),
    _ = try
        undefined = get(joe),
        New_Pid = ?TM:execute_pid_link(Spawn_Type, ?MODULE, put_pdict, [joe, 5]),
        false = (New_Pid =:= Self),
        {links, [Self]} = process_info(New_Pid, links),
        {monitors,  []} = process_info(New_Pid, monitors),
        false = (New_Pid =:= self()),
        New_Pid ! {Self, get_pdict, joe},
        undefined = get(joe),
        ok = receive {get_pdict, New_Pid, 5} -> ok
             after 100 -> timeout
             end
    after put(joe, Old_Joe)
    end,

    ok.

%% maybe_execute_pid_link runs a task with a return value of Pid or {max_pids, Max}.
-spec check_maybe_execute_pid_link(config()) -> ok.
check_maybe_execute_pid_link(_Config) ->
    {Overmax_Type, Spawn_Type} = {pdict_overmax, pdict_spawn},
    Limits = [{Overmax_Type, inline_only, 0}, {Spawn_Type, 3, 5}],
    true = ?TM:init(Limits),
    
    %% When inline, update our process dictionary...
    Old_Joe = erase(joe),
    _ = try
        {max_pids, 0} = ?TM:maybe_execute_pid_link(Overmax_Type, erlang, put, [joe, 5]),
        erlang:yield(),
        undefined = get(joe),
        {max_pids, 0} = ?TM:maybe_execute_pid_link(Overmax_Type, erlang, put, [joe, 7]),
        erlang:yield(),
        undefined = get(joe),
        [0] = [proplists:get_value(active_procs, Props)
               || [{task_type, Type} | _] = Props <- cxy_ctl:concurrency_types(),
                  Type =:= Overmax_Type]

    after put(joe, Old_Joe)
    end,

    %% When spawned, it affects a new process dictionary, not ours.
    Self = self(),
    Old_Joe = erase(joe),
    _ = try
        undefined = get(joe),
        New_Pid = ?TM:maybe_execute_pid_link(Spawn_Type, ?MODULE, put_pdict, [joe, 5]),
        false = (New_Pid =:= Self),
        {links, [Self]} = process_info(New_Pid, links),
        {monitors,  []} = process_info(New_Pid, monitors),
        New_Pid ! {Self, get_pdict, joe},
        undefined = get(joe),
        ok = receive {get_pdict, New_Pid, 5} -> ok
             after 100 -> timeout
             end,
        [0] = [proplists:get_value(active_procs, Props)
               || [{task_type, Type} | _] = Props <- cxy_ctl:concurrency_types(),
                  Type =:= Overmax_Type]

    after put(joe, Old_Joe)
    end,

    ok.

%% execute_pid_monitor runs a task with a return value of {Pid, Monitor_Ref} or {inline, Result}.
-spec check_execute_pid_monitor(config()) -> ok.
check_execute_pid_monitor(_Config) ->
    {Inline_Type, Spawn_Type} = {pdict_inline, pdict_spawn},
    Limits = [{Inline_Type, inline_only, 0}, {Spawn_Type, 3, 5}],
    true = ?TM:init(Limits),
    
    %% When inline, update our process dictionary...
    Old_Joe = erase(joe),
    _ = try
        {inline, undefined} = ?TM:execute_pid_monitor(Inline_Type, erlang, put, [joe, 5]),
        5 = get(joe),
        {inline, 5} = ?TM:execute_pid_monitor(Inline_Type, erlang, put, [joe, 7]),
        7 = get(joe)
    after put(joe, Old_Joe)
    end,

    %% When spawned, it affects a new process dictionary, not ours.
    Self = self(),
    Old_Joe = erase(joe),
    _ = try
        undefined = get(joe),
        {New_Pid, _Monitor_Ref} = ?TM:execute_pid_monitor(Spawn_Type, ?MODULE, put_pdict, [joe, 5]),
        false = (New_Pid =:= self()),
        {links,        []} = process_info(New_Pid, links),
        New_Pid ! {Self, get_pdict, joe},
        undefined = get(joe),
        ok = receive {get_pdict, New_Pid, 5} -> ok
             after 100 -> timeout
             end
    after put(joe, Old_Joe)
    end,

    ok.

%% maybe_execute_pid_monitor runs a task with a return value of {Pid, Monitor_Ref} or {max_pids, Max}.
-spec check_maybe_execute_pid_monitor(config()) -> ok.
check_maybe_execute_pid_monitor(_Config) ->
    {Overmax_Type, Spawn_Type} = {pdict_overmax, pdict_spawn},
    Limits = [{Overmax_Type, inline_only, 0}, {Spawn_Type, 3, 5}],
    true = ?TM:init(Limits),
    
    %% When inline, update our process dictionary...
    Old_Joe = erase(joe),
    _ = try
        {max_pids, 0} = ?TM:maybe_execute_pid_monitor(Overmax_Type, erlang, put, [joe, 5]),
        erlang:yield(),
        undefined = get(joe),
        {max_pids, 0} = ?TM:maybe_execute_pid_monitor(Overmax_Type, erlang, put, [joe, 7]),
        erlang:yield(),
        undefined = get(joe),
        [0] = [proplists:get_value(active_procs, Props)
               || [{task_type, Type} | _] = Props <- cxy_ctl:concurrency_types(),
                  Type =:= Overmax_Type]

    after put(joe, Old_Joe)
    end,

    %% When spawned, it affects a new process dictionary, not ours.
    Self = self(),
    Old_Joe = erase(joe),
    _ = try
        undefined = get(joe),
        {New_Pid, _Monitor_Ref} = ?TM:maybe_execute_pid_monitor(Spawn_Type, ?MODULE, put_pdict, [joe, 5]),
        false = (New_Pid =:= Self),
        {links,        []} = process_info(New_Pid, links),
        New_Pid ! {Self, get_pdict, joe},
        undefined = get(joe),
        ok = receive {get_pdict, New_Pid, 5} -> ok
             after 100 -> timeout
             end,
        [0] = [proplists:get_value(active_procs, Props)
               || [{task_type, Type} | _] = Props <- cxy_ctl:concurrency_types(),
                  Type =:= Overmax_Type]

    after put(joe, Old_Joe)
    end,

    ok.

-spec check_multiple_init_calls(config()) -> ok.
check_multiple_init_calls(_Config) ->
    Limits1 = [{a, unlimited, 0}, {b, 17, 5}, {c, 8, 0}, {d, inline_only, 7}],
    true = ?TM:init(Limits1),
    {error, init_already_executed} = ?TM:init(Limits1),
    {error, init_already_executed} = ?TM:init([]),

    Cxy_Limits = [L || {_, L, _} <- Limits1],
    Cxy_Limits = [proplists:get_value(max_procs, P) || P <- ?TM:concurrency_types()],

    Dup1 = {b, 217, 15},
    Dup2 = {d, inline_only, 17},
    Limits2 = [{f, unlimited, 0}, Dup1, {e, 18, 10}, Dup2],
    {error, {add_duplicate_task_types, [Dup1, Dup2]}} = ?TM:add_task_types(Limits2),

    Error_Dups = [T || {T, _, _} <- Limits2 -- [Dup1, Dup2]],
    {error, {missing_task_types, Error_Dups}} = ?TM:remove_task_types([T || {T, _, _} <- Limits2]),
    2 = ?TM:remove_task_types([element(1,Dup1), element(1,Dup2)]),
    Missing_Task_Types = [T || {T, _, _} <- Limits2],
    {error, {missing_task_types, Missing_Task_Types}} = ?TM:remove_task_types([T || {T, _, _} <- Limits2]),
    true = ?TM:add_task_types(Limits2),
    {error, {add_duplicate_task_types, Limits1}} = ?TM:add_task_types(Limits1),

    [unlimited,217,8,inline_only,18,unlimited]
        = [proplists:get_value(max_procs, P) || P <- ?TM:concurrency_types()],
    ok.

-spec put_pdict(atom(), any()) -> {get_pdict, pid(), any()}.
put_pdict(Key, Value) ->
    put(Key, Value),
    get_pdict(Key).

get_pdict(Key) ->
    receive {From, get_pdict, Key} -> From ! {get_pdict, self(), get(Key)} end.

get_pdict() ->
    Vals = filter_pdict(),
    receive {From, get_pdict} -> From ! {get_pdict, self(), Vals} after 300 -> pdict_timeout end.

filter_pdict() -> [{K, V} || {{cxy_ctl, K}, V} <- get()].

-spec fetch_ages() -> pdict_timeout | {get_pdict, pid(), proplists:proplist()}.
fetch_ages() -> get_pdict().

-spec fetch_ets_ages(atom() | ets:tid()) -> ok.
fetch_ets_ages(Ets_Table) ->
    Vals = [{K, V} || {{cxy_ctl, K}, V} <- get()],
    ets:insert(Ets_Table, {results, Vals}),
    ok.

-spec check_copying_dict(config()) -> ok.
check_copying_dict(_Config) ->
    {Inline_Type, Spawn_Type} = {pd_inline, pd_spawn},
    Limits = [{Inline_Type, inline_only, 2}, {Spawn_Type, 3, 5}],
    true = ?TM:init(Limits),
    
    %% Init the current process dictionary...
    put({cxy_ctl, ann}, 13),
    put({cxy_ctl, joe},  5),
    put({cxy_ctl, sam},  7),
    Stable_Pre_Call_Dict = filter_pdict(),

    _ = try
        Self = self(),
        Ets_Table = ets:new(execute_task, [public, named_table]),
        Joe = ?TM:make_process_dictionary_default_value({cxy_ctl, joe}, 8),
        Sue = ?TM:make_process_dictionary_default_value({cxy_ctl, sue}, 4),

        ok = ?TM:execute_task(Spawn_Type, ?MODULE, fetch_ets_ages, [Ets_Table], all_keys),
        erlang:yield(),
        [{results, [{ann,13},{joe, 5},{sam, 7}]}] = ets:tab2list(Ets_Table),
        Stable_Pre_Call_Dict = filter_pdict(),

        ok = ?TM:execute_task(Spawn_Type, ?MODULE, fetch_ets_ages, [Ets_Table], [{cxy_ctl, joe}, {cxy_ctl, sam}]),
        erlang:yield(),
        [{results, [{sam, 7},{joe, 5}]}] = ets:tab2list(Ets_Table),
        Stable_Pre_Call_Dict = filter_pdict(),

        ok = ?TM:execute_task(Spawn_Type, ?MODULE, fetch_ets_ages, [Ets_Table], [Joe, Sue]),
        erlang:yield(),
        [{results, [{sue, 4},{joe, 5}]}] = ets:tab2list(Ets_Table),
        Stable_Pre_Call_Dict = filter_pdict(),
        

        Pid1b = ?TM:execute_pid_link(Spawn_Type, ?MODULE, fetch_ages, [], all_keys),
        Pid1b ! {Self, get_pdict},
        ok = receive {get_pdict, Pid1b, [{ann,13},{joe, 5},{sam, 7}]} -> ok; Any1b -> {any, Any1b}
             after 300 -> test_timeout
             end,
        Stable_Pre_Call_Dict = filter_pdict(),

        Pid2b = ?TM:execute_pid_link(Spawn_Type, ?MODULE, fetch_ages, [], [{cxy_ctl, joe}, {cxy_ctl, sam}]),
        Pid2b ! {Self, get_pdict},
        ok = receive {get_pdict, Pid2b, [{sam, 7},{joe, 5}]} -> ok; Any2b -> {any, Any2b}
             after 300 -> test_timeout
             end,
        Stable_Pre_Call_Dict = filter_pdict(),

        Joe = ?TM:make_process_dictionary_default_value({cxy_ctl, joe}, 8),
        Sue = ?TM:make_process_dictionary_default_value({cxy_ctl, sue}, 4),
        Pid3b = ?TM:execute_pid_link(Spawn_Type, ?MODULE, fetch_ages, [], [Joe, Sue]),
        Pid3b ! {Self, get_pdict},
        ok = receive {get_pdict, Pid3b, [{sue, 4},{joe, 5}]} -> ok; Any3b -> {any, Any3b}
             after 300 -> test_timeout
             end,
        Stable_Pre_Call_Dict = filter_pdict(),

        %% Inlines mess up the local dictionary, so do them last...
        ok = ?TM:execute_task(Inline_Type, ?MODULE, fetch_ets_ages, [Ets_Table], all_keys),
        [{results, [{ann,13},{joe, 5},{sam, 7}]}] = ets:tab2list(Ets_Table),
        Stable_Pre_Call_Dict = filter_pdict(),

        {inline, ok} = ?TM:execute_pid_link(Inline_Type, ?MODULE, fetch_ets_ages, [Ets_Table], all_keys),
        [{results, [{ann,13},{joe, 5},{sam, 7}]}] = ets:tab2list(Ets_Table),
        Stable_Pre_Call_Dict = filter_pdict(),

        ok = ?TM:execute_task(Inline_Type, ?MODULE, fetch_ets_ages, [Ets_Table], [{cxy_ctl, joe}, {cxy_ctl, sam}]),
        [{results, [{ann,13},{joe, 5},{sam, 7}]}] = ets:tab2list(Ets_Table),
        Stable_Pre_Call_Dict = filter_pdict(),

        {inline, ok} = ?TM:execute_pid_link(Inline_Type, ?MODULE, fetch_ets_ages, [Ets_Table], [{cxy_ctl, joe}, {cxy_ctl, sam}]),
        [{results, [{ann,13},{joe, 5},{sam, 7}]}] = ets:tab2list(Ets_Table),
        Stable_Pre_Call_Dict = filter_pdict(),

        ok = ?TM:execute_task(Inline_Type, ?MODULE, fetch_ets_ages, [Ets_Table], [Joe, Sue]),
        [{results, [{ann,13},{sue,4},{joe, 5},{sam, 7}]}] = ets:tab2list(Ets_Table),
        [{sue,4}] = filter_pdict() -- Stable_Pre_Call_Dict,

        erase({cxy_ctl, sue}),

        Stable_Pre_Call_Dict = filter_pdict(),
        {inline, ok} = ?TM:execute_pid_link(Inline_Type, ?MODULE, fetch_ets_ages, [Ets_Table], [Joe, Sue]),
        [{results, [{ann,13},{sue,4},{joe, 5},{sam, 7}]}] = ets:tab2list(Ets_Table),
        [{sue,4}] = filter_pdict() -- Stable_Pre_Call_Dict,

        true = ets:delete(Ets_Table)

    after [13, 5, 7] = [erase(K) || K <- [{cxy_ctl, ann}, {cxy_ctl, joe}, {cxy_ctl, sam}]]
    end,

    ok.

