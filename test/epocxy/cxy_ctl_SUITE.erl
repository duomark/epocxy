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
         check_execute_pid_all_link_types/1,
         check_maybe_execute_pid_all_link_types/1,
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
          check_execute_pid_all_link_types,
          check_maybe_execute_pid_all_link_types,
          check_multiple_init_calls, check_copying_dict
         ].

-type config() :: proplists:proplist().
-spec init_per_suite(config()) -> config().
-spec end_per_suite(config()) -> config().

init_per_suite(Config) -> Config.
end_per_suite(Config)  -> Config.

%% Test Modules is ?TM
-define(TM, cxy_ctl).
-define(MAX_SLOW_FACTOR, 100000).

-spec check_proc_dict_helper(config()) -> ok.
check_proc_dict_helper(_Config) ->
    {'$$dict_prop', {boo, 22}} = ?TM:make_process_dictionary_default_value(boo, 22),
    {'$$dict_prop', {{k,v}, {key,value}}} = ?TM:make_process_dictionary_default_value({k,v}, {key,value}),
    ok.

-spec check_no_timer_limits(config()) -> ok.
check_no_timer_limits(_Config) ->
    Limits = [{a, 15, 0, ?MAX_SLOW_FACTOR}, {b, 35, 0, ?MAX_SLOW_FACTOR}],
    true = ?TM:init(Limits),
    All_Entries = ets:tab2list(?TM),
    4 = length(All_Entries),
    true = lists:member({a, 15, 0, 0, ?MAX_SLOW_FACTOR}, All_Entries),
    true = lists:member({b, 35, 0, 0, ?MAX_SLOW_FACTOR}, All_Entries),
    true = lists:member({{cma,a}, 0, 0, 0, ?MAX_SLOW_FACTOR}, All_Entries),
    true = lists:member({{cma,b}, 0, 0, 0, ?MAX_SLOW_FACTOR}, All_Entries),
    ok.

-spec check_with_timer_limits(config()) -> ok.
check_with_timer_limits(_Config) ->
    Limits = [{a, 15, 5, ?MAX_SLOW_FACTOR}, {b, 35, 0, ?MAX_SLOW_FACTOR}, {c, 17, 4, ?MAX_SLOW_FACTOR}],
    true = ?TM:init(Limits),
    All_Entries = ets:tab2list(?TM),
    6 = length(All_Entries),
    true = lists:member({a, 15, 0, 5, ?MAX_SLOW_FACTOR}, All_Entries),
    true = lists:member({b, 35, 0, 0, ?MAX_SLOW_FACTOR}, All_Entries),
    true = lists:member({c, 17, 0, 4, ?MAX_SLOW_FACTOR}, All_Entries),
    true = lists:member({{cma,a}, 0, 0, 5, ?MAX_SLOW_FACTOR}, All_Entries),
    true = lists:member({{cma,b}, 0, 0, 0, ?MAX_SLOW_FACTOR}, All_Entries),
    true = lists:member({{cma,c}, 0, 0, 4, ?MAX_SLOW_FACTOR}, All_Entries),
    ok.

-spec check_atom_limits(config()) -> ok.
check_atom_limits(_Config) ->
    Limits = [{a, unlimited,   0, ?MAX_SLOW_FACTOR}, {b, unlimited,   5, ?MAX_SLOW_FACTOR},
              {c, inline_only, 0, ?MAX_SLOW_FACTOR}, {d, inline_only, 7, ?MAX_SLOW_FACTOR}],
    true = ?TM:init(Limits),
    All_Entries = ets:tab2list(?TM),
    8 = length(All_Entries),
    true = lists:member({a, -1, 0, 0, ?MAX_SLOW_FACTOR}, All_Entries),
    true = lists:member({b, -1, 0, 5, ?MAX_SLOW_FACTOR}, All_Entries),
    true = lists:member({c,  0, 0, 0, ?MAX_SLOW_FACTOR}, All_Entries),
    true = lists:member({d,  0, 0, 7, ?MAX_SLOW_FACTOR}, All_Entries),
    true = lists:member({{cma,a},  0, 0, 0, ?MAX_SLOW_FACTOR}, All_Entries),
    true = lists:member({{cma,b},  0, 0, 5, ?MAX_SLOW_FACTOR}, All_Entries),
    true = lists:member({{cma,c},  0, 0, 0, ?MAX_SLOW_FACTOR}, All_Entries),
    true = lists:member({{cma,d},  0, 0, 7, ?MAX_SLOW_FACTOR}, All_Entries),
    ok.

-spec check_limit_errors(config()) -> ok.
check_limit_errors(_Config) ->
    Limits1 = [{a, unlimited, -1, ?MAX_SLOW_FACTOR},
               {b, 5, 0, ?MAX_SLOW_FACTOR},
               {c, unlimited, 0, ?MAX_SLOW_FACTOR}],
    {error, {invalid_init_args, [{a, unlimited, -1, ?MAX_SLOW_FACTOR}]}} = ?TM:init(Limits1),
    Limits2 = [{a, unlimited, -1, ?MAX_SLOW_FACTOR},
               {b, foo, 0, ?MAX_SLOW_FACTOR},
               {c, 0, bar, ?MAX_SLOW_FACTOR}],
    {error, {invalid_init_args, Limits2}} = ?TM:init(Limits2),

    %% Call init with good values to test add/remove/adjust...
    Limits = [{a, unlimited, 0, ?MAX_SLOW_FACTOR}, {b,          17, 5, ?MAX_SLOW_FACTOR},
              {c,         8, 0, ?MAX_SLOW_FACTOR}, {d, inline_only, 7, ?MAX_SLOW_FACTOR}],
    true = ?TM:init(Limits),

    {error, {add_duplicate_task_types, Limits}} = ?TM:add_task_types(Limits),
    Limits3 = [{g, foo, 1, ?MAX_SLOW_FACTOR}, {h, 17, -1, ?MAX_SLOW_FACTOR}],
    {error, {invalid_add_args, Limits3}} = ?TM:add_task_types(Limits3),

    Limits3a = [{TT, L} || {TT, L, _H, _Slow} <- Limits3],
    {error, {missing_task_types,  [{g, foo}, {h, 17}]}} = ?TM:adjust_task_limits(Limits3a),
    Limits4 = [{a, foo}, {b, -1}],
    {error, {invalid_task_limits, Limits4}} = ?TM:adjust_task_limits(Limits4),
    ok.

-spec check_concurrency_types(config()) -> ok.
check_concurrency_types(_Config) ->
    Limits = [{a, unlimited, 0, ?MAX_SLOW_FACTOR}, {b,          17, 5, ?MAX_SLOW_FACTOR},
              {c,         8, 0, ?MAX_SLOW_FACTOR}, {d, inline_only, 7, ?MAX_SLOW_FACTOR}],
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
    Limits = [{Inline_Type, inline_only, 2, ?MAX_SLOW_FACTOR}, {Spawn_Type, 3, 5, ?MAX_SLOW_FACTOR}],
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
    Limits = [{Overmax_Type, inline_only, 0, ?MAX_SLOW_FACTOR}, {Spawn_Type, 3, 5, ?MAX_SLOW_FACTOR}],
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

%% execute_pid_all_link_types runs a task with a return value of Pid or {inline, Result}.
-spec check_execute_pid_all_link_types(config()) -> ok.
check_execute_pid_all_link_types(_Config) ->
    {Inline_Type, Spawn_Type} = {pdict_inline, pdict_spawn},
    Limits = [{Inline_Type, inline_only, 2, ?MAX_SLOW_FACTOR}, {Spawn_Type, 3, 5, ?MAX_SLOW_FACTOR}],
    true = ?TM:init(Limits),
    
    %% When inline, update our process dictionary...
    Old_Joe = erase(joe),
    _ = try
            ok = check_inline_dict_type(Inline_Type, joe, 5, 7, pid),
            ok = check_inline_dict_type(Inline_Type, joe, 6, 8, link),
            ok = check_inline_dict_type(Inline_Type, joe, 7, 9, monitor)
    after put(joe, Old_Joe)
    end,

    %% When spawned, it affects a new process dictionary, not ours.
    Self = self(),
    Old_Joe = erase(joe),
    _ = try
            ok = check_pid_dict_type(Self, joe, 5, Spawn_Type, pid,     false),
            ok = check_pid_dict_type(Self, joe, 6, Spawn_Type, link,    false),
            ok = check_pid_dict_type(Self, joe, 7, Spawn_Type, monitor, false)
    after put(joe, Old_Joe)
    end,

    ok.

check_inline_dict_type(Inline_Type, Key, Value1, Value2, Link_Type) ->
    Exec_Fun = case Link_Type of
                   pid     -> execute_pid;
                   link    -> execute_pid_link;
                   monitor -> execute_pid_monitor
               end,
    {inline, undefined} = ?TM:Exec_Fun(Inline_Type, erlang, put, [Key, Value1]),
    Value1 = get(Key),
    {inline,    Value1} = ?TM:Exec_Fun(Inline_Type, erlang, put, [Key, Value2]),
    Value2 = get(Key),
    erase(Key),
    ok.
    
check_pid_dict_type(Self, Key, Value, Spawn_Type, Link_Type, Maybe) ->
    undefined = get(Key),   % These executions only affect pdict of newly spawned pids.
    Exec_Fun = case {Link_Type, Maybe} of
                   {pid,     false} -> execute_pid;
                   {link,    false} -> execute_pid_link;
                   {monitor, false} -> execute_pid_monitor;

                   {pid,      true} -> maybe_execute_pid;
                   {link,     true} -> maybe_execute_pid_link;
                   {monitor,  true} -> maybe_execute_pid_monitor
               end,
    New_Pid = case Link_Type =:= monitor of
                  true  -> {Pid, _Ref} = ?TM:Exec_Fun(Spawn_Type, ?MODULE, put_pdict, [Key, Value]), Pid;
                  false -> ?TM:Exec_Fun(Spawn_Type, ?MODULE, put_pdict, [Key, Value])
              end,
    ok = check_pid_dict       (New_Pid, Self, Link_Type),
    ok = check_pid_dict_value (New_Pid, Self, Key, Value).

check_pid_dict(New_Pid, Self, pid) ->
    false = (New_Pid =:= Self),
    {links,        []} = process_info(New_Pid, links),
    {monitored_by, []} = process_info(New_Pid, monitored_by),
    ok;
check_pid_dict(New_Pid, Self, link) ->
    false = (New_Pid =:= Self),
    {links,    [Self]} = process_info(New_Pid, links),
    {monitored_by, []} = process_info(New_Pid, monitored_by),
    ok;
check_pid_dict(New_Pid, Self, monitor) ->
    false = (New_Pid =:= Self),
    {links,            []} = process_info(New_Pid, links),
    {monitored_by, [Self]} = process_info(New_Pid, monitored_by),
    ok.

check_pid_dict_value(New_Pid, Self, Key, Value) ->
    New_Pid ! {Self, get_pdict, Key},
    undefined = get(Key),
    ok = receive {get_pdict, New_Pid, Value} -> ok
         after 100 -> timeout
         end.

check_overmax_no_exec(Overmax_Type, Key, Value, Link_Type) ->
    Exec_Fun = case Link_Type of
                   pid     -> maybe_execute_pid;
                   link    -> maybe_execute_pid_link;
                   monitor -> maybe_execute_pid_monitor
               end,
    {max_pids, 0} = ?TM:Exec_Fun(Overmax_Type, erlang, put, [Key, Value]),
    erlang:yield(),
    undefined = get(joe),
    ok.

check_overmax(Overmax_Type) ->
    [0] = [proplists:get_value(active_procs, Props)
           || [{task_type, Type} | _] = Props <- cxy_ctl:concurrency_types(),
              Type =:= Overmax_Type],
    ok.


%% maybe_execute_pid_all_link_types runs a task with a return value of Pid or {max_pids, Max}.
-spec check_maybe_execute_pid_all_link_types(config()) -> ok.
check_maybe_execute_pid_all_link_types(_Config) ->
    {Overmax_Type, Spawn_Type} = {pdict_overmax, pdict_spawn},
    Limits = [{Overmax_Type, inline_only, 0, ?MAX_SLOW_FACTOR}, {Spawn_Type, 3, 5, ?MAX_SLOW_FACTOR}],
    true = ?TM:init(Limits),
    
    %% When inline, update our process dictionary...
    Old_Joe = erase(joe),
    _ = try

            ok = check_overmax_no_exec(Overmax_Type, joe, 5, pid),
            ok = check_overmax_no_exec(Overmax_Type, joe, 7, pid),
            ok = check_overmax(Overmax_Type),

            ok = check_overmax_no_exec(Overmax_Type, joe, 5, link),
            ok = check_overmax_no_exec(Overmax_Type, joe, 7, link),
            ok = check_overmax(Overmax_Type),

            ok = check_overmax_no_exec(Overmax_Type, joe, 5, monitor),
            ok = check_overmax_no_exec(Overmax_Type, joe, 7, monitor),
            ok = check_overmax(Overmax_Type)

    after put(joe, Old_Joe)
    end,

    %% When spawned, it affects a new process dictionary, not ours.
    Self = self(),
    Old_Joe = erase(joe),
    _ = try

        ok = check_pid_dict_type(Self, joe, 5, Spawn_Type, pid,     true),
        ok = check_overmax(Spawn_Type),

        ok = check_pid_dict_type(Self, joe, 6, Spawn_Type, link,    true),
        ok = check_overmax(Spawn_Type),

        ok = check_pid_dict_type(Self, joe, 7, Spawn_Type, monitor, true),
        ok = check_overmax(Spawn_Type)

    after put(joe, Old_Joe)
    end,

    ok.

-spec check_multiple_init_calls(config()) -> ok.
check_multiple_init_calls(_Config) ->
    Limits1 = [{a, unlimited, 0, ?MAX_SLOW_FACTOR}, {b,          17, 5, ?MAX_SLOW_FACTOR},
               {c,         8, 0, ?MAX_SLOW_FACTOR}, {d, inline_only, 7, ?MAX_SLOW_FACTOR}],
    true = ?TM:init(Limits1),
    {error, init_already_executed} = ?TM:init(Limits1),
    {error, init_already_executed} = ?TM:init([]),

    Cxy_Limits = [L || {_, L, _, _} <- Limits1],
    Cxy_Limits = [proplists:get_value(max_procs, P) || P <- ?TM:concurrency_types()],

    Dup1 = {b, 217, 15, ?MAX_SLOW_FACTOR},
    Dup2 = {d, inline_only, 17, ?MAX_SLOW_FACTOR},
    Limits2 = [{f, unlimited, 0, ?MAX_SLOW_FACTOR}, Dup1, {e, 18, 10, ?MAX_SLOW_FACTOR}, Dup2],
    {error, {add_duplicate_task_types, [Dup1, Dup2]}} = ?TM:add_task_types(Limits2),

    Error_Dups = [T || {T, _, _, _} <- Limits2 -- [Dup1, Dup2]],
    {error, {missing_task_types, Error_Dups}} = ?TM:remove_task_types([T || {T, _, _, _} <- Limits2]),
    2 = ?TM:remove_task_types([element(1,Dup1), element(1,Dup2)]),
    Missing_Task_Types = [T || {T, _, _, _} <- Limits2],
    {error, {missing_task_types, Missing_Task_Types}} = ?TM:remove_task_types([T || {T, _, _, _} <- Limits2]),
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
    Limits = [{Inline_Type, inline_only, 2, ?MAX_SLOW_FACTOR}, {Spawn_Type, 3, 5, ?MAX_SLOW_FACTOR}],
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

