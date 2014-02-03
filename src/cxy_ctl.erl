%%%------------------------------------------------------------------------------
%%% @copyright (c) 2013, DuoMark International, Inc.
%%% @author Jay Nelson <jay@duomark.com> [http://duomark.com/]
%%% @reference 2013 Development sponsored by TigerText, Inc. [http://tigertext.com/]
%%% @reference The license is based on the template for Modified BSD from
%%%   <a href="http://opensource.org/licenses/BSD-3-Clause">OSI</a>
%%% @doc
%%%   Concurrency limiter, caps number of processes based on user config,
%%%   reverts to inline execution or refuses to execute when demand is too high.
%%% @since 0.9.0
%%% @end
%%%------------------------------------------------------------------------------
-module(cxy_ctl).
-author('Jay Nelson <jay@duomark.com>').

%% External interface
-export([
         init/1,
         execute_task/4, maybe_execute_task/4,
         execute_task/5, maybe_execute_task/5,
         execute_pid_link/4, execute_pid_monitor/4, 
         execute_pid_link/5, execute_pid_monitor/5, 
         maybe_execute_pid_link/4, maybe_execute_pid_monitor/4, 
         maybe_execute_pid_link/5, maybe_execute_pid_monitor/5,
         concurrency_types/0, history/1, history/3
        ]).

-export([make_process_dictionary_default_value/2]).

%% Spawn interface
-export([execute_wrapper/8]).

%% Internal functions to test
-export([update_inline_times/5, update_spawn_times/5]).

-define(VALID_DICT_VALUE_MARKER, '$$dict_prop').

-type dict_key()       :: any().
-type dict_value()     :: any().
-type dict_entry()     :: {dict_key(), dict_value()}.
-type dict_prop()      :: dict_key() | dict_entry().
-type dict_props()     :: [dict_prop()].
-type dict_prop_vals() :: [{?VALID_DICT_VALUE_MARKER, dict_entry()}].

-spec make_process_dictionary_default_value(Key, Value)
             -> {?VALID_DICT_VALUE_MARKER, {Key, Value}}
                    when Key :: dict_key(),
                         Value :: dict_value().

make_process_dictionary_default_value(Key, Value) ->
    {?VALID_DICT_VALUE_MARKER, {Key, Value}}.


%%%------------------------------------------------------------------------------
%%% Internal interface for maintaining counters and timers in ets table
%%%------------------------------------------------------------------------------

%% Use a raw tuple because two record structs with the same key would
%% be needed, or redundant record name in key position.
-define(MAX_PROCS_POS,    2).
-define(ACTIVE_PROCS_POS, 3).
-define(MAX_HISTORY_POS,  4).

make_proc_values(Task_Type, Max_Procs_Allowed, Max_History) ->
    Active_Procs = 0,
    Stored_Max_Procs_Value = case Max_Procs_Allowed of
                                 unlimited   -> -1;
                                 inline_only ->  0;
                                 _ -> Max_Procs_Allowed
                             end,
    {Task_Type, Stored_Max_Procs_Value, Active_Procs, Max_History}.

incr_active_procs(Task_Type) ->
    ets:update_counter(?MODULE, Task_Type, {?ACTIVE_PROCS_POS, 1}).

decr_active_procs(Task_Type) ->
    ets:update_counter(?MODULE, Task_Type, {?ACTIVE_PROCS_POS, -1}).

update_times(Task_Type_Modified, Task_Fun, Start, Spawn, Done) ->
    Exec_Elapsed  = timer:now_diff(Done, Spawn),
    Spawn_Elapsed = timer:now_diff(Spawn, Start),
    Elapsed = {Task_Fun, Start, Spawn_Elapsed, Exec_Elapsed},
    ets_buffer:write(Task_Type_Modified, Elapsed).

-spec update_spawn_times(atom(), {module(), atom(), list()}, erlang:timestamp(),
                         erlang:timestamp(), erlang:timestamp()) -> true.
update_spawn_times(Task_Type, Task_Fun, Start, Spawn, Done) ->
    update_times(make_buffer_spawn(Task_Type), Task_Fun, Start, Spawn, Done).

-spec update_inline_times(atom(), {module(), atom(), list()}, erlang:timestamp(),
                          erlang:timestamp(), erlang:timestamp()) -> true.
update_inline_times(Task_Type, Task_Fun, Start, Spawn, Done) ->
    update_times(make_buffer_inline(Task_Type), Task_Fun, Start, Spawn, Done).


%%%------------------------------------------------------------------------------
%%% Mechanism for creating ets table, and executing tasks
%%%------------------------------------------------------------------------------

%% @doc
%%   Initialize a named ETS table to hold concurrency limits which is checked
%%   before spawning new processes to ensure limits are not exceeded. The
%%   Limits argument is a list of task types, the corresponding maximum
%%   number of simultaneous processes to allow, and a maximum number of
%%   timestamps to record in a circular buffer for later analysis.
%% @end

-spec init([{Task_Type, Type_Max, Timer_History_Count}]) -> ok when
      Task_Type :: atom(),
      Type_Max  :: pos_integer(),
      Timer_History_Count :: non_neg_integer().

init(Limits) ->
    %% Validate Limits and construct ring buffer params for each concurrency type...
    case lists:foldl(fun(Args, Acc) -> valid_limits(Args, Acc) end, {[], [], []}, Limits) of
        { Buffer_Params,  Cxy_Params,     []} -> do_init(Buffer_Params, Cxy_Params);
        {_Buffer_Params, _Cxy_Params, Errors} -> {error, {invalid_init_args, lists:reverse(Errors)}}
    end.

valid_limits({Type, Max_Procs, History_Count}, {Buffer_Params, Cxy_Params, Errors})
  when is_atom(Type), is_integer(History_Count), History_Count >= 0,
       (Max_Procs =:= unlimited orelse Max_Procs =:= inline_only) ->
    make_limits({Type, Max_Procs, History_Count}, {Buffer_Params, Cxy_Params, Errors});
valid_limits({Type, Max_Procs, History_Count}, {Buffer_Params, Cxy_Params, Errors})
  when is_atom(Type), is_integer(Max_Procs), is_integer(History_Count),
       Max_Procs >= 0, History_Count >= 0 ->
    make_limits({Type, Max_Procs, History_Count}, {Buffer_Params, Cxy_Params, Errors});
valid_limits(Invalid, {Buffer_Params, Cxy_Params, Errors}) ->
    {Buffer_Params, Cxy_Params, [Invalid | Errors]}.

make_limits({Type, Max_Procs, History_Count}, {Buffer_Params, Cxy_Params, Errors}) ->
    {make_buffer_params(Buffer_Params, Type, History_Count),
     make_proc_params(Cxy_Params, Type, Max_Procs, History_Count), Errors}.
    

make_buffer_spawn(Type)  -> list_to_atom("spawn_"  ++ atom_to_list(Type)).
make_buffer_inline(Type) -> list_to_atom("inline_" ++ atom_to_list(Type)).
make_buffer_names(Type) -> {make_buffer_spawn(Type), make_buffer_inline(Type)}.

make_buffer_params(Acc, _Type, 0) -> Acc;
make_buffer_params(Acc,  Type, Max_History) ->
    {Spawn_Type, Inline_Type} = make_buffer_names(Type),
    [{Spawn_Type, ring, Max_History}, {Inline_Type, ring, Max_History} | Acc].

make_proc_params(Acc, Type, Max_Procs, Max_History) ->
    [make_proc_values(Type, Max_Procs, Max_History) | Acc].
    
    
do_init(Buffer_Params, Cxy_Params) ->
    ets_buffer:create(Buffer_Params),
    _ = ets:new(?MODULE, [named_table, ordered_set, public, {write_concurrency, true}]),
    _ = [ets:insert_new(?MODULE, Proc_Values) || Proc_Values <- Cxy_Params],
    ok.


%% @doc
%%   Execute a task by spawning a function to run it, only if the task type
%%   does not have too many currently executing processes. If there are too
%%   many, execute the task inline. Returns neither a pid nor a result.
%% @end

-spec execute_task(atom(), atom(), atom(), list())               -> ok.
-spec execute_task(atom(), atom(), atom(), list(), dict_props()) -> ok.

execute_task(Task_Type, Mod, Fun, Args) ->
    internal_execute_task(Task_Type, Mod, Fun, Args, inline, none).

execute_task(Task_Type, Mod, Fun, Args, Dict_Props) ->
    internal_execute_task(Task_Type, Mod, Fun, Args, inline, Dict_Props).


%% @doc
%%   Execute a task by spawning a function to run it, only if the task type
%%   does not have too many currently executing processes. If there are too
%%   many, return {max_pids, Max} without executing, rather than ok.
%% @end

-spec maybe_execute_task(atom(), atom(), atom(), list())             -> ok | {max_pids, non_neg_integer()}.
-spec maybe_execute_task(atom(), atom(), atom(), list(), dict_props()) -> ok | {max_pids, non_neg_integer()}.

maybe_execute_task(Task_Type, Mod, Fun, Args) ->
    internal_execute_task(Task_Type, Mod, Fun, Args, refuse, none).

maybe_execute_task(Task_Type, Mod, Fun, Args, Dict_Props) ->
    internal_execute_task(Task_Type, Mod, Fun, Args, refuse, Dict_Props).


internal_execute_task(Task_Type, Mod, Fun, Args, Over_Limit_Action, Dict_Props) ->
    [Max, Max_History] = ets:update_counter(?MODULE, Task_Type, [{?MAX_PROCS_POS, 0}, {?MAX_HISTORY_POS, 0}]),
    Start = Max_History > 0 andalso os:timestamp(),
    case {Max, incr_active_procs(Task_Type)} of

        %% Spawn a new process...
        {Unlimited, Below_Max} when Unlimited =:= -1; Below_Max =< Max ->
            Dict_Prop_Vals = get_calling_dictionary_values(Dict_Props),
            Wrapper_Args = [Mod, Fun, Args, Task_Type, Max_History, Start, spawn, Dict_Prop_Vals],
            _ = proc_lib:spawn(?MODULE, execute_wrapper, Wrapper_Args),
            ok;

        %% Execute inline.
        _Over_Max ->
            case Over_Limit_Action of
                refuse -> decr_active_procs(Task_Type),
                          {max_pids, Max};
                inline -> _ = execute_wrapper(Mod, Fun, Args, Task_Type, Max_History, Start, inline, []),
                          ok
            end
    end.

%% @doc
%%   Execute a task by spawning a function to run it, only if the task type
%%   does not have too many currently executing processes. If there are too
%%   many, execute the task inline. Returns a linked pid if spawned, or results
%%   if inlined.
%% @end

-spec execute_pid_link(atom(), atom(), atom(), list())               -> pid() | {inline, any()}.
-spec execute_pid_link(atom(), atom(), atom(), list(), dict_props()) -> pid() | {inline, any()}.

execute_pid_link(Task_Type, Mod, Fun, Args) ->
    internal_execute_pid(Task_Type, Mod, Fun, Args, link, inline, none).

execute_pid_link(Task_Type, Mod, Fun, Args, Dict_Props) ->
    internal_execute_pid(Task_Type, Mod, Fun, Args, link, inline, Dict_Props).

%% @doc
%%   Execute a task by spawning a function to run it, only if the task type
%%   does not have too many currently executing processes. If there are too
%%   many, return {max_pids, Max_Count} instead of linked pid.
%% @end

-spec maybe_execute_pid_link(atom(), atom(), atom(), list())               -> pid() | {max_pids, non_neg_integer()}.
-spec maybe_execute_pid_link(atom(), atom(), atom(), list(), dict_props()) -> pid() | {max_pids, non_neg_integer()}.

maybe_execute_pid_link(Task_Type, Mod, Fun, Args) ->
    internal_execute_pid(Task_Type, Mod, Fun, Args, link, refuse, none).

maybe_execute_pid_link(Task_Type, Mod, Fun, Args, Dict_Props) ->
    internal_execute_pid(Task_Type, Mod, Fun, Args, link, refuse, Dict_Props).

%% @doc
%%   Execute a task by spawning a function to run it, only if the task type
%%   does not have too many currently executing processes. If there are too
%%   many, execute the task inline. Returns a {pid(), reference()} if spawned
%%   so the process can be monitored, or results if inlined.
%% @end

-spec execute_pid_monitor(atom(), atom(), atom(), list())               -> {pid(), reference()} | {inline, any()}.
-spec execute_pid_monitor(atom(), atom(), atom(), list(), dict_props()) -> {pid(), reference()} | {inline, any()}.

execute_pid_monitor(Task_Type, Mod, Fun, Args) ->
    internal_execute_pid(Task_Type, Mod, Fun, Args, monitor, inline, none).

execute_pid_monitor(Task_Type, Mod, Fun, Args, Dict_Props) ->
    internal_execute_pid(Task_Type, Mod, Fun, Args, monitor, inline, Dict_Props).

%% @doc
%%   Execute a task by spawning a function to run it, only if the task type
%%   does not have too many currently executing processes. If there are too
%%   many, return {max_pids, Max_Count} instead of {pid(), reference()}.
%% @end

-spec maybe_execute_pid_monitor(atom(), atom(), atom(), list())               -> {pid(), reference()} | {max_pids, non_neg_integer()}.
-spec maybe_execute_pid_monitor(atom(), atom(), atom(), list(), dict_props()) -> {pid(), reference()} | {max_pids, non_neg_integer()}.

maybe_execute_pid_monitor(Task_Type, Mod, Fun, Args) ->
    internal_execute_pid(Task_Type, Mod, Fun, Args, monitor, refuse, none).

maybe_execute_pid_monitor(Task_Type, Mod, Fun, Args, Dict_Props) ->
    internal_execute_pid(Task_Type, Mod, Fun, Args, monitor, refuse, Dict_Props).


internal_execute_pid(Task_Type, Mod, Fun, Args, Spawn_Type, Over_Limit_Action, Dict_Props) ->
    [Max, Max_History] = ets:update_counter(?MODULE, Task_Type, [{?MAX_PROCS_POS, 0}, {?MAX_HISTORY_POS, 0}]),
    Start = Max_History > 0 andalso os:timestamp(),
    case {Max, incr_active_procs(Task_Type)} of

        %% Spawn a new process...
        {Unlimited, Below_Max} when Unlimited =:= -1; Below_Max =< Max ->
            Dict_Prop_Vals = get_calling_dictionary_values(Dict_Props),
            Wrapper_Args = [Mod, Fun, Args, Task_Type, Max_History, Start, spawn, Dict_Prop_Vals],
            case Spawn_Type of
                link    -> spawn_link   (?MODULE, execute_wrapper, Wrapper_Args);
                monitor -> spawn_monitor(?MODULE, execute_wrapper, Wrapper_Args)
            end;

        %% Too many processes already running...
        _Over_Max ->
            case Over_Limit_Action of
                refuse -> decr_active_procs(Task_Type),
                          {max_pids, Max};
                inline -> {inline, execute_wrapper(Mod, Fun, Args, Task_Type, Max_History, Start, inline, [])}
            end
    end.


-spec get_calling_dictionary_values(none | all_keys | dict_props()) -> dict_prop_vals().

get_calling_dictionary_values(none)     -> [];
get_calling_dictionary_values(all_keys) -> get();
get_calling_dictionary_values(List) when is_list(List) ->
    get_calling_dictionary_values(List, []).

get_calling_dictionary_values([{?VALID_DICT_VALUE_MARKER, {Key, Default}} | More], Props) -> 
    case get(Key) of
        undefined -> get_calling_dictionary_values(More, [{Key, Default} | Props]);
        Value     -> get_calling_dictionary_values(More, [{Key, Value  } | Props])
    end;
get_calling_dictionary_values([Key | More], Props) -> 
    case get(Key) of
        undefined -> get_calling_dictionary_values(More, Props);
        Value     -> get_calling_dictionary_values(More, [{Key, Value} | Props])
    end;
get_calling_dictionary_values([], Props) -> Props.
            

-spec execute_wrapper(atom(), atom(), list(), atom(), integer(), false | erlang:timestamp(), spawn | inline, dict_prop_vals())
                     -> true | no_return().

%% If Start is 'false', we don't want to record elapsed time history...
execute_wrapper(Mod, Fun, Args, Task_Type, _Max_History, false, Spawn_Or_Inline, Dict_Prop_Pairs) ->
    Result = try
                 _ = [put(Key, Val) || {Key, Val} <- Dict_Prop_Pairs],
                 apply(Mod, Fun, Args)
             catch Error:Type -> {error, {mfa_failure, {{Error, Type}, {Mod, Fun, Args}, Task_Type, Spawn_Or_Inline}}}
             after decr_active_procs(Task_Type)
             end,
    case Result of
        {error, Call_Data} -> fail_wrapper(Spawn_Or_Inline, Call_Data, erlang:get_stacktrace());
        Result             -> Result
    end;

%% Otherwise, we incur the overhead cost of recording elapsed time history.
execute_wrapper(Mod, Fun, Args, Task_Type, Max_History, Start, Spawn_Or_Inline, Dict_Prop_Pairs) ->
    MFA = {Mod, Fun, Args},
    Spawn = os:timestamp(),
    Result = try
                 _ = [put(Key, Val) || {Key, Val} <- Dict_Prop_Pairs],
                 apply(Mod, Fun, Args)
             catch Error:Type -> {error, {mfa_failure, {{Error, Type}, MFA, Task_Type, Max_History, Start, Spawn_Or_Inline}}}
             after
                 decr_active_procs(Task_Type),
                 case Spawn_Or_Inline of
                     spawn  -> update_spawn_times (Task_Type, MFA, Start, Spawn, os:timestamp());
                     inline -> update_inline_times(Task_Type, MFA, Start, Spawn, os:timestamp())
                 end
             end,
    case Result of
        {error, Call_Data} -> fail_wrapper(Spawn_Or_Inline, Call_Data, erlang:get_stacktrace());
        Result             -> Result
    end.

-spec fail_wrapper(spawn | inline, any(), any()) -> no_return().
fail_wrapper(spawn,  Call_Data, Stacktrace) -> erlang:error(spawn_failure,  [Call_Data, Stacktrace]);
fail_wrapper(inline, Call_Data, Stacktrace) -> exit       ({inline_failure, [Call_Data, Stacktrace]}).


%% @doc
%%    Provide a list of the registered concurrency limit types and their corresponding limit
%%    values for max_procs, active_procs and max_history size.
%% @end

-spec concurrency_types() -> [proplists:proplist()].

concurrency_types() ->
    [[{task_type, Task_Type}, {max_procs, Max_Procs_Allowed}, {active_procs, Active_Procs}, {max_history, Max_History}]
     || {Task_Type, Max_Procs_Allowed, Active_Procs, Max_History} <- ets:tab2list(?MODULE)].


%% @doc
%%    Provide the entire performance history for a given task_type as a tuple of two elements:
%%    the performance for spawn execution and for inline execution. Each entry includes the
%%    start time for the request, the number of microseconds to spawn the task, and the number
%%    of microseconds to execute the request.
%% @end

-type spawn_history_result()  :: {spawn_execs, [proplists:proplist()]}.
-type inline_history_result() :: {inline_execs, [proplists:proplist()]}.
-type history_result() :: {spawn_history_result(), inline_history_result()}.

-spec history(atom()) -> history_result() | ets_buffer:buffer_error().
-spec history(atom(), inline | spawn, pos_integer())
             -> inline_history_result() | spawn_history_result() | ets_buffer:buffer_error().

%% @doc Provide all the performance history for a given task_type.
history(Task_Type) ->
    {Spawn_Type, Inline_Type} = make_buffer_names(Task_Type),
    case get_buffer_times(Spawn_Type) of
        Spawn_Times_List when is_list(Spawn_Times_List) ->
            {{spawn_execs,  Spawn_Times_List},
             {inline_execs, get_buffer_times(Inline_Type)}};
        Error -> Error
    end.

%% @doc Provide the most recent performance history for a given task_type.
history(Task_Type, inline, Num_Items) ->
    Inline_Type = make_buffer_inline(Task_Type),
    case get_buffer_times(Inline_Type, Num_Items) of
        Inline_Times_List when is_list(Inline_Times_List) -> {inline_execs, Inline_Times_List};
        Error -> Error
    end;
history(Task_Type, spawn, Num_Items) ->
    Spawn_Type = make_buffer_spawn(Task_Type),
    case get_buffer_times(Spawn_Type, Num_Items) of
        Spawn_Times_List when is_list(Spawn_Times_List) -> {spawn_execs, Spawn_Times_List};
        Error -> Error
    end.

get_buffer_times(Buffer_Name) ->
    case ets_buffer:history(Buffer_Name) of
        Times_List when is_list(Times_List) -> [format_buffer_times(Times) || Times <- Times_List];
        Error -> Error
    end.

get_buffer_times(Buffer_Name, Num_Items) ->
    case ets_buffer:history(Buffer_Name, Num_Items) of
        Times_List when is_list(Times_List) -> [format_buffer_times(Times) || Times <- Times_List];
        Error -> Error
    end.

format_buffer_times({Task_Fun, {_,_,Micro} = Start, Spawn_Elapsed, Exec_Elapsed}) ->
    [{task_fun, Task_Fun}, {start, calendar:now_to_universal_time(Start), Micro},
     {spawn_time_micros, Spawn_Elapsed}, {exec_time_micros, Exec_Elapsed}].
