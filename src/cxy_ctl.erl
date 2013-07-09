%%%------------------------------------------------------------------------------
%%% @copyright (c) 2013, DuoMark International, Inc.
%%% @author Jay Nelson <jay@duomark.com> [http://duomark.com/]
%%% @reference 2013 Development sponsored by TigerText, Inc. [http://tigertext.com/]
%%% @reference The license is based on the template for Modified BSD from
%%%   <a href="http://opensource.org/licenses/BSD-3-Clause">OSI</a>
%%% @doc
%%%   Concurrency limiter, caps number of processes based on user config,
%%%   reverts to inline execution when demand is too high.
%%% @since 0.9.0
%%% @end
%%%------------------------------------------------------------------------------
-module(cxy_ctl).
-author('Jay Nelson <jay@duomark.com>').

%% External interface
-export([
         init/1,
         execute_task/4, execute_pid/4,
         concurrency_types/0, history/1, history/3
        ]).

%% Spawn interface
-export([execute_wrapper/7]).

%% Internal functions to test
-export([update_inline_times/5, update_spawn_times/5]).


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
    case lists:foldl(fun({Type, Max_Procs = unlimited, History_Count}, {Buffer_Params, Cxy_Params, Errors})
                           when is_atom(Type), is_integer(History_Count), History_Count >= 0 ->
                             {make_buffer_params(Buffer_Params, Type, History_Count),
                              make_proc_params(Cxy_Params, Type, Max_Procs, History_Count), Errors};

                        ({Type, Max_Procs = inline_only, History_Count}, {Buffer_Params, Cxy_Params, Errors})
                           when is_atom(Type), is_integer(History_Count), History_Count >= 0 ->
                             {make_buffer_params(Buffer_Params, Type, History_Count),
                              make_proc_params(Cxy_Params, Type, Max_Procs, History_Count), Errors};

                        ({Type, Max_Procs, History_Count}, {Buffer_Params, Cxy_Params, Errors})
                           when is_atom(Type), is_integer(Max_Procs), is_integer(History_Count),
                                Max_Procs >= 0, History_Count >= 0 ->
                             {make_buffer_params(Buffer_Params, Type, History_Count),
                              make_proc_params(Cxy_Params, Type, Max_Procs, History_Count), Errors};

                        (Invalid, {Buffer_Params, Cxy_Params, Errors}) ->
                             {Buffer_Params, Cxy_Params, [Invalid | Errors]}

                     end, {[], [], []}, Limits) of

        { Buffer_Params,  Cxy_Params,     []} -> do_init(Buffer_Params, Cxy_Params);
        {_Buffer_Params, _Cxy_Params, Errors} -> {error, {invalid_init_args, lists:reverse(Errors)}}
    end.

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
%%   many, execute the task inline. Does not return a pid nor results.
%% @end

-spec execute_task(atom(), atom(), atom(), list()) -> ok.

execute_task(Task_Type, Mod, Fun, Args) ->
    [Max, Max_History] = ets:update_counter(?MODULE, Task_Type, [{?MAX_PROCS_POS, 0}, {?MAX_HISTORY_POS, 0}]),
    Start = Max_History > 0 andalso os:timestamp(),
    case {Max, incr_active_procs(Task_Type)} of

        %% Spawn a new process...
        {Unlimited, Below_Max} when Unlimited =:= -1; Below_Max =< Max ->
            Wrapper_Args = [Mod, Fun, Args, Task_Type, Max_History, Start, spawn],
            _ = proc_lib:spawn(?MODULE, execute_wrapper, Wrapper_Args),
            ok;

        %% Execute inline.
        _Over_Max ->
            _ = execute_wrapper(Mod, Fun, Args, Task_Type, Max_History, Start, inline),
            ok
    end.

%% @doc
%%   Execute a task by spawning a function to run it, only if the task type
%%   does not have too many currently executing processes. If there are too
%%   many, execute the task inline. Returns a pid or results if inlined.
%% @end

-spec execute_pid(atom(), atom(), atom(), list()) -> pid() | {inline, any()}.

execute_pid(Task_Type, Mod, Fun, Args) ->
    [Max, Max_History] = ets:update_counter(?MODULE, Task_Type, [{?MAX_PROCS_POS, 0}, {?MAX_HISTORY_POS, 0}]),
    Start = Max_History > 0 andalso os:timestamp(),
    case {Max, incr_active_procs(Task_Type)} of

        %% Spawn a new process...
        {Unlimited, Below_Max} when Unlimited =:= -1; Below_Max =< Max ->
            Wrapper_Args = [Mod, Fun, Args, Task_Type, Max_History, Start, spawn],
            proc_lib:spawn(?MODULE, execute_wrapper, Wrapper_Args);

        %% Execute inline.
        _Over_Max ->
            Result = execute_wrapper(Mod, Fun, Args, Task_Type, Max_History, Start, inline),
            {inline, Result}
    end.


-spec execute_wrapper(atom(), atom(), list(), atom(), integer(), false | erlang:timestamp(), spawn | inline)
                     -> true | {error, {ets_table_failure, tuple()} | {mfa_failure, tuple()}}.

%% If Start is 'false', we don't want to record elapsed time history...
execute_wrapper(Mod, Fun, Args, Task_Type, _Max_History, false, _Spawn_Or_Inline) ->
    try apply(Mod, Fun, Args)
    catch Error:Type -> {error, {mfa_failure, {{Error, Type}, {Mod, Fun, Args}, Task_Type, _Spawn_Or_Inline}}}
    after decr_active_procs(Task_Type)
    end;
%% Otherwise, we incur the overhead cost of recording elapsed time history.
execute_wrapper(Mod, Fun, Args, Task_Type, Max_History, Start, Spawn_Or_Inline) ->
    Spawn = os:timestamp(),
    try apply(Mod, Fun, Args)
    catch error:badarg -> {error, {ets_table_failure, {{Mod, Fun, Args}, Task_Type, Max_History, Start, Spawn_Or_Inline}}};
          Error:Type   -> {error, {mfa_failure, {{Error, Type}, {Mod, Fun, Args}, Max_History, Task_Type, Spawn_Or_Inline}}}
    after
        decr_active_procs(Task_Type),
        case Spawn_Or_Inline of
            spawn  -> update_spawn_times (Task_Type, {Mod, Fun, Args}, Start, Spawn, os:timestamp());
            inline -> update_inline_times(Task_Type, {Mod, Fun, Args}, Start, Spawn, os:timestamp())
        end
    end.


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

-spec history(atom()) -> history_result() | not_implemented_yet | {missing_ets_buffer, atom()}.
-spec history(atom(), inline | spawn, pos_integer())
             -> inline_history_result() | spawn_history_result() | not_implemented_yet | {missing_ets_buffer, atom()}.

%% @doc Provide all the performance history for a given task_type.
history(Task_Type) ->
    {Spawn_Type, Inline_Type} = make_buffer_names(Task_Type),
    case get_buffer_times(Spawn_Type) of
        Spawn_Times_List when is_list(Spawn_Times_List) ->
            {{spawn_execs, Spawn_Times_List },
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
