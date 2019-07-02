%%%------------------------------------------------------------------------------
%%% @copyright (c) 2013-2015, DuoMark International, Inc.
%%% @author Jay Nelson <jay@duomark.com> [http://duomark.com/]
%%% @reference 2013-2015 Development sponsored by TigerText, Inc. [http://tigertext.com/]
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
         add_task_types/1, remove_task_types/1, adjust_task_limits/1,
         execute_task/4, maybe_execute_task/4,
         execute_task/5, maybe_execute_task/5,
         execute_pid_link/4, execute_pid_monitor/4, 
         execute_pid_link/5, execute_pid_monitor/5, 
         maybe_execute_pid_link/4, maybe_execute_pid_monitor/4, 
         maybe_execute_pid_link/5, maybe_execute_pid_monitor/5,
         concurrency_types/0, history/1, history/3,
         slow_calls/1, slow_calls/2,
         high_water/1, high_water/2
        ]).

-export([make_process_dictionary_default_value/2]).

%% Spawn interface
-export([execute_wrapper/8]).

%% Internal functions to test
-export([update_inline_times/5, update_spawn_times/5]).

-define(VALID_DICT_VALUE_MARKER, '$$dict_prop').

-type task_type() :: atom().
-type cxy_limit() :: pos_integer() | unlimited | inline_only.
-type cxy_clear() :: clear | no_clear.

-type dict_key()       :: any().
-type dict_value()     :: any().
-type dict_entry()     :: {dict_key(), dict_value()}.
-type dict_prop()      :: dict_key() | dict_entry().
-type dict_props()     :: [dict_prop()] | none | all_keys.
-type dict_prop_vals() :: [{?VALID_DICT_VALUE_MARKER, dict_entry()}].

-spec make_process_dictionary_default_value(Key, Value)
             -> {?VALID_DICT_VALUE_MARKER, {Key, Value}}
                    when Key :: dict_key(),
                         Value :: dict_value().

make_process_dictionary_default_value(Key, Value) ->
    {?VALID_DICT_VALUE_MARKER, {Key, Value}}.


%%%------------------------------------------------------------------------------
%%% Internal interface for maintaining process limits / history counters
%%%------------------------------------------------------------------------------

%%% Used raw tuples for ets counters because two record structs with the
%%% same key would be needed, or redundant record name in key position.
%%% These tuples are also exposed when the history mechanism is used
%%% and are made readable by not including redundant record names.

%%% Cxy process values and cumulative moving avgs are kept in the cxy_ctl ets table as a tuple.
make_proc_values(Task_Type, Max_Procs_Allowed, Max_History, Slow_Factor_As_Percentage) ->
    Active_Procs = 0,
    Stored_Max_Procs_Value = max_procs_to_int(Max_Procs_Allowed),
    High_Water_Procs = 0,

    %% Cxy counters init tuple...
    {{Task_Type, Stored_Max_Procs_Value, Active_Procs, Max_History, Slow_Factor_As_Percentage, High_Water_Procs},

     %% Cumulative performance moving average init tuple...
     {make_cma_key(Task_Type), 0, 0, Max_History, Slow_Factor_As_Percentage}}.


max_procs_to_int(unlimited)   -> -1;
max_procs_to_int(inline_only) ->  0;
max_procs_to_int(Max_Procs)   -> Max_Procs.

int_to_max_procs(-1)  -> unlimited;
int_to_max_procs( 0)  -> inline_only;
int_to_max_procs(Max) -> Max.

is_valid_limit(Max_Procs)
  when is_integer(Max_Procs),
       Max_Procs > 0        -> true;
is_valid_limit(unlimited)   -> true;
is_valid_limit(inline_only) -> true;
is_valid_limit(_)           -> false.

%% Locations of cxy counter tuple positions used for ets:update_counter
%% {Task_Type, Stored_Max_Procs_Value, Active_Procs, Max_History, Slow_Factor_As_Percentage, High_Water_Procs}
-define(MAX_PROCS_POS,    2).
-define(ACTIVE_PROCS_POS, 3).
-define(MAX_HISTORY_POS,  4).
-define(HIGH_WATER_PROCS_POS, 6).

incr_active_procs(Task_Type) ->
    [ Active_Procs, High_Water_Procs ] =
        ets:update_counter(?MODULE, Task_Type, [{?ACTIVE_PROCS_POS, 1}, {?HIGH_WATER_PROCS_POS, 0}]),
    %% Use ets:update_counter to implement max(High_Water_Procs,
    %% Active_Procs).  We use the -Active_Procs because that allows us to use
    %% a quirk of the ets:update_counter threshold interface to atomically
    %% maintain the max.
    Active_Procs > -High_Water_Procs
        andalso ets:update_counter(?MODULE, Task_Type, {?HIGH_WATER_PROCS_POS, 0, -Active_Procs, -Active_Procs}),
    Active_Procs.

decr_active_procs(Task_Type) ->
    ets:update_counter(?MODULE, Task_Type, {?ACTIVE_PROCS_POS, -1}).

reset_proc_counts(Task_Type) ->
    ets:update_counter(?MODULE, Task_Type, [{?MAX_PROCS_POS, 0}, {?MAX_HISTORY_POS, 0}]).

change_max_proc_limit(Task_Type, New_Limit) ->
    ets:update_element(?MODULE, Task_Type, {?MAX_PROCS_POS, max_procs_to_int(New_Limit)}).


%%%------------------------------------------------------------------------------
%%% Internal interface for maintaining moving avgs for slow execution detection
%%%------------------------------------------------------------------------------

make_cma_key(Task_Type) ->
    {cma, Task_Type}.

%% Cumulative performance moving average tuple...
%% {make_cma_key(Task_Type), Spawn_Time_Cma, Execution_Time_Cma, Max_History, Slow_Factor_As_Percentage}
-define(CMA_SPAWN_CMA_POS,   2).
-define(CMA_EXEC_CMA_POS,    3).
-define(CMA_RING_SIZE_POS,   4).
-define(CMA_SLOW_FACTOR_POS, 5).

%% Read all values using update_counter to avoid releasing write_lock for a read_lock.
-define(CMA_READ_CMD,
        [{?CMA_SPAWN_CMA_POS,   0},
         {?CMA_EXEC_CMA_POS,    0},
         {?CMA_RING_SIZE_POS,   0},
         {?CMA_SLOW_FACTOR_POS, 0}
        ]).

get_cma_factors(Cma_Key) ->
    ets:update_counter(?MODULE, Cma_Key, ?CMA_READ_CMD).

%% Update moving averages using update_element to avoid read_lock.
-define(CMA_WRITE_CMD(__Spawn, __Exec),
        [{?CMA_SPAWN_CMA_POS, __Spawn},
         {?CMA_EXEC_CMA_POS,  __Exec}
        ]).

update_cma_avgs(Cma_Key, New_Spawn_Cma, New_Exec_Cma) ->
    ets:update_element(?MODULE, Cma_Key, ?CMA_WRITE_CMD(New_Spawn_Cma, New_Exec_Cma)).


%%%------------------------------------------------------------------------------
%%% Internal interface for updating execution times / detecting slow execution
%%%------------------------------------------------------------------------------

-type snap()        :: erlang:timestamp().
-type exec_triple() :: {module(), atom(), list()}.

-spec update_spawn_times  (task_type(), exec_triple(), snap(), snap(), snap()) -> is_slow | not_slow.
-spec update_inline_times (task_type(), exec_triple(), snap(), snap(), snap()) -> is_slow | not_slow.

update_spawn_times(Task_Type, Task_Fun, Start, Spawn, Done) ->
    case update_times(make_buffer_spawn(Task_Type), Task_Type, Task_Fun, Start, Spawn, Done, true) of
        is_slow   -> update_slow_times(Task_Type, Task_Fun, Start, Spawn, Done);
        _Not_Slow -> not_slow
    end.

update_inline_times(Task_Type, Task_Fun, Start, Spawn, Done) ->
    case update_times(make_buffer_inline(Task_Type), Task_Type, Task_Fun, Start, Spawn, Done, true) of
        is_slow   -> update_slow_times(Task_Type, Task_Fun, Start, Spawn, Done);
        _Not_Slow -> not_slow
    end.

update_slow_times(Task_Type, Task_Fun, Start, Spawn, Done) -> 
    not_checked = update_times(make_buffer_slow(Task_Type), Task_Type, Task_Fun, Start, Spawn, Done, false),
    slow.

update_times(Task_Table, Task_Type, Task_Fun, Start, Spawn, Done, Check_Slowness) ->
    Exec_Elapsed  = timer:now_diff(Done, Spawn),
    Spawn_Elapsed = timer:now_diff(Spawn, Start),
    Elapsed = {Task_Fun, Start, Spawn_Elapsed, Exec_Elapsed},
    case ets_buffer:write(Task_Table, Elapsed) of
        {missing_ets_buffer, _} = Error ->
            Error;
        _Num_Entries  ->
            case Check_Slowness of
                true  -> check_if_slow(Task_Type, Spawn_Elapsed, Exec_Elapsed);
                false -> not_checked
            end
    end.

check_if_slow(Task_Type, Spawn_Elapsed, Exec_Elapsed) ->
    {Spawn_Cma, Exec_Cma, Slow_Factor_As_Percentage} = update_cmas(Task_Type, Spawn_Elapsed, Exec_Elapsed),
    is_slow(Spawn_Elapsed, Exec_Elapsed, Spawn_Cma, Exec_Cma, Slow_Factor_As_Percentage).

%% Slow_Factor is a percentage, so 300 would be 3x the moving average.
%% The slow test combines the spawn time and execution time and compares
%% that to the sum of the two moving averages.
is_slow(_This_Spawn, _This_Exec, _Spawn_Cma, 0 = _Exec_Cma, _Slow_Factor_As_Percentage) -> not_slow;
is_slow( Spawn_Time,  Exec_Time,  Spawn_Cma,      Exec_Cma,  Slow_Factor_As_Percentage) ->
    Cma_Time  = Spawn_Cma  + Exec_Cma,
    Full_Time = Spawn_Time + Exec_Time,
    case round((Full_Time / Cma_Time) * 100) >= Slow_Factor_As_Percentage of
        false -> not_slow;
        true  -> is_slow
    end.

update_cmas(Task_Type, Spawn_Elapsed, Exec_Elapsed) ->

    %% Fetch, compute and update the moving averages...
    Cma_Key = make_cma_key(Task_Type),
    [Old_Spawn_Cma, Old_Exec_Cma, Num_Samples, Slow_Factor_As_Percentage] = get_cma_factors(Cma_Key),
    New_Exec_Cma  = cumulative_moving_avg(Old_Exec_Cma,  Exec_Elapsed,  Num_Samples),
    New_Spawn_Cma = cumulative_moving_avg(Old_Spawn_Cma, Spawn_Elapsed, Num_Samples),
    true = update_cma_avgs(Cma_Key, New_Spawn_Cma, New_Exec_Cma),

    %% The previous moving averages are returned for comparison.
    {Old_Spawn_Cma, Old_Exec_Cma, Slow_Factor_As_Percentage}.

cumulative_moving_avg(      0, New_Case, _Num_Samples) -> New_Case;
cumulative_moving_avg(Old_Avg, New_Case,  Num_Samples) ->
    round((Num_Samples * Old_Avg + New_Case) / (Num_Samples + 1)).


%%%------------------------------------------------------------------------------
%%% Mechanism for creating ets table, and executing tasks
%%%------------------------------------------------------------------------------

%% @doc
%%   Initialize a named ETS table to hold concurrency limits which is checked
%%   before spawning new processes to ensure limits are not exceeded. The
%%   Limits argument is a list of task types, the corresponding maximum
%%   number of simultaneous processes to allow, and a maximum number of
%%   timestamps to record in a circular buffer for later analysis.
%%
%%   Note: this function must be called by a long-lived process, probably a
%%   supervisor, because it will be the owner of the cxy_ctl ets table. If
%%   the owning process (i.e., the caller of cxy_ctl:init/1) ever terminates,
%%   all subsequent attempts to use cxy_ctl to spawn tasks will crash with a
%%   badarg because the ets table holding the limits will be gone.
%% @end

-spec init([{Task_Type, Type_Max, Timer_History_Count, Slow_Factor_As_Percentage}])
          -> boolean() | {error, init_already_executed}
                 | {error, {invalid_init_args, list()}} when
      Task_Type :: task_type(),
      Type_Max  :: cxy_limit(),
      Timer_History_Count :: non_neg_integer(),
      Slow_Factor_As_Percentage :: 101 .. 100000.

init(Limits) ->
    case ets:info(?MODULE, name) of
        ?MODULE   -> {error, init_already_executed};
        undefined ->
            %% Validate Limits and construct ring buffer params for each concurrency type...
            case lists:foldl(fun(Args, Acc) -> valid_limits(Args, Acc) end, {[], [], []}, Limits) of
                { Buffer_Params,  Cxy_Params,     []} -> do_init(Buffer_Params, Cxy_Params);
                {_Buffer_Params, _Cxy_Params, Errors} -> {error, {invalid_init_args, lists:reverse(Errors)}}
            end
    end.

valid_limits({Type, Max_Procs, History_Count, Slow_Factor_As_Percentage} = Limit,
             {Buffer_Params, Cxy_Params, Errors} = Results)
  when is_atom(Type),
       is_integer(History_Count), History_Count >= 0,
       is_integer(Slow_Factor_As_Percentage),
       Slow_Factor_As_Percentage >= 101,
       Slow_Factor_As_Percentage =< 100000 ->
    case is_valid_limit(Max_Procs) of
        true  -> make_limits(Limit, Results);
        false -> {Buffer_Params, Cxy_Params, [Limit | Errors]}
    end;
valid_limits(Invalid, {Buffer_Params, Cxy_Params, Errors}) ->
    {Buffer_Params, Cxy_Params, [Invalid | Errors]}.

make_limits({Type, Max_Procs, History_Count, Slow_Factor_As_Percentage}, {Buffer_Params, Cxy_Params, Errors}) ->
    {make_buffer_params(Buffer_Params, Type, History_Count),
     make_proc_params(Cxy_Params, Type, Max_Procs, History_Count, Slow_Factor_As_Percentage), Errors}.
    
make_buffer_slow  (Type) -> list_to_atom("slow_"   ++ atom_to_list(Type)).
make_buffer_spawn (Type) -> list_to_atom("spawn_"  ++ atom_to_list(Type)).
make_buffer_inline(Type) -> list_to_atom("inline_" ++ atom_to_list(Type)).
make_buffer_names (Type) -> {make_buffer_spawn(Type), make_buffer_inline(Type), make_buffer_slow(Type)}.

make_buffer_params(Acc, _Type,           0) -> Acc;
make_buffer_params(Acc,  Type, Max_History) ->
    {Spawn_Type, Inline_Type, Slow_Type} = make_buffer_names(Type),
    [{Spawn_Type,  ring, Max_History},
     {Inline_Type, ring, Max_History},
     {Slow_Type,   ring, Max_History}
     | Acc].

make_proc_params(Acc, Type, Max_Procs, Max_History, Slow_Factor_As_Percentage) ->
    [make_proc_values(Type, Max_Procs, Max_History, Slow_Factor_As_Percentage) | Acc].
    
    
do_init(Buffer_Params, Cxy_Params) ->
    _ = ets:new(?MODULE, [named_table, ordered_set, public, {write_concurrency, true}]),
    do_insert_limits(Buffer_Params, Cxy_Params).

do_insert_limits(Buffer_Params, Cxy_Cma_Params) ->
    ets_buffer:create(Buffer_Params),
    _ = [begin
             ets:insert_new(?MODULE, Cxy_Params),
             ets:insert_new(?MODULE, Cma_Params)
         end || {Cxy_Params, Cma_Params} <- Cxy_Cma_Params],
    true.


-spec add_task_types([{Task_Type, Type_Max, Timer_History_Count, Slow_Factor_As_Percentage}])
                    -> boolean() | {error, {add_duplicate_task_types, list()}} when
      Task_Type :: task_type(),
      Type_Max  :: cxy_limit(),
      Timer_History_Count :: non_neg_integer(),
      Slow_Factor_As_Percentage :: 101 .. 100000.

add_task_types(Limits) ->
    case [Args || Args = {Task_Type, _Max_Procs, _History_Count, _Slow_Factor_As_Percentage} <- Limits,
                  ets:lookup(?MODULE, Task_Type) =/= []] of
        [] ->
            %% Validate Limits and construct ring buffer params for each concurrency type...
            case lists:foldl(fun(Args, Acc) -> valid_limits(Args, Acc) end, {[], [], []}, Limits) of
                { Buffer_Params,  Cxy_Params,     []} -> do_insert_limits(Buffer_Params, Cxy_Params);
                {_Buffer_Params, _Cxy_Params, Errors} -> {error, {invalid_add_args, lists:reverse(Errors)}}
            end;
        Dups -> {error, {add_duplicate_task_types, Dups}}
    end.


-spec remove_task_types([task_type()])
                       -> pos_integer() | {error, {missing_task_types, [task_type()]}}.

remove_task_types(Task_Types) ->    
    case [Task_Type || Task_Type <- Task_Types, ets:lookup(?MODULE, Task_Type) =/= []] of
        Task_Types  -> Deletes = [begin
                                      {Buff1, Buff2, Buff3} = make_buffer_names(Task_Type),
                                      [ets_buffer:delete(B) || B <- [Buff1, Buff2, Buff3]]
                                  end || Task_Type <- Task_Types, ets:delete(?MODULE, Task_Type)],
                       length(Deletes);
        Found_Types -> {error, {missing_task_types, Task_Types -- Found_Types}}
    end.


-spec adjust_task_limits([{task_type(), cxy_limit()}])
                        -> pos_integer() | {error, {missing_task_types, [task_type()]}}.

adjust_task_limits(Task_Limits) ->
    case [TTL || TTL = {Task_Type, _} <- Task_Limits, ets:lookup(?MODULE, Task_Type) =/= []] of
        Task_Limits -> case [{Task_Type, Limit} || {Task_Type, Limit} <- Task_Limits, is_valid_limit(Limit)] of
                           Task_Limits  ->
                               Changes = [TL || TL = {Task_Type, New_Limit} <- Task_Limits,
                                                change_max_proc_limit(Task_Type, New_Limit)],
                               length(Changes);
                           Legal_Limits ->
                               {error, {invalid_task_limits, Task_Limits -- Legal_Limits}}
                       end;
        Found_Tasks -> {error, {missing_task_types, Task_Limits -- Found_Tasks}}
    end.


%% @doc
%%   Execute a task by spawning a function to run it, only if the task type
%%   does not have too many currently executing processes. If there are too
%%   many, execute the task inline. Returns neither a pid nor a result.
%% @end

-spec execute_task(atom(), atom(), atom(), list())                          -> ok.
-spec execute_task(atom(), atom(), atom(), list(), all_keys | dict_props()) -> ok.

execute_task(Task_Type, Mod, Fun, Args) ->
    internal_execute_task(Task_Type, Mod, Fun, Args, inline, none).

execute_task(Task_Type, Mod, Fun, Args, Dict_Props) ->
    internal_execute_task(Task_Type, Mod, Fun, Args, inline, Dict_Props).


%% @doc
%%   Execute a task by spawning a function to run it, only if the task type
%%   does not have too many currently executing processes. If there are too
%%   many, return {max_pids, Max} without executing, rather than ok.
%% @end

-spec maybe_execute_task(atom(), atom(), atom(), list()) -> ok | {max_pids, non_neg_integer()}.
-spec maybe_execute_task(atom(), atom(), atom(), list(),
                         all_keys | dict_props()) -> ok | {max_pids, non_neg_integer()}.

maybe_execute_task(Task_Type, Mod, Fun, Args) ->
    internal_execute_task(Task_Type, Mod, Fun, Args, refuse, none).

maybe_execute_task(Task_Type, Mod, Fun, Args, Dict_Props) ->
    internal_execute_task(Task_Type, Mod, Fun, Args, refuse, Dict_Props).


internal_execute_task(Task_Type, Mod, Fun, Args, Over_Limit_Action, Dict_Props) ->
    [Max, Max_History] = reset_proc_counts(Task_Type),
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
                inline -> _ = setup_local_process_dictionary(Dict_Props),
                          _ = execute_wrapper(Mod, Fun, Args, Task_Type, Max_History, Start, inline, []),
                          ok
            end
    end.

%% @doc
%%   Execute a task by spawning a function to run it, only if the task type
%%   does not have too many currently executing processes. If there are too
%%   many, execute the task inline. Returns a linked pid if spawned, or results
%%   if inlined.
%% @end

-spec execute_pid_link(atom(), atom(), atom(), list())                          -> pid() | {inline, any()}.
-spec execute_pid_link(atom(), atom(), atom(), list(), all_keys | dict_props()) -> pid() | {inline, any()}.

execute_pid_link(Task_Type, Mod, Fun, Args) ->
    internal_execute_pid(Task_Type, Mod, Fun, Args, link, inline, none).

execute_pid_link(Task_Type, Mod, Fun, Args, Dict_Props) ->
    internal_execute_pid(Task_Type, Mod, Fun, Args, link, inline, Dict_Props).

%% @doc
%%   Execute a task by spawning a function to run it, only if the task type
%%   does not have too many currently executing processes. If there are too
%%   many, return {max_pids, Max_Count} instead of linked pid.
%% @end

-spec maybe_execute_pid_link(atom(), atom(), atom(), list()) -> pid() | {max_pids, non_neg_integer()}.
-spec maybe_execute_pid_link(atom(), atom(), atom(), list(),
                             all_keys | dict_props()) -> pid() | {max_pids, non_neg_integer()}.

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

-spec execute_pid_monitor(atom(), atom(), atom(), list()) -> {pid(), reference()} | {inline, any()}.
-spec execute_pid_monitor(atom(), atom(), atom(), list(),
                          all_keys | dict_props()) -> {pid(), reference()} | {inline, any()}.

execute_pid_monitor(Task_Type, Mod, Fun, Args) ->
    internal_execute_pid(Task_Type, Mod, Fun, Args, monitor, inline, none).

execute_pid_monitor(Task_Type, Mod, Fun, Args, Dict_Props) ->
    internal_execute_pid(Task_Type, Mod, Fun, Args, monitor, inline, Dict_Props).

%% @doc
%%   Execute a task by spawning a function to run it, only if the task type
%%   does not have too many currently executing processes. If there are too
%%   many, return {max_pids, Max_Count} instead of {pid(), reference()}.
%% @end

-spec maybe_execute_pid_monitor(atom(), atom(), atom(), list()) -> {pid(), reference()} | {max_pids, non_neg_integer()}.
-spec maybe_execute_pid_monitor(atom(), atom(), atom(), list(),
                                all_keys | dict_props()) -> {pid(), reference()} | {max_pids, non_neg_integer()}.

maybe_execute_pid_monitor(Task_Type, Mod, Fun, Args) ->
    internal_execute_pid(Task_Type, Mod, Fun, Args, monitor, refuse, none).

maybe_execute_pid_monitor(Task_Type, Mod, Fun, Args, Dict_Props) ->
    internal_execute_pid(Task_Type, Mod, Fun, Args, monitor, refuse, Dict_Props).


internal_execute_pid(Task_Type, Mod, Fun, Args, Spawn_Type, Over_Limit_Action, Dict_Props) ->
    [Max, Max_History] = reset_proc_counts(Task_Type),
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
                inline -> _ = setup_local_process_dictionary(Dict_Props),
                          {inline, execute_wrapper(Mod, Fun, Args, Task_Type, Max_History, Start, inline, [])}
            end
    end.

setup_local_process_dictionary(none)     -> skip;
setup_local_process_dictionary(all_keys) -> skip;
setup_local_process_dictionary(Dict_Props) ->
    Dict = get(),
    [put(K, V) || {?VALID_DICT_VALUE_MARKER, {K,V}} <- Dict_Props, proplists:get_value(K, Dict) =:= undefined].

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
                     -> any() | no_return().

%% If Start is 'false', we don't want to record elapsed time history...
execute_wrapper(Mod, Fun, Args, Task_Type, _Max_History, false, Spawn_Or_Inline, Dict_Prop_Pairs) ->
    Result = try
                 _ = [put(Key, Val) || {Key, Val} <- Dict_Prop_Pairs],
                 apply(Mod, Fun, Args)
             catch Error:Type:STrace -> {error, {mfa_failure, {{Error, Type}, {Mod, Fun, Args}, Task_Type, Spawn_Or_Inline}}, STrace}
             after decr_active_procs(Task_Type)
             end,
    case Result of
        {error, Call_Data, Trace} -> fail_wrapper(Spawn_Or_Inline, Call_Data, Trace);
        Result             -> Result
    end;

%% Otherwise, we incur the overhead cost of recording elapsed time history.
execute_wrapper(Mod, Fun, Args, Task_Type, Max_History, Start, Spawn_Or_Inline, Dict_Prop_Pairs) ->
    MFA = {Mod, Fun, Args},
    Spawn = os:timestamp(),
    Result = try
                 _ = [put(Key, Val) || {Key, Val} <- Dict_Prop_Pairs],
                 apply(Mod, Fun, Args)
             catch Error:Type:STrace -> {error, {mfa_failure, {{Error, Type}, MFA, Task_Type, Max_History, Start, Spawn_Or_Inline}}, STrace}
             after
                 decr_active_procs(Task_Type),
                 case Spawn_Or_Inline of
                     spawn  -> update_spawn_times (Task_Type, MFA, Start, Spawn, os:timestamp());
                     inline -> update_inline_times(Task_Type, MFA, Start, Spawn, os:timestamp())
                 end
             end,
    case Result of
        {error, Call_Data, Trace} -> fail_wrapper(Spawn_Or_Inline, Call_Data, Trace);
        Result             -> Result
    end.

-spec fail_wrapper(spawn | inline, any(), any()) -> no_return().
fail_wrapper(spawn,  Call_Data, Stacktrace) -> erlang:error(spawn_failure,  [Call_Data, Stacktrace]);
fail_wrapper(inline, Call_Data, Stacktrace) -> exit       ({inline_failure, [Call_Data, Stacktrace]}).


%% @doc
%%    Provide a list of the registered concurrency limit types and their corresponding limit
%%    values for max_procs, active_procs, max_history size and slow_factor_as_percentage.
%% @end

-spec concurrency_types() -> [proplists:proplist()].

concurrency_types() ->
    [[{task_type, Task_Type}, {max_procs, int_to_max_procs(Max_Procs_Allowed)},
      {active_procs, Active_Procs}, {max_history, Max_History}, {slow_factor_as_percentage, Slow_Factor},
      {high_water_procs, -High_Water_Procs}]
     || {Task_Type, Max_Procs_Allowed, Active_Procs, Max_History, Slow_Factor, High_Water_Procs} <- ets:tab2list(?MODULE),
        is_atom(Task_Type)].


%% @doc
%%    Provide the entire performance history for a given task_type as a tuple of two elements:
%%    the performance for spawn execution and for inline execution. Each entry includes the
%%    start time for the request, the number of microseconds to spawn the task, and the number
%%    of microseconds to execute the request.
%% @end

-type spawn_history_result()  :: {spawn_execs,  [proplists:proplist()]}.
-type inline_history_result() :: {inline_execs, [proplists:proplist()]}.
-type slow_history_result()   :: {slow_execs,   [proplists:proplist()]}.

-type history_result() :: {spawn_history_result(), inline_history_result(), slow_history_result()}.

-spec history(atom()) -> history_result() | ets_buffer:buffer_error().
-spec history(atom(), inline, pos_integer()) -> inline_history_result() | ets_buffer:buffer_error();
             (atom(), spawn,  pos_integer()) -> spawn_history_result()  | ets_buffer:buffer_error();
             (atom(), slow,   pos_integer()) -> slow_history_result()   | ets_buffer:buffer_error().

%% @doc Provide all the performance history for a given task_type.
history(Task_Type) ->
    {Spawn_Type, Inline_Type, Slow_Type} = make_buffer_names(Task_Type),
    case get_buffer_times(Spawn_Type) of
        Spawn_Times_List when is_list(Spawn_Times_List) ->
            {{spawn_execs,  Spawn_Times_List},
             {inline_execs, get_buffer_times(Inline_Type)},
             {slow_execs,   get_buffer_times(Slow_Type)}} ;
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
    end;
history(Task_Type, slow, Num_Items) ->
    Slow_Type = make_buffer_slow(Task_Type),
    case get_buffer_times(Slow_Type, Num_Items) of
        Slow_Times_List when is_list(Slow_Times_List) -> {slow_execs, Slow_Times_List};
        Error -> Error
    end.

slow_calls(Task_Type) ->
    {_Spawns, _Execs, Slow_Calls} = history(Task_Type),
    Slow_Calls.

slow_calls(Task_Type, Num_Items) ->
    history(Task_Type, slow, Num_Items).
    
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

-define(HW_READ_CMD, {?HIGH_WATER_PROCS_POS, 0}).
-define(HW_RESET_CMD, {?HIGH_WATER_PROCS_POS, -1, 0, 0}).

%% @doc
%%   Return the highest number of concurrent processes for a given task type.
%%   The one-argument form just returns the high-water value, while the
%%   two-argument form allows the value to be reset.
%% @end

-spec high_water(task_type()) -> non_neg_integer().
-spec high_water(task_type(), cxy_clear()) -> non_neg_integer().

high_water(Task_Type) ->
  high_water(Task_Type, no_clear).

high_water(Task_Type, ClearCmd) ->
    case ClearCmd of
        clear ->
            [Old_High_Water, 0] = ets:update_counter(?MODULE, Task_Type, [?HW_READ_CMD, ?HW_RESET_CMD]),
            -Old_High_Water;
        no_clear ->
            -ets:update_counter(?MODULE, Task_Type, ?HW_READ_CMD)
    end.
