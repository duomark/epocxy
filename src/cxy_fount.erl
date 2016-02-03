%%%------------------------------------------------------------------------------
%%% @copyright (c) 2015-2016, DuoMark International, Inc.
%%% @author Jay Nelson <jay@duomark.com>
%%% @reference 2015-2016 Development sponsored by TigerText, Inc. [http://tigertext.com/]
%%% @reference The license is based on the template for Modified BSD from
%%%   <a href="http://opensource.org/licenses/BSD-3-Clause">OSI</a>
%%% @doc
%%%   A cxy_fount is a source of pre-allocated pids. The main operation is
%%%   to get a pid() so that it can be used for a one-shot execution before
%%%   being thrown away. This approach is safer than a worker pool and allows
%%%   similar concurrency execution, however there is a built-in ceiling so
%%%   back pressure can be easily implemented.
%%%
%%%   The fount is implemented as a push-down stack of slab allocated lists
%%%   of processes. The initialization parameters define the size of each
%%%   slab and the depth of the reservoir stack. Whenever a slab is empty,
%%%   a process is created to replace it. The hope is that there is never
%%%   a spike that exhausts the supply, but if there is, the caller will
%%%   be able to signal that the server is busy rather than trying to do
%%%   more work than we are capable of doing.
%%%
%%%   There is an option to set (or clear) a notifier. The notifier is
%%%   expected to be a live gen_event server which will receive notifications
%%%   whenever a new slab is allocated, or a request for pids cannot be
%%%   serviced (either the caller asked for more than the fount can hold
%%%   or more than the fount currently has available). The gen_event is
%%%   monitored so that if it crashes, it will be removed as a valid
%%%   notifier.
%%%
%%%   WARNING: the fount will crash if your Fount_Behaviour:start_pid/1
%%%   callback returns anything other than a pid().
%%% @since 0.9.9
%%% @end
%%%------------------------------------------------------------------------------
-module(cxy_fount).
-author('Jay Nelson <jay@duomark.com>').

-behaviour(gen_fsm).


%%% API
-export([start_link/1, start_link/2,    % Fount with no name
         start_link/3, start_link/4,    % Fount with name
%%         reinit/1,     reinit/2,        % Reset configuration
         get_pid/1,    get_pids/2,      % Get 1 pid or list of pids
         task_pid/2,   task_pids/2,     % Send message to pid
         get_total_rate_per_slab/1,     % Report the round trip allocator slab rate
         get_total_rate_per_process/1,  % Report the round trip allocator process rate
         get_spawn_rate_per_slab/1,     % Report the spawn allocator slab rate
         get_spawn_rate_per_process/1,  % Report the spawn allocator process rate
         get_status/1,                  % Get status of Fount,
         set_notifier/2                 % Set / remove gen_event notifier
        ]).

%%% Convenient test functions.
-export([stop/1]).

%%% gen_fsm callbacks
-export([init/1, format_status/2, handle_sync_event/4,
         handle_event/3, handle_info/3, code_change/4, terminate/3]).

%%% Internally spawned functions.
-export([allocate_slab/5]).

%%% state functions
-export(['EMPTY'/2, 'FULL'/2, 'LOW'/2]).
-export(['EMPTY'/3, 'FULL'/3, 'LOW'/3]).

-type state_name()   :: 'EMPTY' | 'FULL' | 'LOW'.
-type microseconds() :: non_neg_integer().
-type notifier()     :: undefined | pid().

%%% A slab of pids plus time to spawn them, and roundtrip time from
%%% initial request until they are provided to the gen_fsm API.
-record(timed_slab,
        {
          slab       = []      :: [pid()],              % Remaining pids in slab
          start      = {0,0,0} :: erlang:timestamp(),   % Time of slab fill request
          spawn_time = 0       :: microseconds(),       % Elapsed time to spawn slab
          total_time = 0       :: microseconds()        % Time to message and spawn
        }).
-type timed_slab() :: #timed_slab{}.

-record(cf_state,
        {
          behaviour                   :: module(),
          fount       = #timed_slab{} :: timed_slab(),        % One slab, start, spawn time, entire time
          reservoir   = []            :: [timed_slab()],      % Stack of Depth-1 slabs + times
          fount_count = 0             :: non_neg_integer(),   % Num pids in fount
          num_slabs   = 0             :: non_neg_integer(),   % Num slabs in reservoir
          depth       = 0             :: non_neg_integer(),   % Desired reservoir slabs + 1 for fount
          slab_size                   :: pos_integer(),       % Num pids in a single slab
          notifier                    :: notifier()           % Optional gen_event notifier pid
        }).
-type cf_state() :: #cf_state{}.

%%% cxy_fount behaviour callbacks
-type fount_ref() :: pid() | atom().  % gen_fsm reference
-export_type([fount_ref/0]).

-callback start_pid (fount_ref())     ->   pid()  | {error, Reason::any()}.
-callback send_msg  (Worker, tuple()) -> [Worker] | {error, Reason::any()} | []
                                             when Worker :: pid().

default_num_slabs()     ->   5.
default_slab_size()     ->  20.
default_reply_timeout() -> 500.


%%%===================================================================
%%% API
%%%===================================================================

%%% Make a fount without naming it.
-spec start_link(module())                               -> {ok, fount_ref()}.
-spec start_link(module(), pos_integer(), pos_integer()) -> {ok, fount_ref()}.

start_link(Fount_Behaviour)
  when is_atom(Fount_Behaviour) ->
    start_link(Fount_Behaviour, default_slab_size(), default_num_slabs()).

start_link(Fount_Behaviour, Slab_Size, Reservoir_Depth)
  when is_atom(Fount_Behaviour),
       is_integer(Slab_Size),       Slab_Size > 0,
       is_integer(Reservoir_Depth), Reservoir_Depth >= 2 ->
    gen_fsm:start_link(?MODULE, {Fount_Behaviour, Slab_Size, Reservoir_Depth}, []).


%%% Make a fount with a locally registered name.
-spec start_link(atom(), module())                               -> {ok, fount_ref()}.
-spec start_link(atom(), module(), pos_integer(), pos_integer()) -> {ok, fount_ref()}.

start_link(Fount_Name, Fount_Behaviour)
  when is_atom(Fount_Name), is_atom(Fount_Behaviour) ->
    start_link(Fount_Name, Fount_Behaviour, default_slab_size(), default_num_slabs()).

start_link(Fount_Name, Fount_Behaviour, Slab_Size, Reservoir_Depth)
  when is_atom(Fount_Name),         is_atom(Fount_Behaviour),
       is_integer(Slab_Size),       Slab_Size > 0,
       is_integer(Reservoir_Depth), Reservoir_Depth >= 2 ->
    gen_fsm:start_link({local, Fount_Name}, ?MODULE,
                       {Fount_Behaviour, Slab_Size, Reservoir_Depth}, []).

%%% Set or clear a gen_event pid() for notifications of slab replacement.
-spec set_notifier (fount_ref(), undefined | pid()) -> ok.
-spec stop         (fount_ref()) -> ok.

set_notifier(Fount, Pid)
  when Pid =:= undefined; is_pid(Pid) ->
    gen_fsm:sync_send_all_state_event(Fount, {set_notifier, Pid}).

stop(Fount) ->
    gen_fsm:sync_send_all_state_event(Fount, {stop}).


%%% Reinitialize the fount configuration parameters
%% -spec reinit(fount_ref(), module())                               -> ok.
%% -spec reinit(fount_ref(), module(), pos_integer(), pos_integer()) -> ok.

%% reinit(Fount, Fount_Behaviour)
%%   when is_atom (Fount) orelse is_pid (Fount),
%%        is_atom (Fount_Behaviour) ->
%%     reinit(Fount, Fount_Behaviour, default_slab_size(), default_num_slabs()).
%% reinit(Fount, Fount_Behaviour, Slab_Size, Reservoir_Depth)
%%   when is_atom (Fount) orelse is_pid (Fount),
%%        is_atom (Fount_Behaviour),
%%        is_integer (Slab_Size), Slab_Size > 0,
%%        is_integer (Reservoir_Depth), Reservoir_Depth >= 2 ->
%%     gen_fsm:sync_send_all_event(Fount, {reinit, Fount_Behaviour, Slab_Size, Reservoir_Depth}).


-spec get_pid  (fount_ref())                -> [pid()] | {error, any()}.
-spec get_pids (fount_ref(), pos_integer()) -> [pid()] | {error, any()}.

get_pid(Fount) ->
    gen_fsm:sync_send_event(Fount, {get_pids, 1}, default_reply_timeout()).

get_pids(Fount, Num) when is_integer(Num), Num >= 0 ->
    gen_fsm:sync_send_event(Fount, {get_pids, Num}, default_reply_timeout()).


-spec task_pid  (fount_ref(),  any())  -> [pid()] | {error, any()}.
-spec task_pids (fount_ref(), [any()]) -> [pid()] | {error, any()}.

task_pid(Fount, Msg) ->
    gen_fsm:sync_send_event(Fount, {task_pids, [Msg]}, default_reply_timeout()).

task_pids(Fount, Msgs) when is_list(Msgs) ->
    gen_fsm:sync_send_event(Fount, {task_pids,  Msgs}, default_reply_timeout()).


-type status_attr() :: {current_state, atom()}              % FSM State function name
                     | {behaviour,     module()}            % Fount behaviour module
                     | {fount_count,   non_neg_integer()}   % Num pids in top slab
                     | {slab_count,    non_neg_integer()}   % Num of full other slabs
                     | {slab_size,     non_neg_integer()}   % Size of a full slab
                     | {max_slabs,     non_neg_integer()}.  % Max number of slabs including fount

-spec get_total_rate_per_slab    (fount_ref()) -> microseconds().
-spec get_total_rate_per_process (fount_ref()) -> microseconds().
-spec get_spawn_rate_per_slab    (fount_ref()) -> microseconds().
-spec get_spawn_rate_per_process (fount_ref()) -> microseconds().
-spec get_status (fount_ref()) -> [status_attr(), ...].

get_total_rate_per_slab    (Fount) -> gen_fsm:sync_send_all_state_event(Fount, {get_total_rate_per_slab}).
get_total_rate_per_process (Fount) -> gen_fsm:sync_send_all_state_event(Fount, {get_total_rate_per_process}).
get_spawn_rate_per_slab    (Fount) -> gen_fsm:sync_send_all_state_event(Fount, {get_spawn_rate_per_slab}).
get_spawn_rate_per_process (Fount) -> gen_fsm:sync_send_all_state_event(Fount, {get_spawn_rate_per_process}).
get_status                 (Fount) -> gen_fsm:sync_send_all_state_event(Fount, {get_status}).


%%%===================================================================
%%% gen_fsm callbacks
%%%===================================================================

%%%------------------------------------------------------------------------------
%%% Initialize the FSM by spawning Reservoir_Depth slab allocators
%%% and starting in the 'EMPTY' state.
%%%------------------------------------------------------------------------------

-spec init({module(), pos_integer(), pos_integer()}) -> {ok, 'EMPTY', cf_state()}.
-spec format_status(normal | terminate, list()) -> proplists:proplist().

init({Fount_Behaviour, Slab_Size, Reservoir_Depth}) ->

    %% Spawn a new allocator for each slab desired...
    Slab_Allocator_Args = [self(), Fount_Behaviour, os:timestamp(), Slab_Size, []],
    done = spawn_link_allocators(Reservoir_Depth, Slab_Allocator_Args),

    %% Finish initializing, any newly allocated slabs will appear as events.
    Init_State = #cf_state{
                    behaviour  = Fount_Behaviour,
                    slab_size  = Slab_Size,
                    depth      = Reservoir_Depth
                   },
    {ok, 'EMPTY', Init_State}.

%%% Report summarized internal state to sys:get_status or crash logging.
format_status(_Reason, [_Dict, State]) ->
    generate_status(State).

generate_status(State_Name, State) ->
    [{current_state, State_Name} | generate_status(State)].

generate_status(#cf_state{depth=Depth, fount_count=FC, behaviour=Behaviour,
                          num_slabs=Num_Slabs, slab_size=Slab_Size}) ->
    Max_Pid_Count     = Depth * Slab_Size,
    Current_Pid_Count = FC + (Num_Slabs * Slab_Size),
    [
     {fount_count,   FC},
     {max_slabs,     Depth},
     {slab_size,     Slab_Size},
     {slab_count,    Num_Slabs},
     {max_pids,      Max_Pid_Count},
     {pid_count,     Current_Pid_Count},
     {behaviour,     Behaviour}
    ].


%%% Spawn linked slab allocators without using lists:seq/2.
%%% If the gen_fsm goes down, any in progress allocators should also.
spawn_link_allocators(             0, _Slab_Args) -> done;
spawn_link_allocators(Num_Allocators,  Slab_Args)
  when is_integer(Num_Allocators), Num_Allocators > 0 ->
    _ = erlang:spawn_opt(?MODULE, allocate_slab, Slab_Args, [link]),
    spawn_link_allocators(Num_Allocators-1, Slab_Args).

%%% Rely on the client behaviour to create new pids. This means using
%%% spawn or any of the gen_*:start patterns since the pids are unsupervised.
%%% The resulting pids must be linked to the cxy_fount parent so that they are
%%% destroyed if the parent terminates. While idle, the slab allocated pids
%%% should avoid crashing because they can take out the entire cxy_fount.
%%% Once a pid receives a task_pid command, it becomes unlinked and free to
%%% complete its task on its own timeline independently from the fount.
allocate_slab(Parent_Pid, _Module, Start_Time, 0, Slab) ->
    Elapsed_Time = timer:now_diff(os:timestamp(), Start_Time),
    gen_fsm:send_event(Parent_Pid, {slab, Slab, Start_Time, Elapsed_Time});

allocate_slab(Parent_Pid,  Module, Start_Time, Num_To_Spawn, Slab)
 when is_pid(Parent_Pid), is_atom(Module), is_integer(Num_To_Spawn), Num_To_Spawn > 0 ->

    %% Module behaviour needs to explicitly link to the parent_pid,
    %% since this function is executing in the caller's process space,
    %% rather than the gen_fsm of the cxy_fount parent_pid process space.
    case Module:start_pid(Parent_Pid) of
        Allocated_Pid when is_pid(Allocated_Pid) ->
            allocate_slab(Parent_Pid, Module, Start_Time, Num_To_Spawn-1, [Allocated_Pid | Slab])
    end.


%%%------------------------------------------------------------------------------
%%% Asynch state functions (triggered by gen_fsm:send_event/2)
%%%------------------------------------------------------------------------------

-type slab() :: {slab, [pid()], erlang:timestamp(), microseconds()}.

-spec 'EMPTY' (slab(), cf_state()) -> {next_state, 'EMPTY'       , cf_state()}.
-spec 'LOW'   (slab(), cf_state()) -> {next_state, 'FULL' | 'LOW', cf_state()}.
-spec 'FULL'  (slab(), cf_state()) -> {next_state, 'FULL'        , cf_state()}
                                          | {stop, overfull      , cf_state()}.

%%% When empty or low, add newly allocated slab of pids...
'EMPTY' ({slab,  Pids,  Start_Time,  Elapsed}, #cf_state{} = State) -> add_slab(State, Pids, Start_Time, Elapsed);
'EMPTY' (_Event,                               #cf_state{} = State) -> {next_state, 'EMPTY',  State}.

'LOW'   ({slab,  Pids,  Start_Time,  Elapsed}, #cf_state{} = State) -> add_slab(State, Pids, Start_Time, Elapsed);
'LOW'   (_Event,                               #cf_state{} = State) -> {next_state, 'LOW',    State}.

%%% When full, we shouldn't receive a request to add more.
'FULL'  ({slab, _Pids, _Start_Time, _Elapsed}, #cf_state{} = State) -> {stop,       overfull, State};
'FULL'  (_Event,                               #cf_state{} = State) -> {next_state, 'FULL',   State}.


%%% Then to the reservoir of untapped slabs.
add_slab(#cf_state{notifier=Notifier, slab_size=Slab_Size} = State, Pids, Start_Time, Elapsed) ->

    %% Notify the gen_event if it is waiting for slab events...
    Total_Time = timer:now_diff(os:timestamp(), Start_Time),
    [gen_event:notify(Notifier, {slab_added, Slab_Size, Start_Time, Elapsed, Total_Time})
     || is_pid(State#cf_state.notifier)],

    %% Slabs are added to the fount first if empty, otherwise to the reservoir.
    Timed_Slab = #timed_slab{slab=Pids, start=Start_Time, spawn_time=Elapsed, total_time=Total_Time},
    #cf_state{fount_count=Fount_Count, reservoir=Slabs, depth=Depth, num_slabs=Num_Slabs} = State,
    New_State = case Fount_Count of
                    0              -> State#cf_state{fount=Timed_Slab, fount_count=Slab_Size};
                    FC when FC > 0 -> State#cf_state{reservoir=[Timed_Slab | Slabs], num_slabs=Num_Slabs+1}
                end,

    %% Next FSM state is 'FULL' only when reservoir + fount = Depth.
    #cf_state{num_slabs=New_Num_Slabs} = New_State,
    New_State_Name = case Depth-1 of    % Reservoir is full, without fount.
                         New_Num_Slabs            -> 'FULL';
                         N when N > New_Num_Slabs -> 'LOW'
                         %% Crash if New_Num_Slabs >= Depth, too many slabs created
                     end,
    {next_state, New_State_Name, New_State}.


%%%------------------------------------------------------------------------------
%%% Synchronous state functions (triggered by gen_fsm:sync_send_event/2,3)
%%%------------------------------------------------------------------------------

-type synch_request() :: {task_pids, [any()]} | {get_pids, pos_integer()}.

-spec 'EMPTY' (synch_request(), {pid(), reference()}, cf_state()) -> {reply,      [], 'EMPTY'                 , cf_state()}.
-spec 'FULL'  (synch_request(), {pid(), reference()}, cf_state()) -> {reply, [pid()], 'EMPTY' | 'LOW' | 'FULL', cf_state()}.
-spec 'LOW'   (synch_request(), {pid(), reference()}, cf_state()) -> {reply, [pid()], 'EMPTY' | 'LOW'         , cf_state()}.

%%% 'EMPTY' state is only exited when a slab of pids is delivered...
'EMPTY' ({task_pids, _Msgs}, _From, #cf_state{} = State) -> {reply,      [], 'EMPTY', State};
'EMPTY' ({get_pids,  _Num},  _From, #cf_state{} = State) -> {reply,      [], 'EMPTY', State};
'EMPTY' (_Event,             _From, #cf_state{} = State) -> {reply, ignored, 'EMPTY', State}.

%%% 'FULL' state is exited when the fount becomes empty and one more pid is needed...
'FULL'  ({task_pids, Msgs},  _From, #cf_state{} = State) -> task_pids (Msgs, 'FULL',  State);
'FULL'  ({get_pids,  Num},   _From, #cf_state{} = State) -> get_pids  (Num,  'FULL',  State);
'FULL'  (_Event,             _From, #cf_state{} = State) -> {reply, ignored, 'FULL',  State}.

%%% 'LOW' state is exited when there are no reserve slabs or reserve is full.
'LOW'   ({task_pids, Msgs},  _From, #cf_state{} = State) -> task_pids (Msgs, 'LOW',   State);
'LOW'   ({get_pids,  Num},   _From, #cf_state{} = State) -> get_pids  (Num,  'LOW',   State);
'LOW'   (_Event,             _From, #cf_state{} = State) -> {reply, ignored, 'LOW',   State}.


%%%------------------------------------------------------------------------------
%%% task_pids uses get_pids, then sends messages to them
%%%------------------------------------------------------------------------------

%%% Get as many workers as there are messages to send, then give a message
%%% to each one without traversing the worker or message list more than once.
task_pids(Msgs, State_Fn, #cf_state{behaviour=Module} = State)
  when is_list(Msgs) ->
    %% Well, ok, twice for the message list...
    Num_Pids = length(Msgs),
    Reply = {reply, Workers, _New_State_Name, _New_State}
        = get_pids(Num_Pids, State_Fn, State),
    _ = [all_sent = msg_workers(Module, Workers, Msgs) || Workers =/= []],
    Reply.

msg_workers(_Module,                 [],           []) -> all_sent;
msg_workers( Module, [Worker | Workers], [Msg | Msgs]) ->
    send_msg    (Module, Worker,  Msg),
    msg_workers (Module, Workers, Msgs).

send_msg(Module, Pid, Msg) ->
    try   Module:send_msg(Pid, Msg), Pid
    catch Class:Type -> {error, {Module, send_msg, Class, Type, Msg}}
    end.


%%%------------------------------------------------------------------------------
%%% get_pids responds with the desired worker list of pid()
%%%------------------------------------------------------------------------------

%% 0 Pids wanted...
get_pids (0,  State_Fn, #cf_state{} = State) ->
    {reply, [], State_Fn, State};
 
%% 1 Pid wanted...
get_pids (1, _State_Fn, #cf_state{fount=#timed_slab{slab=[Pid]}} = State) ->
    replace_slab_then_return_fount([Pid], State);
get_pids (1,  State_Fn, #cf_state{fount=#timed_slab{slab=[Pid | More]}, fount_count=FC} = State) ->
    New_State = State#cf_state{fount=#timed_slab{slab=More}, fount_count=FC-1},
    reply([Pid], State_Fn, New_State);
get_pids (1, _State_Fn, #cf_state{fount=#timed_slab{slab=[]}, num_slabs=Num_Slabs, slab_size=Slab_Size} = State) ->
    [#timed_slab{slab=[Pid | Rest]} = Slab | More_Slabs] = State#cf_state.reservoir,
    New_Fount = Slab#timed_slab{slab=Rest},
    New_State = State#cf_state{fount=New_Fount, reservoir=More_Slabs, fount_count=Slab_Size-1, num_slabs=Num_Slabs-1},
    reply([Pid], 'LOW', New_State);

%%% More than 1 pid wanted, can be supplied by Fount...
%%% (Fount might be greater than slab_size, so this clause comes before slab checks)
get_pids (Num_Pids, _State_Fn, #cf_state{fount=#timed_slab{slab=Fount}, fount_count=FC} = State)
  when Num_Pids =:= FC ->
    replace_slab_then_return_fount(Fount, State);

get_pids (Num_Pids,  State_Fn, #cf_state{fount=#timed_slab{slab=Fount}, fount_count=FC} = State)
  when Num_Pids < FC ->
    Fount_Count = FC - Num_Pids,
    {Pids, Remaining} = lists:split(Num_Pids, Fount),
    New_Fount = #timed_slab{slab=Remaining},
    reply(Pids, State_Fn, State#cf_state{fount=New_Fount, fount_count=Fount_Count});

%%% More than 1 pid wanted, matches Slab_Size, grab the top of the reservoir if it's not empty...
get_pids (Num_Pids, _State_Fn, #cf_state{slab_size=Slab_Size, num_slabs=Num_Slabs} = State)
  when Num_Pids =:= Slab_Size, Num_Slabs > 0 ->
    #cf_state{behaviour=Mod, reservoir=[#timed_slab{slab=Slab} | More_Slabs]} = State,
    replace_slabs(Mod, 1, Slab_Size),
    reply(Slab, 'LOW', State#cf_state{reservoir=More_Slabs, num_slabs=Num_Slabs-1});

%%% More than 1 pid wanted, less than Slab_Size, grab the front of top slab, add balance to fount...
get_pids (Num_Pids, _State_Fn, #cf_state{slab_size=Slab_Size, num_slabs=Num_Slabs} = State)
  when Num_Pids < Slab_Size, Num_Slabs > 0 ->
    #cf_state{behaviour=Mod, fount=#timed_slab{slab=Fount}, fount_count=FC,
              reservoir=[#timed_slab{slab=Slab} = Reservoir_Slab | More_Slabs]} = State,
    replace_slabs(Mod, 1, Slab_Size),
    {Pids, Remaining} = lists:split(Num_Pids, Slab),
    Partial_Slab_Size = Slab_Size - Num_Pids,
    Fount_Count = FC + Partial_Slab_Size,

    %% Try to be efficient about reconstructing Fount (may end up larger than a slab)...
    %% The timing will be off, because we reflect the time to spawn Remaining.
    New_Fount = case Partial_Slab_Size > FC of
                    true  -> Reservoir_Slab#timed_slab{slab=Fount ++ Remaining};
                    false -> Reservoir_Slab#timed_slab{slab=Remaining ++ Fount}
                end,
    New_State = State#cf_state{fount=New_Fount, fount_count=Fount_Count,
                               reservoir=More_Slabs, num_slabs=Num_Slabs-1},
    reply(Pids, 'LOW', New_State);

%%% More than 1 Pid wanted, but not enough available...
get_pids (Num_Pids,  State_Fn, #cf_state{fount_count=FC, slab_size=Slab_Size, num_slabs=Num_Slabs} = State)
  when Num_Pids > (Num_Slabs * Slab_Size) + FC ->
    {reply, [], State_Fn, State};

%%% More than 1 pid wanted, more than Slab_Size, see if there are enough to return...
get_pids (Num_Pids, _State_Fn, #cf_state{fount_count=FC, slab_size=Slab_Size, num_slabs=Num_Slabs} = State)
  when Num_Pids > Slab_Size, Num_Pids < (Num_Slabs * Slab_Size) + FC -> 
    Excess       = Num_Pids rem Slab_Size,
    Slabs_Needed = (Num_Pids - Excess) div Slab_Size,
    #cf_state{behaviour=Mod, fount=Fount_Slab, reservoir=[#timed_slab{slab=First_Slab} | More_Slabs] = All_Slabs} = State,
    #timed_slab{slab=Fount} = Fount_Slab,
    replace_slabs(Mod, Slabs_Needed, Slab_Size),

    %% Append the slabs and the excess into a single list...
    Full_Slabs_Left = Num_Slabs - Slabs_Needed,
    {{Pids, Remaining_Fount_Pids}, {Slabs_Requested, Remaining_Slabs}, {New_Num_Slabs, New_Fount_Count}}
        = case FC of
              Excess ->
                  replace_slabs(Mod, 1, Slab_Size),
                  {{Fount, []}, lists:split(Slabs_Needed, All_Slabs), {Full_Slabs_Left, 0}};
              Enough when Enough > Excess ->
                  {lists:split(Excess, Fount), lists:split(Slabs_Needed, All_Slabs), {Full_Slabs_Left, FC-Excess}};
              _Not_Enough ->
                  replace_slabs(Mod, 1, Slab_Size),
                  {lists:split(Excess, Fount ++ First_Slab), lists:split(Slabs_Needed, More_Slabs), {Full_Slabs_Left-1, FC+Slab_Size-Excess}}
          end,
    Remaining_Fount = case Remaining_Fount_Pids of
                          []  -> [];
                          RFP -> Fount_Slab#timed_slab{slab=RFP}
                      end,
    Pids_Requested = lists:append([Pids | [S || #timed_slab{slab=S} <- Slabs_Requested]]),
    New_State_Fn = case Remaining_Fount =:= [] andalso Remaining_Slabs =:= [] of true -> 'EMPTY'; false -> 'LOW' end,
    New_State = State#cf_state{fount=Remaining_Fount, fount_count=New_Fount_Count, reservoir=Remaining_Slabs, num_slabs=New_Num_Slabs},
    reply(Pids_Requested, New_State_Fn, New_State);

%%% All the pids wanted, change to the EMPTY state.
get_pids (Num_Pids, _State_Fn, #cf_state{fount_count=FC, slab_size=Slab_Size, num_slabs=Num_Slabs} = State)
  when Num_Pids =:= (Num_Slabs * Slab_Size) + FC ->
    #cf_state{behaviour=Mod, fount=#timed_slab{slab=Fount}, reservoir=Reservoir} = State, 
    replace_slabs(Mod, Num_Slabs + 1, Slab_Size),
    Pids_Requested = lists:append([Fount | [S || #timed_slab{slab=S} <- Reservoir]]),
    reply(Pids_Requested, 'EMPTY', State#cf_state{fount=[], reservoir=[], fount_count=0, num_slabs=0}).


%%% Unlink the reply pids so they can no longer take down the cxy_fount.
reply(Pids, New_State_Fn, New_State_Record) ->
    _ = [unlink(Pid) || Pid <- Pids],
    {reply, Pids, New_State_Fn, New_State_Record}.

replace_slabs(Mod, Num_Slabs, Slab_Size) ->
    Slab_Allocator_Args = [self(), Mod, os:timestamp(), Slab_Size, []],
    done = spawn_link_allocators(Num_Slabs, Slab_Allocator_Args).

replace_slab_then_return_fount(Pids, #cf_state{behaviour=Mod, slab_size=Slab_Size, num_slabs=Num_Slabs} = State) ->
    replace_slabs(Mod, 1, Slab_Size),
    New_State = State#cf_state{fount=#timed_slab{slab=[]}, fount_count=0},
    case Num_Slabs of
        0 -> reply(Pids, 'EMPTY', New_State);
        _ -> reply(Pids, 'LOW',   New_State)
    end.


%%%------------------------------------------------------------------------------
%%% Synchronous state functions (trigger gen_fsm:sync_send_all_state_event/2)
%%%------------------------------------------------------------------------------

-type from()     :: {pid(), reference()}.
-type rate()     :: pos_integer().
-type status()   :: proplists:proplist().

-spec handle_sync_event ({get_spawn_rate_per_slab},      from(), State_Name, State)
         -> {reply, rate(), State_Name,   State} when State_Name :: state_name(), State :: cf_state();
                        ({get_spawn_rate_per_process},   from(), State_Name, State)
         -> {reply, rate(), State_Name,   State} when State_Name :: state_name(), State :: cf_state();
                        ({get_status},                   from(), State_Name, State)
         -> {reply, status(), State_Name, State} when State_Name :: state_name(), State :: cf_state();
                        ({set_notifier_pid, notifier()}, from(), State_Name, State)
         -> {reply, status(), State_Name, State} when State_Name :: state_name(), State :: cf_state().

get_slab_times(Fount, Fount_Time, Num_Slabs, Slab_Times) ->
    case Fount of
        [] -> {Slab_Times,                Num_Slabs};
        _  -> {[Fount_Time | Slab_Times], Num_Slabs+1}
    end.

compute_percentage(    0, _Times) -> 0;
compute_percentage(Denom,  Times) -> (lists:sum(Times) * 100 div Denom) / 100.

handle_sync_event ({get_total_rate_per_slab}, _From, State_Name, State) ->
    #cf_state{fount=#timed_slab{slab=Fount, total_time=Total_Time}, reservoir=Slabs, num_slabs=Num_Slabs} = State,
    Slab_Times = [Slab_Total_Time || #timed_slab{total_time=Slab_Total_Time} <- Slabs],
    {Times, Slab_Count} = get_slab_times(Fount, Total_Time, Num_Slabs, Slab_Times),
    Rate = compute_percentage(Slab_Count, Times),
    {reply, Rate, State_Name, State};

handle_sync_event ({get_total_rate_per_process}, _From, State_Name, State) ->
    #cf_state{fount=#timed_slab{slab=Fount, total_time=Total_Time}, reservoir=Slabs,
              fount_count=FC, num_slabs=Num_Slabs, slab_size=Slab_Size} = State,
    Slab_Times = [Slab_Total_Time || #timed_slab{total_time=Slab_Total_Time} <- Slabs],
    {Times, Slab_Count} = get_slab_times(Fount, Total_Time, Num_Slabs, Slab_Times),
    Num_Processes = FC + (Slab_Count*Slab_Size),
    Rate = compute_percentage(Num_Processes, Times),
    {reply, Rate, State_Name, State};

handle_sync_event ({get_spawn_rate_per_slab}, _From, State_Name, State) ->
    #cf_state{fount=#timed_slab{slab=Fount, spawn_time=Fount_Time}, reservoir=Slabs, num_slabs=Num_Slabs} = State,
    Slab_Times = [Slab_Time || #timed_slab{spawn_time=Slab_Time} <- Slabs],
    {Times, Slab_Count} = get_slab_times(Fount, Fount_Time, Num_Slabs, Slab_Times),
    Rate = compute_percentage(Slab_Count, Times),
    {reply, Rate, State_Name, State};

handle_sync_event ({get_spawn_rate_per_process}, _From, State_Name, State) ->
    #cf_state{fount=#timed_slab{slab=Fount, spawn_time=Fount_Time}, reservoir=Slabs,
              fount_count=FC, num_slabs=Num_Slabs, slab_size=Slab_Size} = State,
    Slab_Times = [Slab_Time || #timed_slab{spawn_time=Slab_Time} <- Slabs],
    {Times, Slab_Count} = get_slab_times(Fount, Fount_Time, Num_Slabs, Slab_Times),
    Num_Processes = FC + (Slab_Count*Slab_Size),
    Rate = compute_percentage(Num_Processes, Times),
    {reply, Rate, State_Name, State};

handle_sync_event ({get_status}, _From, State_Name, State) ->
    {reply, generate_status(State_Name, State), State_Name, State};

%% handle_sync_event ({reinit, New_Fount_Behaviour, New_Slab_Size, New_Reservoir_Depth}, _From, State_Name,
%%                    #cf_state{behaviour=New_Fount_Behaviour, fount={Fount, Time}, reservoir=Slabs, fount_count=FC, slab_size=Old_Slab_Size} = State) ->
%%     {Slab_Count_To_Allocate, New_Fount_Count, New_Fount, New_Time}
%%         = kill_idle_slabs(Old_Slab_Size, Slabs, FC, Fount, New_Reservoir_Depth),

%%     %% Spawn a new allocator for each slab desired...
%%     Slab_Allocator_Args = [self(), New_Fount_Behaviour, os:timestamp(), New_Slab_Size, []],
%%     done = spawn_link_allocators(Slab_Count_To_Allocate, Slab_Allocator_Args),
%%     New_State = #cf_state{
%%                    behaviour   = New_Fount_Behaviour,
%%                    fount       = {New_Fount, New_Time},
%%                    fount_count = New_Fount_Count,
%%                    reservoir   = [],
%%                    slab_size   = New_Slab_Size
%%                   },
%%     {reply, ok, State_Name, New_State};

handle_sync_event ({set_notifier, Pid}, _From, State_Name, #cf_state{} = State)
  when Pid =:= undefined; is_pid(Pid) ->
    {reply, ok, State_Name, State#cf_state{notifier=Pid}};
     
handle_sync_event ({stop}, _From, _State_Name, #cf_state{} = State) -> {stop, normal,   stop_workers(State), State};
handle_sync_event (_Event, _From,  State_Name, #cf_state{} = State) -> {reply, ignored, State_Name,          State}.

stop_workers(State) ->
    #cf_state{fount=#timed_slab{slab=Fount}, reservoir=Raw_Slabs} = State,
    Slabs = [Slab || #timed_slab{slab=Slab} <- Raw_Slabs],
    _ = [ [stop_worker(Pid) || Pid <- Slab] || Slab <- [Fount | Slabs] ],
    ok.

stop_worker(Pid) -> unlink(Pid), exit(Pid, kill).
    

%% kill_idle_slabs(_Slab_Size,    [] = _Slabs,      0,   [], New_Num_Slabs) -> {New_Num_Slabs,         0,   []};
%% kill_idle_slabs(_Slab_Size,    [] = _Slabs,  Count, Pids, New_Num_Slabs) -> {New_Num_Slabs - 1, Count, Pids};
%% kill_idle_slabs( Slab_Size, [H|T] = _Slabs, _Count, Pids, New_Num_Slabs) ->
%%     _ = [ [stop_worker(Pid) || Pid <- Slab] || Slab <- T],
%%     _ = [  stop_worker(Pid) || Pid <- Pids],
%%     {Num_Slabs - 1, Slab_Size, H}.


%%%===================================================================
%%% Unused functions
%%%===================================================================

-spec handle_event (any(), State_Name, State)
        -> {next_state, State_Name, State} when State_Name :: state_name(), State :: cf_state().
-spec handle_info  (any(), State_Name, State)
        -> {next_state, State_Name, State} when State_Name :: state_name(), State :: cf_state().
-spec code_change  (any(), State_Name, State, any())
        -> {next_state, State_Name, State} when State_Name :: state_name(), State :: cf_state().

handle_event (_Event,   State_Name,  State) -> {next_state, State_Name, State}.
handle_info  (_Info,    State_Name,  State) -> {next_state, State_Name, State}.
code_change  (_OldVsn,  State_Name,  State, _Extra) -> {ok, State_Name, State}.

%%% Pre-spawned pids are linked and die when FSM dies.
-spec terminate(atom(), state_name(), cf_state()) -> ok.
terminate(_Reason, _State_Name,  _State) -> ok.
