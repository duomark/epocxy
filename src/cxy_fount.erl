%%%------------------------------------------------------------------------------
%%% @copyright (c) 2015, DuoMark International, Inc.
%%% @author Jay Nelson <jay@duomark.com>
%%% @reference 2015 Development sponsored by TigerText, Inc. [http://tigertext.com/]
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
         %% reinit/1,     reinit/2,        % Reset configuration
         get_pid/1,    get_pids/2,      % Get 1 pid or list of pids
         task_pid/2,                    % Send message to pid
         get_status/1                   % Get status of Fount
        ]).

%%% gen_fsm callbacks
-export([init/1, handle_event/3, handle_sync_event/4,
         handle_info/3, terminate/3, code_change/4]).

%%% Internally spawned functions.
-export([allocate_slab/4]).

%%% state functions
-export(['EMPTY'/2, 'FULL'/2, 'LOW'/2]).
-export(['EMPTY'/3, 'FULL'/3, 'LOW'/3]).

-record(cf_state,
        {
          behaviour        :: module(),
          fount       = [] :: [pid()],
          reservoir   = [] :: [[pid()]],
          fount_count = 0  :: non_neg_integer(),   % Num pids in fount
          num_slabs   = 0  :: non_neg_integer(),   % Num slabs in reservoir
          depth       = 0  :: non_neg_integer(),   % Desired reservoir slabs + 1 for fount
          slab_size        :: pos_integer()        % Num pids in a single slab
        }).
-type cf_state() :: #cf_state{}.

%%% cxy_fount behaviour callbacks
-type fount_ref() :: pid() | atom().  % gen_fsm reference
-export_type([fount_ref/0]).

-callback start_pid (fount_ref())     ->   pid()  | {error, Reason::any()}.
-callback send_msg  (Worker, tuple()) -> [Worker] | {error, Reason::any()} | []
                                             when Worker :: pid().

default_num_slabs()      ->   5.
default_slab_size()      ->  10.
default_notify_timeout() -> 500.


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


%%% Reinitialize the fount configuration parameters
%% -spec reinit(fount_ref(), module())                               -> ok.
%% -spec reinit(fount_ref(), module(), pos_integer(), pos_integer()) -> ok.

%% reinit(Fount, Fount_Behaviour)
%%   when is_pid  (Fount), is_atom (Fount_Behaviour);
%%        is_atom (Fount), is_atom (Fount_Behaviour) ->
%%     reinit(Fount, Fount_Behaviour, default_slab_size(), default_num_slabs()).
%% reinit(Fount, Fount_Behaviour, Slab_Size, Reservoir_Depth)
%%   when is_pid(Fount) orelse is_atom(Fount),
%%        is_atom(Fount_Behaviour),
%%        is_integer(Slab_Size), Slab_Size > 0,
%%        is_integer(Reservoir_Depth), Reservoir_Depth >= 2 ->
%%     gen_fsm:sync_send_all_event(Fount, {reinit, Fount_Behaviour, Slab_Size, Reservoir_Depth}).


-spec task_pid (fount_ref(), tuple()) -> [pid()] | {error, any()}.

task_pid(Fount, Msg) ->
    gen_fsm:sync_send_event(Fount, {task_pid, Msg}, default_notify_timeout()).


-spec get_pid  (fount_ref())                -> [pid()] | {error, any()}.
-spec get_pids (fount_ref(), pos_integer()) -> [pid()] | {error, any()}.

get_pid(Fount) ->
    gen_fsm:sync_send_event(Fount, {get_pids, 1}, default_notify_timeout()).

get_pids(Fount, Num)
  when is_integer(Num), Num >= 0 ->
    gen_fsm:sync_send_event(Fount, {get_pids, Num}, default_notify_timeout()).


-type status_attr() :: {current_state, atom()}              % FSM State function name
                     | {behaviour,     module()}            % Fount behaviour module
                     | {fount_count,   non_neg_integer()}   % Num pids in top slab
                     | {slab_count,    non_neg_integer()}   % Num of full other slabs
                     | {slab_size,     non_neg_integer()}   % Size of a full slab
                     | {max_slabs,     non_neg_integer()}.  % Max number of slabs including fount

-spec get_status (fount_ref()) -> [status_attr(), ...].

get_status(Fount) ->
    gen_fsm:sync_send_all_state_event(Fount, {get_status}).


%%%===================================================================
%%% gen_fsm callbacks
%%%===================================================================

%%%------------------------------------------------------------------------------
%%% Initialize the FSM by spawning Reservoir_Depth slab allocators
%%% and starting in the 'EMPTY' state.
%%%------------------------------------------------------------------------------

-spec init({module(), pos_integer(), pos_integer()}) -> {ok, 'EMPTY', cf_state()}.

init({Fount_Behaviour, Slab_Size, Reservoir_Depth}) ->

    %% Spawn a new allocator for each slab desired...
    Slab_Allocator_Args = [self(), Fount_Behaviour, Slab_Size, []],
    done = spawn_allocators(Reservoir_Depth, Slab_Allocator_Args),

    %% Finish initializing, any newly allocated slabs will appear as events.
    Init_State = #cf_state{
                    behaviour  = Fount_Behaviour,
                    slab_size  = Slab_Size,
                    depth      = Reservoir_Depth
                   },
    {ok, 'EMPTY', Init_State}.

%%% Spawn linked slab allocators without using lists:seq/2.
%%% If the gen_fsm goes down, any in progress allocators should also.
spawn_allocators(             0, _Slab_Args) -> done;
spawn_allocators(Num_Allocators,  Slab_Args) ->
    erlang:spawn_opt(?MODULE, allocate_slab, Slab_Args, [link]),
    spawn_allocators(Num_Allocators-1, Slab_Args).
    
%%% Rely on the client behaviour to create new pids. This means using
%%% spawn or any of the gen_*:start patterns since the pids are unsupervised.
%%% The resulting pids must be linked to the cxy_fount parent so that they are
%%% destroyed if the parent terminates. While idle, the slab allocated pids
%%% should avoid crashing because they can take out the entire cxy_fount.
%%% Once a pid receives a task_pid command, it becomes unlinked and free to
%%% complete its task on its own timeline independently from the fount.
allocate_slab(Parent_Pid, _Module, 0, Slab) ->
    gen_fsm:send_event(Parent_Pid, {slab, Slab});

allocate_slab(Parent_Pid, Module, Num_To_Spawn, Slab)
 when is_pid(Parent_Pid),       is_atom(Module),
      is_integer(Num_To_Spawn), Num_To_Spawn > 0 ->

    %% Module behaviour needs to explicitly link to the parent_pid,
    %% since this function is executing in the caller's process space,
    %% rather than the gen_fsm of the cxy_fount parent_pid process space.
    case Module:start_pid(Parent_Pid) of
        Allocated_Pid when is_pid(Allocated_Pid) ->
            allocate_slab(Parent_Pid, Module, Num_To_Spawn-1, [Allocated_Pid | Slab])
    end.


%%%------------------------------------------------------------------------------
%%% Asynch state functions (triggered by gen_fsm:send_event/2)
%%%------------------------------------------------------------------------------

-spec 'EMPTY' (any(), cf_state()) -> {next_state, 'EMPTY',  cf_state()}.
-spec 'LOW'   (any(), cf_state()) -> {next_state, 'LOW',    cf_state()}.
-spec 'FULL'  (any(), cf_state()) -> {next_state, 'FULL',   cf_state()}
                                   | {stop,       overfull, cf_state()}.

%%% When empty or low, add newly allocated slab of pids...
'EMPTY' ({slab,  Pids}, #cf_state{} = State) -> add_slab(State, Pids);
'EMPTY' (_Event,        #cf_state{} = State) -> {next_state, 'EMPTY',  State}.

'LOW'   ({slab,  Pids}, #cf_state{} = State) -> add_slab(State, Pids);
'LOW'   (_Event,        #cf_state{} = State) -> {next_state, 'LOW',    State}.

%%% When full, we shouldn't receive a request to add more.
'FULL'  ({slab, _Pids}, #cf_state{} = State) -> {stop,       overfull, State};
'FULL'  (_Event,        #cf_state{} = State) -> {next_state, 'FULL',   State}.


%%% Slabs are added to the fount first if reservoir is full...
add_slab(#cf_state{fount=[], depth=Depth, num_slabs=Num_Slabs, slab_size=Slab_Size} = State,
         [_Pid | _More] = Pids) when is_pid(_Pid), Depth =:= Num_Slabs + 1 ->

    %% Goal depth is reservoir depth + 1 for the fount (which just arrived)
    {next_state, 'FULL', State#cf_state{fount=Pids, fount_count=Slab_Size}};

%%% Then to the reservoir of untapped slabs.
add_slab(#cf_state{fount=Fount, reservoir=Slabs, depth=Depth, num_slabs=Num_Slabs, slab_size=Slab_Size} = State,
         [_Pid | _More] = Pids) when is_pid(_Pid) ->

    %% Goal depth includes the fount (even if partial) and the new_slab being received
    %% Crash if the stack of slabs ever exceeds the goal depth.
    case {Fount, Depth > Num_Slabs + 2} of
        {[],     _} -> {next_state, 'LOW',  State#cf_state{fount=Pids, fount_count=Slab_Size}};
        { _,  true} -> {next_state, 'LOW',  State#cf_state{reservoir=[Pids | Slabs], num_slabs=Num_Slabs+1}};
        { _, false} -> {next_state, 'FULL', State#cf_state{reservoir=[Pids | Slabs], num_slabs=Num_Slabs+1}}
    end.
    

%%%------------------------------------------------------------------------------
%%% Synchronous state functions (trigger gen_fsm:sync_send_event/2,3)
%%%------------------------------------------------------------------------------

-type synch_request() :: {task_pid, any()} | {get_pids, pos_integer()}.

-spec 'EMPTY' (synch_request(), {pid(), reference()}, cf_state()) -> {reply,      [],         'EMPTY', cf_state()}.
-spec 'FULL'  (synch_request(), {pid(), reference()}, cf_state()) -> {reply, [pid()], 'LOW' | 'EMPTY', cf_state()}.
-spec 'LOW'   (synch_request(), {pid(), reference()}, cf_state()) -> {reply, [pid()], 'LOW' | 'EMPTY', cf_state()}.

%%% 'EMPTY' state is only exited when a slab of pids is delivered...
'EMPTY' ({task_pid, _Msg}, _From, #cf_state{} = State) -> {reply,      [], 'EMPTY', State};
'EMPTY' ({get_pids, _Num}, _From, #cf_state{} = State) -> {reply,      [], 'EMPTY', State};
'EMPTY' (_Event,           _From, #cf_state{} = State) -> {reply, ignored, 'EMPTY', State}.

%%% 'FULL' state is exited when the fount becomes empty and one more pid is needed...
'FULL'  ({task_pid,  Msg}, _From, #cf_state{} = State) -> task_pid (Msg, State, 'FULL');
'FULL'  ({get_pids,  Num}, _From, #cf_state{} = State) -> get_pids (Num, State, 'FULL');
'FULL'  (_Event,           _From, #cf_state{} = State) -> {reply, ignored, 'FULL', State}.

%%% 'LOW' state is exited when there are no reserve slabs or reserve is full.
'LOW'   ({task_pid,  Msg}, _From, #cf_state{} = State) -> task_pid (Msg, State, 'LOW');
'LOW'   ({get_pids,  Num}, _From, #cf_state{} = State) -> get_pids (Num, State, 'LOW');
'LOW'   (_Event,           _From, #cf_state{} = State) -> {reply, ignored, 'LOW', State}.

task_pid (Msg, #cf_state{fount=[Pid]                                        } = State, _State_Fn) -> replace_slab_then_send_pid(Pid, Msg, State);
task_pid (Msg, #cf_state{fount=[Pid | More], fount_count = FC, behaviour=Mod} = State,  State_Fn) -> {reply, send_msg(Pid, Mod, Msg), State_Fn, State#cf_state{fount=More, fount_count=FC-1}};
task_pid (Msg, #cf_state{fount=[],  num_slabs=Num_Slabs, slab_size=Slab_Size, behaviour=Mod} = State, _State_Fn) ->
    [[Pid | Rest] = _Slab | More_Slabs] = State#cf_state.reservoir,
    {reply, send_msg(Pid, Mod, Msg), 'LOW', State#cf_state{fount=Rest, reservoir=More_Slabs, fount_count=Slab_Size-1, num_slabs=Num_Slabs-1}}.

replace_slabs(Mod, Num_Slabs, Slab_Size) ->
    Slab_Allocator_Args = [self(), Mod, Slab_Size, []],
    done = spawn_allocators(Num_Slabs, Slab_Allocator_Args).

replace_slab_then_send_pid(Pid, Msg, #cf_state{behaviour=Mod, slab_size=Slab_Size, num_slabs=Num_Slabs} = State) ->
    replace_slabs(Mod, 1, Slab_Size),
    New_State_Fn = case Num_Slabs of 0 -> 'EMPTY'; _ -> 'LOW' end,
    {reply, send_msg(Pid, Mod, Msg), New_State_Fn, State#cf_state{fount=[], fount_count=0}}.

send_msg(Pid, Module,  Msg) ->
    try   unlink(Pid), Module:send_msg(Pid, Msg), Pid
    catch Class:Type -> {error, {Module, send_msg, Class, Type, Msg}}
    end.

replace_slab_then_return_pid(Pid, #cf_state{behaviour=Mod, slab_size=Slab_Size, num_slabs=Num_Slabs} = State) ->
    replace_slabs(Mod, 1, Slab_Size),
    New_State_Fn = case Num_Slabs of 0 -> 'EMPTY'; _ -> 'LOW' end,
    {reply, [Pid], New_State_Fn, State#cf_state{fount=[], fount_count=0}}.


%%%------------------------------------------------------------------------------
%%% get_pids responds with the desired worker list of pid()
%%%------------------------------------------------------------------------------

%% 0 Pids wanted...
get_pids (0, #cf_state{} = State, State_Fn) -> {reply, [], State_Fn, State};
 
%% 1 Pid wanted...
get_pids (1, #cf_state{fount=[Pid]                       } = State, _State_Fn) -> replace_slab_then_return_pid(Pid, State);
get_pids (1, #cf_state{fount=[Pid | More], fount_count=FC} = State,  State_Fn) -> {reply, [Pid], State_Fn, State#cf_state{fount=More, fount_count=FC-1}};
get_pids (1, #cf_state{fount=[], num_slabs=Num_Slabs, slab_size=Slab_Size} = State, _State_Fn) ->
    [[Pid | Rest] = _Slab | More_Slabs] = State#cf_state.reservoir,
    {reply, [Pid], 'LOW', State#cf_state{fount=Rest, reservoir=More_Slabs, fount_count=Slab_Size-1, num_slabs=Num_Slabs-1}};

%% More than 1 pid wanted, can be supplied by Fount...
%% (Fount might be greater than slab_size, so this clause comes before slab checks)
get_pids (Num_Pids, #cf_state{fount_count=FC, num_slabs=Num_Slabs} = State, _State_Name)
  when Num_Pids =:= FC ->
    #cf_state{behaviour=Mod, fount=Fount, slab_size=Slab_Size} = State,
    replace_slabs(Mod, 1, Slab_Size),
    case Num_Slabs of
        0 -> {reply, Fount, 'EMPTY', State#cf_state{fount=[], fount_count=0}};
        _ -> {reply, Fount, 'LOW',   State#cf_state{fount=[], fount_count=0}}
    end;

get_pids (Num_Pids, #cf_state{fount_count=FC} = State, State_Name)
  when Num_Pids < FC ->
    Fount_Count = FC - Num_Pids,
    {Pids, Remaining} = lists:split(Num_Pids, State#cf_state.fount),
    {reply, Pids, State_Name, State#cf_state{fount=Remaining, fount_count=Fount_Count}};

%% More than 1 pid wanted, matches Slab_Size, grab the top of the reservoir if it's not empty...
get_pids (Num_Pids, #cf_state{slab_size=Slab_Size, num_slabs=Num_Slabs} = State, _State_Name)
  when Num_Pids =:= Slab_Size, Num_Slabs > 0 ->
    #cf_state{behaviour=Mod, reservoir=[Slab | More_Slabs]} = State,
    replace_slabs(Mod, 1, Slab_Size),
    {reply, Slab, 'LOW', State#cf_state{reservoir=More_Slabs, num_slabs=Num_Slabs-1}};

%% More than 1 pid wanted, less than Slab_Size, grab the front of top slab, add balance to fount...
get_pids (Num_Pids, #cf_state{slab_size=Slab_Size, num_slabs=Num_Slabs} = State, _State_Name)
  when Num_Pids < Slab_Size, Num_Slabs > 0 ->
    #cf_state{behaviour=Mod, fount=Fount, fount_count=FC, reservoir=[Slab | More_Slabs]} = State,
    replace_slabs(Mod, 1, Slab_Size),
    {Pids, Remaining} = lists:split(Num_Pids, Slab),
    Partial_Slab_Size = Slab_Size - Num_Pids,
    Fount_Count = FC + Partial_Slab_Size,

    %% Try to be efficient about reconstructing Fount (may end up larger than a slab)...
    New_Fount = case Partial_Slab_Size > FC of
                    true  -> Fount ++ Remaining;
                    false -> Remaining ++ Fount
                end,
    {reply, Pids, 'LOW', State#cf_state{fount=New_Fount, fount_count=Fount_Count, reservoir=More_Slabs, num_slabs=Num_Slabs-1}};

%% More than 1 Pid wanted, but not enough available...
get_pids (Num_Pids, #cf_state{fount_count=FC, slab_size=Slab_Size, num_slabs=Num_Slabs} = State, State_Name)
  when Num_Pids > (Num_Slabs * Slab_Size) + FC ->
    {reply, [], State_Name, State};

%% More than 1 pid wanted, more than Slab_Size, see if there are enough to return...
get_pids (Num_Pids, #cf_state{fount_count=FC, slab_size=Slab_Size, num_slabs=Num_Slabs} = State, _State_Name)
  when Num_Pids > Slab_Size, Num_Pids < (Num_Slabs * Slab_Size) + FC -> 
    Excess       = Num_Pids rem Slab_Size,
    Slabs_Needed = (Num_Pids - Excess) div Slab_Size,
    #cf_state{behaviour=Mod, fount=Fount, reservoir=[First_Slab | More_Slabs] = All_Slabs} = State,
    replace_slabs(Mod, Slabs_Needed, Slab_Size),

    %% Append the slabs and the excess into a single list...
    {{Pids, Remaining_Fount}, {Slabs_Requested, Remaining_Slabs}, {New_Fount_Count, New_Num_Slabs}}
        = case FC >= Excess of
              true  -> {lists:split(Excess, Fount),               lists:split(Slabs_Needed, All_Slabs),  {FC - Excess,             Num_Slabs - Slabs_Needed}};
              false -> {lists:split(Excess, First_Slab ++ Fount), lists:split(Slabs_Needed, More_Slabs), {FC + Slab_Size - Excess, Num_Slabs - Slabs_Needed - 1}}
          end,
    Pids_Requested = lists:append([Pids | Slabs_Requested]),

    %% We might need one more slab replacement...
    New_Fount_Count =/= 0
        orelse replace_slabs(Mod, 1, Slab_Size),
    {reply, Pids_Requested, 'LOW', State#cf_state{fount=Remaining_Fount, fount_count=New_Fount_Count, reservoir=Remaining_Slabs, num_slabs=New_Num_Slabs}};

%% All the pids wanted, change to the EMPTY state.
get_pids (Num_Pids, #cf_state{fount_count=FC, slab_size=Slab_Size, num_slabs=Num_Slabs} = State, _State_Name)
  when Num_Pids =:= (Num_Slabs * Slab_Size) + FC ->
    #cf_state{behaviour=Mod, fount=Fount, reservoir=Reservoir} = State, 
    replace_slabs(Mod, Num_Slabs + 1, Slab_Size),
    {reply, lists:append([Fount | Reservoir]), 'EMPTY', State#cf_state{fount=[], reservoir=[], fount_count=0, num_slabs=0}}.


%%%------------------------------------------------------------------------------
%%% Synchronous state functions (trigger gen_fsm:sync_send_all_state_event/2)
%%%------------------------------------------------------------------------------

handle_sync_event ({get_status}, _From, State_Name, State) ->

    #cf_state{depth=Depth, fount_count=FC, num_slabs=Num_Slabs, slab_size=Slab_Size} = State,
    Max_Pid_Count     = Depth * Slab_Size,
    Current_Pid_Count = FC + (Num_Slabs * Slab_Size),

    Status = [
              {current_state, State_Name},
              {fount_count,   FC},
              {max_slabs,     Depth},
              {slab_size,     Slab_Size},
              {slab_count,    Num_Slabs},
              {max_pids,      Max_Pid_Count},
              {pid_count,     Current_Pid_Count},
              {behaviour,     State#cf_state.behaviour}
             ],

    {reply, Status, State_Name, State};

%% handle_sync_event ({reinit, New_Fount_Behaviour, New_Slab_Size, New_Reservoir_Depth}, _From, State_Name,
%%                    #cf_state{behaviour=New_Fount_Behaviour, fount=Fount, reservoir=Slabs, fount_count=FC, slab_size=Old_Slab_Size} = State) ->
%%     {Slab_Count_To_Allocate, New_Fount_Count, New_Fount}
%%         = kill_idle_slabs(Old_Slab_Size, Slabs, FC, Fount, New_Reservoir_Depth),

%%     %% Spawn a new allocator for each slab desired...
%%     Slab_Allocator_Args = [self(), New_Fount_Behaviour, New_Slab_Size, []],
%%     done = spawn_allocators(Slab_Count_To_Allocate, Slab_Allocator_Args),
%%     New_State = #cf_state{
%%                    behaviour   = New_Fount_Behaviour,
%%                    fount       = New_Fount,
%%                    fount_count = New_Fount_Count,
%%                    reservoir   = [],
%%                    slab_size   = New_Slab_Size
%%                   },
%%     {reply, ok, State_Name, New_State};
     
handle_sync_event (_Event,       _From, State_Name, #cf_state{} = State) ->
    {reply, ignored, State_Name, State}.


%% kill_idle_slabs(_Slab_Size,    [] = _Slabs,      0,   [], New_Num_Slabs) -> {New_Num_Slabs,         0,   []};
%% kill_idle_slabs(_Slab_Size,    [] = _Slabs,  Count, Pids, New_Num_Slabs) -> {New_Num_Slabs - 1, Count, Pids};
%% kill_idle_slabs( Slab_Size, [H|T] = _Slabs, _Count, Pids, New_Num_Slabs) ->
%%     _ = [ [catch(exit(kill, Pid)) || Pid <- Slab] || Slab <- T],
%%     _ = [  catch(exit(kill, Pid)) || Pid <- Pids],
%%     {Num_Slabs - 1, Slab_Size, H}.


%%%===================================================================
%%% Unused functions
%%%===================================================================

handle_event (_Event,   State_Name,  State) -> {next_state, State_Name, State}.
handle_info  (_Info,    State_Name,  State) -> {next_state, State_Name, State}.
code_change  (_OldVsn,  State_Name,  State, _Extra) -> {ok, State_Name, State}.

terminate    (_Reason, _State_Name, _State) -> ok. % All pids should be linked and die when FSM dies.

