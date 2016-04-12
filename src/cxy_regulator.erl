%%%------------------------------------------------------------------------------
%%% @copyright (c) 2016, DuoMark International, Inc.
%%% @author Jay Nelson <jay@duomark.com>
%%% @reference 2016 Development sponsored by TigerText, Inc. [http://tigertext.com/]
%%% @reference The license is based on the template for Modified BSD from
%%%   <a href="http://opensource.org/licenses/BSD-3-Clause">OSI</a>
%%% @doc
%%%   A cxy_regulator is used in conjunction with cxy_fount. The fount
%%%   requests slabs of newly spawned processes, whilst the regulator
%%%   controls the rate at which the processes are generated. Throttling
%%%   the rate ensures that the amount of work can be constrained to
%%%   avoid overloading a VM node.
%%%
%%%   The regulator is implemented as a gen_fsm so that it can be
%%%   paused and resumed for maintenance purposes.
%%% @since 1.1.0
%%% @end
%%%------------------------------------------------------------------------------
-module(cxy_regulator).
-author('Jay Nelson <jay@duomark.com>').

-behaviour(gen_fsm).


%%% API
-export([start_link/0, pause/1, resume/1, status/1]).
-export([allow_spawn/2]).

%%% gen_fsm callbacks
-export([init/1, format_status/2, handle_sync_event/4,
         handle_event/3, handle_info/3, code_change/4, terminate/3]).

%%% state functions
-export(['NORMAL'/2, 'OVERMAX'/2, 'PAUSED'/2]).
-export(['NORMAL'/3, 'OVERMAX'/3, 'PAUSED'/3]).

-type state_name() :: 'NORMAL' | 'OVERMAX' | 'PAUSED'.

-type regulator_ref() :: pid().
-export_type([regulator_ref/0]).

-type thruput()               :: normal | overmax.
-type allocate_slab_args()    :: {pid(), module(), tuple(), erlang:timestamp(), pos_integer()}.
-type allocate_slab_request() :: {allocate_slab, allocate_slab_args()}.

-define(NUM_SLOTS, 10).
-record(epoch_slab_counts, {
          epoch = 0                                             :: non_neg_integer(),
          slots = list_to_tuple(lists:duplicate(?NUM_SLOTS, 0)) :: tuple()
         }).
-type epoch_slab_counts() :: #epoch_slab_counts{}.

-record(cr_state, {
          init_time        = os:timestamp()        :: erlang:timestamp(),
          thruput          = normal                :: thruput(),
          slab_counts      = #epoch_slab_counts{}  :: epoch_slab_counts(),
          pending_requests = queue:new()           :: queue:queue()
         }).
-type cr_state() :: #cr_state{}.


%%%===================================================================
%%% API
%%%===================================================================

-spec start_link() -> {ok, regulator_ref()}.

start_link() ->
    gen_fsm:start_link(?MODULE, {}, []).

-type status_attr() :: {current_state, atom()}   % FSM State function name
                     | {thruput,    thruput()}.  % Paused thruput state

-spec pause  (regulator_ref()) -> paused.
-spec resume (regulator_ref()) -> thruput().
-spec status (regulator_ref()) -> [status_attr(), ...].

pause  (Regulator) -> gen_fsm:sync_send_event(Regulator, pause).
resume (Regulator) -> gen_fsm:sync_send_event(Regulator, resume).

status (Regulator) -> gen_fsm:sync_send_all_state_event(Regulator, status).


%%%===================================================================
%%% gen_fsm callbacks
%%%===================================================================

-spec init({}) -> {ok, 'NORMAL', cr_state()}.
-spec format_status(normal | terminate, list()) -> proplists:proplist().

init({}) ->
    {ok, 'NORMAL', #cr_state{}}.

format_status(_Reason, [_Dict, State]) ->
    generate_status(State).

generate_status(State_Name, State) ->
    [{current_state, State_Name} | generate_status(State)].

generate_status(#cr_state{thruput=Thruput}) ->
    [{thruput, Thruput}].


%%%------------------------------------------------------------------------------
%%% Spawn pace regulation logic
%%%   Slots per second slices the spawning to timing buckets.
%%%   Currently allowing 1/10th of a second per slot, with
%%%   a single slab generation allowed per time slot.
%%%------------------------------------------------------------------------------
micros_per_slot()       -> (timer:seconds(1) * 1000) div ?NUM_SLOTS.
overload_pause_millis() -> (micros_per_slot() div 2) div 1000.
     
    
time_slot(Start_Time) ->
    Micros_Since_Start = timer:now_diff(os:timestamp(), Start_Time),
    Raw_Epoch = (Micros_Since_Start div micros_per_slot()),
    Epoch =  Raw_Epoch div ?NUM_SLOTS  + 1,   % don't allow 0
    Slot  = (Raw_Epoch rem ?NUM_SLOTS) + 1,   % tuples number 1-N
    {Epoch, Slot}.

allow_slab_generation(Slot, #epoch_slab_counts{slots=Slot_Stats} = ESC)
  when is_tuple(Slot_Stats), tuple_size(Slot_Stats) =:= ?NUM_SLOTS,
       is_integer(Slot), Slot > 0, Slot =< ?NUM_SLOTS ->
    case element(Slot, Slot_Stats) of
        1 -> {false, ESC};                                    % Disallow
        0 -> New_Slots = setelement(Slot, Slot_Stats, 1),     % Mark slot
             {true,  ESC#epoch_slab_counts{slots=New_Slots}}  % Allow and mark
    end.

get_epoch_slots(#epoch_slab_counts{epoch=Slab_Epoch} = ESC, Slab_Epoch) -> ESC;
get_epoch_slots(_Old_Epoch_Counts, Current_Epoch) ->
    #epoch_slab_counts{epoch=Current_Epoch}.

allow_spawn(Server_Start_Time, #epoch_slab_counts{} = ESC) ->
    {Epoch, Slot}  = time_slot(Server_Start_Time),
    New_ESC = get_epoch_slots(ESC, Epoch),
    allow_slab_generation(Slot, New_ESC).


%%%------------------------------------------------------------------------------
%%% Asynch state functions (triggered by gen_fsm:send_event/2)
%%%------------------------------------------------------------------------------

-spec 'NORMAL'  (allocate_slab_request(), cr_state()) -> {next_state, 'NORMAL',  cr_state()}.
-spec 'OVERMAX' (allocate_slab_request(), cr_state()) -> {next_state, 'OVERMAX', cr_state()}.
-spec 'PAUSED'  (allocate_slab_request(), cr_state()) -> {next_state, 'PAUSED',  cr_state()}.

%%% Let allocate_slab requests through immediately when 'NORMAL'.
'NORMAL'  ({allocate_slab, {Fount, Module, Mod_State, Timestamp, Slab_Size}} = Req,
           #cr_state{init_time=Init_Time, slab_counts=Slab_Counts} = State) ->
    case allow_spawn(Init_Time, Slab_Counts) of
        {false, New_Slab_Counts} ->
            pace_next_slab(),
            queue_request(Req, 'OVERMAX', State#cr_state{slab_counts=New_Slab_Counts});
        {true,  New_Slab_Counts} ->
            allocate_slab(Fount, Module, Mod_State, Timestamp, Slab_Size, []),
            {next_state, 'NORMAL', State#cr_state{slab_counts=New_Slab_Counts}}
    end;
'NORMAL'  (queued_request, #cr_state{} = State) -> pop_pending ('NORMAL', State);
'NORMAL'  (_Event,         #cr_state{} = State) -> {next_state, 'NORMAL', State}.

%%% Queue up requests if 'OVERMAX' or 'PAUSED'.
'OVERMAX' (queued_request,                #cr_state{} = State) -> pop_pending        ('NORMAL',  State);
'OVERMAX' ({allocate_slab, _Args} = Req,  #cr_state{} = State) -> queue_request (Req, 'OVERMAX', State);
'OVERMAX' (_Event,                        #cr_state{} = State) -> {next_state,        'OVERMAX', State}.

'PAUSED'  ({allocate_slab, _Args} = Req,  #cr_state{} = State) -> queue_request (Req, 'PAUSED',  State);
'PAUSED'  (queued_request,                #cr_state{} = State) -> {next_state,        'PAUSED',  State};
'PAUSED'  (_Event,                        #cr_state{} = State) -> {next_state,        'PAUSED',  State}.

queue_request(Slab_Request, Next_State_Name, #cr_state{pending_requests=PR} = State) ->
    New_Pending = queue:in({os:timestamp(), Slab_Request}, PR),
    {next_state, Next_State_Name, State#cr_state{pending_requests=New_Pending}}.

pop_pending('NORMAL', #cr_state{pending_requests=PR} = State) ->
    case queue:out(PR) of
        %% Nothing queued, just change state.
        {empty,                            _} -> {next_state, 'NORMAL', State};
        {{value, {_Timestamp, Request}}, PR2} ->
            pace_next_slab(),
            'NORMAL' (Request, State#cr_state{pending_requests=PR2})
    end.

pace_next_slab()           -> pace_next_slab(overload_pause_millis()).
pace_next_slab(Pause_Time) ->
    timer:apply_after(Pause_Time, gen_fsm, send_event, [self(), queued_request]).


%%% Rely on the client behaviour to create new pids. This means using
%%% spawn or any of the gen_*:start patterns since the pids are unsupervised.
%%% The resulting pids must be linked to the cxy_fount parent so that they are
%%% destroyed if the parent terminates. While idle, the slab allocated pids
%%% should avoid crashing because they can take out the entire cxy_fount.
%%% Once a pid receives a task_pid command, it becomes unlinked and free to
%%% complete its task on its own timeline, independently from the fount.
allocate_slab(Fount_Pid, _Module, _Mod_State, Start_Time, 0, Slab) ->
    Elapsed_Time = timer:now_diff(os:timestamp(), Start_Time),
    gen_fsm:send_event(Fount_Pid, {slab, Slab, Start_Time, Elapsed_Time});

allocate_slab(Fount_Pid, Module, Mod_State, Start_Time, Num_To_Spawn, Slab)
 when is_pid(Fount_Pid), is_atom(Module), is_integer(Num_To_Spawn), Num_To_Spawn > 0 ->

    %% Module behaviour needs to explicitly link to the parent_pid,
    %% since this function is executing in the caller's process space,
    %% rather than the gen_fsm of the cxy_fount parent_pid process space.
    case Module:start_pid(Fount_Pid, Mod_State) of
        Allocated_Pid when is_pid(Allocated_Pid) ->
            allocate_slab(Fount_Pid, Module, Mod_State, Start_Time, Num_To_Spawn-1, [Allocated_Pid | Slab])
    end.


%%%------------------------------------------------------------------------------
%%% Synchronous state functions (triggered by gen_fsm:sync_send_event/2,3)
%%%------------------------------------------------------------------------------

-type synch_request() :: pause | resume | status.

-spec 'NORMAL'  (synch_request(), {pid(), reference()}, cr_state()) -> {reply, [], 'NORMAL',  cr_state()}.
-spec 'OVERMAX' (synch_request(), {pid(), reference()}, cr_state()) -> {reply, [], 'OVERMAX', cr_state()}.
-spec 'PAUSED'  (synch_request(), {pid(), reference()}, cr_state()) -> {reply, [], 'PAUSED',  cr_state()}.

%%% 'NORMAL' means no throttling is occurring
'NORMAL' (pause, _From,  #cr_state{} = State) -> {reply, paused,           'PAUSED',  State};
'NORMAL' (Event, _From,  #cr_state{} = State) -> {reply, {ignored, Event}, 'NORMAL',  State}.

%%% 'OVERMAX' means spawning is stopped by the regulator
'OVERMAX' (pause, _From, #cr_state{} = State) -> {reply, paused,           'PAUSED',  State};
'OVERMAX' (Event, _From, #cr_state{} = State) -> {reply, {ignored, Event}, 'OVERMAX', State}.

%%% 'PAUSED' means manually stopped, will resume either 'NORMAL' or 'OVERMAX'
'PAUSED' (resume, _From, #cr_state{thruput=normal}  = State) -> pace_next_slab(0), {reply, {resumed, normal},  'NORMAL',  State};
'PAUSED' (resume, _From, #cr_state{thruput=overmax} = State) -> pace_next_slab(0), {reply, {resumed, overmax}, 'OVERMAX', State};
'PAUSED' (Event,  _From, #cr_state{}                = State) ->                    {reply, {ignored, Event},   'PAUSED',  State}.


%%%------------------------------------------------------------------------------
%%% Synchronous state functions (trigger gen_fsm:sync_send_all_state_event/2)
%%%------------------------------------------------------------------------------

-type from()     :: {pid(), reference()}.
-type rate()     :: pos_integer().
-type status()   :: proplists:proplist().

-spec handle_sync_event (status, from(), State_Name, State)
                        -> {reply, status(), State_Name, State}
                               when State_Name :: state_name(), State :: cr_state().

handle_sync_event (status, _From, State_Name, #cr_state{} = State) ->
    {reply, generate_status(State_Name, State), State_Name, State};
handle_sync_event (Event,  _From, State_Name, #cr_state{} = State) ->
    {reply, {ignored, Event}, State_Name, State}.
    

%%%===================================================================
%%% Unused functions
%%%===================================================================

-spec handle_event (any(), State_Name, State)
        -> {next_state, State_Name, State} when State_Name :: state_name(), State :: cr_state().
-spec handle_info  (any(), State_Name, State)
        -> {next_state, State_Name, State} when State_Name :: state_name(), State :: cr_state().
-spec code_change  (any(), State_Name, State, any())
        -> {ok,         State_Name, State} when State_Name :: state_name(), State :: cr_state().

handle_event (_Event,   State_Name,  State) -> {next_state, State_Name, State}.
handle_info  (_Info,    State_Name,  State) -> {next_state, State_Name, State}.
code_change  (_OldVsn,  State_Name,  State, _Extra) -> {ok, State_Name, State}.

%%% Pre-spawned pids are linked and die when FSM dies.
-spec terminate(atom(), state_name(), cr_state()) -> ok.
terminate(_Reason, _State_Name,  _State) -> ok.
