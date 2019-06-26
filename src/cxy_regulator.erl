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
%%%   The regulator is implemented as a gen_statem so that it can be
%%%   paused and resumed for maintenance purposes.
%%% @since 1.1.0
%%% @end
%%%------------------------------------------------------------------------------
-module(cxy_regulator).
-author('Jay Nelson <jay@duomark.com>').

-behaviour(gen_statem).


%%% API
-export([start_link/0, start_link/1, pause/1, resume/1, status/1]).
-export([allow_spawn/2]).

%% gen_statem callbacks
-export([init/1, callback_mode/0, terminate/3, code_change/4, format_status/2]).

%%% state functions
-export(['NORMAL'/3, 'OVERMAX'/3, 'PAUSED'/3]).

-type state_name() :: 'NORMAL' | 'OVERMAX' | 'PAUSED'.

-type regulator_ref() :: pid().
-export_type([regulator_ref/0]).

-type thruput()               :: normal | overmax.
-type allocate_slab_args()    :: {pid(), module(), tuple(), erlang:timestamp(), pos_integer()}.
-type allocate_slab_request() :: {allocate_slab, allocate_slab_args()}.

-record(epoch_slab_counts, {
          epoch = 0  :: non_neg_integer(),
          slots = {} :: tuple()
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

-spec start_link()                     -> {ok, regulator_ref()}.
-spec start_link(proplists:proplist()) -> {ok, regulator_ref()}.

start_link()       -> gen_statem:start_link(?MODULE,     {[]}, []).
start_link(Config) -> gen_statem:start_link(?MODULE, {Config}, []).


-type status_attr() :: {current_state, atom()}   % FSM State function name
                     | {thruput,    thruput()}.  % Paused thruput state

-spec pause  (regulator_ref()) -> paused.
-spec resume (regulator_ref()) -> thruput().
-spec status (regulator_ref()) -> [status_attr(), ...].

pause  (Regulator) -> gen_statem:call(Regulator, pause).
resume (Regulator) -> gen_statem:call(Regulator, resume).

status (Regulator) ->
  {status, _, _, [_Pdict, _State, _Parent, _Dbg, Status]} = sys:get_status(Regulator),
  Status.

%%%===================================================================
%%% gen_statem callbacks
%%%===================================================================

-spec init({proplists:proplist()}) -> {ok, 'NORMAL', cr_state()}.
-spec format_status(normal | terminate, list()) -> proplists:proplist().

default_num_slots ()  -> 100.
make_slot_stats   (N) -> list_to_tuple(lists:duplicate(N, 0)).

init({Config}) ->
    Num_Slots   = proplists:get_value(time_slice, Config, default_num_slots()),
    Slab_Counts = #epoch_slab_counts{slots=make_slot_stats(Num_Slots)},
    {ok, 'NORMAL', #cr_state{slab_counts=Slab_Counts}}.

format_status(_Reason, [_Dict, State_Name, State]) ->
    generate_status(State_Name, State).

generate_status(State_Name, State) ->
    [{current_state, State_Name} | generate_status(State)].

generate_status(#cr_state{init_time=Started, thruput=Thruput,
                          slab_counts=SC,    pending_requests=PR}) ->
    [
     {init_time,        Started},
     {thruput,          Thruput},
     {slab_counts,      SC},
     {pending_requests, queue:len(PR)}
    ].


%%%------------------------------------------------------------------------------
%%% Spawn pace regulation logic
%%%   Slots per second slices the spawning to timing buckets.
%%%   Default is 100 slots per second timing, with one slab allowed per slot.
%%%   Config 'time_slice' property on init changes from 100 to any 1 to N value.
%%%------------------------------------------------------------------------------
millis_per_micro()           -> 1000.
micros_per_slot(Slots)       -> (timer:seconds(1) * millis_per_micro()) div Slots.
overload_pause_millis(Slots) -> (micros_per_slot(Slots) div 2) div millis_per_micro().
     
time_slot(Start_Time, Num_Slots) ->
    Micros_Since_Start = timer:now_diff(os:timestamp(), Start_Time),
    Raw_Epoch = (Micros_Since_Start div micros_per_slot(Num_Slots)),
    Epoch     =  Raw_Epoch div Num_Slots  + 1,   % don't allow 0
    Slot      = (Raw_Epoch rem Num_Slots) + 1,   % tuples number 1-N
    {Epoch, Slot}.

allow_slab_generation(Slot, #epoch_slab_counts{slots=Slot_Stats} = ESC)
  when is_tuple(Slot_Stats),
       is_integer(Slot), Slot > 0, Slot =< tuple_size(Slot_Stats) ->
    case element(Slot, Slot_Stats) of
        1 -> {false, ESC};                                    % Disallow
        0 -> New_Slots = setelement(Slot, Slot_Stats, 1),     % Mark slot
             {true,  ESC#epoch_slab_counts{slots=New_Slots}}  % Allow and mark
    end.

get_epoch_slots(#epoch_slab_counts{epoch=Slab_Epoch} = ESC, Slab_Epoch) -> ESC;
get_epoch_slots(Old_Epoch_Counts, Current_Epoch) ->
    Num_Slots = tuple_size(Old_Epoch_Counts#epoch_slab_counts.slots),
    #epoch_slab_counts{epoch=Current_Epoch, slots=make_slot_stats(Num_Slots)}.

allow_spawn(Server_Start_Time, #epoch_slab_counts{slots=Slot_Stats} = ESC) ->
    Num_Slots      = tuple_size(Slot_Stats),
    {Epoch, Slot}  = time_slot(Server_Start_Time, Num_Slots),
    New_ESC        = get_epoch_slots(ESC, Epoch),
    allow_slab_generation(Slot, New_ESC).


%%%------------------------------------------------------------------------------
%%% Asynch state functions (triggered by gen_statem:cast/call/2)
%%%------------------------------------------------------------------------------

-spec 'NORMAL'  ({call, term()} | cast, allocate_slab_request(), cr_state()) -> term().
-spec 'OVERMAX' ({call, term()} | cast, allocate_slab_request(), cr_state()) -> term().
-spec 'PAUSED'  ({call, term()} | cast, allocate_slab_request(), cr_state()) -> term().

%%% 'NORMAL' means no throttling is occurring
'NORMAL' ({call, From}, pause, State) -> {next_state, 'PAUSED', State, [{reply, From, paused}]};
'NORMAL' ({call, From}, Event, State) -> handle_event({call, From}, Event, State);

'NORMAL'(cast, {allocate_slab, Args}, State) ->
  allocate({allocate_slab, Args}, State);

'NORMAL'(cast, queued_request, State) ->
  pop_pending(normal, State);

'NORMAL'(cast, _Unknown, _State) ->
  keep_state_and_data.

%%% 'OVERMAX' means spawning is stopped by the regulator
'OVERMAX' ({call, From}, pause, State) -> {next_state, 'PAUSED', State, [{reply, From, paused}]};
'OVERMAX' ({call, From}, Event, State) -> handle_event({call, From}, Event, State);

%%% Queue up requests if 'OVERMAX' or 'PAUSED'.
'OVERMAX'(cast, {allocate_slab, Args}, State) ->
  queue_request({allocate_slab, Args}, 'OVERMAX', State);

'OVERMAX'(cast, queued_request, State) ->
  pop_pending(normal, State);

'OVERMAX'(cast, _Unknown, _State) ->
  keep_state_and_data.

%%% 'PAUSED' means manually stopped, will resume either 'NORMAL' or 'OVERMAX'
'PAUSED' ({call, From}, resume, #cr_state{thruput=normal}  = State) ->
  _ = pace_next_slab(0),
  {next_state, 'NORMAL', State, [{reply, From, {resumed, normal}}]};

'PAUSED' ({call, From}, resume, #cr_state{thruput=overmax} = State) ->
  _ = pace_next_slab(0),
  {next_state, 'OVERMAX', State, [{reply, From, {resumed, overmax}}]};

'PAUSED' ({call, From}, Event, State) ->
  handle_event({call, From}, Event, State);

%%% Paused swallows queued_request events silently, 'resume' required to restart events.
%%% Slab requests are queued up even if the queue is currently empty.
'PAUSED'(cast, {allocate_slab, Args}, State) ->
  queue_request({allocate_slab, Args}, 'PAUSED', State);

'PAUSED'(cast, queued_request, _State) ->
  keep_state_and_data;

'PAUSED'(cast, _Unknown, _State) ->
  keep_state_and_data.

%%%------------------------------------------------------------------------------
%%% Asynch internal support functions
%%%------------------------------------------------------------------------------
allocate({allocate_slab, {Fount, Module, Mod_State, Timestamp, Slab_Size}} = Request,
         #cr_state{init_time=Init_Time, slab_counts=Slab_Counts} = State) ->
    case allow_spawn(Init_Time, Slab_Counts) of
        {false, New_Slab_Counts} ->
            Num_Slots = tuple_size(Slab_Counts#epoch_slab_counts.slots),
            _ = pace_slab(Num_Slots),
            queue_request(Request, 'OVERMAX', State#cr_state{slab_counts=New_Slab_Counts});
        {true,  New_Slab_Counts} ->
            allocate_slab(Fount, Module, Mod_State, Timestamp, Slab_Size, []),
            {next_state, 'NORMAL', State#cr_state{slab_counts=New_Slab_Counts}}
    end.

queue_request(Slab_Request, Next_State_Name, #cr_state{pending_requests=PR} = State) ->
    New_Pending = queue:in({os:timestamp(), Slab_Request}, PR),
    New_State   = case Next_State_Name of
                      'OVERMAX' -> State#cr_state{pending_requests=New_Pending, thruput=overmax};
                      'PAUSED'  -> State#cr_state{pending_requests=New_Pending}
                  end,
    {next_state, Next_State_Name, New_State}.

pop_pending(normal, #cr_state{pending_requests=PR, slab_counts=Slab_Counts} = State) ->
    Num_Slots = tuple_size(Slab_Counts#epoch_slab_counts.slots),
    New_State = State#cr_state{thruput=normal},
    case queue:out(PR) of
        %% Nothing queued, just change state.
        {empty,                            _} -> {next_state, 'NORMAL', New_State};
        %% Something queued, handle it.
        {{value, {_Timestamp, Request}}, PR2} ->
            _ = pace_slab(Num_Slots),
            'NORMAL'(cast, Request, New_State#cr_state{pending_requests=PR2})
    end.

pace_slab(Num_Slots) -> pace_next_slab(overload_pause_millis(Num_Slots)).

pace_next_slab(           0) -> gen_statem:cast(self(), queued_request);
pace_next_slab(Pause_Millis) ->
    timer:apply_after(Pause_Millis, gen_statem, cast, [self(), queued_request]).


%%% Rely on the client behaviour to create new pids. This means using
%%% spawn or any of the gen_*:start patterns since the pids are unsupervised.
%%% The resulting pids must be linked to the cxy_fount parent so that they are
%%% destroyed if the parent terminates. While idle, the slab allocated pids
%%% should avoid crashing because they can take out the entire cxy_fount.
%%% Once a pid receives a task_pid command, it becomes unlinked and free to
%%% complete its task on its own timeline, independently from the fount.
allocate_slab(Fount_Pid, _Module, _Mod_State, Start_Time, 0, Slab) ->
    Elapsed_Time = timer:now_diff(os:timestamp(), Start_Time),
    gen_statem:cast(Fount_Pid, {slab, Slab, Start_Time, Elapsed_Time});

allocate_slab(Fount_Pid, Module, Mod_State, Start_Time, Num_To_Spawn, Slab)
 when is_pid(Fount_Pid), is_atom(Module), is_integer(Num_To_Spawn), Num_To_Spawn > 0 ->

    %% Module behaviour needs to explicitly link to the parent_pid,
    %% since this function is executing in the caller's process space,
    %% rather than the gen_statem of the cxy_fount parent_pid process space.
    case Module:start_pid(Fount_Pid, Mod_State) of
        Allocated_Pid when is_pid(Allocated_Pid) ->
            allocate_slab(Fount_Pid, Module, Mod_State, Start_Time, Num_To_Spawn-1, [Allocated_Pid | Slab])
    end.

-type from()     :: {pid(), reference()}.
-type status()   :: proplists:proplist().

-spec handle_event (status, from(), State)
                        -> {reply, status(), State_Name, State}
                               when State_Name :: state_name(), State :: cr_state().

handle_event({call, From}, Event, _State) ->
  {keep_state_and_data, [{reply, From, {ignored, Event}}]}.

%%%===================================================================
%%% Unused functions
%%%===================================================================

-spec code_change  (any(), State_Name, State, any())
        -> {ok,         State_Name, State} when State_Name :: state_name(), State :: cr_state().
code_change  (_OldVsn,  State_Name,  State, _Extra) -> {ok, State_Name, State}.

%%% Pre-spawned pids are linked and die when FSM dies.
-spec terminate(atom(), state_name(), cr_state()) -> ok.
terminate(_Reason, _State_Name,  _State) -> ok.

-spec callback_mode() -> atom().
callback_mode() ->
  state_functions.
