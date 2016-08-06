%%%------------------------------------------------------------------------------
%%% @copyright (c) 2015-2016, DuoMark International, Inc.
%%% @author Jay Nelson <jay@duomark.com>
%%% @reference 2015-2016 Development sponsored by TigerText, Inc. [http://tigertext.com/]
%%% @reference The license is based on the template for Modified BSD from
%%%   <a href="http://opensource.org/licenses/BSD-3-Clause">OSI</a>
%%% @doc
%%%   Validation of cxy_regulator using common test and PropEr.
%%%
%%% @since 0.9.9
%%% @end
%%%------------------------------------------------------------------------------
-module(cxy_regulator_SUITE).
-author('Jay Nelson <jay@duomark.com>').
-vsn('').

%%% Common_test exports
-export([all/0,               groups/0,
         init_per_suite/1,    end_per_suite/1,
         init_per_group/1,    end_per_group/1,
         init_per_testcase/2, end_per_testcase/2
        ]).

%%% Test case exports
-export([
         check_construction/1,      check_pause_resume/1,
         check_add_slabs/1
        ]).

-include("epocxy_common_test.hrl").


%%%===================================================================
%%% Test cases
%%%===================================================================

-type test_case()  :: atom().
-type test_group() :: atom().

-spec all() -> [test_case() | {group, test_group()}].
all() -> [
          %% Uncomment to test failing user-supplied module.
          %% {group, check_behaviour} % Ensure behaviour crashes properly.

          {group, check_create},    % Verify construction and pause/resume
          {group, check_slabs}
         ].

-spec groups() -> [{test_group(), [sequence], [test_case() | {group, test_group()}]}].
groups() -> [
             {check_create, [sequence], [check_construction, check_pause_resume]},
             {check_slabs,  [sequence], [check_add_slabs]}    
            ].


-type config() :: proplists:proplist().
-spec init_per_suite (config()) -> config().
-spec end_per_suite  (config()) -> config().

init_per_suite (Config) -> Config.
end_per_suite  (Config)  -> Config.

-spec init_per_group (config()) -> config().
-spec end_per_group  (config()) -> config().

init_per_group (Config) -> Config.
end_per_group  (Config) -> Config.

-spec init_per_testcase (atom(), config()) -> config().
-spec end_per_testcase  (atom(), config()) -> config().

init_per_testcase (_Test_Case, Config) -> [{time_slice, 100} | Config].
end_per_testcase  (_Test_Case, Config) -> Config.

%% Test Module is ?TM
-define(TM, cxy_regulator).

%%%===================================================================
%%% check_construction/1
%%%===================================================================
-spec check_construction(config()) -> ok.
check_construction(Config) ->
    Test = "Check that a regulator can be constructed",
    ct:comment(Test), ct:log(Test),

    Pid1 = start_regulator(Config),

    Config2 = [{time_slice, 30} | Config],
    Pid2    = start_regulator(Config2),

    Config3 = [{time_slice, 300} | Config],
    Pid3    = start_regulator(Config3),

    _ = [begin unlink(Pid), exit(Pid, kill) end || Pid <- [Pid1, Pid2, Pid3]],

    Test_Complete = "Regulator construction verified",
    ct:comment(Test_Complete), ct:log(Test_Complete),
    ok.
    
start_regulator(Config) ->
    Tuple_Slots = proplists:get_value(time_slice, Config),
    Init_Tuple  = list_to_tuple(lists:duplicate(Tuple_Slots, 0)),
    ct:log("Time slice: ~p~nTuple: ~p", [Tuple_Slots, Init_Tuple]),

    {ok, Pid}   = ?TM:start_link(Config),
    Full_Status = get_full_status(Pid),
    'NORMAL'    = get_status_internal           (Full_Status),
    normal      = get_thruput_internal          (Full_Status),
    {epoch_slab_counts, 0, Init_Tuple}
                = get_slab_counts_internal      (Full_Status),
    0           = get_pending_requests_internal (Full_Status),
    Pid.

get_full_status      (Pid) -> ?TM:status(Pid).
     
get_status           (Pid) -> get_status_internal           (get_full_status(Pid)).
get_thruput          (Pid) -> get_thruput_internal          (get_full_status(Pid)).
get_init_time        (Pid) -> get_init_time_internal        (get_full_status(Pid)).
get_slab_counts      (Pid) -> get_slab_counts_internal      (get_full_status(Pid)).
get_pending_requests (Pid) -> get_pending_requests_internal (get_full_status(Pid)).

get_status_internal           (Props) -> proplists:get_value(current_state,    Props).
get_thruput_internal          (Props) -> proplists:get_value(thruput,          Props).
get_init_time_internal        (Props) -> proplists:get_value(init_time,        Props).
get_slab_counts_internal      (Props) -> proplists:get_value(slab_counts,      Props).
get_pending_requests_internal (Props) -> proplists:get_value(pending_requests, Props).


%%%===================================================================
%%% check_pause_resume/1
%%%===================================================================
-spec check_pause_resume(config()) -> ok.
check_pause_resume(Config) ->
    Test = "Check that regulators can be paused and resumed",
    ct:comment(Test), ct:log(Test),

    Pid = start_regulator(Config),

    paused            = ?TM:pause(Pid),
    'PAUSED'          = get_status(Pid),
    {ignored, pause}  = ?TM:pause(Pid),
    'PAUSED'          = get_status(Pid),

    {resumed, normal} = ?TM:resume(Pid),
    'NORMAL'          = get_status(Pid),
    {ignored, resume} = ?TM:resume(Pid),
    'NORMAL'          = get_status(Pid),

    Test_Complete = "Reservoir refill edge conditions verified",
    ct:comment(Test_Complete), ct:log(Test_Complete),
    ok.


%%%===================================================================
%%% check_add_slabs/1
%%%===================================================================
-spec check_add_slabs(config()) -> ok.
check_add_slabs(Config) ->
    Test = "Check that regulators can add slabs",
    ct:comment(Test), ct:log(Test),

    Regulator         = start_regulator(Config),
    Num_Pids_Per_Slab = 10,
    Num_Slabs         = 7,
    {Status1, Msgs}   = receive_add_slab(Config, Regulator, Num_Pids_Per_Slab, Num_Slabs),
    Exp_Results       = lists:duplicate(Num_Slabs, true),
    Exp_Results = lists:foldl(
                    fun({Pid_Num, Msg}, Results) ->
                            Result = validate_msg(Config, Num_Pids_Per_Slab, Pid_Num, Msg),
                            [Result | Results]
                    end,
                    [], lists:zip(lists:seq(1, Num_Slabs), lists:sort(Msgs))),

    6         = get_pending_requests_internal (Status1),
    'OVERMAX' = get_status_internal           (Status1),
    overmax   = get_thruput_internal          (Status1),

    Status2   = cxy_regulator:status(Regulator),

    0         = get_pending_requests_internal (Status2),
    'NORMAL'  = get_status_internal           (Status2),
    normal    = get_thruput_internal          (Status2),

    Test_Complete = "Slab delivered via gen_fsm:send_event to Fount",
    ct:comment(Test_Complete), ct:log(Test_Complete),
    ok.

validate_msg(Config, Num_Pids, Pid_Num, {'$gen_event', {slab, Pids, _Time_Stamp, Elapsed}}) ->
    Time_Slice        = proplists:get_value(time_slice, Config),
    Time_Slice_Millis = timer:seconds(1) div Time_Slice,
    Num_Pids          = length([Pid || Pid <- Pids, is_pid(Pid)]),
    case Pid_Num of

        %% First spawned slab is immediate...
        1 -> (Elapsed div timer:seconds(1)) < 100;

        %% Subsequent slabs are delayed by the time slice regulator.
        N -> Avg_Elapsed = (Elapsed div ((N-1) * timer:seconds(1))),
             Min_Allowed = 0.8 * Time_Slice_Millis,
             Max_Allowed = 1.5 * Time_Slice_Millis,
             Avg_Elapsed > Min_Allowed
                 andalso Avg_Elapsed < Max_Allowed
    end.

receive_add_slab(Config, Regulator, Slab_Size, Num_Slabs) ->
    Self       = self(),
    Fake_Fount = spawn_link(fun() -> fake_fount(Self, []) end),
    Cmd        = allocate_slab_cmd(Fake_Fount, Slab_Size),
    _ = [gen_fsm:send_event(Regulator, Cmd) || _N <- lists:seq(1, Num_Slabs)],
    wait_for_add_slab(Config, Fake_Fount, Regulator).

allocate_slab_cmd(Fount, Slab_Size) ->
    {allocate_slab, {Fount, cxy_fount_hello_behaviour, {}, os:timestamp(), Slab_Size}}.

wait_for_add_slab(Config, Fake_Fount, Regulator) ->
    Time_Slice        = proplists:get_value(time_slice, Config),
    Time_Slice_Millis = timer:seconds(1) div Time_Slice,
    _ = receive after Time_Slice_Millis -> continue end,
    Status = cxy_regulator:status(Regulator),
    _ = receive after timer:seconds(1) -> Fake_Fount ! stop end,
    receive {fount_msgs, Fake_Fount, Msgs} -> {Status, Msgs}
    after Time_Slice_Millis -> timeout
    end.

fake_fount(Receiver, Msgs) ->
    receive
        stop -> deliver_msgs (Receiver, Msgs);
        Msg  -> fake_fount   (Receiver, [Msg | Msgs])
    end.

deliver_msgs(Receiver, Msgs) ->
    Receiver ! {fount_msgs, self(), Msgs},
    delivered.
