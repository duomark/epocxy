%%%------------------------------------------------------------------------------
%%% @copyright (c) 2015-2016, DuoMark International, Inc.
%%% @author Jay Nelson <jay@duomark.com>
%%% @reference 2015-2016 Development sponsored by TigerText, Inc. [http://tigertext.com/]
%%% @reference The license is based on the template for Modified BSD from
%%%   <a href="http://opensource.org/licenses/BSD-3-Clause">OSI</a>
%%% @doc
%%%   Validation of cxy_fount using common test and PropEr.
%%%
%%% @since 0.9.9
%%% @end
%%%------------------------------------------------------------------------------
-module(cxy_fount_SUITE).
-auth('jay@duomark.com').
-vsn('').

%%% Common_test exports
-export([all/0, groups/0,
         init_per_suite/1,    end_per_suite/1,
         init_per_group/1,    end_per_group/1,
         init_per_testcase/2, end_per_testcase/2
        ]).

%%% Test case exports
-export([
         check_construction/1, check_edge_pid_allocs/1, check_reservoir_refills/1,
         check_faulty_behaviour/1
        ]).

-include("epocxy_common_test.hrl").


%%%===================================================================
%%% Test cases
%%%===================================================================

-type test_case()  :: atom().
-type test_group() :: atom().

-spec all() -> [test_case() | {group, test_group()}].
all() -> [
          {group, check_create}   % Verify construction and reservoir refill.
          %% {group, check_behaviour} % Ensure behaviour crashes properly.
         ].

-spec groups() -> [{test_group(), [sequence], [test_case() | {group, test_group()}]}].
groups() -> [
             {check_create,  [sequence],
              [check_construction, check_edge_pid_allocs, check_reservoir_refills]}
             %% {check_behaviour, [sequence],
             %%  [check_faulty_behaviour]}
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

init_per_testcase (_Test_Case, Config) -> Config.
end_per_testcase  (_Test_Case, Config) -> Config.

%% Test Modules is ?TM
-define(TM, cxy_fount).

%%%===================================================================
%%% check_construction/1
%%%===================================================================
-spec check_construction(config()) -> ok.
check_construction(_Config) ->
    Test = "Check that founts can be constructed and refill fully",
    ct:comment(Test), ct:log(Test),

    Case1 = "Verify construction results in a full reservoir",
    ct:comment(Case1), ct:log(Case1),
    Fount = start_fount(cxy_fount_hello_behaviour, 3, 5),
    Full_Fount_Size = 3 * 5,

    Case2 = "Verify that an empty fount refills itself",
    ct:comment(Case2), ct:log(Case2),
    Pids1 = ?TM:get_pids(Fount, Full_Fount_Size),
    'FULL' = verify_reservoir_is_full(Fount),
    Pids2 = ?TM:get_pids(Fount, Full_Fount_Size),
    'FULL' = verify_reservoir_is_full(Fount),

    Case3 = "Verify that fetches get different pids",
    ct:comment(Case3), ct:log(Case3),
    true = sets:is_disjoint(sets:from_list(Pids1), sets:from_list(Pids2)),

    Test_Complete = "Fount construction verified",
    ct:comment(Test_Complete), ct:log(Test_Complete),
    ok.

start_fount(Behaviour, Slab_Size, Depth) ->
    {ok, Fount} = ?TM:start_link(Behaviour, Slab_Size, Depth),
    'FULL' = verify_reservoir_is_full(Fount),
    Fount.

verify_reservoir_is_full(Fount) ->
    {Final_State, _Loops}
        = lists:foldl(fun (_Pid, {'FULL', N}) -> {'FULL', N};
                          ( Pid, {_,  Count}) ->
                              erlang:yield(),
                              Status = ?TM:get_status(Pid),
                              {proplists:get_value(current_state, Status), Count+1}
                      end, {'INIT', 0}, lists:duplicate(50, Fount)),
    ct:log("Looped ~p times before reservoir was ~p", [_Loops, Final_State]),
    Props = ?TM:get_status(Fount),
    [FC, Num_Slabs, Max_Slabs, Slab_Size, Pid_Count, Max_Pids]
        = [proplists:get_value(P, Props)
           || P <- [fount_count, slab_count, max_slabs, slab_size, pid_count, max_pids]],
    Ok_Full_Count = (Max_Pids - Pid_Count) < Slab_Size,
    Ok_Slab_Count = (Max_Slabs - 1) =:= Num_Slabs andalso FC > 0,
    {true, true, FC, Num_Slabs, Slab_Size, Max_Pids, Final_State, _Loops}
        = {Ok_Full_Count, Ok_Slab_Count, FC, Num_Slabs, Slab_Size, Max_Pids, Final_State, _Loops},
    Final_State.


%%%===================================================================
%%% check_edge_pid_allocs/1 looks for simple edge case number of pids
%%%===================================================================
-spec check_edge_pid_allocs(config()) -> ok.
check_edge_pid_allocs(_Config) ->
    Test = "Check that founts can dole out various sized lists of pids",
    ct:comment(Test), ct:log(Test),

    Case1 = "Verify construction results in a full reservoir",
    ct:comment(Case1), ct:log(Case1),
    Slab_Size = 4, Depth = 7,
    Fount = start_fount(cxy_fount_hello_behaviour, Slab_Size, Depth),

    Case2 = "Verify allocation in slab multiples",
    ct:comment(Case2), ct:log(Case2),
    Pids1 = ?TM:get_pids(Fount, Slab_Size),
    'FULL' = verify_reservoir_is_full(Fount),
    Pids2 = ?TM:get_pids(Fount, Slab_Size * 2),
    'FULL' = verify_reservoir_is_full(Fount),
    Pids3 = ?TM:get_pids(Fount, Slab_Size * 3),
    'FULL' = verify_reservoir_is_full(Fount),

    Pids4 = [?TM:get_pid(Fount), ?TM:get_pid(Fount)],
    Pids5 = ?TM:get_pids(Fount, Slab_Size),
    'FULL' = verify_reservoir_is_full(Fount),
    Pids6 = ?TM:get_pids(Fount, Slab_Size * 2),
    'FULL' = verify_reservoir_is_full(Fount),
    Pids7 = ?TM:get_pids(Fount, Slab_Size * 3),
    'FULL' = verify_reservoir_is_full(Fount),

    %% Make sure all pids are unique
    All_Pids = lists:append([Pids1, Pids2, Pids3, Pids4, Pids5, Pids6, Pids7]),
    Num_Pids = length(All_Pids),
    Num_Pids = sets:size(sets:from_list(All_Pids)),
    
    Case3 = "Verify allocation in < slab counts",
    ct:comment(Case3), ct:log(Case3),
    Pids8 = ?TM:get_pids(Fount, 3),
    Pids9 = ?TM:get_pids(Fount, 2),
    {3,2} = {length(Pids8), length(Pids9)},
    true = sets:is_disjoint(sets:from_list(Pids8), sets:from_list(Pids9)),
    'FULL' = verify_reservoir_is_full(Fount),

    Case4 = "Verify allocation in > slab counts",
    ct:comment(Case4), ct:log(Case4),
    Pids10 = ?TM:get_pids(Fount, 13),
    Pids11 = ?TM:get_pids(Fount, 7),
    {13,7} = {length(Pids10), length(Pids11)},
    true = sets:is_disjoint(sets:from_list(Pids10), sets:from_list(Pids11)),
    'FULL' = verify_reservoir_is_full(Fount),

    Test_Complete = "Fount pid allocation verified",
    ct:comment(Test_Complete), ct:log(Test_Complete),
    ok.
        

%%%===================================================================
%%% check_reservoir_refills/1
%%%===================================================================
-spec check_reservoir_refills(config()) -> ok.
check_reservoir_refills(_Config) ->
    ct:comment("Check that repeated fount requests are quickly replaced"),
    Test_Allocators = 
        ?FORALL({Slab_Size, Depth, Num_Pids},
                {range(1,20), range(2,10), non_empty(list(range(1,30)))},
                begin
                    ct:log(io_lib:format("PropEr testing slab_size ~p, depth ~p",
                                         [Slab_Size, Depth])),
                    ct:log(io_lib:format("Testing ~w get_pid fetches", [Num_Pids])),
                    Fount = start_fount(cxy_fount_hello_behaviour, Slab_Size, Depth),
                    ct:log(io_lib:format("Testing get",  [])),
                    [validate_pids(Fount, N, Depth*Slab_Size,  get) || N <- Num_Pids],
                    ct:log(io_lib:format("Testing task", [])),
                    [validate_pids(Fount, N, Depth*Slab_Size, task) || N <- Num_Pids],
                    true
                end),
    true = proper:quickcheck(Test_Allocators, ?PQ_NUM(100)),
        
    Test_Complete = "No failures detected",
    ct:comment(Test_Complete), ct:log(Test_Complete),
    ok.

validate_pids(Fount, Num_Pids, Max_Available, Task_Or_Get) ->
    erlang:yield(),
    Pids = case Task_Or_Get of
               get  -> ?TM:get_pids(Fount, Num_Pids);
               task -> ?TM:task_pids(Fount, lists:duplicate(Num_Pids, hello))
           end,
    case Max_Available >= Num_Pids of
        false -> [] = Pids,
                 verify_reservoir_is_full(Fount);
        true  -> case length(Pids) of
                     0 -> ran_out;
                     Num_Pids -> allocated
                 end,

                 %% make sure workers are unlinked, then kill them.
                 [begin
                      case process_info(Pid, links) of
                          undefined      -> skip;
                          {links, Links} ->
                              false = lists:member(Pid, Links),
                              Task_Or_Get =:= task
                                  orelse cxy_fount_hello_behaviour:say_to(Pid, hello)
                              %% Killing crashes the test, but normal end above doesn't???
                              %% exit(Pid, kill)
                      end
                  end || Pid <- Pids],

                 %% check that the reservoir is 'FULL' again
                 'FULL' = verify_reservoir_is_full(Fount)
    end.


%%%===================================================================
%%% check_faulty_behaviour/1
%%%===================================================================
-spec check_faulty_behaviour(config()) -> ok.
check_faulty_behaviour(_Config) ->
    Test = "Check that non-pid returns crash the fount",
    ct:comment(Test), ct:log(Test),

    Case1 = "Verify a bad behaviour crashes the fount",
    ct:comment(Case1), ct:log(Case1),
    Old_Trap = process_flag(trap_exit, true),
    try
        Slab_Size = 10, Num_Slabs = 3,
        {ok, Fount1} = ?TM:start_link(cxy_fount_fail_behaviour, Slab_Size, Num_Slabs),
        crashed = bad_pid(Fount1),

        {ok, Fount2} = ?TM:start_link(cxy_fount_fail_behaviour),
        crashed = bad_pid(Fount2)

    after true = process_flag(trap_exit, Old_Trap)
    end,

    Test_Complete = "Fount failure verified",
    ct:comment(Test_Complete), ct:log(Test_Complete),
    ok.

bad_pid(Fount) ->
    receive {'EXIT', Fount,
             {{case_clause, bad_pid},
              [{cxy_fount, allocate_slab, 5,
                [{file, "src/cxy_fount.erl"}, {line,_}]}]}} -> crashed
    after 1000 -> timeout
    end.
