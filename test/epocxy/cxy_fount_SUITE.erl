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
         check_construction/1,      check_edge_pid_allocs/1,
         check_reservoir_refills/1, check_faulty_behaviour/1,
         report_speed/1
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

          {group, check_create}    % Verify construction and reservoir refill.
         ].

-spec groups() -> [{test_group(), [sequence], [test_case() | {group, test_group()}]}].
groups() -> [
             %% Uncomment to test failing user-supplied module.
             %% {check_behaviour, [sequence], [check_faulty_behaviour]},

             {check_create,    [sequence], [check_construction, check_edge_pid_allocs,
                                            check_reservoir_refills, report_speed]}
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

    Full_Fn = ?FORALL({Slab_Size, Depth}, {range(1,37), range(2,17)},
                      verify_full_fount(Slab_Size, Depth)),
    true = proper:quickcheck(Full_Fn, ?PQ_NUM(5)),

    Test_Complete = "Fount construction and full refill verified",
    ct:comment(Test_Complete), ct:log(Test_Complete),
    ok.

fount_dims(Slab_Size, Depth) ->
    "(" ++ integer_to_list(Slab_Size) ++ " pids x " ++ integer_to_list(Depth) ++ " slabs)".

verify_full_fount(Slab_Size, Depth) ->
    Fount_Dims = fount_dims(Slab_Size, Depth),
    Case1 = "Verify construction of a " ++ Fount_Dims ++ " fount results in a full reservoir",
    ct:comment(Case1), ct:log(Case1),
    Fount = start_fount(cxy_fount_hello_behaviour, Slab_Size, Depth),

    Case2 = "Verify that an empty " ++ Fount_Dims ++ " fount refills itself",
    ct:comment(Case2), ct:log(Case2),
    Full_Fount_Size = Depth * Slab_Size,
    Pids1  = ?TM:get_pids(Fount, Full_Fount_Size),
    'FULL' = verify_reservoir_is_full(Fount),
    Pids2  = ?TM:get_pids(Fount, Full_Fount_Size),
    'FULL' = verify_reservoir_is_full(Fount),

    Case3 = "Verify that fetches get different pids",
    ct:comment(Case3), ct:log(Case3),
    true = sets:is_disjoint(sets:from_list(Pids1), sets:from_list(Pids2)),

    Complete = "Fount " ++ Fount_Dims ++ " construction verified",
    ct:comment(Complete), ct:log(Complete),
    true.
    
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
                      end, {'INIT', 0}, lists:duplicate(500, Fount)),
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

    Edge_Fn = ?FORALL({Slab_Size, Depth}, {range(1,37), range(2,17)},
                      verify_edges(Slab_Size, Depth)),
    true = proper:quickcheck(Edge_Fn, ?PQ_NUM(5)),

    Test_Complete = "Reservoir refill edge conditions verified",
    ct:comment(Test_Complete), ct:log(Test_Complete),
    ok.

verify_edge(Fount, Alloc_Size) ->
    Pids = ?TM:get_pids(Fount, Alloc_Size),
    'FULL' = verify_reservoir_is_full(Fount),
    Pids.
    
verify_edges(Slab_Size, Depth) ->
    Fount_Dims = fount_dims(Slab_Size, Depth),
    Case1 = "Verify reservoir " ++ Fount_Dims ++ " fount refill edge conditions",
    ct:comment(Case1), ct:log(Case1),

    Fount = start_fount(cxy_fount_hello_behaviour, Slab_Size, Depth),

    Case2 = "Verify " ++ Fount_Dims ++ " fount allocation in slab multiples",
    ct:comment(Case2), ct:log(Case2),
    Multiples = [N * Slab_Size || N <- [1,2,3]],
    [Pids1, Pids2, Pids3]
        = [verify_edge(Fount, Alloc_Size) || Alloc_Size <- Multiples],

    %% Deviate from slab modulo arithmetic...
    Pids4 = [?TM:get_pid(Fount), ?TM:get_pid(Fount)],

    [Pids5, Pids6, Pids7]
        = [verify_edge(Fount, Alloc_Size) || Alloc_Size <- Multiples],

    %% Make sure all pids are unique
    All_Pids = lists:append([Pids1, Pids2, Pids3, Pids4, Pids5, Pids6, Pids7]),
    Num_Pids = length(All_Pids),
    Num_Pids = sets:size(sets:from_list(All_Pids)),
    
    Get8_Count = max(1, Slab_Size div 3),
    Get9_Count = max(1, Slab_Size div 2),
    Case3 = "Verify " ++ Fount_Dims ++ " allocation in < slab counts ("
        ++ integer_to_list(Get8_Count) ++ "," ++ integer_to_list(Get9_Count) ++ ")",
    ct:comment(Case3), ct:log(Case3),
    Pids8 = ?TM:get_pids(Fount, Get8_Count),
    Pids9 = ?TM:get_pids(Fount, Get9_Count),
    {Get8_Count, Get9_Count} = {length(Pids8), length(Pids9)},
    true = sets:is_disjoint(sets:from_list(Pids8), sets:from_list(Pids9)),
    'FULL' = verify_reservoir_is_full(Fount),

    Max_Pids = Slab_Size * Depth,
    Get10_Count = min(Max_Pids, round(Slab_Size * 2.4)),
    Get11_Count = min(Max_Pids, round(Slab_Size * 1.7)),
    Case4 = "Verify " ++ Fount_Dims ++ " allocation in > slab counts ("
        ++ integer_to_list(Get10_Count) ++ "," ++ integer_to_list(Get11_Count) ++ ")",
    ct:comment(Case4), ct:log(Case4),
    'FULL' = verify_reservoir_is_full(Fount),
    Pids10 = ?TM:get_pids(Fount, Get10_Count),
    'FULL' = verify_reservoir_is_full(Fount),
    Pids11 = ?TM:get_pids(Fount, Get11_Count),
    {Get10_Count, Get11_Count} = {length(Pids10), length(Pids11)},
    true = sets:is_disjoint(sets:from_list(Pids10), sets:from_list(Pids11)),
    'FULL' = verify_reservoir_is_full(Fount),

    Test_Complete = "Fount " ++ Fount_Dims ++ " pid allocation verified",
    ct:comment(Test_Complete), ct:log(Test_Complete),
    true.
        

%%%===================================================================
%%% check_reservoir_refills/1
%%%===================================================================
-spec check_reservoir_refills(config()) -> ok.
check_reservoir_refills(_Config) ->
    Test = "Check that repeated fount requests are quickly replaced",
    ct:comment(Test), ct:log(Test),

    Test_Allocators = 
        ?FORALL({Slab_Size, Depth}, {range(1,20), range(2,10)},
                ?FORALL(Num_Pids, non_empty(list(min(Slab_Size*Depth, range(1, 30)))),
                        verify_slab_refills(Slab_Size, Depth, Num_Pids))),
    true = proper:quickcheck(Test_Allocators, ?PQ_NUM(100)),

    Test_Complete = "Verified repeated fount requests are quickly replaced",
    ct:comment(Test_Complete), ct:log(Test_Complete),
    ok.

verify_slab_refills(Slab_Size, Depth, Num_Pids) ->
    ct:log("PropEr testing slab_size ~p, depth ~p with ~w get_pid_fetches",
           [Slab_Size, Depth, Num_Pids]),
    Fount = start_fount(cxy_fount_hello_behaviour, Slab_Size, Depth),

    ct:log("Testing get"),
    [verify_pids(Fount, N, Depth*Slab_Size,  get) || N <- Num_Pids],
    ct:log("Testing task"),
    [verify_pids(Fount, N, Depth*Slab_Size, task) || N <- Num_Pids],
    true.

verify_pids(Fount, Num_Pids, Max_Available, Task_Or_Get) ->
    erlang:yield(),
    Pids = case Task_Or_Get of
               get  -> ?TM:get_pids  (Fount, Num_Pids);
               task -> ?TM:task_pids (Fount, lists:duplicate(Num_Pids, hello))
           end,
    case Max_Available >= Num_Pids of
        false -> [] = Pids,
                 'FULL' = verify_reservoir_is_full(Fount);
        true  -> case length(Pids) of
                     0        -> ran_out;
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


%%%===================================================================
%%% report_speed/1
%%%===================================================================
-spec report_speed(config()) -> ok.
report_speed(_Config) ->
    Test = "Report the spawning speed",
    ct:comment(Test), ct:log(Test),

    Slab_Size = 100, Num_Slabs = 50,
    {ok, Fount} = ?TM:start_link(cxy_fount_hello_behaviour, Slab_Size, Num_Slabs),
    ct:log("Spawn rate per slab with ~p pids for ~p slabs: ~p microseconds",
           [Slab_Size, Num_Slabs, cxy_fount:get_spawn_rate_per_slab(Fount)]),
    ct:log("Spawn rate per process with ~p pids for ~p slabs: ~p microseconds",
           [Slab_Size, Num_Slabs, cxy_fount:get_spawn_rate_per_process(Fount)]),

    Test_Complete = "Fount reporting speed reported",
    ct:comment(Test_Complete), ct:log(Test_Complete),
    ok.
