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
    'FULL' = verify_reservoir_is_full(Fount, Depth),
    Pids2  = ?TM:get_pids(Fount, Full_Fount_Size),
    'FULL' = verify_reservoir_is_full(Fount, Depth),

    Case3 = "Verify that fetches get different pids",
    ct:comment(Case3), ct:log(Case3),
    true = sets:is_disjoint(sets:from_list(Pids1), sets:from_list(Pids2)),

    Complete = "Fount " ++ Fount_Dims ++ " construction verified",
    ct:comment(Complete), ct:log(Complete),
    true.
    
start_fount(Behaviour, Slab_Size, Depth) ->
    {ok, Sup} = cxy_fount_sup:start_link(Behaviour, [none], Slab_Size, Depth),
    Fount     = cxy_fount_sup:get_fount(Sup),
    'FULL'    = verify_reservoir_is_full(Fount, Depth),
    Fount.

verify_reservoir_is_full(Fount, Num_Of_Spawn_Slices) ->
    Time_To_Sleep = (Num_Of_Spawn_Slices + 1) * 100,
    timer:sleep(Time_To_Sleep),   % 1/10th second per slab
    Status = ?TM:get_status(Fount),
    Final_State = proplists:get_value(current_state, Status),
    finish_full(Time_To_Sleep, Final_State, Status).

finish_full(Time_Slept, Final_State, Status) ->
    ct:log("Slept ~p milliseconds before reservoir was ~p", [Time_Slept, Final_State]),
    [FC, Num_Slabs, Max_Slabs, Slab_Size, Pid_Count, Max_Pids]
        = [proplists:get_value(P, Status)
           || P <- [fount_count, slab_count, max_slabs, slab_size, pid_count, max_pids]],
    Ok_Full_Count = (Max_Pids - Pid_Count) < Slab_Size,
    Ok_Slab_Count = (Max_Slabs - 1) =:= Num_Slabs andalso FC > 0,

    %% Provide extra data on failure
    {true, true, FC, Num_Slabs, Slab_Size, Max_Pids, Final_State, Time_Slept}
        = {Ok_Full_Count, Ok_Slab_Count, FC, Num_Slabs, Slab_Size,
           Max_Pids, Final_State, Time_Slept},
    Final_State.


%%%===================================================================
%%% check_edge_pid_allocs/1 looks for simple edge case number of pids
%%%===================================================================
-spec check_edge_pid_allocs(config()) -> ok.
check_edge_pid_allocs(_Config) ->
    Test = "Check that founts can dole out various sized lists of pids",
    ct:comment(Test), ct:log(Test),

    %% Depth at least 3 to avoid 'FULL' with depth 3 or less and partial fount.
    Edge_Fn = ?FORALL({Slab_Size, Depth}, {range(1,37), range(4,17)},
                      verify_edges(Slab_Size, Depth)),
    true = proper:quickcheck(Edge_Fn, ?PQ_NUM(5)),

    Test_Complete = "Reservoir refill edge conditions verified",
    ct:comment(Test_Complete), ct:log(Test_Complete),
    ok.

verify_edge(Fount, Alloc_Size, Depth) ->
    Pids = ?TM:get_pids(Fount, Alloc_Size),
    'FULL' = verify_reservoir_is_full(Fount, Depth),
    Pids.
    
verify_edges(Slab_Size, Depth) ->
    Fount_Dims = fount_dims(Slab_Size, Depth),
    Case1 = "Verify reservoir " ++ Fount_Dims ++ " fount refill edge conditions",
    ct:comment(Case1), ct:log(Case1),

    Fount = start_fount(cxy_fount_hello_behaviour, Slab_Size, Depth),

    Multiples = [{N, N * Slab_Size} || N <- [1,2,3]],
    Case2_Msg = "Verify ~s fount allocation in slab multiples ~p",
    Case2 = lists:flatten(io_lib:format(Case2_Msg, [Fount_Dims, Multiples])),
    ct:comment(Case2), ct:log(Case2),
    [Pids1, Pids2, Pids3]
        = [verify_edge(Fount, Alloc_Size, Alloc_Depth)
           || {Alloc_Depth, Alloc_Size} <- Multiples],

    %% Deviate from slab modulo arithmetic...
    Pids4 = [?TM:get_pid(Fount), ?TM:get_pid(Fount)],

    [Pids5, Pids6, Pids7]
        = [verify_edge(Fount, Alloc_Size, Alloc_Depth)
           || {Alloc_Depth, Alloc_Size} <- Multiples],

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
    'FULL' = verify_reservoir_is_full(Fount, 1),

    Max_Pids = Slab_Size * Depth,
    Get10_Count = min(Max_Pids, round(Slab_Size * 2.4)),
    Get11_Count = min(Max_Pids, round(Slab_Size * 1.7)),
    Case4 = "Verify " ++ Fount_Dims ++ " allocation in > slab counts ("
        ++ integer_to_list(Get10_Count) ++ "," ++ integer_to_list(Get11_Count) ++ ")",
    ct:comment(Case4), ct:log(Case4),
    'FULL' = verify_reservoir_is_full(Fount, 1),
    Pids10 = ?TM:get_pids(Fount, Get10_Count),
    'FULL' = verify_reservoir_is_full(Fount, 3),
    Pids11 = ?TM:get_pids(Fount, Get11_Count),
    {{Get10_Count, Get10_Count}, {Get11_Count, Get11_Count}}
        = {{Get10_Count, length(Pids10)}, {Get11_Count, length(Pids11)}},
    true = sets:is_disjoint(sets:from_list(Pids10), sets:from_list(Pids11)),
    'FULL' = verify_reservoir_is_full(Fount, 2),

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
        ?FORALL({Slab_Size, Depth, Num_Pids},
                {range(1,20), range(2,10), non_empty(list(range(1, 30)))},
                verify_slab_refills(Slab_Size, Depth, Num_Pids)),
    true = proper:quickcheck(Test_Allocators, ?PQ_NUM(100)),

    Test_Complete = "Verified repeated fount requests are quickly replaced",
    ct:comment(Test_Complete), ct:log(Test_Complete),
    ok.

verify_slab_refills(Slab_Size, Depth, Num_Pids) ->
    ct:log("PropEr testing slab_size ~p, depth ~p with ~w get_pid_fetches",
           [Slab_Size, Depth, Num_Pids]),
    Fount = start_fount(cxy_fount_hello_behaviour, Slab_Size, Depth),

    ct:log("Testing get"),
    [verify_pids(Fount, N, Slab_Size, Depth,  get) || N <- Num_Pids],
    ct:log("Testing task"),
    [verify_pids(Fount, N, Slab_Size, Depth, task) || N <- Num_Pids],
    true.

verify_pids(Fount, Num_Pids, Slab_Size, Depth, Task_Or_Get) ->
    erlang:yield(),
    Pids = case Task_Or_Get of
               get  -> ?TM:get_pids  (Fount, Num_Pids);
               task -> ?TM:task_pids (Fount, lists:duplicate(Num_Pids, hello))
           end,
    case Num_Pids of
        Num_Pids when Num_Pids > Slab_Size * Depth ->
            [] = Pids,
            'FULL' = verify_reservoir_is_full(Fount, 1);
        Num_Pids when Num_Pids > Slab_Size * (Depth - 1) ->
            case length(Pids) of
                0        -> 'FULL' = verify_reservoir_is_full(Fount, 1);
                Num_Pids ->
                    Num_Pids = length(Pids),
                    _ = unlink_workers(Pids, Task_Or_Get),
                    'FULL' = verify_reservoir_is_full(Fount, Num_Pids div Slab_Size)
            end;
        Num_Pids ->
            _ = unlink_workers(Pids, Task_Or_Get),
            'FULL' = verify_reservoir_is_full(Fount, Num_Pids div Slab_Size)
    end.

unlink_workers(Pids, Task_Or_Get) ->
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
     end || Pid <- Pids].

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

    lists:foreach(
      fun({Slab_Size, Num_Slabs}) ->
              Fount = start_fount(cxy_fount_hello_behaviour, Slab_Size, Num_Slabs),
              'FULL' = verify_reservoir_is_full(Fount, Num_Slabs), % Give it a chance to fill up
              ct:log("Spawn rate per process with ~p pids for ~p slabs: ~p microseconds",
                     [Slab_Size, Num_Slabs, cxy_fount:get_spawn_rate_per_process(Fount)]),
              ct:log("Spawn rate per slab with ~p pids for ~p slabs: ~p microseconds",
                     [Slab_Size, Num_Slabs, cxy_fount:get_spawn_rate_per_slab(Fount)]),
              ct:log("Replacement rate per process with ~p pids for ~p slabs: ~p microseconds",
                     [Slab_Size, Num_Slabs, cxy_fount:get_total_rate_per_process(Fount)]),
              ct:log("Replacement rate per slab with ~p pids for ~p slabs: ~p microseconds",
                     [Slab_Size, Num_Slabs, cxy_fount:get_total_rate_per_slab(Fount)]),
              cxy_fount:stop(Fount)
      end, [{5,100}, {20, 50}, {40, 50}, {60, 50}, {80, 50}, {100, 50},
            {150, 50}, {200, 50}, {250, 50}, {300, 50}, {500, 50}]),

    Test_Complete = "Fount reporting speed reported",
    ct:comment(Test_Complete), ct:log(Test_Complete),
    ok.
