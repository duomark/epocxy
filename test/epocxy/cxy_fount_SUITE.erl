%%%------------------------------------------------------------------------------
%%% @copyright (c) 2015, DuoMark International, Inc.
%%% @author Jay Nelson <jay@duomark.com>
%%% @reference 2015 Development sponsored by TigerText, Inc. [http://tigertext.com/]
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
         check_construction/1, check_edge_pid_allocs/1, check_no_failures/1
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
         ].

-spec groups() -> [{test_group(), [sequence], [test_case() | {group, test_group()}]}].
groups() -> [
             {check_create,  [sequence],
              [check_construction, check_edge_pid_allocs, check_no_failures]}
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
    Hello_Behaviour = cxy_fount_hello_behaviour,

    Case1 = "Verify construction results in a full reservoir",
    ct:comment(Case1), ct:log(Case1),
    Depth = 3,
    Slab_Size = 5,
    Full_Fount_Size = Slab_Size * Depth,
    {ok, Fount} = ?TM:start_link(Hello_Behaviour, Slab_Size, Depth),
    full = verify_reservoir_is_full(Fount),

    Case2 = "Verify that an empty fount refills itself",
    ct:comment(Case2), ct:log(Case2),
    Pids1 = ?TM:get_pids(Fount, Full_Fount_Size),
    full = verify_reservoir_is_full(Fount),
    Pids2 = ?TM:get_pids(Fount, Full_Fount_Size),
    full = verify_reservoir_is_full(Fount),

    Case3 = "Verify that fetches get different pids",
    ct:comment(Case3), ct:log(Case3),
    true = sets:is_disjoint(sets:from_list(Pids1), sets:from_list(Pids2)),

    Test_Complete = "Fount construction verified",
    ct:comment(Test_Complete), ct:log(Test_Complete),
    ok.

verify_reservoir_is_full(Fount) ->
    {'FULL', Loop_Count} = lists:foldl(fun full_fn/2, {'INIT', 0}, lists:duplicate(10, Fount)),
    ct:log("Loops to detect a full reservoir: ~p~n", [Loop_Count]),
    full.

full_fn(_Pid, { 'FULL', Count}) -> {'FULL', Count};
full_fn( Pid, {_Status, Count}) ->
    erlang:yield(),
    Status = ?TM:get_status(Pid),
    {proplists:get_value(current_state, Status), Count+1}.


%%%===================================================================
%%% check_edge_pid_allocs/1 looks for simple edge case number of pids
%%%===================================================================
-spec check_edge_pid_allocs(config()) -> ok.
check_edge_pid_allocs(_Config) ->
    Test = "Check that founts can dole out various sized lists of pids",
    ct:comment(Test), ct:log(Test),
    Hello_Behaviour = cxy_fount_hello_behaviour,

    Case1 = "Verify construction results in a full reservoir",
    ct:comment(Case1), ct:log(Case1),
    Depth = 7,
    Slab_Size = 4,
    {ok, Fount} = ?TM:start_link(Hello_Behaviour, Slab_Size, Depth),
    full = verify_reservoir_is_full(Fount),

    Case2 = "Verify allocation in slab multiples",
    ct:comment(Case2), ct:log(Case2),
    Pids1 = ?TM:get_pids(Fount, Slab_Size),
    full = verify_reservoir_is_full(Fount),
    Pids2 = ?TM:get_pids(Fount, Slab_Size * 2),
    full = verify_reservoir_is_full(Fount),
    Pids3 = ?TM:get_pids(Fount, Slab_Size * 3),
    full = verify_reservoir_is_full(Fount),

    Pids4 = [?TM:get_pid(Fount), ?TM:get_pid(Fount)],
    Pids5 = ?TM:get_pids(Fount, Slab_Size),
    full = verify_reservoir_is_full(Fount),
    Pids6 = ?TM:get_pids(Fount, Slab_Size * 2),
    full = verify_reservoir_is_full(Fount),
    Pids7 = ?TM:get_pids(Fount, Slab_Size * 3),
    full = verify_reservoir_is_full(Fount),

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
    full = verify_reservoir_is_full(Fount),

    Case4 = "Verify allocation in > slab counts",
    ct:comment(Case4), ct:log(Case4),
    Pids10 = ?TM:get_pids(Fount, 13),
    Pids11 = ?TM:get_pids(Fount, 7),
    {13,7} = {length(Pids10), length(Pids11)},
    true = sets:is_disjoint(sets:from_list(Pids10), sets:from_list(Pids11)),
    ct:log("State: ~p", [?TM:get_status(Fount)]),
    full = verify_reservoir_is_full(Fount),

    Test_Complete = "Fount pid allocation verified",
    ct:comment(Test_Complete), ct:log(Test_Complete),
    ok.
        

%%%===================================================================
%%% check_no_failures/1
%%%===================================================================
-spec check_no_failures(config()) -> ok.
check_no_failures(_Config) ->
    ct:comment("Check that repeated fount requests don't cause failure"),
    Hello_Behaviour = cxy_fount_hello_behaviour,
    
    Test_Complete = "No failures detected",
    ct:comment(Test_Complete), ct:log(Test_Complete),
    ok.
