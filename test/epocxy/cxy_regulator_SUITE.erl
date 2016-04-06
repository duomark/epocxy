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
         check_construction/1,      check_pause_resume/1
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

          {group, check_create}    % Verify construction and pause/resume
         ].

-spec groups() -> [{test_group(), [sequence], [test_case() | {group, test_group()}]}].
groups() -> [
             {check_create,    [sequence], [check_construction, check_pause_resume]}
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

%% Test Module is ?TM
-define(TM, cxy_regulator).

%%%===================================================================
%%% check_construction/1
%%%===================================================================
-spec check_construction(config()) -> ok.
check_construction(_Config) ->
    Test = "Check that a regulator can be constructed",
    ct:comment(Test), ct:log(Test),
    _Pid = start_regulator(),
    Test_Complete = "Regulator construction verified",
    ct:comment(Test_Complete), ct:log(Test_Complete),
    ok.
    
start_regulator() ->
    {ok, Pid} = ?TM:start_link(),
    'NORMAL' = get_status(Pid),
    Pid.

get_status(Pid) ->
    proplists:get_value(current_state, ?TM:status(Pid)).


%%%===================================================================
%%% check_pause_resume/1
%%%===================================================================
-spec check_pause_resume(config()) -> ok.
check_pause_resume(_Config) ->
    Test = "Check that regulators can be paused and resumed",
    ct:comment(Test), ct:log(Test),

    Pid = start_regulator(),

    paused = ?TM:pause(Pid),
    'PAUSED' = get_status(Pid),
    {ignored, pause} = ?TM:pause(Pid),
    'PAUSED' = get_status(Pid),

    {resumed, normal} = ?TM:resume(Pid),
    'NORMAL' = get_status(Pid),
    {ignored, resume} = ?TM:resume(Pid),
    'NORMAL' = get_status(Pid),

    Test_Complete = "Reservoir refill edge conditions verified",
    ct:comment(Test_Complete), ct:log(Test_Complete),
    ok.
