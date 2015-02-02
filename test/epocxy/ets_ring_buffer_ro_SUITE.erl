%%%------------------------------------------------------------------------------
%%% @copyright (c) 2015, DuoMark International, Inc.
%%% @author Jay Nelson <jay@duomark.com>
%%% @reference 2015 Development sponsored by TigerText, Inc. [http://tigertext.com/]
%%% @reference The license is based on the template for Modified BSD from
%%%   <a href="http://opensource.org/licenses/BSD-3-Clause">OSI</a>
%%% @doc
%%%   Tests for read only ets ring buffers.
%%%
%%% @since 0.9.8e
%%% @end
%%%------------------------------------------------------------------------------
-module(ets_ring_buffer_ro_SUITE).
-auth('jay@duomark.com').
-vsn('').

-export([all/0,
%%         groups/0,
         init_per_suite/1,    end_per_suite/1,
         init_per_group/1,    end_per_group/1,
         init_per_testcase/2, end_per_testcase/2
        ]).

-export([
         proper_check_create/1
        ]).

-include("epocxy_common_test.hrl").

-type test_case()  :: atom().
-type test_group() :: atom().

-spec all() -> [test_case() | {group, test_group()}].

all() -> [
          proper_check_create     % Ensure we can create new rings
         ].

%% -spec groups() -> [test_group(), [sequence], [test_case() | {group, test_group()}]].

%% groups() -> [].
    

-type config() :: proplists:proplist().
-spec init_per_suite (config()) -> config().
-spec end_per_suite  (config()) -> config().

init_per_suite (Config) -> Config.
end_per_suite  (Config)  -> Config.

-spec init_per_group (config()) -> config().
-spec end_per_group  (config()) -> config().

init_per_group (Config) -> Config.
end_per_group  (Config)  -> Config.

-spec init_per_testcase (atom(), config()) -> config().
-spec end_per_testcase  (atom(), config()) -> config().

init_per_testcase (_Test_Case, Config) -> Config.
end_per_testcase  (_Test_Case, Config) -> Config.

-define(TM, ets_ring_buffer_ro).


%%%------------------------------------------------------------------------------
%%% Unit tests for ets_ring_buffer_ro core
%%%------------------------------------------------------------------------------

gen_ring_names() ->
    ?LET(Ring_Names, list(atom()), Ring_Names).

%% Validate any atom can be used as a ring_name and list will report properly.
-spec proper_check_create(config()) -> ok.
proper_check_create(_Config) ->
    {ok, Sup_Pid} = epocxy_sup:start_link(),
    Fsm_Pid       = whereis(epocxy_ets_fsm),

    ct:log("Test using an atom as a ring name"),
    Test_Ring_Name = ?FORALL(Names, gen_ring_names(),
                             case [N || N <- Names, N =/= ''] of
                                 []         -> true;
                                 Ring_Names -> check_create_test(Ring_Names, Sup_Pid, Fsm_Pid)
                             end),
    true = proper:quickcheck(Test_Ring_Name, ?PQ_NUM(5)),
    ct:comment("Successfully tested atoms as ring_names"),

    %% Terminate the ets fsm server, then the epocxy supervisor.
    cleanup(Sup_Pid, Fsm_Pid),
    ok.
                              
check_create_test(Ring_Names, Sup_Pid, Fsm_Pid) ->
    %% No rings exist yet...
    [] = ?TM:list(),
    [] = ?TM:list(missing_ring),

    ct:comment ("Testing ring_names ~p", [Ring_Names]),
    ct:log     ("Testing ring_names ~p", [Ring_Names]),
    Num_Rings   = length(Ring_Names),
    Exp_Counts  = lists:duplicate(Num_Rings, 0),
    Exp_Counts  = [check_one_ring(R) || R <- Ring_Names],

    ct:comment("Deleting all rings"),
    Exp_Deletes = lists:duplicate(Num_Rings, true),
    Exp_Deletes = [?TM:delete(R) || R <- Ring_Names],
    [] = ?TM:list(),

    true.

check_one_ring(Ring_Name) ->
    true = ?TM:create    (Ring_Name),
    0    = ?TM:ring_size (Ring_Name).

cleanup(Pid, Fsm_Pid) ->
    supervisor:terminate_child(Pid, Fsm_Pid),
    unlink(Pid),
    exit(Pid, kill).
