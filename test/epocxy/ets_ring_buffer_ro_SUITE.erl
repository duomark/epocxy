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
         proper_new_ring_is_empty/1
        ]).

-include("epocxy_common_test.hrl").

-type test_case()  :: atom().
-type test_group() :: atom().

-spec all() -> [test_case() | {group, test_group()}].

all() -> [
          proper_new_ring_is_empty     % Ensure we can create new rings
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

%% Validate any atom can be used as a ring_name and ring is empty by default.
-spec proper_new_ring_is_empty(config()) -> ok.
proper_new_ring_is_empty(_Config) ->
    {ok, Sup_Pid} = epocxy_sup:start_link(),
    Fsm_Pid       = whereis(epocxy_ets_fsm),

    comment_and_log("Test using an atom as a ring name"),
    Test_Ring_Name = ?FORALL(Names, gen_ring_names(),
                             case [N || N <- Names, N =/= ''] of
                                 []         -> true;
                                 Ring_Names -> check_empty_create(Ring_Names, Sup_Pid, Fsm_Pid)
                             end),
    true = proper:quickcheck(Test_Ring_Name, ?PQ_NUM(5)),
    comment_and_log("Successfully tested atoms as ring_names"),

    %% Terminate the ets fsm server, then the epocxy supervisor.
    cleanup(Sup_Pid, Fsm_Pid),
    ok.
                              
check_empty_create(Ring_Names, _Sup_Pid, _Fsm_Pid) ->
    %% No rings exist yet...
    [] = ?TM:list(),
    [] = ?TM:list(missing_ring),

    comment_and_log("Testing ring_names ~p", [Ring_Names]),
    [check_empty_ring(R) || R <- Ring_Names],

    comment_and_log("Deleting all rings"),
    Num_Rings = length(Ring_Names),
    Exp_Deletes = lists:duplicate(Num_Rings, true),
    Exp_Deletes = [?TM:delete(R) || R <- Ring_Names],
    [] = ?TM:list(),

    true.

check_empty_ring(Ring_Name) ->
    true = ?TM:create    (Ring_Name),
    0    = ?TM:ring_size (Ring_Name).

cleanup(Pid, Fsm_Pid) ->
    supervisor:terminate_child(Pid, Fsm_Pid),
    unlink(Pid),
    exit(Pid, kill).

comment_and_log(Msg)       -> comment_and_log(Msg, []).
comment_and_log(Msg, Args) -> ct:comment (Msg, Args),
                              ct:log     (Msg, Args).
