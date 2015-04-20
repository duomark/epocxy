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

-export([all/0, init_per_suite/1, end_per_suite/1]).
-export([check_construction/1]).

%%%===================================================================
%%% Fount worker behaviour
%%%===================================================================
-behaviour(cxy_fount).
-export([start_pid/1, send_msg/2]).
-export([]).

start_pid (Fount)       -> spawn      (fun() -> link(Fount), wait_for_hello() end).
send_msg  (Worker, Msg) -> spawn_link (fun() -> say_to(Worker, Msg) end).

%% Idle workers may wait a while before being used in a test.
wait_for_hello() ->
    receive {Ref, From, hello} -> From ! {Ref, goodbye, now()}
    after 30000 -> wait_for_hello_timeout
    end.

%% Just verify the goodbye response comes after saying hello.
say_to(Worker, Msg) ->
    Ref = make_ref(),
    Now1 = now(),
    Worker ! {Ref, self(), Msg},
    receive {Ref, goodbye, Now2} -> true = Now1 < Now2
    after 1000 -> throw(say_hello_timeout)
    end.


%%%===================================================================
%%% Test cases
%%%===================================================================
-include_lib("common_test/include/ct.hrl").

-spec all() -> [atom()].

all() -> [check_construction].

-type config() :: proplists:proplist().
-spec init_per_suite(config()) -> config().
-spec end_per_suite(config()) -> config().

init_per_suite(Config) -> Config.
end_per_suite(Config)  -> Config.

%% Test Modules is ?TM
-define(TM, cxy_fount).

-spec check_construction(config()) -> ok.
check_construction(_Config) ->
    ct:comment("Check that founts can be constructed and refill fully"),
    Hello_Behaviour = ?MODULE,

    ct:comment("Verify construction results in a full reservoir"),
    Depth = 3,
    Slab_Size = 5,
    Full_Fount_Size = Slab_Size * Depth,
    {ok, Fount} = ?TM:start_link(Hello_Behaviour, Slab_Size, Depth),
    full = verify_reservoir_is_full(Fount, Full_Fount_Size),

    ct:comment("Verify that an empty fount refills itself"),
    Pids1 = ?TM:get_pids(Fount, Full_Fount_Size),
    full = verify_reservoir_is_full(Fount, Full_Fount_Size),
    Pids2 = ?TM:get_pids(Fount, Full_Fount_Size),
    full = verify_reservoir_is_full(Fount, Full_Fount_Size),

    ct:comment("Verify that fetches get different pids"),
    true = Pids1 -- Pids2 =:= Pids1,
    true = Pids2 -- Pids1 =:= Pids2,

    ct:comment("Fount construction verified"),
    ok.

verify_reservoir_is_full(Fount, Capacity) ->
    {'FULL', Capacity, Capacity, Loop_Count} =
        lists:foldl(fun full_fn/2, {'INIT', Capacity, 0, 0}, lists:duplicate(10, Fount)),
    ct:log("Loops to detect a full reservoir: ~p~n", [Loop_Count]),
    full.

full_fn(_Pid, {'FULL',  Capacity,      Capacity, Count}) ->
    {'FULL', Capacity, Capacity, Count};
full_fn( Pid, {_Status, Capacity, _Not_Capacity, Count}) ->
    erlang:yield(),
    Status = ?TM:get_status(Pid),
    {proplists:get_value(current_state, Status),
     Capacity,
     proplists:get_value(pid_count,     Status),
     Count+1}.
