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
    Hello_Behaviour = ?MODULE,
    {ok, Fount} = ?TM:start_link(Hello_Behaviour, 5, 3),
    ['FULL', 15, Loop_Count] =
        lists:foldl(fun(_Pid, ['FULL', 15, Count]) -> ['FULL', 15, Count];
                       (_Pid, [_Status, _, Count]) ->
                            erlang:yield(),
                            Status = ?TM:get_status(Fount),
                            [proplists:get_value(E, Status) || E <- [current_state, pid_count]]
                                ++ [Count+1]
                    end, ['INIT', 0, 0], lists:duplicate(10, Fount)),
    ct:log("Loops to detect a full reservoir: ~p~n", [Loop_Count]),
    ok.
