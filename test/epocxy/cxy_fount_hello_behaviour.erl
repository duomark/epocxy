%%%------------------------------------------------------------------------------
%%% @copyright (c) 2015, DuoMark International, Inc.
%%% @author Jay Nelson <jay@duomark.com>
%%% @reference 2015 Development sponsored by TigerText, Inc. [http://tigertext.com/]
%%% @reference The license is based on the template for Modified BSD from
%%%   <a href="http://opensource.org/licenses/BSD-3-Clause">OSI</a>
%%% @doc
%%%   Example behaviour for cxy_fount testing.
%%%
%%% @since 0.9.9
%%% @end
%%%------------------------------------------------------------------------------
-module(cxy_fount_hello_behaviour).
-behaviour(cxy_fount).

%% Behaviour API
-export([start_pid/1, send_msg/2]).

%% For testing only
-export([say_to/2]).

-include("cxy_fount.hrl").

?START_FOUNT_PID(Fount, wait_for_hello).
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
    %% now() is used to guarantee monotonic increasing time
    receive {Ref, goodbye, Now2} -> true = Now1 < Now2
    after 1000 -> throw(say_hello_timeout)
    end.


