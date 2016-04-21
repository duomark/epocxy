%%%------------------------------------------------------------------------------
%%% @copyright (c) 2015-2016, DuoMark International, Inc.
%%% @author Jay Nelson <jay@duomark.com>
%%% @reference 2015-2016 Development sponsored by TigerText, Inc. [http://tigertext.com/]
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
-export([init/1, start_pid/2, send_msg/2]).

%% For testing only
-export([say_to/2]).

-type fount   () :: cxy_fount:fount_ref().
-type stamp   () :: erlang:timestamp().
-type pid_msg () :: any().

-spec init      (any())             -> stamp().
-spec start_pid (fount(),  stamp()) -> pid()  | {error, Reason::any()}.
-spec send_msg  (Worker, pid_msg()) -> Worker | {error, Reason::any()}
                                           when Worker :: pid().

init(_) ->
    os:timestamp().
    
start_pid(Fount, Started) ->
    cxy_fount:spawn_worker(Fount, fun wait_for_hello/1, [Started]).
    
send_msg(Worker, Msg) ->
    spawn_link(fun() -> say_to(Worker, Msg) end),
    Worker.

%% Idle workers may wait a while before being used in a test.
wait_for_hello(Started) ->
    receive
        {Ref, From,      hello} -> reply(Started, From, Ref, goodbye);
        {Ref, From, Unexpected} -> reply(Started, From, Ref, {unexpected, Unexpected})
    after 30000 -> wait_for_hello_timeout
    end.

reply(Started, From, Ref, Msg) ->
    Elapsed = timer:now_diff(os:timestamp(), Started),
    From ! {Ref, Msg, now(), {elapsed, Elapsed}}.

%% Just verify the goodbye response comes after saying hello.
say_to(Worker, Msg) ->
    Ref = make_ref(),
    Now1 = now(),
    Worker ! {Ref, self(), Msg},
    %% now() is used to guarantee monotonic increasing time
    receive {Ref, goodbye, Now2} -> true = Now1 < Now2
    after 1000 -> throw(say_hello_timeout)
    end.
