-module(example_fount).
-behaviour(cxy_fount).

%%% External API
-export([init/1, start_pid/2, send_msg/2]).

%%% Export spawned and linked to the fount.
-export([wait_for_request/1]).


%%%===================================================================
%%% Behaviour callback functions
%%%===================================================================

-spec init      (any()) -> {}.
-spec start_pid (cxy_fount:fount_ref(), {}) -> pid()  | {error, Reason::any()}.
-spec send_msg  (Worker,             any()) -> Worker | {error, Reason::any()}
                                                   when Worker :: pid().
init(_) -> {}.

start_pid(Fount, Config) ->
    cxy_fount:spawn_worker(Fount, ?MODULE, wait_for_request, [Config]).

send_msg(Worker_Pid, Msg) ->
    cxy_fount:send_msg(Worker_Pid, Msg).

                              
%%%===================================================================
%%% Worker implementation: response to messages
%%%===================================================================

-spec wait_for_request({}) -> no_return().

wait_for_request({} = _Config) ->
    receive
        Msg -> io:format("~p Got msg: ~p~n", [self(), Msg])
    end.
