-module(example_gen_event).
-behaviour(gen_event).

-export([init/1, handle_event/2, handle_call/2, handle_info/2,
         terminate/2, code_change/3]).

-record(ege_state, {
          num_events       = 0 :: non_neg_integer(),
          unexpected_calls = 0 :: non_neg_integer(),
          unexpected_infos = 0 :: non_neg_integer()
         }).
-type ege_state() :: #ege_state{}.

-spec init({}) -> ege_state().

init({}) ->
    {ok, #ege_state{}}.

-spec handle_event(tuple(), ege_state()) -> {ok, ege_state()}.

handle_event(Event, #ege_state{num_events=Num_Events} = State) ->
    io:format("Got event ~p~n", [Event]),
    {ok, State#ege_state{num_events=Num_Events + 1}}.

%%% Unused callbacks
-spec handle_call (any(), ege_state())        -> {ok, ege_state()}.
-spec handle_info (any(), ege_state())        -> {ok, ege_state()}.
-spec terminate   (any(), ege_state())        ->  ok.
-spec code_change (any(), ege_state(), any()) -> {ok, ege_state()}.

handle_call(Unexpected_Event, #ege_state{unexpected_calls=Unexpected_Call} = State) ->
    io:format("Got unexpected event ~p~n", [Unexpected_Event]),
    {ok, State#ege_state{unexpected_calls=Unexpected_Call + 1}}.

handle_info(Unexpected_Event, #ege_state{unexpected_infos=Unexpected_Info} = State) ->
    io:format("Got unexpected info ~p~n", [Unexpected_Event]),
    {ok, State#ege_state{unexpected_infos=Unexpected_Info + 1}}.

terminate(_Args, #ege_state{num_events=NE, unexpected_calls=UC, unexpected_infos=UI}) ->
    io:format("Num Events: ~p  Bad Calls: ~p  Bad Infos: ~p~n", [NE, UC, UI]),
    ok.

code_change(_OldVsn, #ege_state{} = State, _Extra) ->
    {ok, State}.
