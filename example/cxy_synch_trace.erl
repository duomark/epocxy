%%%------------------------------------------------------------------------------
%%% @copyright (c) 2014, DuoMark International, Inc.
%%% @author Jay Nelson <jay@duomark.com> [http://duomark.com/]
%%% @reference 2013 Development sponsored by TigerText, Inc. [http://tigertext.com/]
%%% @reference The license is based on the template for Modified BSD from
%%%   <a href="http://opensource.org/licenses/BSD-3-Clause">OSI</a>
%%% @doc
%%%   Synchronization barriers force multiple processes to pause until all
%%%   participants reach the same point. They then may proceed independently.
%%%
%%% @since 0.9.8
%%% @end
%%%------------------------------------------------------------------------------
-module(cxy_synch_trace).
-author('Jay Nelson <jay@duomark.com>').

-include("tracing_levels.hrl").

%% External API
-export([
         example/0,
         start_debug/0,
         start/0,
         start/2
        ]).

example() ->
    start(),
    F = fun() -> 3+4 end,
    cxy_synch:before_task(3, F).

start()       -> start("cxy_synch:before_task", ?TRACE_TIMINGS).
start_debug() -> start("cxy_synch:before_task", ?TRACE_DEBUG).

start(Title, Trace_Level) ->
    et_viewer:start([
                     {title,         Title},
                     {trace_global,  true},
                     {trace_pattern, {et, Trace_Level}},
                     {max_actors,    10}
                    ]),
    dbg:p(all,call),
    dbg:tpl(et, trace_me, 5, []),
    ok.
