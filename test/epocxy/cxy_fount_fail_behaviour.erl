%%%------------------------------------------------------------------------------
%%% @copyright (c) 2015-2016, DuoMark International, Inc.
%%% @author Jay Nelson <jay@duomark.com>
%%% @reference 2015-2016 Development sponsored by TigerText, Inc. [http://tigertext.com/]
%%% @reference The license is based on the template for Modified BSD from
%%%   <a href="http://opensource.org/licenses/BSD-3-Clause">OSI</a>
%%% @doc
%%%   Example behaviour for cxy_fount failure testing.
%%%
%%% @since 0.9.9
%%% @end
%%%------------------------------------------------------------------------------
-module(cxy_fount_fail_behaviour).
-behaviour(cxy_fount).

%% Behaviour API
-export([start_pid/1, send_msg/2]).

start_pid (_Fount)        -> bad_pid.
send_msg  (_Worker, _Msg) -> spawn_link (fun() -> ok end).
