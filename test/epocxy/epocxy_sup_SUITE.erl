%%%------------------------------------------------------------------------------
%%% @copyright (c) 2015, DuoMark International, Inc.
%%% @author Jay Nelson <jay@duomark.com>
%%% @reference 2015 Development sponsored by TigerText, Inc. [http://tigertext.com/]
%%% @reference The license is based on the template for Modified BSD from
%%%   <a href="http://opensource.org/licenses/BSD-3-Clause">OSI</a>
%%% @doc
%%%   Tests for epocxy_sup.
%%%
%%% @since 0.9.8e
%%% @end
%%%------------------------------------------------------------------------------
-module(epocxy_sup_SUITE).
-auth('jay@duomark.com').
-vsn('').

-export([all/0, init_per_suite/1, end_per_suite/1]).
-export([check_create/1]).

-include_lib("common_test/include/ct.hrl").

-spec all() -> [atom()].

all() -> [check_create].

-type config() :: proplists:proplist().
-spec init_per_suite (config()) -> config().
-spec end_per_suite  (config()) -> config().

init_per_suite (Config) -> Config.
end_per_suite  (Config)  -> Config.

%% Test Module is ?TM
-define(TM, epocxy_sup).

-spec check_create(proplists:proplist()) -> ok.
check_create(_Config) ->
    undefined = whereis(?TM),
    {ok, Pid} = ?TM:start_link(),
    Pid       = whereis(?TM),
    Ets_Pid   = whereis(epocxy_ets_fsm),
    [{epocxy_ets_fsm, Ets_Pid, worker, [epocxy_ets_fsm]}]
        = supervisor:which_children(?TM),
    cleanup(Pid, Ets_Pid),
    ok.

cleanup(Pid, Ets_Pid) ->
    supervisor:terminate_child(Pid, Ets_Pid),
    unlink(Pid),
    exit(Pid, kill).
