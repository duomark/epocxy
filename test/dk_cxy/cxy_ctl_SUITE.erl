-module(cxy_ctl_SUITE).
-auth('jay@duomark.com').
-vsn('').

-export([all/0, init_per_suite/1, end_per_suite/1]).
-export([
         check_no_timer_limits/1, check_with_timer_limits/1,
         check_atom_limits/1, check_limit_errors/1,
         check_concurrency_types/1
        ]).

-include_lib("common_test/include/ct.hrl").

-spec all() -> [atom()].

all() -> [
          check_no_timer_limits, check_with_timer_limits,
          check_atom_limits, check_limit_errors,
          check_concurrency_types
         ].

-type config() :: proplists:proplist().
-spec init_per_suite(config()) -> config().
-spec end_per_suite(config()) -> config().

init_per_suite(Config) -> Config.
end_per_suite(Config)  -> Config.

%% Test Modules is ?TM
-define(TM, cxy_ctl).

check_no_timer_limits(_Config) ->
    Limits = [{a, 15, 0}, {b, 35, 0}],
    ok = ?TM:init(Limits),
    All_Entries = ets:tab2list(?TM),
    2 = length(All_Entries),
    true = lists:member({a, 15, 0, 0}, All_Entries),
    true = lists:member({b, 35, 0, 0}, All_Entries),
    ok.

check_with_timer_limits(_Config) ->
    Limits = [{a, 15, 5}, {b, 35, 0}, {c, 17, 4}],
    ok = ?TM:init(Limits),
    All_Entries = ets:tab2list(?TM),
    3 = length(All_Entries),
    true = lists:member({a, 15, 0, 5}, All_Entries),
    true = lists:member({b, 35, 0, 0}, All_Entries),
    true = lists:member({c, 17, 0, 4}, All_Entries),
    ok.

check_atom_limits(_Config) ->
    Limits = [{a, unlimited, 0},   {b, unlimited, 5},
              {c, inline_only, 0}, {d, inline_only, 7}],
    ok = ?TM:init(Limits),
    All_Entries = ets:tab2list(?TM),
    4 = length(All_Entries),
    true = lists:member({a, -1, 0, 0}, All_Entries),
    true = lists:member({b, -1, 0, 5}, All_Entries),
    true = lists:member({c,  0, 0, 0}, All_Entries),
    true = lists:member({d,  0, 0, 7}, All_Entries),
    ok.

check_limit_errors(_Config) ->
    Limits1 = [{a, unlimited, -1}, {b, 5, 0}, {c, unlimited, 0}],
    {error, {invalid_init_args, [{a, unlimited, -1}]}} = ?TM:init(Limits1),
    Limits2 = [{a, unlimited, -1}, {b, foo, 0}, {c, 0, bar}],
    {error, {invalid_init_args, Limits2}} = ?TM:init(Limits2),
    ok.

check_concurrency_types(_Config) ->
    Limits = [{a, unlimited, 0}, {b, 17, 5}, {c, 8, 0}, {d, inline_only, 7}],
    ok = ?TM:init(Limits),
    Types = ?TM:concurrency_types(),
    [[a, -1, 0, 0], [b, 17, 0, 5], [c, 8, 0, 0], [d, 0, 0, 7]]
        = [[proplists:get_value(P, This_Type_Props)
            || P <- [task_type, max_procs, active_procs, max_history]]
           || This_Type_Props <- Types],
    ok.
