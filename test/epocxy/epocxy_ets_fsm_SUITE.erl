%%%------------------------------------------------------------------------------
%%% @copyright (c) 2015, DuoMark International, Inc.
%%% @author Jay Nelson <jay@duomark.com>
%%% @reference 2015 Development sponsored by TigerText, Inc. [http://tigertext.com/]
%%% @reference The license is based on the template for Modified BSD from
%%%   <a href="http://opensource.org/licenses/BSD-3-Clause">OSI</a>
%%% @doc
%%%   Tests for creating / deleting ets tables owned by epocxy_ets_fsm.
%%%
%%% @since 0.9.8e
%%% @end
%%%------------------------------------------------------------------------------
-module(epocxy_ets_fsm_SUITE).
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

init_per_suite (Config)  -> Config.
end_per_suite  (Config)  -> Config.

%% Test Module is ?TM
-define(TM, epocxy_ets_fsm).

%%%------------------------------------------------------------------------------
%%% Unit tests for cxy_cache core
%%%------------------------------------------------------------------------------


%% Validate any atom can be used as a cache_name and info/1 will report properly.
-spec check_create(config()) -> ok.
check_create(_Config) ->
    epocxy_sup:start_link(),

    ct:log("Create and delete unnamed ets tables"),
    Fsm_Pid = whereis(?TM),
    [ok,ok,ok,ok] = [validate_create_table(Fsm_Pid, Cxy_Type, no_name)
                     || Cxy_Type <- [none, read_only, write_only, read_and_write]],

    ct:log("Create and delete named ets tables"),
    Fsm_Pid = whereis(?TM),
    [ok,ok,ok,ok] = [validate_create_table(Fsm_Pid, Cxy_Type, Name)
                     || {Name, Cxy_Type} <- [{t1, none},       {t2, read_only},
                                             {t3, write_only}, {t4, read_and_write}]],
    
    ct:comment("Successfully tested creating and deleting ets tables"),
    cleanup(whereis(epocxy_sup), Fsm_Pid),
    ok.

cleanup(Pid, Fsm_Pid) ->
    supervisor:terminate_child(Pid, Fsm_Pid),
    unlink(Pid),
    exit(Pid, kill).

validate_create_table(Fsm_Pid, Cxy_Type, Named) ->
    Tid = case Named of
              no_name -> ?TM:create_ets_table(Cxy_Type);
              Named   -> ?TM:create_ets_table(Named, Cxy_Type)
          end,
    [Fsm_Pid, 0, 2, public, set]
        = [ets:info(Tid, Attr) || Attr <- [owner, size, keypos, protection, type]],
    case Named of
        no_name -> [no_name, false] = [ets:info(Tid, Attr) || Attr <- [name, named_table]];
        Named   -> [Named,    true] = [ets:info(Tid, Attr) || Attr <- [name, named_table]]
    end,
    ok = ?TM:delete_ets_table(Tid),
    ok.
