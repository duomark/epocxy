%%%------------------------------------------------------------------------------
%%% @copyright (c) 2014-2015, DuoMark International, Inc.
%%% @author Jay Nelson <jay@duomark.com>
%%% @reference 2014-2015 Development sponsored by TigerText, Inc. [http://tigertext.com/]
%%% @reference The license is based on the template for Modified BSD from
%%%   <a href="http://opensource.org/licenses/BSD-3-Clause">OSI</a>
%%% @doc
%%%   The epocxy supervisor provides a supervised FSM to manage any ets
%%%   tables that are needed by the concurrency library. This FSM is
%%%   intended to outlive any patterns which are built on ets tables.
%%%
%%% @since v0.9.8d
%%% @end
%%%------------------------------------------------------------------------------
-module(epocxy_sup).
-author('Jay Nelson <jay@duomark.com>').

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).


%% ===================================================================
%% API functions
%% ===================================================================

-spec start_link() -> {ok, pid()}.

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, {}).


%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

-type restart() :: {supervisor:strategy(), non_neg_integer(), non_neg_integer()}.
-type sup_init_return() :: {ok, {restart(), [supervisor:child_spec()]}}.

-spec init({}) -> sup_init_return().

-define(CHILD(__Mod, __Args), {__Mod, {__Mod, start_link, __Args}, temporary, 2000, worker, [__Mod]}).

init({}) ->
    Epocxy_Ets_Fsm = ?CHILD(epocxy_ets_fsm, []),
    {ok, { {one_for_one, 5, 60}, [Epocxy_Ets_Fsm]} }.
