%%%------------------------------------------------------------------------------
%%% @copyright (c) 2013-2015, DuoMark International, Inc.
%%% @author Jay Nelson <jay@duomark.com>
%%% @reference 2013-2015 Development sponsored by TigerText, Inc. [http://tigertext.com/]
%%% @reference The license is based on the template for Modified BSD from
%%%   <a href="http://opensource.org/licenses/BSD-3-Clause">OSI</a>
%%% @doc
%%%   Generational caching supervisor. Manages fsm children which are
%%%   owners of the underlying ets tables implementing the cache. The
%%%   ets tables need to be owned by long-lived processes which do
%%%   little computation to avoid crashing and losing the cached data.
%%%
%%%   As of v0.9.8c the poll frequency for cxy_cache_fsm cannot be less
%%%   than 10 milliseconds.
%%%
%%% @since v0.9.6
%%% @end
%%%------------------------------------------------------------------------------
-module(cxy_cache_sup).
-author('Jay Nelson <jay@duomark.com>').

-behaviour(supervisor).

%% API
-export([start_link/0, start_cache/2, start_cache/3, start_cache/4]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).


%% ===================================================================
%% API functions
%% ===================================================================

-spec start_link() -> {ok, pid()}.
-spec start_cache(cxy_cache:cache_name(), module())                      -> {ok, pid()}.
-spec start_cache(cxy_cache:cache_name(), module(), pos_integer())       -> {ok, pid()};
                 (cxy_cache:cache_name(), module(), cxy_cache:gen_fun()) -> {ok, pid()}.

-spec start_cache(cxy_cache:cache_name(), module(), cxy_cache:thresh_type(), pos_integer()) -> {ok, pid()}.

%% start_link creates the single metadata supervisor for all caches.
%% start_cache creates an instance of a cache as a child of this supervisor.

start_link() -> supervisor:start_link({local, ?SERVER}, ?MODULE, {}).

%% Start a non-generational cache (all data fits in memory).
start_cache(Cache_Name, Cache_Mod)
  when is_atom(Cache_Name), is_atom(Cache_Mod) ->
    Args = [Cache_Name, Cache_Mod],
    supervisor:start_child(?SERVER, Args).

%% Start a generational cache with a specific poll time.
start_cache(Cache_Name, Cache_Mod, Poll_Time)
  when is_atom(Cache_Name), is_atom(Cache_Mod),
       is_integer(Poll_Time), Poll_Time >= 10 ->
    Args = [Cache_Name, Cache_Mod, none, Poll_Time],
    supervisor:start_child(?SERVER, Args);

%% Start a generational cache which ages using a fun call.
start_cache(Cache_Name, Cache_Mod, Gen_Fun)
  when is_atom(Cache_Name), is_atom(Cache_Mod), is_function(Gen_Fun, 3) ->
    Args = [Cache_Name, Cache_Mod, Gen_Fun],
    supervisor:start_child(?SERVER, Args).

%% Start other types of caches.
start_cache(Cache_Name, Cache_Mod, Type, Threshold)
  when is_atom(Cache_Name), is_atom(Cache_Mod),
       (Type =:= count orelse Type =:= time),
       is_integer(Threshold), Threshold > 0 ->
    Args = [Cache_Name, Cache_Mod, Type, Threshold],
    supervisor:start_child(?SERVER, Args).


%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

-type restart() :: {supervisor:strategy(), non_neg_integer(), non_neg_integer()}.
-type sup_init_return() :: {ok, {restart(), [supervisor:child_spec()]}}.

-spec init({}) -> sup_init_return().

-define(CHILD(__Mod, __Args), {__Mod, {__Mod, start_link, __Args}, transient, 2000, worker, [__Mod]}).

init({}) ->
    Cache_Fsm = ?CHILD(cxy_cache_fsm, []),
    {ok, { {simple_one_for_one, 5, 10}, [Cache_Fsm]} }.
