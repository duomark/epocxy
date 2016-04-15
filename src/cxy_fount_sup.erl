%%%------------------------------------------------------------------------------
%%% @copyright (c) 2016, DuoMark International, Inc.
%%% @author Jay Nelson <jay@duomark.com>
%%% @reference 2016 Development sponsored by TigerText, Inc. [http://tigertext.com/]
%%% @reference The license is based on the template for Modified BSD from
%%%   <a href="http://opensource.org/licenses/BSD-3-Clause">OSI</a>
%%% @doc
%%%   Supervisor to manage cxy_regulator + cxy_fount. These two servers
%%%   cooperate to spawn new workers, but control the pace at which they
%%%   are allowed to start.
%%%
%%% @since v1.0.0
%%% @end
%%%------------------------------------------------------------------------------
-module(cxy_fount_sup).
-author('Jay Nelson <jay@duomark.com>').

-behaviour(supervisor).

%% API
-export([start_link/2, start_link/3, start_link/4, start_link/5,start_link/6, get_regulator/1]).

%% Supervisor callbacks
-export([init/1]).


%% ===================================================================
%% API functions
%% ===================================================================

-spec start_link(        module(), list())                               -> {ok, pid()}.

-spec start_link(        module(), list(), cxy_fount:notifier())         -> {ok, pid()};
                (atom(), module(), list())                               -> {ok, pid()}.

-spec start_link(atom(), module(), list(), cxy_fount:notifier())         -> {ok, pid()};
                (        module(), list(), pos_integer(), pos_integer()) -> {ok, pid()}.

-spec start_link(        module(), list(), pos_integer(), pos_integer(), cxy_fount:notifier()) -> {ok, pid()};
                (atom(), module(), list(), pos_integer(), pos_integer())                       -> {ok, pid()}.

-spec start_link(atom(), module(), list(), pos_integer(), pos_integer(), cxy_fount:notifier()) -> {ok, pid()}.

start_link(Fount_Behaviour, Init_Args) 
  when is_atom(Fount_Behaviour), is_list(Init_Args) ->
    supervisor:start_link(?MODULE, {Fount_Behaviour, Init_Args}).

start_link(Fount_Behaviour, Init_Args, Notifier)
  when is_atom(Fount_Behaviour), is_list(Init_Args),
       is_pid(Notifier) orelse Notifier =:= no_notifier ->
    supervisor:start_link(?MODULE, {Fount_Behaviour, Init_Args, Notifier});
start_link(Fount_Name, Fount_Behaviour, Init_Args)
  when is_atom(Fount_Name), is_atom(Fount_Behaviour), is_list(Init_Args) ->
    supervisor:start_link({local, make_sup_name(Fount_Name)}, ?MODULE,
                          {Fount_Name, Fount_Behaviour, Init_Args}).

start_link(Fount_Behaviour, Init_Args, Slab_Size, Reservoir_Depth)
  when is_atom(Fount_Behaviour),
       is_list(Init_Args),
       is_integer(Slab_Size),       Slab_Size > 0,
       is_integer(Reservoir_Depth), Reservoir_Depth >= 2 ->
    supervisor:start_link(?MODULE, {Fount_Behaviour, Init_Args, Slab_Size, Reservoir_Depth});
start_link(Fount_Name, Fount_Behaviour, Init_Args, Notifier)
  when is_atom(Fount_Name), is_atom(Fount_Behaviour), is_list(Init_Args),
       is_pid(Notifier) orelse Notifier =:= no_notifier ->
    supervisor:start_link({local, make_sup_name(Fount_Name)}, ?MODULE,
                          {Fount_Name, Fount_Behaviour, Init_Args, Notifier}).

start_link(Fount_Behaviour, Init_Args, Slab_Size, Reservoir_Depth, Notifier)
  when is_atom(Fount_Behaviour),
       is_list(Init_Args),
       is_integer(Slab_Size),       Slab_Size > 0,
       is_integer(Reservoir_Depth), Reservoir_Depth >= 2,
       is_pid(Notifier) orelse Notifier =:= no_notifier ->
    supervisor:start_link(?MODULE,
                          {Fount_Behaviour, Init_Args, Slab_Size, Reservoir_Depth, Notifier});
start_link(Fount_Name, Fount_Behaviour, Init_Args, Slab_Size, Reservoir_Depth)
  when is_atom(Fount_Behaviour),
       is_list(Init_Args),
       is_integer(Slab_Size),       Slab_Size > 0,
       is_integer(Reservoir_Depth), Reservoir_Depth >= 2 ->
    supervisor:start_link({local, make_sup_name(Fount_Name)}, ?MODULE,
                           {Fount_Behaviour, Init_Args, Slab_Size, Reservoir_Depth}).

start_link(Fount_Name, Fount_Behaviour, Init_Args, Slab_Size, Reservoir_Depth, Notifier)
  when is_atom(Fount_Behaviour),
       is_list(Init_Args),
       is_integer(Slab_Size),       Slab_Size > 0,
       is_integer(Reservoir_Depth), Reservoir_Depth >= 2,
       is_pid(Notifier) orelse Notifier =:= no_notifier ->
    supervisor:start_link({local, make_sup_name(Fount_Name)}, ?MODULE,
                           {Fount_Behaviour, Init_Args, Slab_Size, Reservoir_Depth, Notifier}).

make_sup_name(Fount_Name) ->
    list_to_atom(atom_to_list(Fount_Name) ++ "_sup").

-spec get_regulator(pid()) -> pid().

get_regulator(Fount_Sup) ->
    hd([Pid || {cxy_regulator, Pid, worker, _Modules} <- supervisor:which_children(Fount_Sup)]).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

-type restart() :: {supervisor:strategy(), non_neg_integer(), non_neg_integer()}.
-type sup_init_return() :: {ok, {restart(), [supervisor:child_spec()]}}.

-define(CHILD(__Mod, __Args), {__Mod, {__Mod, start_link, __Args}, temporary, 2000, worker, [__Mod]}).

%%% Init without or with Fount_Name.
-spec init({        module(), list()})                               -> sup_init_return();
          ({        module(), list(), pos_integer(), pos_integer()}) -> sup_init_return();
          ({atom(), module(), list()})                               -> sup_init_return();
          ({atom(), module(), list(), pos_integer(), pos_integer()}) -> sup_init_return().

init({Fount_Behaviour, Init_Args})
  when is_atom(Fount_Behaviour), is_list(Init_Args) ->
    Fount_Args = [self(), Fount_Behaviour, Init_Args],
    init(internal, Fount_Args);

init({Fount_Behaviour, Init_Args, Notifier})
  when is_atom(Fount_Behaviour), is_list(Init_Args),
       is_pid(Notifier) orelse Notifier =:= no_notifier ->
    Fount_Args = [self(), Fount_Behaviour, Init_Args, Notifier],
    init(internal, Fount_Args);

init({Fount_Behaviour, Init_Args, Slab_Size, Reservoir_Depth})
  when is_atom(Fount_Behaviour),
       is_list(Init_Args),
       is_integer(Slab_Size),       Slab_Size > 0,
       is_integer(Reservoir_Depth), Reservoir_Depth >= 2 ->
    Fount_Args = [self(), Fount_Behaviour, Init_Args, Slab_Size, Reservoir_Depth],
    init(internal, Fount_Args);

init({Fount_Name, Fount_Behaviour, Init_Args})
  when is_atom(Fount_Name), is_atom(Fount_Behaviour), is_list(Init_Args) ->
    Fount_Args = [self(), Fount_Name, Fount_Behaviour, Init_Args],
    init(internal, Fount_Args);

init({Fount_Name, Fount_Behaviour, Init_Args, Notifier})
  when is_atom(Fount_Name), is_atom(Fount_Behaviour), is_list(Init_Args),
       is_pid(Notifier) orelse Notifier =:= no_notifier ->
    Fount_Args = [self(), Fount_Name, Fount_Behaviour, Init_Args, Notifier],
    init(internal, Fount_Args);

init({Fount_Behaviour, Init_Args, Slab_Size, Reservoir_Depth, Notifier})
  when is_atom(Fount_Behaviour),
       is_list(Init_Args),
       is_integer(Slab_Size),       Slab_Size > 0,
       is_integer(Reservoir_Depth), Reservoir_Depth >= 2,
       is_pid(Notifier) orelse Notifier =:= no_notifier ->
    Fount_Args = [self(), Fount_Behaviour, Init_Args, Slab_Size, Reservoir_Depth, Notifier],
    init(internal, Fount_Args);

init({Fount_Name, Fount_Behaviour, Init_Args, Slab_Size, Reservoir_Depth})
  when is_atom(Fount_Name),
       is_atom(Fount_Behaviour),
       is_list(Init_Args),
       is_integer(Slab_Size),       Slab_Size > 0,
       is_integer(Reservoir_Depth), Reservoir_Depth >= 2 ->
    Fount_Args = [self(), Fount_Behaviour, Init_Args, Slab_Size, Reservoir_Depth],
    init(internal, Fount_Args);

init({Fount_Name, Fount_Behaviour, Init_Args, Slab_Size, Reservoir_Depth, Notifier})
  when is_atom(Fount_Name),
       is_atom(Fount_Behaviour),
       is_list(Init_Args),
       is_integer(Slab_Size),       Slab_Size > 0,
       is_integer(Reservoir_Depth), Reservoir_Depth >= 2,
       is_pid(Notifier) orelse Notifier =:= no_notifier ->
    Fount_Args = [self(), Fount_Behaviour, Init_Args, Slab_Size, Reservoir_Depth, Notifier],
    init(internal, Fount_Args).

%%% Internal init only differs in the cxy_fount:start_link args.
init(internal, Fount_Args) ->
    Fount_Fsm     = ?CHILD(cxy_fount,     Fount_Args),
    Regulator_Fsm = ?CHILD(cxy_regulator, []),
    {ok, { {rest_for_one, 5, 60}, [Regulator_Fsm, Fount_Fsm]} }.