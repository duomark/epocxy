%%%------------------------------------------------------------------------------
%%% @copyright (c) 2016, DuoMark International, Inc.
%%% @author Jay Nelson <jay@duomark.com>
%%% @reference 2016 Development sponsored by TigerText, Inc. [http://tigertext.com/]
%%% @reference The license is based on the template for Modified BSD from
%%%   <a href="http://opensource.org/licenses/BSD-3-Clause">OSI</a>
%%% @doc
%%%   Supervisor to manage cxy_regulator + cxy_fount. These two servers
%%%   cooperate to spawn new workers, but control the pace at which workers
%%%   are allowed to start.
%%%
%%% @since v1.1.0
%%% @end
%%%------------------------------------------------------------------------------
-module(cxy_fount_sup).
-author('Jay Nelson <jay@duomark.com>').

-behaviour(supervisor).

%%% API
-export([start_link/2, start_link/3, start_link/4,
         get_fount/1, get_regulator/1]).

%%% Supervisor callbacks
-export([init/1]).


%%%===================================================================
%%% API functions
%%%===================================================================

-spec start_link(        module(), cxy_fount:fount_args())                            -> {ok, pid()}.
-spec start_link(        module(), cxy_fount:fount_args(), cxy_fount:fount_options()) -> {ok, pid()};
                (atom(), module(), cxy_fount:fount_args())                            -> {ok, pid()}.
-spec start_link(atom(), module(), cxy_fount:fount_args(), cxy_fount:fount_options()) -> {ok, pid()}.

start_link(Fount_Behaviour, Init_Args)
  when is_atom(Fount_Behaviour), is_list(Init_Args) ->
    supervisor:start_link(?MODULE, {Fount_Behaviour, Init_Args}).

start_link(Fount_Behaviour, Init_Args, Fount_Options)
  when is_atom(Fount_Behaviour),
       is_list(Init_Args), is_list(Fount_Options) ->
    supervisor:start_link(?MODULE, {Fount_Behaviour, Init_Args, Fount_Options});
start_link(Fount_Name, Fount_Behaviour, Init_Args)
  when is_atom(Fount_Name), is_atom(Fount_Behaviour), is_list(Init_Args) ->
    supervisor:start_link({local, make_sup_name(Fount_Name)}, ?MODULE,
                          {Fount_Name, Fount_Behaviour, Init_Args}).


start_link(Fount_Name, Fount_Behaviour, Init_Args, Fount_Options)
  when is_atom(Fount_Name), is_atom(Fount_Behaviour),
       is_list(Init_Args),  is_list(Fount_Options) ->
    supervisor:start_link({local, make_sup_name(Fount_Name)}, ?MODULE,
                          {Fount_Name, Fount_Behaviour, Init_Args, Fount_Options}).

make_sup_name(Fount_Name) ->
    list_to_atom(atom_to_list(Fount_Name) ++ "_sup").
    
    
-spec get_fount     (pid()) -> pid().
-spec get_regulator (pid()) -> pid().

get_fount(Fount_Sup) ->
    hd([Pid || {cxy_fount,     Pid, worker, _Modules} <- supervisor:which_children(Fount_Sup)]).

get_regulator(Fount_Sup) ->
    hd([Pid || {cxy_regulator, Pid, worker, _Modules} <- supervisor:which_children(Fount_Sup)]).


%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================

-type restart() :: {supervisor:strategy(), non_neg_integer(), non_neg_integer()}.
-type sup_init_return() :: {ok, {restart(), [supervisor:child_spec()]}}.

-define(CHILD(__Mod, __Args), {__Mod, {__Mod, start_link, __Args}, temporary, 2000, worker, [__Mod]}).

%%% Init without or with Fount_Name.
-spec init({        module(), cxy_fount:fount_args()})                            -> sup_init_return();
          ({atom(), module(), cxy_fount:fount_args()})                            -> sup_init_return();
          ({        module(), cxy_fount:fount_args(), cxy_fount:fount_options()}) -> sup_init_return;
          ({atom(), module(), cxy_fount:fount_args(), cxy_fount:fount_options()}) -> sup_init_return().

init({Fount_Behaviour, Init_Args})
  when is_atom(Fount_Behaviour), is_list(Init_Args) ->
    Fount_Args     = [self(), Fount_Behaviour, Init_Args, []],
    Regulator_Args = [],
    init_internal(Fount_Args, Regulator_Args);

init({Fount_Behaviour, Init_Args, Fount_Options})
  when is_atom(Fount_Behaviour),
       is_list(Init_Args), is_list(Fount_Options) ->
    Fount_Args     = [self(), Fount_Behaviour, Init_Args, Fount_Options],
    Regulator_Args = case proplists:lookup(time_slice, Fount_Options) of
                         none              -> [];
                         Time_Slice_Option -> [Time_Slice_Option]
                     end,
    init_internal(Fount_Args, Regulator_Args);

init({Fount_Name, Fount_Behaviour, Init_Args})
  when is_atom(Fount_Name), is_atom(Fount_Behaviour), is_list(Init_Args) ->
    Fount_Args     = [self(), Fount_Name, Fount_Behaviour, Init_Args, []],
    Regulator_Args = [],
    init_internal(Fount_Args, Regulator_Args);

init({Fount_Name, Fount_Behaviour, Init_Args, Fount_Options})
  when is_atom(Fount_Name), is_atom(Fount_Behaviour),
       is_list(Init_Args),  is_list(Fount_Options) ->
    Fount_Args     = [self(), Fount_Name, Fount_Behaviour, Init_Args, Fount_Options],
    Regulator_Args = case proplists:lookup(time_slice, Fount_Options) of
                         none              -> [];
                         Time_Slice_Option -> [Time_Slice_Option]
                     end,
    init_internal(Fount_Args, Regulator_Args).

%%% Internal init only differs in the cxy_fount:start_link args.
init_internal(Fount_Args, Regulator_Args) ->
    Fount_Fsm     = ?CHILD(cxy_fount,     Fount_Args),
    Regulator_Fsm = ?CHILD(cxy_regulator, Regulator_Args),
    {ok, { {rest_for_one, 5, 60}, [Regulator_Fsm, Fount_Fsm]} }.
