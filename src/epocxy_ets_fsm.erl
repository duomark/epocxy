%%%------------------------------------------------------------------------------
%%% @copyright (c) 2014-2014, DuoMark International, Inc.
%%% @author Jay Nelson <jay@duomark.com>
%%% @reference 2014-2015 Development sponsored by TigerText, Inc. [http://tigertext.com/]
%%% @reference The license is based on the template for Modified BSD from
%%%   <a href="http://opensource.org/licenses/BSD-3-Clause">OSI</a>
%%% @doc
%%%   The epocxy ETS FSM is the owner of all ETS tables needed when
%%%   constructing ets_buffers and other patterns. It allows the
%%%   generation of an unnamed ETS table so that atomic operations
%%%   which swap one table for another to replace a set of data can
%%%   be efficiently implemented without worrying about the ETS
%%%   owning process dying and losing the table.
%%%
%%%   Tables are created as sets with public access and a key that
%%%   is the first field of a record. The caller may request read
%%%   and/or write concurrency to be set. The purpose of generated
%%%   tables is for maximum concurrency, so they must be accessible
%%%   by all processes.
%%%
%%% @since v0.9.8d
%%% @end
%%%------------------------------------------------------------------------------
-module(epocxy_ets_fsm).
-author('Jay Nelson <jay@duomark.com>').

-behaviour(gen_fsm).

%% API
-export([
         start_link/0,
         create_ets_table/1, create_ets_table/2,
         delete_ets_table/1
        ]).

%% gen_fsm state functions
-export(['READY'/2, 'READY'/3]).

%% gen_fsm callbacks
-export([init/1, terminate/3, code_change/4,
         handle_event/3, handle_sync_event/4, handle_info/3]).

-define(SERVER, ?MODULE).

-record(eef_state, {}).


%%%===================================================================
%%% External API
%%%===================================================================

-type ets_concurrency() :: none | read_only | write_only | read_and_write.
-export_type([ets_concurrency/0]).

-spec start_link() -> {ok, pid()}.
-spec create_ets_table(ets_concurrency()) -> ets:tid().
-spec create_ets_table(atom(), ets_concurrency()) -> ets:tid().
-spec delete_ets_table(atom() | ets:tid()) -> ok.

start_link() ->
    gen_fsm:start_link({local, ?SERVER}, ?MODULE, {}, []).

create_ets_table(Concurrency_Type) ->
    Options = make_options(Concurrency_Type),
    gen_fsm:sync_send_event(?SERVER, {create_ets_table, Options}).

create_ets_table(Name, Concurrency_Type)
  when is_atom(Name) ->
    Options = [named_table | make_options(Concurrency_Type)],
    gen_fsm:sync_send_event(?SERVER, {create_ets_table, Name, Options}).

delete_ets_table(Table_Id_Or_Name) ->
    gen_fsm:sync_send_event(?SERVER, {delete_ets_table, Table_Id_Or_Name}).

make_options(none)           -> make_base_options([]);
make_options(read_only)      -> make_base_options([{read_concurrency,  true}]);
make_options(write_only)     -> make_base_options([{write_concurrency, true}]);
make_options(read_and_write) -> make_base_options([{read_concurrency,  true},
                                                   {write_concurrency, true}]).

make_base_options(Concurrency_Options) ->
    [set, public, {keypos, 2}] ++ Concurrency_Options.
    

%%%===================================================================
%%% gen_fsm callbacks
%%%===================================================================

-type internal_state() :: #eef_state{}.
-type state_name()     :: 'READY'.
-type create_ets_cmd() :: {create_ets_table, proplists:proplist()}
                        | {create_ets_table, Name::atom(), proplists:proplist()}.
-type delete_ets_cmd() :: {delete_ets_table, ets:tid() | atom()}.
-type stop_cmd()       :: stop.
%% -type ready_cmds()     :: create_ets_cmd() | delete_ets_cmd() | stop_cmd().

-spec init({}) -> {ok, 'READY', internal_state()}.
-spec terminate   (any(), state_name(), internal_state())        -> ok.
-spec code_change (any(), state_name(), internal_state(), any()) -> {ok, state_name(), #eef_state{}}.

init({}) ->
    {ok, 'READY', #eef_state{}}.

terminate   (_Reason, _State_Name, #eef_state{}) -> ok.
code_change (_OldVsn,  State_Name, #eef_state{} = State, _Extra) -> {ok, State_Name, State}.

%% The FSM has only the 'READY' state.
-type from() :: {pid(), reference()}.
-spec 'READY'(create_ets_cmd(), from(), internal_state()) -> ets:tid();
             (delete_ets_cmd(), from(), internal_state()) -> ok;
             (stop_cmd(),       from(), internal_state()) -> {stop, normal}.

'READY'({create_ets_table, Name, Options}, _From, #eef_state{} = State) when is_atom(Name), is_list(Options)   -> {reply, ets:new(Name,   Options), 'READY', State};
'READY'({create_ets_table, Options},       _From, #eef_state{} = State) when                is_list(Options)   -> {reply, ets:new(noname, Options), 'READY', State};
'READY'({delete_ets_table, Table},         _From, #eef_state{} = State) when is_atom(Table); is_integer(Table) -> {reply, ets:delete(Table),        'READY', State};

%% Stop the ets owner FSM process.
'READY'(stop, _From, #eef_state{}) -> {stop, normal}.

%% No asynch events are expected.
'READY'(_Any, #eef_state{} = State) -> {next_state, 'READY', State}.

-spec handle_info (any(), state_name(), internal_state())
                  -> {next_state, state_name(), internal_state()}.

handle_info (_Info, State_Name, #eef_state{} = State) ->
    {next_state, State_Name, State}.

%% Handle event and sync_event aren't used
-spec handle_event(any(), state_name(), internal_state())
                  -> {next_state, state_name(), internal_state()}.
-spec handle_sync_event(any(), {pid(), reference()}, state_name(), internal_state())
                       -> {reply, any(), state_name(), internal_state()}.

handle_event      (_Event,        State_Name, #eef_state{} = State) -> {next_state, State_Name, State}.
handle_sync_event (_Event, _From, State_Name, #eef_state{} = State) -> {reply, ok,  State_Name, State}.
