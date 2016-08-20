%%%------------------------------------------------------------------------------
%%% @copyright (c) 2016, DuoMark International, Inc.
%%% @author Jay Nelson <jay@duomark.com>
%%% @reference The license is based on the template for Modified BSD from
%%%   <a href="http://opensource.org/licenses/BSD-3-Clause">OSI</a>
%%% @doc
%%%   An example cxy_fount behaviour which splits data into lines,
%%%   then uses separate cxy_fount allocated processes to format
%%%   a line-oriented dump of the data.
%%%
%%%   The following steps are used to format the data:
%%%
%%%     1) Grab a process for the source data
%%%     2) Read and split the source data to lines
%%%     3) Grab a process for each line plus a collector
%%%     4) Route the data to the line processes
%%%     5) Send expected summary data to collector
%%%     6) Collect formatted results in an array
%%%     7) Collector prints results out
%%%
%%% @since v1.1.0
%%% @end
%%%------------------------------------------------------------------------------
-module(hexdump_fount).
-author('Jay Nelson <jay@duomark.com>').

-behaviour(cxy_fount).

%%% External API
-export([format_data/2]).

%%% Behaviour API
-export([init/1, start_pid/2, send_msg/2]).

%%% Exported for spawn access
-export([formatter/2]).
-export([hex/1, split_lines/3, addr/1]).


%%%===================================================================
%%% External API
%%%===================================================================
format_data(Fount, Data) ->
    cxy_fount:task_pid(Fount, {load, Data}).


%%%===================================================================
%%% Behaviour callback datatypes and functions
%%%===================================================================
-type loader()      :: pid().
-type collector()   :: pid().
-type formatter()   :: pid().

-type data()        :: binary().
-type line()        :: binary().
-type hexchars()    :: binary().
-type window()      :: binary().

-type num_workers() :: pos_integer().
-type position()    :: pos_integer().
-type address()     :: pos_integer().

-type load_cmd()    :: {load,    data()}.
-type format_cmd()  :: {format,  position(), address(), line(),     collector()}.
-type collect_cmd() :: {collect, position(), address(), hexchars(), window(), worker()}
                     | {collect, num_workers()}.

-type worker()      :: loader()   | formatter()  | collector().
-type hexdump_cmd() :: load_cmd() | format_cmd() | collect_cmd().

-spec init({}) -> {}.
-spec start_pid(cxy_fount:fount_ref(), {}) -> pid().
-spec send_msg(Worker, hexdump_cmd()) -> Worker when Worker :: worker().

init({}) -> {}.

start_pid(Fount, State) ->
    cxy_fount:spawn_worker(Fount, ?MODULE, formatter, [Fount, State]).

send_msg(Worker, Msg) ->
    cxy_fount:send_msg(Worker, Msg).


%%%===================================================================
%%% Customized behaviour message handling
%%%   - all idle, untasked workers are waiting on this receive
%%%   - a single message arrives after unlinking from fount
%%%   - the freed worker runs to completion
%%%===================================================================
formatter(Fount, {}) ->

    %% Workers can be 1 loader, N formatters, or 1 collector
    %% arranged in a fan out -> fan in structure.
    %% Each responds to one particular message only.
    receive

        %% First stage data loader
        {load, Data} when is_binary(Data) ->
            Lines = split_lines(Data, [], 0),
            Num_Workers = length(Lines),
            [Collector | Workers] = cxy_fount:get_pids(Fount, Num_Workers+1),
            Collector ! {collect, Num_Workers},
            done = send_format_msgs(Workers, Lines, Collector);

        %% Data formatting worker
        {format, Position, Address, Line, Collector} ->
            Collector ! {collect, Position, addr(Address), hex(Line), Line, self()};

        %% Collector
        {collect, Num_Workers} ->
            collect_hexdump_lines(array:new(), Num_Workers)
    end.

%%% Collect and display the hexdump.
line_fmt(Index, {Address, Hexchars, Window, Pid}, {}) ->
    error_logger:error_msg("  ~p.  ~s ~s  |~s|  ~p~n",
                           [Index, Address, string:join(Hexchars, " "), Window, Pid]),
    {}.

collect_hexdump_lines(Array, 0) ->
    error_logger:error_msg("Collector ~p Hexdump:~n", [self()]),
    array:foldl(fun line_fmt/3, {}, Array);
collect_hexdump_lines(Array, Remaining_Responses) ->
    receive
        {collect, Position, Address, Hexchars, Window, Pid}
          when is_integer(Position), Position >= 0,
               is_binary(Address), is_list(Hexchars), is_binary(Window) ->
            Array_Value = {Address, Hexchars, Window, Pid},
            New_Array = array:set(Position, Array_Value, Array),
            collect_hexdump_lines(New_Array, Remaining_Responses-1)
    end.


%%%===================================================================
%%% Worker reformatting
%%%===================================================================

%%% Order doesn't matter since we are formatting each line
%%% concurrently in a separate process. The returned list
%%% tags each line with its original position.
split_lines(Data, Lines, Pos) ->
    case Data of
        <<>> ->
            lists:reverse(Lines);
        <<Line:16/binary, Rest/binary>> ->
            split_lines(Rest, [{Pos, Line} | Lines], Pos+1);
        Last_Line ->
            [{Pos, Last_Line} | Lines]
    end.

send_format_msgs([], [], _Collector) ->
    done;
send_format_msgs([Worker | More], [{Pos, Line} | Lines], Collector) ->
    Worker ! {format, Pos, Pos*16, Line, Collector},
    send_format_msgs(More, Lines, Collector).

addr(Address) ->
    list_to_binary(lists:flatten(io_lib:format("~8.16.0b", [Address]))).

hex(Line) ->
    Hex_Line = [hexval(Char) || <<Char>> <= Line],
    case length(Hex_Line) of
        16 -> Hex_Line;
         N -> Pad_Size = 16 - N,
              Pad_Chars = lists:duplicate(Pad_Size, "  "),
              Hex_Line ++ Pad_Chars
    end.

hexval(Char) ->
    Dig1 = hexdigit(Char div 16),
    Dig2 = hexdigit(Char rem 16),
    [Dig1, Dig2].

hexdigit(0)  -> $0;
hexdigit(1)  -> $1;
hexdigit(2)  -> $2;
hexdigit(3)  -> $3;
hexdigit(4)  -> $4;
hexdigit(5)  -> $5;
hexdigit(6)  -> $6;
hexdigit(7)  -> $7;
hexdigit(8)  -> $8;
hexdigit(9)  -> $9;
hexdigit(10) -> $a;
hexdigit(11) -> $b;
hexdigit(12) -> $c;
hexdigit(13) -> $d;
hexdigit(14) -> $e;
hexdigit(15) -> $f.
