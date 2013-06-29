%%%------------------------------------------------------------------------------
%%% @copyright (c) 2013, DuoMark International, Inc.
%%% @author Jay Nelson <jay@duomark.com>
%%% @reference The license is based on the template for Modified BSD from
%%%   <a href="http://opensource.org/licenses/BSD-3-Clause">OSI</a>
%%% @doc
%%%   ETS buffers implement circular, FIFO and LIFO access.
%%% @since v1.0.0
%%% @end
%%%------------------------------------------------------------------------------
-module(ets_buffer).
-author('Jay Nelson <jay@duomark.com>').

%% External interface
-export([
         %% API for buffers in a shared 'ets_buffer' named ets table...
         create/1, write/2,

         %% API when each buffer is in a dedicated named ets table.
         create_unique/3, write_unique/2
        ]).

-type buffer_name() :: atom().
-type buffer_size() :: pos_integer().
-type buffer_loc()  :: pos_integer().

-type buffer_type()     :: circular | fifo | lifo.
-type buffer_type_num() :: 1        | 2    | 3.

-record(ets_buffer, {
          name           :: buffer_name(),
          size = 0       :: buffer_size(),
          type = 1       :: buffer_type(),       %% Default is circular buffer
          write_loc = 0  :: non_neg_integer(),
          read_loc = 0   :: integer()
         }).

-record(buffer_data, {
          key            :: {buffer_name(), buffer_loc()},
          data           :: any()
         }).

-define(GET_BUFFER_TYPE, [{#ets_buffer.type, 0}, {#ets_buffer.size, 0}]).
-define(BUFFER_DATA(__Name, __Loc, __Data), #buffer_data{key={__Name, __Loc}, data=__Data}).

-define(CIRC_WRITER_LOC(__Max), {#ets_buffer.write_loc, 1, __Max, 1}).
-define(GET_BUFFER_READER, {#ets_buffer.read_loc}).


%%%------------------------------------------------------------------------------
%%% External API for colocated ets_buffers (in a single ?MODULE ets table)
%%%------------------------------------------------------------------------------

-spec create([{buffer_name(), buffer_type(), buffer_size()}]) -> ok.
-spec write(buffer_name(), any()) -> true.

%% @doc Initialize a single ETS table with multiple named buffers.
create(Init_Data) ->
    _ = ets:info(?MODULE, named_table)
        orelse ets:new(?MODULE, [named_table, ordered_set, public, {keypos, 2}]),
    _ = [begin
             Buffer_Meta = make_buffer_meta(Buffer_Name, Buffer_Type, Buffer_Size),
             ets:insert_new(?MODULE, Buffer_Meta)
         end || {Buffer_Name, Buffer_Type, Buffer_Size} <- Init_Data],
    ?MODULE.

make_buffer_meta(Buffer_Name, Buffer_Type, Buffer_Size)
  when is_atom(Buffer_Name), is_integer(Buffer_Size), Buffer_Size > 0,
       (Buffer_Type =:= circular orelse Buffer_Type =:= fifo orelse Buffer_Type =:= lifo) ->
    #ets_buffer{name=Buffer_Name, type=buffer_type_num(Buffer_Type), size=Buffer_Size}.

write(Buffer_Name, Data) ->
    write(?MODULE, Buffer_Name, Data).

write(Table_Name, Buffer_Name, Data) ->
    {Type_Num, Max_Loc} = ets:update_counter(Table_Name, Buffer_Name, ?GET_BUFFER_TYPE),
    case buffer_type(Type_Num) of
        circular ->
            Loc = ets:update_counter(Table_Name, Buffer_Name, ?CIRC_WRITER_LOC(Max_Loc)),
            ets:insert(Table_Name, ?BUFFER_DATA(Buffer_Name, Loc, Data));
        fifo -> not_implemented_yet;
        lifo -> not_implemented_yet
    end.


%%%------------------------------------------------------------------------------
%%% External API for separately named ets_buffers (one per ets table)
%%%------------------------------------------------------------------------------

-spec create_unique(buffer_name(), buffer_type(), buffer_size()) -> ok.
-spec write_unique(buffer_name(), any()) -> true.

%% @doc Initialize a named ETS table to hold the buffer.
create_unique(Buffer_Name, Buffer_Type, Buffer_Size) ->
    ets:new(Buffer_Name, [named_table, ordered_set, public]),
    Buffer_Meta = make_buffer_meta(Buffer_Name, Buffer_Type, Buffer_Size),
    ets:insert_new(?MODULE, Buffer_Meta).

write_unique(Buffer_Name, Data) ->
    write(Buffer_Name, Buffer_Name, Data).


%%%------------------------------------------------------------------------------
%%% Internal functions
%%%------------------------------------------------------------------------------

-spec buffer_type(buffer_type_num()) -> buffer_type().
-spec buffer_type_num(buffer_type()) -> buffer_type_num().

buffer_type(1) -> circular;
buffer_type(2) -> fifo;
buffer_type(3) -> lifo.
    
buffer_type_num(circular) -> 1;
buffer_type_num(fifo)     -> 2;
buffer_type_num(lifo)     -> 3.
