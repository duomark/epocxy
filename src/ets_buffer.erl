%%%------------------------------------------------------------------------------
%%% @copyright (c) 2013, DuoMark International, Inc.
%%% @author Jay Nelson <jay@duomark.com> [http://duomark.com/]
%%% @reference 2013 Development sponsored by TigerText, Inc. [http://tigertext.com/]
%%% @reference The license is based on the template for Modified BSD from
%%%   <a href="http://opensource.org/licenses/BSD-3-Clause">OSI</a>
%%% @doc
%%%   ETS buffer implements a storage area that resides in an ets table.
%%%   Note that this means that buffered data is in RAM only, never on disk.
%%%   A buffer consists of a metadata record for the buffer as a whole,
%%%   and a data record for each entry in the buffer. There are three
%%%   types of buffers supported: Ring, FIFO and LIFO. The main purpose
%%%   of this storage structure is to allow a communication mechanism
%%%   between processes that is concurrently accessable while also safe
%%%   from mutual destruction (although not foolproof as publicly
%%%   writeable ets tables are used and destructive commands such as
%%%   delete and clear are provided).
%%%
%%%   Ring buffers hold a pre-determined number of entries, but are not
%%%   pre-allocated. Once an entry has been written it is never erased.
%%%   The purpose of a ring buffer is to allow many concurrent writers
%%%   to contribute data (e.g., telemetry data) while placing some limits
%%%   on the amount of data stored (there is a limit on the number of
%%%   data records, but no limit on the size of each data record, so
%%%   writer beware). The metadata record is a serialized lock point,
%%%   but once a write location is reserved, data is written in more
%%%   than one location simultaneously. History snapshots provide the
%%%   buffered data in the order written with oldest entries first.
%%%
%%%   FIFO buffers are unlimited in size (but constrained by crashing
%%%   the VM node when too much memory is consumed). They are used
%%%   when data is collected and is to be processed in the order it is
%%%   received, but each entry should only be handled once by workers
%%%   processing a buffer. Entries are reserved for reading, and deleted
%%%   after reading, so that concurrent readers are only bottlenecked
%%%   while reserving buffered entries in the metadata record access.
%%%
%%%   LIFO buffers work exactly as FIFO buffers except that the order
%%%   of data appears reversed so that the most recently inserted item
%%%   is delivered before older items.
%%%
%%%   A separate API is maintained for all ets buffers residing in a
%%%   single ets_buffer named table, and for a dedicated ets table
%%%   named with the buffer_name. This is unfortunate as it requires
%%%   a bigger change to client code when switching from a shared ets
%%%   table to a dedicated ets table, but avoids the overhead of
%%%   checking the metadata on every request. The reason for using a
%%%   dedicated ets table if when a single buffer has an excessive
%%%   number of concurrent accessors, orders of magnitude greater
%%%   than other buffers in the application. A dedicated buffer may
%%%   also be more efficient if it manually garbage collected by the
%%%   application frequently through the use of delete_dedicated.
%%%
%%% @since 0.9.0
%%% @end
%%%------------------------------------------------------------------------------
-module(ets_buffer).
-author('Jay Nelson <jay@duomark.com>').

-compile({inline, [
                   buffer_type/1, buffer_type_num/1,
                   buffer_key/2, buffer_data/3,
                   ring_writer_inc/1,
                   get_buffer_type/2, get_buffer_read/2, get_buffer_write/2,
                   get_buffer_type_and_pos/3
                  ]}).

%% External interface
-export([
         %% API for buffers in a shared 'ets_buffer' named ets table...
         create/1, create/2, create/3,
         write/2,
         read/1, read/2,
         history/1, history/2,
         clear/1, delete/1, list/0, list/1,

         %% API when each buffer is in a dedicated named ets table.
         create_dedicated/2, create_dedicated/3,
         write_dedicated/2,
         read_dedicated/1, read_dedicated/2,
         history_dedicated/1, history_dedicated/2,
         clear_dedicated/1, delete_dedicated/1, list_dedicated/1
        ]).

-type buffer_name() :: atom().
-type buffer_size() :: non_neg_integer().
-type buffer_loc()  :: pos_integer().
-type buffer_data() :: any().

-type buffer_error() :: not_implemented | {missing_ets_buffer, buffer_name()}.

-type buffer_type()     :: ring | fifo | lifo.
-type buffer_type_num() :: 1    | 2    | 3.

-export_type([buffer_type/0, buffer_error/0]).


%%%------------------------------------------------------------------------------
%%% Support functions for consistent access to data structures
%%%------------------------------------------------------------------------------

-spec buffer_type(buffer_type_num()) -> buffer_type().
-spec buffer_type_num(buffer_type()) -> buffer_type_num().

buffer_type(1) -> ring;
buffer_type(2) -> fifo;
buffer_type(3) -> lifo.
    
buffer_type_num(ring) -> 1;
buffer_type_num(fifo) -> 2;
buffer_type_num(lifo) -> 3.

-define(RING_NUM, buffer_type_num(ring)).
-define(FIFO_NUM, buffer_type_num(fifo)).
-define(LIFO_NUM, buffer_type_num(lifo)).

%% Record stored in ets table, or used for match spec.
-record(ets_buffer, {
          name                   :: buffer_name()     | '_',
          size      = 0          :: buffer_size()     | '_',
          type      = ?RING_NUM  :: buffer_type_num() | '_',   %% Default is ring buffer
          write_loc = 0          :: non_neg_integer() | '_',
          read_loc  = 0          :: non_neg_integer() | '_'
         }).

%% Convert record to proplist.
make_buffer_proplist(#ets_buffer{name=Name, size=Size, type=Type_Num, write_loc=Write_Loc}) ->
    [{name, Name}, {size, Size}, {type, buffer_type(Type_Num)}, {write_loc, Write_Loc}].

%% Match specs for buffers.
all_buffers(Table_Name) ->
    ets:match_object(Table_Name, #ets_buffer{name='_', size='_', type='_', write_loc='_', read_loc='_'}).
one_buffer(Table_Name, Buffer_Name) ->
    ets:match_object(Table_Name, #ets_buffer{name=Buffer_Name, size='_', type='_', write_loc='_', read_loc='_'}).

%% Increment write pointer according to buffer_type semantics.
ring_writer_inc(Max) -> {#ets_buffer.write_loc, 1, Max, 1}.
fifo_writer_inc()    -> {#ets_buffer.write_loc, 1}.

%% Increment read pointer according to buffer_type semantics.
fifo_reader_inc(Num_Items, Write_Loc) -> {#ets_buffer.read_loc, Num_Items, Write_Loc-1, Write_Loc}.

%% Record format of all ets_buffer data.
-record(buffer_data, {
          key          :: {buffer_name(), buffer_loc()},
          data         :: buffer_data()
         }).

buffer_key(Name, Loc) -> {Name, Loc}.
buffer_data(Name, Loc, Data) ->
    #buffer_data{key=buffer_key(Name, Loc), data=Data}.
    

%%%------------------------------------------------------------------------------
%%% External API for colocated ets_buffers (in a single ?MODULE ets table)
%%%------------------------------------------------------------------------------

-spec list() -> proplists:proplist().
-spec list(buffer_name()) -> proplists:proplist().
-spec create([{buffer_name(), ring, buffer_size()}
              | {buffer_name(), fifo | lifo}]) -> ets_buffer.
-spec create(buffer_name(), fifo | lifo) -> buffer_name().
-spec create(buffer_name(), ring, buffer_size()) -> buffer_name().
-spec clear(buffer_name()) -> boolean().
-spec delete(buffer_name()) -> boolean().

-spec write(buffer_name(), buffer_data()) -> true | buffer_error().
-spec read(buffer_name()) -> [buffer_data()] | buffer_error().
-spec read(buffer_name(), pos_integer()) -> [buffer_data()] | buffer_error().
-spec history(buffer_name()) -> [buffer_data()] | buffer_error().
-spec history(buffer_name(), pos_integer()) -> [buffer_data()] | buffer_error().


%% @doc Get a set of proplists for all buffers in the shared ets table.
list() -> [make_buffer_proplist(Buffer) || Buffer <- all_buffers(?MODULE)].

%% @doc Get a single proplist for a given buffer in the shared ets table.
list(Buffer_Name) when is_atom(Buffer_Name) ->
    [Buffer] = one_buffer(?MODULE, Buffer_Name),
    make_buffer_proplist(Buffer).


%% @doc Initialize or add to a single ETS table with multiple named buffers.
create(Init_Data) when is_list(Init_Data) ->
    Tid = ets:info(?MODULE, named_table) =/= undefined
        orelse ets:new(?MODULE, [named_table, ordered_set, public, {keypos, 2},
                                 {write_concurrency, true}]),
    _ = [begin
             Buffer_Meta = make_buffer_meta(Buffer_Name, Buffer_Type, Buffer_Size),
             ets:insert_new(?MODULE, Buffer_Meta)
         end || {Buffer_Name, Buffer_Type, Buffer_Size} <- Init_Data],
    Tid.

make_buffer_meta(Buffer_Name, Buffer_Type)
  when is_atom(Buffer_Name), (Buffer_Type =:= fifo orelse Buffer_Type =:= lifo) ->
    #ets_buffer{name=Buffer_Name, type=buffer_type_num(Buffer_Type), size=0}.

make_buffer_meta(Buffer_Name, Buffer_Type, Buffer_Size)
  when is_atom(Buffer_Name), is_integer(Buffer_Size), Buffer_Size > 0,
       (Buffer_Type =:= ring orelse Buffer_Type =:= fifo orelse Buffer_Type =:= lifo) ->
    #ets_buffer{name=Buffer_Name, type=buffer_type_num(Buffer_Type), size=Buffer_Size}.

%% @doc Add to a single ETS table to hold another FIFO or LIFO buffer.
create(Buffer_Name, Buffer_Type)
  when is_atom(Buffer_Name), (Buffer_Type =:= fifo orelse Buffer_Type =:= lifo) ->
    create([{Buffer_Name, Buffer_Type}]).

%% @doc Add to a single ETS table to hold a ring buffer.
create(Buffer_Name, ring, Buffer_Size)
  when is_atom(Buffer_Name), is_integer(Buffer_Size), Buffer_Size > 0 ->
    create([{Buffer_Name, ring, Buffer_Size}]).

%% @doc Remove all entries from a specific buffer, but keep the empty buffer.
clear(Buffer_Name) when is_atom(Buffer_Name) ->
    clear_internal(?MODULE, Buffer_Name) =:= true.

%% @doc Remove all entries and delete a specific buffer.
delete(Buffer_Name) when is_atom(Buffer_Name) ->
    clear_internal(?MODULE, Buffer_Name) =:= true
        andalso ets:delete(?MODULE, Buffer_Name).

%% @doc Write data to the buffer following the semantics of the buffer type.
write(Buffer_Name, Data) when is_atom(Buffer_Name) ->
    write_internal(?MODULE, Buffer_Name, Data).

%% @doc Read one data item from a buffer following the semantics of the buffer type.
read(Buffer_Name) when is_atom(Buffer_Name) ->
    read(Buffer_Name, 1).

%% @doc Read multiple data items from a buffer following the semantics of the buffer type.
read(Buffer_Name, Max_Num_Items)
  when is_atom(Buffer_Name), is_integer(Max_Num_Items), Max_Num_Items > 0 ->
    read_internal(?MODULE, Buffer_Name, Max_Num_Items).

%% @doc
%%   Return all buffered data which is still present, even if previously read.
%%   The order of the list is according to the semantics of the buffer type.
%% @end
history(Buffer_Name) when is_atom(Buffer_Name) ->
    history_internal(?MODULE, Buffer_Name).

%% @doc
%%   Return the last N buffered items still present, even if previously read.
%%   The order of the list is according to the semantics of the buffer type.
%% @end
history(Buffer_Name, Num_Items)
  when is_atom(Buffer_Name), is_integer(Num_Items), Num_Items > 0 ->
    history_internal(?MODULE, Buffer_Name, Num_Items).

    
%%%------------------------------------------------------------------------------
%%% External API for separately named ets_buffers (one per ets table)
%%%------------------------------------------------------------------------------

-spec list_dedicated(buffer_name()) -> proplists:proplist().
-spec create_dedicated(buffer_name(), fifo | lifo) -> buffer_name().
-spec create_dedicated(buffer_name(), ring, buffer_size()) -> buffer_name().
-spec clear_dedicated(buffer_name()) -> boolean().
-spec delete_dedicated(buffer_name()) -> boolean().

-spec write_dedicated(buffer_name(), any()) -> true | not_implemented.
-spec read_dedicated(buffer_name()) -> [buffer_data()] | buffer_error().
-spec read_dedicated(buffer_name(), pos_integer()) -> [buffer_data()] | buffer_error().
-spec history_dedicated(buffer_name()) -> [buffer_data()].
-spec history_dedicated(buffer_name(), pos_integer()) -> [buffer_data()].


%% @doc Get a single proplist for the buffer in a named ets table.
list_dedicated(Buffer_Name) when is_atom(Buffer_Name) ->
    [Buffer] = all_buffers(Buffer_Name),
    make_buffer_proplist(Buffer).

%% @doc
%%   Initialize a new empty named ETS table to hold a FIFO or LIFO buffer.
%%   This function is used when the number of accesses to a shared
%%   ets table would be too high, or when independent ets life cycle
%%   provides a quicker way to eliminate buffer memory.
%% @end
create_dedicated(Buffer_Name, Buffer_Type)
  when is_atom(Buffer_Name), (Buffer_Type =:= fifo orelse Buffer_Type =:= lifo) ->
    Tid = ets:new(Buffer_Name, [named_table, ordered_set, public, {write_concurrency, true}]),
    Buffer_Meta = make_buffer_meta(Buffer_Name, Buffer_Type),
    ets:insert_new(Buffer_Name, Buffer_Meta),
    Tid.

%% @doc
%%   Initialize a new empty named ETS table to hold a ring buffer.
%%   This function is used when the number of accesses to a shared
%%   ets table would be too high, or when independent ets life cycle
%%   provides a quicker way to eliminate buffer memory.
%% @end
create_dedicated(Buffer_Name, ring, Buffer_Size)
  when is_atom(Buffer_Name), is_integer(Buffer_Size), Buffer_Size > 0  ->
    Tid = ets:new(Buffer_Name, [named_table, ordered_set, public, {write_concurrency, true}]),
    Buffer_Meta = make_buffer_meta(Buffer_Name, ring, Buffer_Size),
    ets:insert_new(Buffer_Name, Buffer_Meta),
    Tid.

%% @doc Remove all entries from a dedicated buffer, but keep the empty buffer.
clear_dedicated(Buffer_Name) when is_atom(Buffer_Name) ->
    clear_internal(Buffer_Name, Buffer_Name).

%% @doc Delete the entire dedicate ets table.
delete_dedicated(Buffer_Name) when is_atom(Buffer_Name) ->
    ets:delete(Buffer_Name).

%% @doc Write data to the named ets buffer table following the semantics of the buffer type.
write_dedicated(Buffer_Name, Data) when is_atom(Buffer_Name) ->
    write_internal(Buffer_Name, Buffer_Name, Data).

%% @doc Read one data item from a dedicated buffer following the semantics of the buffer type.
read_dedicated(Buffer_Name) when is_atom(Buffer_Name) ->
    read_dedicated(Buffer_Name, 1).

%% @doc Read multiple data items from a dedicated buffer following the semantics of the buffer type.
read_dedicated(Buffer_Name, Num_Items)
  when is_atom(Buffer_Name), is_integer(Num_Items), Num_Items > 0 ->
    read_internal(Buffer_Name, Buffer_Name, Num_Items).

%% @doc
%%   Return all buffered data which is still present in a named ets buffer table,
%%   even if previously read. The order of the list is from oldest item to newest item.
%% @end
history_dedicated(Buffer_Name) when is_atom(Buffer_Name) ->
    history_internal(Buffer_Name, Buffer_Name).

%% @doc
%%   Return the last N buffered items still present in a named ets buffer table,
%%   even if previously read. The order of the list is from oldest item to newest item.
%% @end
history_dedicated(Buffer_Name, Num_Items)
  when is_atom(Buffer_Name), is_integer(Num_Items), Num_Items > 0 ->
    history_internal(Buffer_Name, Buffer_Name, Num_Items).


%%%------------------------------------------------------------------------------
%%% Internal functions
%%%------------------------------------------------------------------------------

-define(READ_LOC,  {#ets_buffer.read_loc,  0}).
-define(WRITE_LOC, {#ets_buffer.write_loc, 0}).

-define(TYPE_AND_SIZE,  {#ets_buffer.type, 0}, {#ets_buffer.size, 0}).
-define(TYPE_AND_WRITE, ?TYPE_AND_SIZE,  ?WRITE_LOC).
-define(TYPE_AND_READ,  ?TYPE_AND_WRITE, ?READ_LOC).

get_buffer_type (Table_Name, Buffer_Name) -> get_buffer_type_and_pos(Table_Name, Buffer_Name, [?TYPE_AND_SIZE]).
get_buffer_write(Table_Name, Buffer_Name) -> get_buffer_type_and_pos(Table_Name, Buffer_Name, [?TYPE_AND_WRITE]).
get_buffer_read (Table_Name, Buffer_Name) -> get_buffer_type_and_pos(Table_Name, Buffer_Name, [?TYPE_AND_READ]).

get_buffer_type_and_pos(Table_Name, Buffer_Name, Update_Cmd) ->
    try ets:update_counter(Table_Name, Buffer_Name, Update_Cmd)
    catch error:badarg -> false
    end.

clear_internal(Table_Name, Buffer_Name) ->
    try 
        ets:match_delete(Table_Name, #buffer_data{key=buffer_key(Buffer_Name, '_'), data='_'}),
        ets:update_element(Table_Name, Buffer_Name, ?WRITE_LOC)
    catch error:badarg -> {missing_ets_buffer, Buffer_Name}
    end.

write_internal(Table_Name, Buffer_Name, Data) ->
    case get_buffer_type(Table_Name, Buffer_Name) of
        false -> {missing_ets_buffer, Buffer_Name};
        [Type_Num, Max_Loc] ->
            case buffer_type(Type_Num) of

                %% Ring buffer wraps around on inserts...
                ring -> Loc = ets:update_counter(Table_Name, Buffer_Name, ring_writer_inc(Max_Loc)),
                        ets:insert(Table_Name, buffer_data(Buffer_Name, Loc, Data));

                %% Both FIFO and LIFO continuously increment...
                fifo -> Loc = ets:update_counter(Table_Name, Buffer_Name, fifo_writer_inc()),
                        ets:insert(Table_Name, buffer_data(Buffer_Name, Loc, Data));

                lifo -> Loc = ets:update_counter(Table_Name, Buffer_Name, fifo_writer_inc()),
                        ets:insert(Table_Name, buffer_data(Buffer_Name, Loc, Data))
            end
    end.

read_internal(Table_Name, Buffer_Name, Num_Items) ->
    case get_buffer_read(Table_Name, Buffer_Name) of
        false -> {missing_ets_buffer, Buffer_Name};
        [Type_Num, _Max_Loc, Write_Loc, Read_Loc] ->
            case buffer_type(Type_Num) of

                %% Bump read pointer with wraparound if any entries available...
                ring -> not_implemented;

                %% Bump read pointer atomically, then read the data...
                fifo -> End_Loc = ets:update_counter(Table_Name, Buffer_Name, fifo_reader_inc(Num_Items, Write_Loc)),
                        read_ets(Table_Name, Buffer_Name, Read_Loc, End_Loc);

                lifo -> not_implemented
            end
    end.

read_ets(Table_Name, Buffer_Name, Read_Loc, End_Loc) ->
    {Buffer_Entry_Match, Buffer_Deletes} = get_read_match_specs(Buffer_Name, Read_Loc, End_Loc),
    try   ets:select(Table_Name, Buffer_Entry_Match)
    after ets:select_delete(Table_Name, Buffer_Deletes)
    end.

%% Select all buffer_data Read_Loc =< Key =< End_Loc...
get_read_match_specs(Buffer_Name, Read_Loc, End_Loc) ->
    Key = buffer_key(Buffer_Name, '$1'),
    Match = #buffer_data{key=Key, data='$2'},
    Guard = [{'=<', Read_Loc, '$1'}, {'=<', '$1', End_Loc}],
    {[{Match, Guard, ['$2']}],    %% Get the data item
     [{Match, Guard, [true]}]}.   %% Delete the data item

%% Currently this function assumes the number of items is not excessive and fetches all in one try.
history_internal(Table_Name, Buffer_Name) ->
    case get_buffer_write(Table_Name, Buffer_Name) of
        false -> {missing_ets_buffer, Buffer_Name};
        [Type_Num, Max_Loc, Write_Pos] ->
            case buffer_type(Type_Num) of
                ring -> history_ring(Table_Name, Buffer_Name, Max_Loc, Write_Pos, Max_Loc);
                fifo -> not_implemented;
                lifo -> not_implemented
            end
    end.

history_internal(Table_Name, Buffer_Name, Num_Items) ->
    case get_buffer_write(Table_Name, Buffer_Name) of
        false -> {missing_ets_buffer, Buffer_Name};
        [Type_Num, Max_Loc, Write_Pos] ->
            case buffer_type(Type_Num) of
                ring -> history_ring(Table_Name, Buffer_Name, Num_Items, Write_Pos, Max_Loc);
                fifo -> not_implemented;
                lifo -> not_implemented
            end
    end.

history_ring(_Table_Name, _Buffer_Name, _Num_Items, 0, _Max_Loc) -> [];
history_ring(_Table_Name, _Buffer_Name, 0, _Write_Pos, _Max_Loc) -> [];
history_ring(Table_Name, Buffer_Name, Num_Items, Write_Pos, _Max_Loc)
 when Num_Items < Write_Pos ->
    history_ring_get(Table_Name, Buffer_Name, Num_Items, Write_Pos - Num_Items + 1);
history_ring(Table_Name, Buffer_Name, Num_Items, Write_Pos, Max_Loc) ->
    case Num_Items - Write_Pos of
        0         -> history_ring_get(Table_Name, Buffer_Name, Num_Items, 1);
        Old_Chunk -> history_ring_get(Table_Name, Buffer_Name, Old_Chunk, Max_Loc - Old_Chunk + 1)
                      ++ history_ring_get(Table_Name, Buffer_Name, Write_Pos, 1)
    end.

history_ring_get(Table_Name, Buffer_Name, Num_Items, Start_Pos) ->
    case ets:select(Table_Name, [{#buffer_data{key=buffer_key(Buffer_Name, '$1'), data='$2'},
                                 [{'>=', '$1', Start_Pos}], ['$2']}], Num_Items) of
        '$end_of_table'          -> [];
        {Matches, _Continuation} -> Matches
    end.
