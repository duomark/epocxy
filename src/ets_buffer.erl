%%%------------------------------------------------------------------------------
%%% @copyright (c) 2013-2014, DuoMark International, Inc.
%%% @author Jay Nelson <jay@duomark.com> [http://duomark.com/]
%%% @reference 2013-2014 Development sponsored by TigerText, Inc. [http://tigertext.com/]
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
%%%   is delivered before older items. Because of limitations with
%%%   ETS table updates and the potential for race conditions, only
%%%   one item at a time may be retrieved from a LIFO buffer.
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
                   meta_key/1, buffer_key/2, buffer_data/3,
                   ring_reserve_write_cmd/1, ring_reserve_read_cmd/4,
                   ring_reserve_read_all_cmd/1,
                   fifo_publish_write_cmd/0, fifo_reserve_read_cmd/2,
                   fifo_reserve_read_all_cmd/2,
                   lifo_reserve_write_cmd/0, %% lifo_reserve_read_cmd/2, 
                   get_buffer_type/2, get_buffer_readw/2, get_buffer_write/2,
                   get_buffer_type_and_pos/3,
                   insert_ets_internal/4,
                   set_high_water/3, set_high_water_cmd/1
                  ]}).

%% External interface
-export([
         %% API for buffers in a shared 'ets_buffer' named ets table...
         create/1, create/2, create/3,
         write/2,
         read/1, read/2, read_all/1,
         read_timestamped/1, read_timestamped/2,
         read_all_timestamped/1,
         history/1, history/2,
         history_timestamped/1, history_timestamped/2,
         clear/1, delete/1, list/0, list/1,
         num_entries/1, capacity/1,
         clear_high_water/1,

         %% API when each buffer is in a dedicated named ets table.
         create_dedicated/2, create_dedicated/3,
         write_dedicated/2,
         read_dedicated/1, read_dedicated/2, read_all_dedicated/1,
         read_timestamped_dedicated/1, read_timestamped_dedicated/2,
         read_all_timestamped_dedicated/1,
         history_dedicated/1, history_dedicated/2,
         history_timestamped_dedicated/1, history_timestamped_dedicated/2,
         clear_dedicated/1, delete_dedicated/1, list_dedicated/1,
         num_entries_dedicated/1, capacity_dedicated/1,
         clear_high_water_dedicated/1
        ]).

-type buffer_name() :: atom().
-type buffer_size() :: non_neg_integer().
-type buffer_loc()  :: pos_integer().
-type buffer_data() :: any().
-type buffer_data_timestamped() :: {erlang:timestamp(), buffer_data()}.

-type buffer_error() :: not_supported
                      | {missing_ets_buffer, buffer_name()}
                      | {missing_ets_data,   buffer_name(), buffer_loc()}.

-type buffer_type()     :: ring | fifo | lifo.
-type buffer_type_num() :: 1    | 2    | 3.

-export_type([buffer_name/0, buffer_type/0, buffer_error/0]).


%%%------------------------------------------------------------------------------
%%% Support functions for consistent access to data structures
%%%------------------------------------------------------------------------------

-spec buffer_type(buffer_type_num()) -> buffer_type().
-spec buffer_type_num(buffer_type()) -> buffer_type_num().

%% Buffer type is indicated by an integer to use update_counter to read it.
buffer_type(1) -> ring;
buffer_type(2) -> fifo;
buffer_type(3) -> lifo.
    
buffer_type_num(ring) -> 1;
buffer_type_num(fifo) -> 2;
buffer_type_num(lifo) -> 3.

%% Some macro constants equivalent to above function call.
-define(RING_NUM, 1).   %% buffer_type_num(ring)).
-define(FIFO_NUM, 2).   %% buffer_type_num(fifo)).
-define(LIFO_NUM, 3).   %% buffer_type_num(lifo)).

%% Record stored in ets table (also used for matchspecs).
-record(ets_buffer, {
          name                     :: {meta, buffer_name()} | {meta, '_'},
          size        = 0          :: buffer_size()         | '_',
          high_water  = 0          :: buffer_size()         | '_',
          type        = ?RING_NUM  :: buffer_type_num()     | '_',   %% Default is ring buffer
          reserve_loc = 0          :: non_neg_integer()     | '_',
          write_loc   = 0          :: non_neg_integer()     | '_',
          read_loc    = 0          :: non_neg_integer()     | '_'
         }).

meta_key(Buffer_Name) -> {meta, Buffer_Name}.
    
%% Convert record to proplist.
make_buffer_proplist(#ets_buffer{name={meta, Name}, size=Size, high_water=High_Water, type=Type_Num,
                                 reserve_loc=Reserve_Loc, write_loc=Write_Loc, read_loc=Read_Loc}) ->
    [{name, Name}, {size, Size}, {high_water, High_Water}, {type, buffer_type(Type_Num)},
     {reserve_loc, Reserve_Loc}, {write_loc, Write_Loc},   {read_loc, Read_Loc}].

%% Match specs for buffers.
all_buffers(Table_Name) ->
    try ets:match_object(Table_Name, #ets_buffer{name=meta_key('_'), _='_'})
    catch error:badarg -> []
    end.
one_buffer(Table_Name, Buffer_Name) ->
    try ets:match_object(Table_Name, #ets_buffer{name=meta_key(Buffer_Name), _='_'})
    catch error:badarg -> []
    end.


%%%------------------------------------------------------------------------------
%%% New entries are written to buffers in three atomic steps to ensure that
%%% readers can concurrently access a buffer and not read the wrong information,
%%% whilst also ensuring the writers don't overwrite each other's values
%%% (n.b., other actions can interleave between the 3 atomic steps).
%%%
%%%   1) Reserve a write slot for the new buffer entry (atomic inc)
%%%   2) Write the new buffer entry to the reserved slot (insert_new key)
%%%   3) Move the write pointer to indicate entry available to read (atomic inc)
%%%      a) move the read pointer if buffer semantics require
%%%
%%% The pointers are maintained in a meta-data record of the format:
%%%
%%%   #ets_buffer{name={meta, Buffer_Name}, ...}
%%%
%%% All buffers use #buffer_data{key={Buffer_Name, Buffer_Slot_Num}, ...}.
%%%
%%% FIFO and Ring use an increasing Buffer_Slot_Num so older entries are smaller.
%%% LIFO uses a decreasing negative Buffer_Slot_Num so newer entries are smaller.
%%%------------------------------------------------------------------------------

%% FIFO bumps only write location to publish, read is trailing behind.
fifo_reserve_write_cmd() ->  {#ets_buffer.reserve_loc, 1}.
fifo_publish_write_cmd() -> [{#ets_buffer.write_loc,   1}, {#ets_buffer.read_loc, 0}].

%% FIFO read reserve atomically gets latest read location and increments it.
fifo_reserve_read_cmd(Num_Items, Write_Loc) ->
    [{#ets_buffer.read_loc, 0}, {#ets_buffer.read_loc, Num_Items, Write_Loc-1, Write_Loc}].

fifo_reserve_read_all_cmd(Write_Loc, Read_Loc) ->
    [{#ets_buffer.read_loc, 0}, {#ets_buffer.read_loc, Write_Loc - Read_Loc}].


%% Ring bumps write location, but only moves read location if the write pointer wraps to it.
ring_reserve_write_cmd(Max)                              -> [{#ets_buffer.reserve_loc, 1, Max, 1}, {#ets_buffer.read_loc, 0}].
ring_publish_write_cmd(Max,  Reserve_Loc,  Reserve_Loc)  -> [{#ets_buffer.write_loc,   1, Max, 1}, {#ets_buffer.read_loc, 1, Max, 1}];
ring_publish_write_cmd(Max, _Reserve_Loc, _Old_Read_Loc) -> [{#ets_buffer.write_loc,   1, Max, 1}, {#ets_buffer.read_loc, 0}].

%% Increment read pointer
ring_reserve_read_cmd(Num_Items, Write_Loc, Max_Loc, Read_Loc) ->
    case Write_Loc >= Read_Loc of
        true  -> [{#ets_buffer.read_loc, 0}, {#ets_buffer.read_loc, Num_Items, Write_Loc-1, Write_Loc}];
        false -> New_Read_Loc = min(Num_Items - (Max_Loc - Read_Loc), Max_Loc),
                 [{#ets_buffer.read_loc, 0}, {#ets_buffer.read_loc, 0, Write_Loc, New_Read_Loc}]
    end.

ring_reserve_read_all_cmd(Write_Loc) ->
    [{#ets_buffer.read_loc, 0}, {#ets_buffer.read_loc, 1, 0, Write_Loc}].

%% LIFO bumps write location, but only moves read location if it is not already past the reserve/write for this publish.
lifo_reserve_write_cmd()            ->  {#ets_buffer.reserve_loc, -1}.
lifo_publish_write_cmd(Reserve_Loc) -> [{#ets_buffer.write_loc,   -1}, {#ets_buffer.read_loc, 0, Reserve_Loc, Reserve_Loc}].

set_high_water_cmd(Count) -> {#ets_buffer.high_water, Count}.
    

%%%------------------------------------------------------------------------------
%%% Buffer entries use a different record structure and have the buffer name as
%%% part of the key to take advantage of the ordered set characteristics of the
%%% ets table used. The following key sequences are used:
%%%
%%%   1) Ring: {Buffer_Name,  1 ...  Bignum}  => ever increasing integer
%%%   1) FIFO: {Buffer_Name,  1 ...  Bignum}  => ever increasing integer
%%%   2) LIFO: {Buffer_Name, -1 ... -Bignum}  => ever decreasing integer
%%%
%%% Note that the integers may turn into bignums, causing a slight slowdown
%%% when indexing and accessing the keys. To avoid this, periodically delete
%%% the buffer and then re-create it to start the sequence over again. Be
%%% sure to do this only after you have read all the data and are certain
%%% that no writers are accessing the buffer.
%%%------------------------------------------------------------------------------

%% Record format of all ets_buffer data (also used for matchspecs).
-record(buffer_data, {
          key          :: {buffer_name(), buffer_loc()  | '_' | '$1'},
          created      :: erlang:timestamp()            | '_' | '$1' | '$2' | '$3',
          data         :: buffer_data()                 | '_' | '$1' | '$2'
         }).

buffer_key(Name, Loc) -> {Name, Loc}.
buffer_data(Name, Loc, Data) ->
    #buffer_data{key=buffer_key(Name, Loc), created=os:timestamp(), data=Data}.
buffer_data(Name, Loc, Time, Data) ->    
    #buffer_data{key=buffer_key(Name, Loc), created=Time,           data=Data}.


%%%------------------------------------------------------------------------------
%%% External API for colocated ets_buffers (in a single ?MODULE ets table)
%%%------------------------------------------------------------------------------

-spec list() -> [proplists:proplist()].
-spec list(buffer_name()) -> proplists:proplist().
-spec create([{buffer_name(), ring, buffer_size()}
              | {buffer_name(), fifo | lifo}]) -> ets_buffer.
-spec create(buffer_name(), fifo | lifo) -> buffer_name().
-spec create(buffer_name(), ring, buffer_size()) -> buffer_name().
-spec clear(buffer_name()) -> boolean().
-spec delete(buffer_name()) -> boolean().

-spec write(buffer_name(), buffer_data()) -> non_neg_integer() | true | buffer_error().
-spec read(buffer_name()) -> [buffer_data()] | buffer_error().
-spec read(buffer_name(), pos_integer()) -> [buffer_data()] | buffer_error().
-spec read_all(buffer_name()) -> [buffer_data()] | buffer_error().
-spec read_timestamped(buffer_name()) -> [buffer_data_timestamped()] | buffer_error().
-spec read_timestamped(buffer_name(), pos_integer()) -> [buffer_data_timestamped()] | buffer_error().
-spec read_all_timestamped(buffer_name()) -> [buffer_data_timestamped()] | buffer_error().
-spec history(buffer_name()) -> [buffer_data()] | buffer_error().
-spec history(buffer_name(), pos_integer()) -> [buffer_data()] | buffer_error().
-spec history_timestamped(buffer_name()) -> [buffer_data_timestamped()] | buffer_error().
-spec history_timestamped(buffer_name(), pos_integer()) -> [buffer_data_timestamped()] | buffer_error().
-spec num_entries(buffer_name()) -> non_neg_integer() | buffer_error().
-spec capacity(buffer_name()) -> pos_integer() | unlimited | buffer_error().
-spec clear_high_water(buffer_name()) -> true | buffer_error().


%% @doc Get a set of proplists for all buffers in the shared ets table.
list() -> [make_buffer_proplist(Buffer) || Buffer <- all_buffers(?MODULE)].

%% @doc Get a single proplist for a given buffer in the shared ets table.
list(Buffer_Name) when is_atom(Buffer_Name) ->
    case one_buffer(?MODULE, Buffer_Name) of
        []       -> [];
        [Buffer] -> make_buffer_proplist(Buffer)
    end.


%% @doc Initialize or add to a single ETS table with multiple named buffers.
create(Init_Data) when is_list(Init_Data) ->
    ets:info(?MODULE, named_table) =/= undefined
        orelse ets:new(?MODULE, [named_table, ordered_set, public, {keypos, 2}]),
    _ = [ets:insert_new(?MODULE, Buffer_Meta)
         || Buffer_Attrs <- Init_Data,
            begin
                Buffer_Meta = case Buffer_Attrs of
                                  {Buffer_Name, Buffer_Type, Buffer_Size} ->
                                      make_buffer_meta(Buffer_Name, Buffer_Type, Buffer_Size);
                                  {Buffer_Name, Buffer_Type} ->
                                      make_buffer_meta(Buffer_Name, Buffer_Type)
                              end,
                true
            end],
    ?MODULE.

make_buffer_meta(Buffer_Name, Buffer_Type)
  when is_atom(Buffer_Name), (Buffer_Type =:= fifo orelse Buffer_Type =:= lifo) ->
    #ets_buffer{name=meta_key(Buffer_Name), type=buffer_type_num(Buffer_Type), size=0}.

make_buffer_meta(Buffer_Name, Buffer_Type, Buffer_Size)
  when is_atom(Buffer_Name), is_integer(Buffer_Size), Buffer_Type =:= ring, Buffer_Size > 0 ->
    #ets_buffer{name=meta_key(Buffer_Name), type=buffer_type_num(Buffer_Type), size=Buffer_Size}.

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
        andalso ets:delete(?MODULE, meta_key(Buffer_Name)).

%% @doc Write data to the buffer following the semantics of the buffer type.
write(Buffer_Name, Data) when is_atom(Buffer_Name) ->
    write_internal(?MODULE, Buffer_Name, Data).

%% @doc Read all currently queued data items (ring and FIFO only).
read_all(Buffer_Name)
  when is_atom(Buffer_Name) ->
    read_all_internal(?MODULE, Buffer_Name, false).

%% @doc Read one data item from a buffer following the semantics of the buffer type.
read(Buffer_Name) when is_atom(Buffer_Name) ->
    read(Buffer_Name, 1).

%% @doc Read multiple data items from a buffer following the semantics of the buffer type.
read(Buffer_Name, Num_Items)
  when is_atom(Buffer_Name), is_integer(Num_Items), Num_Items > 0 ->
    read_internal(?MODULE, Buffer_Name, Num_Items, false).

%% @doc Read all currently queued data items (ring and FIFO only).
read_all_timestamped(Buffer_Name)
  when is_atom(Buffer_Name) ->
    read_all_internal(?MODULE, Buffer_Name, true).

%% @doc Read one data item from a buffer following the semantics of the buffer type.
read_timestamped(Buffer_Name) when is_atom(Buffer_Name) ->
    read_timestamped(Buffer_Name, 1).

%% @doc Read multiple data items from a buffer following the semantics of the buffer type.
read_timestamped(Buffer_Name, Num_Items)
  when is_atom(Buffer_Name), is_integer(Num_Items), Num_Items > 0 ->
    read_internal(?MODULE, Buffer_Name, Num_Items, true).

%% @doc
%%   Return all buffered data which is still present, even if previously read.
%%   The order of the list is according to the semantics of the buffer type.
%% @end
history(Buffer_Name) when is_atom(Buffer_Name) ->
    history_internal(?MODULE, Buffer_Name, false).

%% @doc
%%   Return the last N buffered items still present, even if previously read.
%%   The order of the list is according to the semantics of the buffer type.
%% @end
history(Buffer_Name, Num_Items)
  when is_atom(Buffer_Name), is_integer(Num_Items), Num_Items > 0 ->
    history_internal(?MODULE, Buffer_Name, Num_Items, false).

%% @doc
%%   Return all buffered data which is still present, even if previously read.
%%   The order of the list is according to the semantics of the buffer type.
%% @end
history_timestamped(Buffer_Name) when is_atom(Buffer_Name) ->
    history_internal(?MODULE, Buffer_Name, true).

%% @doc
%%   Return the last N buffered items still present, even if previously read.
%%   The order of the list is according to the semantics of the buffer type.
%% @end
history_timestamped(Buffer_Name, Num_Items)
  when is_atom(Buffer_Name), is_integer(Num_Items), Num_Items > 0 ->
    history_internal(?MODULE, Buffer_Name, Num_Items, true).

%% @doc Return the number of unread entries present in a buffer
num_entries(Buffer_Name) when is_atom(Buffer_Name) ->
    num_entries_internal(?MODULE, Buffer_Name).

%% @doc Return the potential capacity of the buffer
capacity(Buffer_Name) when is_atom(Buffer_Name) ->
    capacity_internal(?MODULE, Buffer_Name).

%% @doc Reset the high water count to zero.
clear_high_water(Buffer_Name) when is_atom(Buffer_Name) ->
    clear_high_water_internal(?MODULE, Buffer_Name).

    
%%%------------------------------------------------------------------------------
%%% External API for separately named ets_buffers (one per ets table)
%%%------------------------------------------------------------------------------

-spec list_dedicated(buffer_name()) -> proplists:proplist().
-spec create_dedicated(buffer_name(), fifo | lifo) -> buffer_name().
-spec create_dedicated(buffer_name(), ring, buffer_size()) -> buffer_name().
-spec clear_dedicated(buffer_name()) -> boolean().
-spec delete_dedicated(buffer_name()) -> boolean().

-spec write_dedicated(buffer_name(), any()) -> non_neg_integer() | true | buffer_error().
-spec read_dedicated(buffer_name()) -> [buffer_data()] | buffer_error().
-spec read_dedicated(buffer_name(), pos_integer()) -> [buffer_data()] | buffer_error().
-spec read_all_dedicated(buffer_name()) -> [buffer_data()] | buffer_error().
-spec read_timestamped_dedicated(buffer_name()) -> [buffer_data_timestamped()] | buffer_error().
-spec read_timestamped_dedicated(buffer_name(), pos_integer()) -> [buffer_data_timestamped()] | buffer_error().
-spec read_all_timestamped_dedicated(buffer_name()) -> [buffer_data_timestamped()] | buffer_error().
-spec history_dedicated(buffer_name()) -> [buffer_data()].
-spec history_dedicated(buffer_name(), pos_integer()) -> [buffer_data()].
-spec history_timestamped_dedicated(buffer_name()) -> [buffer_data()].
-spec history_timestamped_dedicated(buffer_name(), pos_integer()) -> [buffer_data()].
-spec num_entries_dedicated(buffer_name()) -> non_neg_integer() | buffer_error().
-spec capacity_dedicated(buffer_name()) -> pos_integer() | unlimited | buffer_error().
-spec clear_high_water_dedicated(buffer_name()) -> true | buffer_error().


%% @doc Get a single proplist for the buffer in a named ets table.
list_dedicated(Buffer_Name) when is_atom(Buffer_Name) ->
    case all_buffers(Buffer_Name) of
        []       -> [];
        [Buffer] -> make_buffer_proplist(Buffer)
    end.

%% @doc
%%   Initialize a new empty named ETS table to hold a FIFO or LIFO buffer.
%%   This function is used when the number of accesses to a shared
%%   ets table would be too high, or when independent ets life cycle
%%   provides a quicker way to eliminate buffer memory.
%% @end
create_dedicated(Buffer_Name, Buffer_Type)
  when is_atom(Buffer_Name), (Buffer_Type =:= fifo orelse Buffer_Type =:= lifo) ->
    Tid = ets:new(Buffer_Name, [named_table, ordered_set, public, {keypos, 2}]),
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
    Tid = ets:new(Buffer_Name, [named_table, ordered_set, public, {keypos, 2}]),
    Buffer_Meta = make_buffer_meta(Buffer_Name, ring, Buffer_Size),
    ets:insert_new(Buffer_Name, Buffer_Meta),
    Tid.

%% @doc Remove all entries from a dedicated buffer, but keep the empty buffer.
clear_dedicated(Buffer_Name) when is_atom(Buffer_Name) ->
    clear_internal(Buffer_Name, Buffer_Name).

%% @doc Delete the entire dedicate ets table.
delete_dedicated(Buffer_Name) when is_atom(Buffer_Name) ->
    try   ets:delete(Buffer_Name)
    catch error:badarg -> {missing_ets_buffer, Buffer_Name}
    end.

%% @doc Write data to the named ets buffer table following the semantics of the buffer type.
write_dedicated(Buffer_Name, Data) when is_atom(Buffer_Name) ->
    write_internal(Buffer_Name, Buffer_Name, Data).

%% @doc Read all currently queued data items from a dedicated buffer (ring and FIFO only).
read_all_dedicated(Buffer_Name)
  when is_atom(Buffer_Name) ->
    read_all_internal(Buffer_Name, Buffer_Name, false).

%% @doc Read one data item from a dedicated buffer following the semantics of the buffer type.
read_dedicated(Buffer_Name) when is_atom(Buffer_Name) ->
    read_dedicated(Buffer_Name, 1).

%% @doc Read multiple data items from a dedicated buffer following the semantics of the buffer type.
read_dedicated(Buffer_Name, Num_Items)
  when is_atom(Buffer_Name), is_integer(Num_Items), Num_Items > 0 ->
    read_internal(Buffer_Name, Buffer_Name, Num_Items, false).

%% @doc Read all currently queued data items from a dedicated buffer (ring and FIFO only).
read_all_timestamped_dedicated(Buffer_Name)
  when is_atom(Buffer_Name) ->
    read_all_internal(Buffer_Name, Buffer_Name, true).

%% @doc Read one data item from a dedicated buffer following the semantics of the buffer type.
read_timestamped_dedicated(Buffer_Name) when is_atom(Buffer_Name) ->
    read_timestamped_dedicated(Buffer_Name, 1).

%% @doc Read multiple data items from a dedicated buffer following the semantics of the buffer type.
read_timestamped_dedicated(Buffer_Name, Num_Items)
  when is_atom(Buffer_Name), is_integer(Num_Items), Num_Items > 0 ->
    read_internal(Buffer_Name, Buffer_Name, Num_Items, true).

%% @doc
%%   Return all buffered data which is still present in a named ets buffer table,
%%   even if previously read. The order of the list is from oldest item to newest item.
%% @end
history_dedicated(Buffer_Name) when is_atom(Buffer_Name) ->
    history_internal(Buffer_Name, Buffer_Name, false).

%% @doc
%%   Return the last N buffered items still present in a named ets buffer table,
%%   even if previously read. The order of the list is from oldest item to newest item.
%% @end
history_dedicated(Buffer_Name, Num_Items)
  when is_atom(Buffer_Name), is_integer(Num_Items), Num_Items > 0 ->
    history_internal(Buffer_Name, Buffer_Name, Num_Items, false).

%% @doc
%%   Return all buffered data which is still present in a named ets buffer table,
%%   even if previously read. The order of the list is from oldest item to newest item.
%% @end
history_timestamped_dedicated(Buffer_Name) when is_atom(Buffer_Name) ->
    history_internal(Buffer_Name, Buffer_Name, true).

%% @doc
%%   Return the last N buffered items still present in a named ets buffer table,
%%   even if previously read. The order of the list is from oldest item to newest item.
%% @end
history_timestamped_dedicated(Buffer_Name, Num_Items)
  when is_atom(Buffer_Name), is_integer(Num_Items), Num_Items > 0 ->
    history_internal(Buffer_Name, Buffer_Name, Num_Items, true).

%% @doc Return the number of unread entries present in a buffer
num_entries_dedicated(Buffer_Name) when is_atom(Buffer_Name) ->
    num_entries_internal(Buffer_Name, Buffer_Name).

%% @doc Return the potential capacity of the buffer
capacity_dedicated(Buffer_Name) when is_atom(Buffer_Name) ->
    capacity_internal(Buffer_Name, Buffer_Name).

%% @doc Return the potential capacity of the buffer
clear_high_water_dedicated(Buffer_Name) when is_atom(Buffer_Name) ->
    clear_high_water_internal(Buffer_Name, Buffer_Name).


%%%------------------------------------------------------------------------------
%%% Internal functions
%%%------------------------------------------------------------------------------

-define(READ_RETRIES, 30).

-define(READ_LOC,    {#ets_buffer.read_loc,    0}).
-define(WRITE_LOC,   {#ets_buffer.write_loc,   0}).
-define(RESERVE_LOC, {#ets_buffer.reserve_loc, 0}).

-define(TYPE_AND_SIZE,  {#ets_buffer.type, 0}, {#ets_buffer.size, 0}, {#ets_buffer.high_water, 0}).
-define(TYPE_AND_WRITE, ?TYPE_AND_SIZE,  ?WRITE_LOC).
-define(TYPE_AND_READW, ?TYPE_AND_WRITE, ?READ_LOC).

%% Use only writers to get the values so that a read lock isn't used.
get_buffer_type (Table_Name, Buffer_Name) -> get_buffer_type_and_pos(Table_Name, Buffer_Name, [?TYPE_AND_SIZE]).
get_buffer_write(Table_Name, Buffer_Name) -> get_buffer_type_and_pos(Table_Name, Buffer_Name, [?TYPE_AND_WRITE]).
get_buffer_readw(Table_Name, Buffer_Name) -> get_buffer_type_and_pos(Table_Name, Buffer_Name, [?TYPE_AND_READW]).

get_buffer_type_and_pos(Table_Name, Buffer_Name, Update_Cmd) ->
    try   ets:update_counter(Table_Name, meta_key(Buffer_Name), Update_Cmd)
    catch error:badarg -> false
    end.

clear_internal(Table_Name, Buffer_Name) ->
    try 
        ets:match_delete(Table_Name, #buffer_data{key=buffer_key(Buffer_Name, '_'), _='_'}),
        ets:update_element(Table_Name, meta_key(Buffer_Name), [?RESERVE_LOC, ?WRITE_LOC, ?READ_LOC])
    catch error:badarg -> {missing_ets_buffer, Buffer_Name}
    end.

%% All writes use 1) reserve, 2) write, 3) publish semantics.
write_internal(Table_Name, Buffer_Name, Data) ->
    Meta_Key = meta_key(Buffer_Name),
    case get_buffer_type(Table_Name, Buffer_Name) of
        false -> {missing_ets_buffer, Buffer_Name};
        [Type_Num, Max_Loc, High_Water_Count] ->
                case buffer_type(Type_Num) of

                    %% FIFO continuously increments...
                    fifo -> Reserved_Loc = ets:update_counter(Table_Name, Meta_Key, fifo_reserve_write_cmd()),
                            true = insert_ets_internal(Table_Name, Buffer_Name, Reserved_Loc, Data),
                            [New_Write_Loc, Read_Loc] = ets:update_counter(Table_Name, Meta_Key, fifo_publish_write_cmd()),
                            Num_Entries = compute_num_entries(Type_Num, Max_Loc, New_Write_Loc, Read_Loc),
                            Num_Entries > High_Water_Count andalso set_high_water(Table_Name, Meta_Key, Num_Entries),
                            Num_Entries;

                    %% Ring buffer wraps around on inserts...
                    ring -> [Reserved_Loc, Read_Loc]  = ets:update_counter(Table_Name, Meta_Key, ring_reserve_write_cmd(Max_Loc)),
                            true = insert_ets_internal(Table_Name, Buffer_Name, Reserved_Loc, Data),
                            [New_Write_Loc, New_Read_Loc] = ets:update_counter(Table_Name, Meta_Key, ring_publish_write_cmd(Max_Loc, Reserved_Loc, Read_Loc)),
                            Num_Entries = compute_num_entries(Type_Num, Max_Loc, New_Write_Loc, New_Read_Loc),
                            Num_Entries > High_Water_Count andalso set_high_water(Table_Name, Meta_Key, Num_Entries),
                            Num_Entries;

                    %% LIFO continuously decrements...
                    lifo -> Reserved_Loc = ets:update_counter(Table_Name, Meta_Key, lifo_reserve_write_cmd()),
                            true = insert_ets_internal(Table_Name, Buffer_Name, Reserved_Loc, Data),
                            _ = ets:update_counter(Table_Name, Meta_Key, lifo_publish_write_cmd(Reserved_Loc)),
                            %% High water mark not possible with current LIFO approach
                            true
                end
    end.

insert_ets_internal(Table_Name, Buffer_Name, Write_Loc, Data) ->
    ets:insert(Table_Name, buffer_data(Buffer_Name, Write_Loc, Data)).

set_high_water(Table_Name, Meta_Key, New_High_Water) ->
    ets:update_element(Table_Name, Meta_Key, set_high_water_cmd(New_High_Water)).

%% Use read pointer to reserve entries, obtain and then delete them.
read_all_internal(Table_Name, Buffer_Name, With_Timestamps) ->
    case get_buffer_readw(Table_Name, Buffer_Name) of
        false                                            -> {missing_ets_buffer, Buffer_Name};
        [?LIFO_NUM, _Max_Loc, _High_Water, _Write_Loc, _Old_Read_Loc] -> not_supported;
        [_Type_Num, _Max_Loc, _High_Water,  Write_Loc,  Write_Loc]    -> [];
        [?FIFO_NUM, _Max_Loc, _High_Water,  Write_Loc,  Old_Read_Loc] ->
            read_internal_finish(fifo_reserve_read_all_cmd(Write_Loc, Old_Read_Loc), Table_Name, Buffer_Name, fifo, With_Timestamps);
        [?RING_NUM, _Max_Loc, _High_Water,  Write_Loc, _Old_Read_Loc] ->
            read_internal_finish(ring_reserve_read_all_cmd(Write_Loc),               Table_Name, Buffer_Name, ring, With_Timestamps)
    end.

read_internal_finish(New_Read_Loc_Cmd, Table_Name, Buffer_Name, Buffer_Type, With_Timestamps) ->
            [Start_Read_Loc, End_Read_Loc] = ets:update_counter(Table_Name, meta_key(Buffer_Name), New_Read_Loc_Cmd),
            read_ets(Table_Name, Buffer_Name, Buffer_Type, Start_Read_Loc, End_Read_Loc, ?READ_RETRIES, With_Timestamps).

%% Use read pointer to reserve entries, obtain and then delete them.
read_internal(Table_Name, Buffer_Name, Num_Items, With_Timestamps) ->
    case get_buffer_readw(Table_Name, Buffer_Name) of
        false -> {missing_ets_buffer, Buffer_Name};
        [Type_Num, Max_Loc, _High_Water, Write_Loc, Old_Read_Loc] ->
            case buffer_type(Type_Num) of
                fifo -> read_internal_finish(fifo_reserve_read_cmd(Num_Items, Write_Loc),                        Table_Name, Buffer_Name, fifo, With_Timestamps);
                ring -> read_internal_finish(ring_reserve_read_cmd(Num_Items, Write_Loc, Max_Loc, Old_Read_Loc), Table_Name, Buffer_Name, ring, With_Timestamps);
                lifo -> case Num_Items of
                            1 -> read_lifo(Table_Name, Buffer_Name, Old_Read_Loc, With_Timestamps);
                            _ -> not_supported
                        end
            end
    end.

read_lifo(Table_Name, Buffer_Name, Read_Loc, With_Timestamps) ->
    Buffer_Entry_Match = get_lifo_match_specs(Buffer_Name, Read_Loc),
    case ets:select(Table_Name, Buffer_Entry_Match, 1) of
        '$end_of_table' -> [];
        {[#buffer_data{key=Key, created=Timestamp, data=Data}], _Continuation} ->
            %% The time gap from the select might allow another pid to read this value prior
            %% to the following delete, resulting in a double-read from the LIFO queue.
            %% select_delete(Tab, MatchSpec, Count, true) would solve this issue.
            ets:delete(Table_Name, Key),
            case With_Timestamps of
                true  -> [{Timestamp, Data}];
                false -> [Data]
            end
    end.

get_lifo_match_specs(Buffer_Name, Read_Loc) ->
    Key = buffer_key(Buffer_Name, '$1'),
    Match = #buffer_data{key=Key, _='_'},
    Guard = [{'=<', Read_Loc, '$1'}],
    [{Match, Guard, ['$_']}].    %% Get the full object so it can be deleted.

%%%------------------------------------------------------------------------------
%%% Read logic has several possibilities:
%%%   If Start/End are the same, return an empty set of data...
%%%   If Retry attempts yield nothing, return an empty set without deleting any data...
%%%   If an attempt yields any data, delete (except for ring) and return the results
%%%
%%% Retries are necessary when read slot(s) is reserved, but not yet written,
%%% because other processes jumped in with reservations and even writes before
%%% the first reservation had a chance to publish its write. Presumably the
%%% data will show up before we give up, but it is possible that the return
%%% value is {missing_ets_data, Buffer_Name, Read_Loc}.
%%%------------------------------------------------------------------------------

read_ets(_Table_Name, _Buffer_Name, _Buffer_Type,  Read_Loc, Read_Loc, _Retries, _With_Timestamps) -> [];
read_ets( Table_Name,  Buffer_Name,  Buffer_Type,  Read_Loc,  End_Loc,  Retries,  With_Timestamps) ->
    {Buffer_Entry_Match, Buffer_Deletes} = get_read_match_specs(Buffer_Name, Buffer_Type, Read_Loc, End_Loc, With_Timestamps),
    read_ets_retry(Table_Name, Buffer_Name, Buffer_Type, Read_Loc, End_Loc, Retries, Buffer_Entry_Match, Buffer_Deletes).

read_ets_retry(_Table_Name, Buffer_Name, _Buffer_Type, Read_Loc, _End_Loc, 0, _, _) -> {missing_ets_data, Buffer_Name, Read_Loc};
read_ets_retry( Table_Name, Buffer_Name,  Buffer_Type, Read_Loc,  End_Loc, Retries, Buffer_Entry_Match, Buffer_Deletes) ->
    case ets:select(Table_Name, Buffer_Entry_Match) of
        []   -> erlang:yield(),
                read_ets_retry(Table_Name, Buffer_Name, Buffer_Type, Read_Loc, End_Loc, Retries-1, Buffer_Entry_Match, Buffer_Deletes);
        Data -> Buffer_Type =/= ring andalso ets:select_delete(Table_Name, Buffer_Deletes),
                Data
    end.

%% Select all ring buffer_data [Read_Loc =< Max_Loc] ++ [1 =< Key =< End_Loc]...
get_read_match_specs(Buffer_Name, ring, Read_Loc, End_Loc, With_Timestamps)
  when End_Loc < Read_Loc ->
    disjoint_match_specs(Buffer_Name, Read_Loc, End_Loc, 'orelse', With_Timestamps);

%% Select all ring/fifo/lifo buffer_data Read_Loc =< Key =< End_Loc.
get_read_match_specs(Buffer_Name, _Buffer_Type, Read_Loc, End_Loc, With_Timestamps) ->
    disjoint_match_specs(Buffer_Name, Read_Loc, End_Loc, 'andalso', With_Timestamps).

disjoint_match_specs(Buffer_Name, Read_Loc, End_Loc, Operator, With_Timestamps) ->
    Key = buffer_key(Buffer_Name, '$1'),
    Guard = [{Operator, {'<', Read_Loc, '$1'}, {'>=', End_Loc, '$1'}}],
    case With_Timestamps of
        true  -> Match = #buffer_data{key=Key, data='$2', created='$3'},
                 {[{Match, Guard, [{{'$3', '$2'}}]}],  %% Get the timestamp + data item
                  [{Match, Guard, [true]}]};           %% Delete the data item
        false -> Match = #buffer_data{key=Key, data='$2', created='_' },
                 {[{Match, Guard, ['$2']}],            %% Get only the data item
                  [{Match, Guard, [true]}]}            %% Delete the data item
    end.
    

%% Currently this function assumes the number of items is not excessive and fetches all in one try.
history_internal(Table_Name, Buffer_Name, With_Timestamps) ->
    case get_buffer_write(Table_Name, Buffer_Name) of
        false -> {missing_ets_buffer, Buffer_Name};
        [Type_Num, Max_Loc, _High_Water, Write_Pos] ->
            case {buffer_type(Type_Num), With_Timestamps} of
                {ring,              _ } -> history_ring(Table_Name, Buffer_Name, Max_Loc, Write_Pos, Max_Loc, With_Timestamps);
                {_Fifo_Or_Lifo, false } -> [Elem || [Elem] <- ets:match(Table_Name, buffer_data(Buffer_Name, '_', '_', '$1'))];
                {_Fifo_Or_Lifo, true  } -> [{Time, Elem} || [Time, Elem] <- ets:match(Table_Name, buffer_data(Buffer_Name, '_', '$1', '$2'))]
            end
    end.

history_internal(Table_Name, Buffer_Name, Num_Items, With_Timestamps) ->
    case get_buffer_write(Table_Name, Buffer_Name) of
        false -> {missing_ets_buffer, Buffer_Name};
        [Type_Num, Max_Loc, _High_Water, Write_Pos] ->
            True_Num_Items = case Max_Loc of 0 -> Num_Items; _ -> min(Num_Items, Max_Loc) end,
            case buffer_type(Type_Num) of
                ring -> history_ring(Table_Name, Buffer_Name, True_Num_Items, Write_Pos, Max_Loc, With_Timestamps);
                fifo -> history_internal_limited(Table_Name, Buffer_Name, True_Num_Items, With_Timestamps);
                lifo -> history_internal_limited(Table_Name, Buffer_Name, True_Num_Items, With_Timestamps)
            end
    end.

history_internal_limited(Table_Name, Buffer_Name, Num_Items, _With_Timestamps=false) ->
    Pattern = {buffer_data(Buffer_Name, '_',  '_', '$1'),[],['$1']},
    select_history_matches(Table_Name, Pattern, Num_Items);
history_internal_limited(Table_Name, Buffer_Name, Num_Items, _With_Timestamps=true) ->
    Pattern = {buffer_data(Buffer_Name, '_', '$2', '$1'),[],[{{'$2', '$1'}}]},
    select_history_matches(Table_Name, Pattern, Num_Items).

history_ring(_Table_Name, _Buffer_Name, _Num_Items,         0, _Max_Loc, _With_Timestamps) -> [];
history_ring( Table_Name,  Buffer_Name,  Num_Items, Write_Pos, _Max_Loc,  With_Timestamps)
 when Num_Items < Write_Pos ->
    history_ring_get(Table_Name, Buffer_Name, Num_Items, Write_Pos - Num_Items + 1, With_Timestamps);
history_ring( Table_Name,  Buffer_Name,  Num_Items, Write_Pos,  Max_Loc,  With_Timestamps) ->
    case Num_Items - Write_Pos of
        0         -> history_ring_get(Table_Name, Buffer_Name, Num_Items, 1, With_Timestamps);
        Old_Chunk -> history_ring_get(Table_Name, Buffer_Name, Old_Chunk, Max_Loc - Old_Chunk + 1, With_Timestamps)
                      ++ history_ring_get(Table_Name, Buffer_Name, Write_Pos, 1, With_Timestamps)
    end.

history_ring_get(_Table_Name, _Buffer_Name,         0, _Start_Pos, _With_Timestamps) -> [];
history_ring_get( Table_Name,  Buffer_Name, Num_Items,  Start_Pos,  false          ) ->
    Pattern = {buffer_data(Buffer_Name, '$1', '_', '$2'), [{'>=', '$1', Start_Pos}], ['$2']},
    select_history_matches(Table_Name, Pattern, Num_Items);
history_ring_get( Table_Name,  Buffer_Name, Num_Items,  Start_Pos,  true           ) ->
    Pattern = {buffer_data(Buffer_Name, '$1', '$3', '$2'), [{'>=', '$1', Start_Pos}], [{{'$3', '$2'}}]},
    select_history_matches(Table_Name, Pattern, Num_Items).

select_history_matches(Table_Name, Pattern, Num_Items) ->
    case ets:select(Table_Name, [Pattern], Num_Items) of
        '$end_of_table'          -> [];
        {Matches, _Continuation} -> Matches
    end.

%% Compute the number of entries using read and write pointers
%% This is not supported for LIFO buffers because they may have
%% holes in the buffer and an accurate size cannot simply be
%% computed. Scanning the table also fails because it is non-atomic
%% and the result might not reflect the number of LIFO entries.
num_entries_internal(Table_Name, Buffer_Name) ->
    case get_buffer_readw(Table_Name, Buffer_Name) of
        false -> {missing_ets_buffer, Buffer_Name};
        [Type_Num, Max_Loc, _High_Water, Write_Loc, Raw_Read_Loc] ->
            compute_num_entries(Type_Num, Max_Loc, Write_Loc, Raw_Read_Loc)
    end.

compute_num_entries(Type_Num, Max_Loc, Write_Loc, Raw_Read_Loc) ->
    Read_Loc = case {Write_Loc =:= 0, Raw_Read_Loc} of
                   {true,  _} -> 0;             %% Nothing ever written
                   {false, 0} -> 0;             %% Written, but nothing ever read
                   {false, _} -> Raw_Read_Loc   %% Both written and read before
               end,
    case buffer_type(Type_Num) of
        lifo -> not_supported;                  %% Note: R/W indexes are negative
        fifo -> Write_Loc - Read_Loc;
        ring -> case Write_Loc >= Read_Loc of
                    true  -> Write_Loc - Read_Loc;
                    false -> (Max_Loc - Read_Loc) + Write_Loc
                end
    end.

%% Ring buffers have capacity limits; all others do not
capacity_internal(Table_Name, Buffer_Name) ->
    case get_buffer_readw(Table_Name, Buffer_Name) of
        false -> {missing_ets_buffer, Buffer_Name};
        [Type_Num, Max_Loc, _High_Water, _Write_Loc, _Read_Loc] ->
            case buffer_type(Type_Num) of
                fifo -> unlimited;
                lifo -> unlimited;
                ring -> Max_Loc
            end
    end.

clear_high_water_internal(Table_Name, Buffer_Name) ->
    set_high_water(Table_Name, meta_key(Buffer_Name), 0).
