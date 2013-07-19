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
                   fifo_publish_write_cmd/0, fifo_reserve_read_cmd/2,
                   lifo_reserve_write_cmd/0, %% lifo_reserve_read_cmd/2, 
                   get_buffer_type/2, get_buffer_readw/2, get_buffer_write/2,
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

-type buffer_error() :: not_supported
                      | {missing_ets_buffer, buffer_name()}
                      | {missing_ets_data,   buffer_name(), buffer_loc()}.

-type buffer_type()     :: ring | fifo | lifo.
-type buffer_type_num() :: 1    | 2    | 3.

-export_type([buffer_type/0, buffer_error/0]).


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

-define(RING_NUM, buffer_type_num(ring)).
-define(FIFO_NUM, buffer_type_num(fifo)).
-define(LIFO_NUM, buffer_type_num(lifo)).

%% Record stored in ets table, or used for match spec.
-record(ets_buffer, {
          name                     :: {meta, buffer_name()} | {meta, '_'},
          size        = 0          :: buffer_size()         | '_',
          type        = ?RING_NUM  :: buffer_type_num()     | '_',   %% Default is ring buffer
          reserve_loc = 0          :: non_neg_integer()     | '_',
          write_loc   = 0          :: non_neg_integer()     | '_',
          read_loc    = 0          :: non_neg_integer()     | '_'
         }).

meta_key(Buffer_Name) -> {meta, Buffer_Name}.
    
%% Convert record to proplist.
make_buffer_proplist(#ets_buffer{name={meta, Name}, size=Size, type=Type_Num,
                                 reserve_loc=Reserve_Loc, write_loc=Write_Loc, read_loc=Read_Loc}) ->
    [{name, Name}, {size, Size}, {type, buffer_type(Type_Num)},
     {reserve_loc, Reserve_Loc}, {write_loc, Write_Loc}, {read_loc, Read_Loc}].

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
fifo_publish_write_cmd() ->  {#ets_buffer.write_loc,   1}.

%% FIFO read reserve atomically gets latest read location and increments it.
fifo_reserve_read_cmd(Num_Items, Write_Loc) ->
    [{#ets_buffer.read_loc, 0}, {#ets_buffer.read_loc, Num_Items, Write_Loc-1, Write_Loc}].


%% Ring bumps write location, but only moves read location if the write pointer wraps to it.
ring_reserve_write_cmd(Max)                              -> [{#ets_buffer.reserve_loc, 1, Max, 1}, {#ets_buffer.read_loc, 0}].
ring_publish_write_cmd(Max,  Reserve_Loc,  Reserve_Loc)  -> [{#ets_buffer.write_loc,   1, Max, 1}, {#ets_buffer.read_loc, 1, Max, 1}];
ring_publish_write_cmd(Max, _Reserve_Loc, _Old_Read_Loc) ->  {#ets_buffer.write_loc,   1, Max, 1}.

%% Increment read pointer
ring_reserve_read_cmd(Num_Items, Write_Loc, Max_Loc, Read_Loc) ->
    case Write_Loc >= Read_Loc of
        true  -> [{#ets_buffer.read_loc, 0}, {#ets_buffer.read_loc, Num_Items, Write_Loc-1, Write_Loc}];
        false -> New_Read_Loc = min(Num_Items - (Max_Loc - Read_Loc), Max_Loc),
                 [{#ets_buffer.read_loc, 0}, {#ets_buffer.read_loc, 0, Write_Loc, New_Read_Loc}]
    end.

%% LIFO bumps write location, but only moves read location if it is not already past the reserve/write for this publish.
lifo_reserve_write_cmd()            ->  {#ets_buffer.reserve_loc, -1}.
lifo_publish_write_cmd(Reserve_Loc) -> [{#ets_buffer.write_loc,   -1}, {#ets_buffer.read_loc, 0, Reserve_Loc, Reserve_Loc}].
    

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
    case one_buffer(?MODULE, Buffer_Name) of
        []       -> [];
        [Buffer] -> make_buffer_proplist(Buffer)
    end.


%% @doc Initialize or add to a single ETS table with multiple named buffers.
create(Init_Data) when is_list(Init_Data) ->
    ets:info(?MODULE, named_table) =/= undefined
        orelse ets:new(?MODULE, [named_table, ordered_set, public, {keypos, 2}, {write_concurrency, true}]),
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
  when is_atom(Buffer_Name), is_integer(Buffer_Size), Buffer_Size > 0,
       (Buffer_Type =:= ring orelse Buffer_Type =:= fifo orelse Buffer_Type =:= lifo) ->
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

%% @doc Read one data item from a buffer following the semantics of the buffer type.
read(Buffer_Name) when is_atom(Buffer_Name) ->
    read(Buffer_Name, 1).

%% @doc Read multiple data items from a buffer following the semantics of the buffer type.
read(Buffer_Name, Num_Items)
  when is_atom(Buffer_Name), is_integer(Num_Items), Num_Items > 0 ->
    read_internal(?MODULE, Buffer_Name, Num_Items).

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

-spec write_dedicated(buffer_name(), any()) -> true.
-spec read_dedicated(buffer_name()) -> [buffer_data()] | buffer_error().
-spec read_dedicated(buffer_name(), pos_integer()) -> [buffer_data()] | buffer_error().
-spec history_dedicated(buffer_name()) -> [buffer_data()].
-spec history_dedicated(buffer_name(), pos_integer()) -> [buffer_data()].


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
    Tid = ets:new(Buffer_Name, [named_table, ordered_set, public, {keypos, 2}, {write_concurrency, true}]),
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
    Tid = ets:new(Buffer_Name, [named_table, ordered_set, public, {keypos, 2}, {write_concurrency, true}]),
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

-define(READ_RETRIES, 3).

-define(READ_LOC,    {#ets_buffer.read_loc,    0}).
-define(WRITE_LOC,   {#ets_buffer.write_loc,   0}).
-define(RESERVE_LOC, {#ets_buffer.reserve_loc, 0}).

-define(TYPE_AND_SIZE,  {#ets_buffer.type, 0}, {#ets_buffer.size, 0}).
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
    case get_buffer_type(Table_Name, Buffer_Name) of
        false -> {missing_ets_buffer, Buffer_Name};
        [Type_Num, Max_Loc] ->
            _ = case buffer_type(Type_Num) of

                    %% FIFO continuously increments...
                    fifo -> Write_Loc = ets:update_counter(Table_Name, meta_key(Buffer_Name), fifo_reserve_write_cmd()),
                            true = ets:insert(Table_Name, buffer_data(Buffer_Name, Write_Loc, Data)),
                            ets:update_counter(Table_Name, meta_key(Buffer_Name), fifo_publish_write_cmd());

                    %% Ring buffer wraps around on inserts...
                    ring -> [Write_Loc, Read_Loc]  = ets:update_counter(Table_Name, meta_key(Buffer_Name), ring_reserve_write_cmd(Max_Loc)),
                            true = ets:insert(Table_Name, buffer_data(Buffer_Name, Write_Loc, Data)),
                            ets:update_counter(Table_Name, meta_key(Buffer_Name), ring_publish_write_cmd(Max_Loc, Write_Loc, Read_Loc));

                    %% LIFO continuously decrements...
                    lifo -> Write_Loc = ets:update_counter(Table_Name, meta_key(Buffer_Name), lifo_reserve_write_cmd()),
                            true = ets:insert(Table_Name, buffer_data(Buffer_Name, Write_Loc, Data)),
                            ets:update_counter(Table_Name, meta_key(Buffer_Name), lifo_publish_write_cmd(Write_Loc))
                end,
            true
    end.

%% Use read pointer to reserve entries, obtain and then delete them.
read_internal(Table_Name, Buffer_Name, Num_Items) ->
    case get_buffer_readw(Table_Name, Buffer_Name) of
        false -> {missing_ets_buffer, Buffer_Name};
        [Type_Num, Max_Loc, Write_Loc, Old_Read_Loc] ->
            case buffer_type(Type_Num) of

                %% Bump read pointer atomically, then read the data from old location to new...
                fifo -> New_Read_Loc_Cmd = fifo_reserve_read_cmd(Num_Items, Write_Loc),
                        [Start_Read_Loc, End_Read_Loc] = ets:update_counter(Table_Name, meta_key(Buffer_Name), New_Read_Loc_Cmd),
                        read_ets(Table_Name, Buffer_Name, fifo, Start_Read_Loc, End_Read_Loc, ?READ_RETRIES);

                %% Bump read pointer with wraparound if any entries available...
                ring -> New_Loc_Cmd = ring_reserve_read_cmd(Num_Items, Write_Loc, Max_Loc, Old_Read_Loc),
                        [Start_Read_Loc, End_Read_Loc] = ets:update_counter(Table_Name, meta_key(Buffer_Name), New_Loc_Cmd),
                        read_ets(Table_Name, Buffer_Name, ring, Start_Read_Loc, End_Read_Loc, ?READ_RETRIES);

                lifo -> case Num_Items of
                            1 -> read_lifo(Table_Name, Buffer_Name, Old_Read_Loc);
                            _ -> not_supported
                        end
            end
    end.

read_lifo(Table_Name, Buffer_Name, Read_Loc) ->
    Buffer_Entry_Match = get_lifo_match_specs(Buffer_Name, Read_Loc),
    case ets:select(Table_Name, Buffer_Entry_Match, 1) of
        '$end_of_table' -> [];
        {[#buffer_data{key=Key, data=Data}], _Continuation} ->
            %% The time gap from the select might allow another pid to read this value prior
            %% to the following delete, resulting in a double-read from the LIFO queue.
            %% select_delete(Tab, MatchSpec, Count, true) would solve this issue.
            ets:delete(Table_Name, Key),
            [Data]
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
%%%   If an attempt yields any data, delete and return the results
%%%
%%% Retries are necessary because read slot(s) may be reserved, but not written,
%%% because other processes jumped in with reservations and even writes before
%%% the first reservation had a chance to publish its write. Presumably the
%%% data will show up before we give up, but it is possible that the return
%%% value is {missing_ets_data, Buffer_Name, Read_Loc}.
%%%------------------------------------------------------------------------------

read_ets(_Table_Name, _Buffer_Name, _Buffer_Type,  Read_Loc, Read_Loc, _Retries) -> [];
read_ets( Table_Name,  Buffer_Name,  Buffer_Type,  Read_Loc,  End_Loc,  Retries) ->
    {Buffer_Entry_Match, Buffer_Deletes} = get_read_match_specs(Buffer_Name, Buffer_Type, Read_Loc, End_Loc),
    read_ets_retry(Table_Name, Buffer_Name, Buffer_Type, Read_Loc, End_Loc, Retries, Buffer_Entry_Match, Buffer_Deletes).

read_ets_retry(_Table_Name, Buffer_Name, _Buffer_Type, Read_Loc, _End_Loc, 0, _, _) -> {missing_ets_data, Buffer_Name, Read_Loc};
read_ets_retry( Table_Name, Buffer_Name,  Buffer_Type, Read_Loc,  End_Loc, Retries, Buffer_Entry_Match, Buffer_Deletes) ->
    case ets:select(Table_Name, Buffer_Entry_Match) of
        []   -> erlang:yield(),
                read_ets_retry(Table_Name, Buffer_Name, Buffer_Type, Read_Loc, End_Loc, Retries-1, Buffer_Entry_Match, Buffer_Deletes);
        Data -> ets:select_delete(Table_Name, Buffer_Deletes),
                Data
    end.

%% Select all ring buffer_data [Read_Loc =< Max_Loc] ++ [1 =< Key =< End_Loc]...
get_read_match_specs(Buffer_Name, ring, Read_Loc, End_Loc)
  when End_Loc < Read_Loc ->
    disjoint_match_specs(Buffer_Name, Read_Loc, End_Loc, 'orelse');

%% Select all ring/fifo/lifo buffer_data Read_Loc =< Key =< End_Loc.
get_read_match_specs(Buffer_Name, _Buffer_Type, Read_Loc, End_Loc) ->
    disjoint_match_specs(Buffer_Name, Read_Loc, End_Loc, 'andalso').

disjoint_match_specs(Buffer_Name, Read_Loc, End_Loc, Operator) ->
    Key = buffer_key(Buffer_Name, '$1'),
    Match = #buffer_data{key=Key, data='$2'},
    Guard = [{Operator, {'<', Read_Loc, '$1'}, {'>=', End_Loc, '$1'}}],
    {[{Match, Guard, ['$2']}],    %% Get the data item
     [{Match, Guard, [true]}]}.   %% Delete the data item

%% Currently this function assumes the number of items is not excessive and fetches all in one try.
history_internal(Table_Name, Buffer_Name) ->
    case get_buffer_write(Table_Name, Buffer_Name) of
        false -> {missing_ets_buffer, Buffer_Name};
        [Type_Num, Max_Loc, Write_Pos] ->
            case buffer_type(Type_Num) of
                ring -> history_ring(Table_Name, Buffer_Name, Max_Loc, Write_Pos, Max_Loc);
                fifo -> [Elem || [Elem] <- ets:match(Table_Name, buffer_data(Buffer_Name, '_', '$1'))];
                lifo -> [Elem || [Elem] <- ets:match(Table_Name, buffer_data(Buffer_Name, '_', '$1'))]
            end
    end.

history_internal(Table_Name, Buffer_Name, Num_Items) ->
    case get_buffer_write(Table_Name, Buffer_Name) of
        false -> {missing_ets_buffer, Buffer_Name};
        [Type_Num, Max_Loc, Write_Pos] ->
            case buffer_type(Type_Num) of
                ring          -> history_ring(Table_Name, Buffer_Name, Num_Items, Write_Pos, Max_Loc);
                _Fifo_Or_Lifo -> Pattern = {buffer_data(Buffer_Name, '_', '$1'),[],[['$1']]},
                                 history_internal_limited(Table_Name, Pattern, Num_Items)
            end
    end.

history_internal_limited(Table_Name, Pattern, Num_Items) ->
    [Elem || [Elem] <- case ets:select(Table_Name, [Pattern], Num_Items) of
                           '$end_of_table'          -> [];
                           {Matches, _Continuation} -> Matches
                       end].

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
    case ets:select(Table_Name, [{buffer_data(Buffer_Name, '$1', '$2'),
                                 [{'>=', '$1', Start_Pos}], ['$2']}], Num_Items) of
        '$end_of_table'          -> [];
        {Matches, _Continuation} -> Matches
    end.
