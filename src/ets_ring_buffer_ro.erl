%%%------------------------------------------------------------------------------
%%% @copyright (c) 2014-2015, DuoMark International, Inc.
%%% @author Jay Nelson <jay@duomark.com> [http://duomark.com/]
%%% @reference 2014-2015 Development sponsored by TigerText, Inc. [http://tigertext.com/]
%%% @reference The license is based on the template for Modified BSD from
%%%   <a href="http://opensource.org/licenses/BSD-3-Clause">OSI</a>
%%% @doc
%%%   An ets_ring_buffer_ro is implemented as an unsorted hash ets
%%%   table with array semantics for the keys. Each key contains
%%%   {Array_Name, Index_Pos} and is accessed by using ets:update_counter
%%%   to traverse the array. Multiple processes may access concurrently,
%%%   with the ets table locking the single metadata record for a given
%%%   array to allow only single atomic increment of the current read
%%%   position. Once a read position index is obtained, it may be read
%%%   concurrently since the ets table maintains multiple record-level
%%%   locks and we serially allocate read positions.
%%%
%%%   This is the read-only variant (signified by "_ro" in the name)
%%%   which allows only the atomic creation or replacement of an entire
%%%   ring buffer. Most accesses are by readers, so the underlying ets
%%%   tables holding ring data will always have only read_concurrency
%%%   set true. As with other buffer data types driven by metadata, the
%%%   metadata ets table is mainly accessed by writing and has only
%%%   write_concurrency set true.
%%% @since 0.9.8d
%%% @end
%%%------------------------------------------------------------------------------
-module(ets_ring_buffer_ro).
-author('Jay Nelson <jay@duomark.com>').

-export([
         create/1,       % create an empty ring buffer
         create/2,       % create a ring buffer with initial data
         replace/2,      % replace an existing ring buffer's data with new values
         clear/1,        % remove all data from a ring buffer
         delete/1,       % remove data and delete metadata
         list/0,         % provide a list of metadata for all ring buffers
         list/1,         % provide metadata for a single ring buffer
         read/1          % read the next value and increment read position
        ]).

-type ring_name()       :: atom().
-type ring_generation() :: pos_integer().
-type ring_size()       :: non_neg_integer().
-type ring_loc()        :: pos_integer().
-type ring_data()       :: any().

-type ring_error() :: {missing_ring_name,     ring_name()}
                    | {missing_ring_metadata, ring_name()}
                    | {missing_ring_data,     ring_name(), ring_loc()}
                    | {buffer_is_empty,       ring_name()}.

-export_type([ring_error/0]).

-define(RING_RO_TABLE, ring_buffer_ro).


%%%------------------------------------------------------------------------------
%%% Ring metadata definitions
%%%------------------------------------------------------------------------------

%% Record stored in ets table (also used for matchspecs).
-record(ring_ro_metadata, {
          name              :: {meta, ring_name()} | {meta, '_'},
          generation  = 1   :: ring_generation()   | '_',
          buffer            :: ets:tid()           | '_',
          created           :: erlang:timestamp()  | '_',
          size        = 0   :: ring_size()         | '_',
          read_loc    = 0   :: ring_loc()      | 0 | '_'   % 0 signifies a buffer that has never been read
         }).

-define(RING_SIZE,       {#ring_ro_metadata.size,       0}).
-define(RING_BUFFER,     {#ring_ro_metadata.buffer,     0}).
-define(LAST_READ_LOC,   {#ring_ro_metadata.read_loc,   0}).
-define(BUMP_GENERATION, {#ring_ro_metadata.generation, 1}).

-define(RESERVE_READ_LOC(__Size), [?RING_BUFFER, {#ring_ro_metadata.read_loc, 1, __Size, 1}]).

meta_key(Ring_Name) -> {meta, Ring_Name}.
make_meta(Ring_Name, Ring_Table_Id, Size) ->
    #ring_ro_metadata{name=meta_key(Ring_Name), buffer=Ring_Table_Id, created=os:timestamp(), size=Size}.
    
%% Convert record to proplist.
make_ring_proplist(#ring_ro_metadata{name={meta, Name}, generation=Gen, buffer=Buffer,
                                     created=Created, size=Size, read_loc=Read_Loc}) ->
    [{name, Name},  {generation, Gen}, {buffer, Buffer},
     {created, Created}, {size, Size}, {read_loc, Read_Loc}].

%% Match specs for buffers.
all_rings() ->
    try   ets:match_object(?RING_RO_TABLE, #ring_ro_metadata{name=meta_key('_'), _='_'})
    catch error:badarg -> []
    end.
one_ring(Ring_Name) ->
    try   ets:match_object(?RING_RO_TABLE, #ring_ro_metadata{name=meta_key(Ring_Name), _='_'})
    catch error:badarg -> []
    end.

%% Use only writers to get the values so that a read lock isn't used on metadata.
get_ring_size      (Ring_Name) -> get_ring_metadata_field(Ring_Name, ?RING_SIZE).
get_ring_last_read (Ring_Name) -> get_ring_metadata_field(Ring_Name, ?LAST_READ_LOC).

%% Reserve the next read location for the calling process to read.
%% Since the pointer starts at 0, the reserved location is after increment
%% and wrapping is applied. The LAST_READ_LOC will return this same value
%% if get_ring_last_read/1 is called after calling get_ring_next_read/1.
%% Since this is a read-only data structure, fast readers on a small ring
%% may result in a quick wraparound resulting in several processes
%% contending for the read lock on a single reserved read location.
%% In general we expect multiple readers up to the maximum number of
%% row locks in ets to be able to read fully concurrently, once they
%% have obtained a read location from the single metadata record which
%% will be highly contended itself.
get_ring_next_read(Ring_Name) ->
    %% Unfortunately, must fetch size before increment so we can properly wrap around the read pointer.
    case get_ring_size(Ring_Name) of
        0    -> {undefined, 0, 0};
        Size -> [Ring_Buffer, Read_Loc] = get_ring_metadata_field(Ring_Name, ?RESERVE_READ_LOC(Size)),
                {Ring_Buffer, Size, Read_Loc}
    end.

%% Uses update_counter to maintain the write_concurrency lock.
get_ring_metadata_field(Ring_Name, Update_Cmd) ->
    try   ets:update_counter(?RING_RO_TABLE, meta_key(Ring_Name), Update_Cmd)
    catch error:badarg -> false
    end.

%% Uses lookup so hits a read lock penalty.
get_ring_metadata(Ring_Name) ->
    try ets:lookup(?RING_RO_TABLE, meta_key(Ring_Name)) of
        [Metadata] -> Metadata;
        []         -> missing
    catch error:badarg -> false
    end.


%%%------------------------------------------------------------------------------
%%% Ring data definitions
%%%------------------------------------------------------------------------------

%% Record format of stored ring data
-record(ring_ro_data, {
          key          :: {ring_name(), ring_loc()},
          data         :: ring_data()
         }).

ring_key(Name, Loc) -> {Name, Loc}.
make_ring_data(Name, Loc, Data) -> #ring_ro_data{key=ring_key(Name, Loc), data=Data}.


%%%------------------------------------------------------------------------------
%%% External API
%%%------------------------------------------------------------------------------

-spec list()            -> [proplists:proplist()].
-spec list(ring_name()) ->  proplists:proplist().

-spec create (ring_name(), [ring_data()]) -> boolean().  % with values
-spec create (ring_name()) -> boolean().                 % empty ring
-spec clear  (ring_name()) -> boolean().                 % eliminate ring data only
-spec delete (ring_name()) -> boolean().                 % eliminate ring and metadata
-spec read   (ring_name()) -> {ok, ring_data()} | {error, ring_error()}.

%% @doc Get a set of proplists for all ring buffers in the metadata ets table.
list() -> [make_ring_proplist(Ring_Metadata) || Ring_Metadata <- all_rings()].

%% @doc Get a single proplist for a given ring buffer in the metadata ets table.
list(Ring_Name) when is_atom(Ring_Name) ->
    case one_ring(Ring_Name) of
        []              -> [];
        [Ring_Metadata] -> make_ring_proplist(Ring_Metadata)
    end.

%% @doc Initialize an empty ring buffer.
create(Ring_Name) when is_atom(Ring_Name) ->

    %% Create global read_only ring ets meta-table when first ring is created...
    ets:info(?RING_RO_TABLE, named_table) =/= undefined
        orelse epocxy_ets_fsm:create_ets_table(?RING_RO_TABLE, write_only),

    %% Then insert the metadata record.
    ets:insert_new(?RING_RO_TABLE, make_meta(Ring_Name, undefined, 0)).


%% @doc
%%   Initialize a ring buffer with a set of values. The metadata is initialized
%%   with the buffer size set to the number of values and the current read pointer
%%   set to an initial value prior to the first element of the ring.
%% @end
create(Ring_Name, Ring_Values)
  when is_atom(Ring_Name), is_list(Ring_Values) ->

    %% Create global read_only ring ets meta-table when first ring is created...
    ets:info(?RING_RO_TABLE, named_table) =/= undefined
        orelse epocxy_ets_fsm:create_ets_table(?RING_RO_TABLE, write_only),

    %% Allocate a new unnamed ets table to hold the ring values...
    Ring_Buffer = epocxy_ets_fsm:create_ets_table(read_only),
    case create_ring(Ring_Name, Ring_Buffer, Ring_Values) of
        true  -> true;

        %% Eliminating created tables if there are errors inserting any of the values:
        %%   1) Ring name already exists
        %%   2) Duplicate keys already exist in the ring_buffer
        false -> ets:delete(?RING_RO_TABLE, meta_key(Ring_Name)),
                 epocxy_ets_fsm:delete_ets_table(Ring_Buffer)
    end.

%% @doc
%%   Create a new ring buffer table, then replace the metadata definition of the
%%   ring buffer, and finally delete the original ring buffer data table.
%% @end
replace(Ring_Name, Ring_Values)
  when is_atom(Ring_Name), is_list(Ring_Values) ->

    %% Allocate a new unnamed ets table to hold the ring values...
    Ring_Buffer = epocxy_ets_fsm:create_ets_table(read_only),
    replace_ring(Ring_Name, Ring_Buffer, Ring_Values).

%% @doc
%%   Remove all entries from a specific buffer, but keep the empty metadata record.
%%   This function does a read on the ring metadata table so it will incur a slow
%%   lock penalty, but it is used infrequently.
%% @end
clear(Ring_Name) when is_atom(Ring_Name) ->
    case get_ring_metadata(Ring_Name) of
        missing -> false;

        #ring_ro_metadata{buffer=Ring_Buffer} ->
            %% Clear the pointers in the metadata first, for immediate effect...
            try   reset_metadata(Ring_Name, undefined, [])
            after epocxy_ets_fsm:delete_ets_table(Ring_Buffer)
            end
    end.

%% @doc
%%   Delete the ring metadata, then delete the ring data buffer. Thie function
%%   does a read on the ring metadata table so it will incur a slow lock
%%   penalty, but it is used infrequently.
%% @end
delete(Ring_Name) when is_atom(Ring_Name) ->
    case get_ring_metadata(Ring_Name) of
        missing -> false;
        #ring_ro_metadata{buffer=Ring_Buffer} ->
            try   true = ets:delete(?RING_RO_TABLE, meta_key(Ring_Name))
            after Ring_Buffer =/= undefined
                      andalso epocxy_ets_fsm:delete_ets_table(Ring_Buffer)
            end
    end.

%% @doc
%%   Reserve the next read location, then read the data from the ring
%%   buffer ets table. The reservation is highly contended on a metadata
%%   record lock, but the actual read is much more concurrent.
%% @end
read(Ring_Name) when is_atom(Ring_Name) ->
    case get_ring_next_read(Ring_Name) of
        {undefined, 0, 0} ->
            {error, {buffer_is_empty, Ring_Name}};
        {Ring_Buffer, _Size, Location} when is_integer(Location), Location > 0 ->
            read_value(Ring_Name, Ring_Buffer, Location)
    end.


%%%------------------------------------------------------------------------------
%%% Internal functions
%%%------------------------------------------------------------------------------

create_ring(Ring_Name, Ring_Buffer, Ring_Values) ->
    case ets:insert_new(?RING_RO_TABLE, make_meta(Ring_Name, Ring_Buffer, length(Ring_Values))) of
        false -> false;
        true  -> insert_values(Ring_Name, Ring_Buffer, Ring_Values, 1)
    end.

replace_ring(Ring_Name, New_Ring_Buffer, Ring_Values) ->
    case get_ring_metadata(Ring_Name) of
        missing ->
            epocxy_ets_fsm:delete_ets_table(New_Ring_Buffer),
            false;
        #ring_ro_metadata{buffer=Old_Ring_Buffer} ->
            true = insert_values (Ring_Name, New_Ring_Buffer, Ring_Values, 1),
            try   reset_metadata (Ring_Name, New_Ring_Buffer, Ring_Values)
            after Old_Ring_Buffer =/= undefined
                      andalso epocxy_ets_fsm:delete_ets_table(Old_Ring_Buffer)
            end
    end.

reset_metadata(Ring_Name, New_Ring_Buffer, Ring_Values) ->
    Meta_Key     = meta_key(Ring_Name),
    Reset_Values = [{#ring_ro_metadata.buffer,       New_Ring_Buffer},
                    {#ring_ro_metadata.created,       os:timestamp()},
                    {#ring_ro_metadata.size,     length(Ring_Values)},
                    {#ring_ro_metadata.read_loc,                   0}],
    ets:update_element(?RING_RO_TABLE, Meta_Key, Reset_Values),

    %% Then increment the generation (in case someone else updated), and remove ring data.
    %% Any outstanding references to ring data table will crash if used after the delete.
    _ = ets:update_counter(?RING_RO_TABLE, Meta_Key, ?BUMP_GENERATION),
    true.

insert_values(Ring_Name, Ring_Buffer, Values, Pos) ->
    case format_values(Ring_Name, Values, Pos, []) of
        []           -> true;
        Table_Values -> ets:insert_new(Ring_Buffer, Table_Values)
    end.
        
format_values(_Ring_Name,             [], _Pos, Table_Values) -> Table_Values;
format_values( Ring_Name, [Value | More],  Pos, Table_Values) ->
    New_Value = make_ring_data(Ring_Name, Pos, Value),
    format_values(Ring_Name, More, Pos+1, [New_Value | Table_Values]).

read_value(Ring_Name, Ring_Buffer, Location) ->
    Key = ring_key(Ring_Name, Location),
    try   ets:lookup_element(Ring_Buffer, Key, #ring_ro_data.data)
    catch error:badarg -> {missing_ring_data, Key}
    end.