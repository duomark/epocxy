%%%------------------------------------------------------------------------------
%%% @copyright (c) 2013, DuoMark International, Inc.
%%% @author Jay Nelson <jay@duomark.com>
%%% @reference The license is based on the template for Modified BSD from
%%%   <a href="http://opensource.org/licenses/BSD-3-Clause">OSI</a>
%%% @doc
%%%   ETS buffers implement ring, FIFO and LIFO access.
%%% @since v1.0.0
%%% @end
%%%------------------------------------------------------------------------------
-module(ets_buffer).
-author('Jay Nelson <jay@duomark.com>').

-compile({inline, [
                   buffer_type/1, buffer_type_num/1,
                   buffer_key/2, buffer_data/3,
                   ring_writer_inc/1,
                   get_buffer_type/2, get_buffer_type_and_pos/2
                  ]}).

%% External interface
-export([
         %% API for buffers in a shared 'ets_buffer' named ets table...
         create/1, write/2, history/1, history/2,
         clear/1, delete/1, list/0, list/1,

         %% API when each buffer is in a dedicated named ets table.
         create_unique/3, write_unique/2, history_unique/1, history_unique/2,
         clear_unique/1, delete_unique/1, list_unique/1
        ]).

-type buffer_name() :: atom().
-type buffer_size() :: pos_integer().
-type buffer_loc()  :: pos_integer().
-type buffer_data() :: any().

-type buffer_type()     :: ring | fifo | lifo.
-type buffer_type_num() :: 1    | 2    | 3.

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
          write_loc = 0          :: non_neg_integer() | '_'
         }).

%% Convert record to proplist.
make_buffer_proplist(#ets_buffer{name=Name, size=Size, type=Type_Num, write_loc=Write_Loc}) ->
    [{name, Name}, {size, Size}, {type, buffer_type(Type_Num)}, {write_loc, Write_Loc}].

%% Match specs for buffers.
all_buffers(Table_Name) ->
    ets:match_object(Table_Name, #ets_buffer{name='_', size='_', type='_', write_loc='_'}).
one_buffer(Table_Name, Buffer_Name) ->
    ets:match_object(Table_Name, #ets_buffer{name=Buffer_Name, size='_', type='_', write_loc='_'}).

%% Wraparound ets update_counter formula.
ring_writer_inc(Max) ->
    {#ets_buffer.write_loc, 1, Max, 1}.

-record(buffer_data, {
          key                    :: {buffer_name(), buffer_loc()},
          data                   :: buffer_data()
         }).

%% Record format of all ets_buffer data.
buffer_key(Name, Loc) -> {Name, Loc}.
buffer_data(Name, Loc, Data) ->
    #buffer_data{key=buffer_key(Name, Loc), data=Data}.
    

%%%------------------------------------------------------------------------------
%%% External API for colocated ets_buffers (in a single ?MODULE ets table)
%%%------------------------------------------------------------------------------

-spec list() -> proplists:proplist().
-spec list(buffer_name()) -> proplists:proplist().
-spec create([{buffer_name(), buffer_type(), buffer_size()}]) -> ets_buffer.
-spec clear(buffer_name()) -> boolean().
-spec delete(buffer_name()) -> boolean().

-spec write(buffer_name(), buffer_data()) -> true | not_implemented_yet.
-spec history(buffer_name()) -> [buffer_data()].
-spec history(buffer_name(), pos_integer()) -> [buffer_data()].


%% @doc Get a set of proplists for all buffers in the shared ets table.
list() ->
    [make_buffer_proplist(Buffer) || Buffer <- all_buffers(?MODULE)].

%% @doc Get a single proplist for a given buffer in the shared ets table.
list(Buffer_Name) ->
    [make_buffer_proplist(Buffer) || Buffer <- one_buffer(?MODULE, Buffer_Name)].


%% @doc Initialize or add to a single ETS table with multiple named buffers.
create(Init_Data) ->
    Tid = ets:info(?MODULE, named_table) =/= undefined
        orelse ets:new(?MODULE, [named_table, ordered_set, public, {keypos, 2},
                                 {write_concurrency, true}]),
    _ = [begin
             Buffer_Meta = make_buffer_meta(Buffer_Name, Buffer_Type, Buffer_Size),
             ets:insert_new(?MODULE, Buffer_Meta)
         end || {Buffer_Name, Buffer_Type, Buffer_Size} <- Init_Data],
    Tid.

make_buffer_meta(Buffer_Name, Buffer_Type, Buffer_Size)
  when is_atom(Buffer_Name), is_integer(Buffer_Size), Buffer_Size > 0,
       (Buffer_Type =:= ring orelse Buffer_Type =:= fifo orelse Buffer_Type =:= lifo) ->
    #ets_buffer{name=Buffer_Name, type=buffer_type_num(Buffer_Type), size=Buffer_Size}.

%% @doc Remove all entries from a specific buffer, but keep the empty buffer.
clear(Buffer_Name) ->
    clear(?MODULE, Buffer_Name).

%% @doc Remove all entries and delete a specific buffer.
delete(Buffer_Name) ->
    clear(?MODULE, Buffer_Name),
    ets:delete(?MODULE, Buffer_Name).

%% @doc Write data to the buffer following the semantics of the buffer type.
write(Buffer_Name, Data) ->
    write(?MODULE, Buffer_Name, Data).

%% @doc
%%   Return all buffered data which is still present, even if previously read.
%%   The order of the list is from oldest item to newest item.
%% @end
history(Buffer_Name) ->
    history_internal(?MODULE, Buffer_Name).

%% @doc
%%   Return the last N buffered items still present, even if previously read.
%%   The order of the list is from oldest item to newest item.
%% @end
history(Buffer_Name, Num_Items) ->
    history_internal(?MODULE, Buffer_Name, Num_Items).

    
%%%------------------------------------------------------------------------------
%%% External API for separately named ets_buffers (one per ets table)
%%%------------------------------------------------------------------------------

-spec list_unique(buffer_name()) -> proplists:proplist().
-spec create_unique(buffer_name(), buffer_type(), buffer_size()) -> buffer_name().
-spec clear_unique(buffer_name()) -> boolean().
-spec delete_unique(buffer_name()) -> boolean().

-spec write_unique(buffer_name(), any()) -> true | not_implemented_yet.
-spec history_unique(buffer_name()) -> [buffer_data()].
-spec history_unique(buffer_name(), pos_integer()) -> [buffer_data()].


%% @doc Get a single proplist for the buffer in a named ets table.
list_unique(Buffer_Name) ->
    [Buffer] = all_buffers(Buffer_Name),
    make_buffer_proplist(Buffer).

%% @doc
%%   Initialize a new empty named ETS table to hold a buffer.
%%   This function is used when the number of accesses to a shared
%%   ets table would be too high, or when independent ets life cycle
%%   provides a quicker way to eliminate buffer memory.
%% @end
create_unique(Buffer_Name, Buffer_Type, Buffer_Size) ->
    Tid = ets:new(Buffer_Name, [named_table, ordered_set, public]),
    Buffer_Meta = make_buffer_meta(Buffer_Name, Buffer_Type, Buffer_Size),
    ets:insert_new(Buffer_Name, Buffer_Meta),
    Tid.

%% @doc Remove all entries from a specific buffer, but keep the empty buffer.
clear_unique(Buffer_Name) ->
    clear(Buffer_Name, Buffer_Name).

%% @doc Remove all entries and delete a specific buffer.
delete_unique(Buffer_Name) ->
    clear(Buffer_Name, Buffer_Name),
    ets:delete(Buffer_Name, Buffer_Name).

%% @doc Write data to the named ets buffer table following the semantics of the buffer type.
write_unique(Buffer_Name, Data) ->
    write(Buffer_Name, Buffer_Name, Data).

%% @doc
%%   Return all buffered data which is still present in a named ets buffer table,
%%   even if previously read. The order of the list is from oldest item to newest item.
%% @end
history_unique(Buffer_Name) ->
    history_internal(Buffer_Name, Buffer_Name).

%% @doc
%%   Return the last N buffered items still present in a named ets buffer table,
%%   even if previously read. The order of the list is from oldest item to newest item.
%% @end
history_unique(Buffer_Name, Num_Items) ->
    history_internal(Buffer_Name, Buffer_Name, Num_Items).


%%%------------------------------------------------------------------------------
%%% Internal functions
%%%------------------------------------------------------------------------------

get_buffer_type(Table_Name, Buffer_Name) ->
    ets:update_counter(Table_Name, Buffer_Name, [{#ets_buffer.type, 0}, {#ets_buffer.size, 0}]).

get_buffer_type_and_pos(Table_Name, Buffer_Name) ->
    ets:update_counter(Table_Name, Buffer_Name,
                       [{#ets_buffer.type, 0}, {#ets_buffer.size, 0}, {#ets_buffer.write_loc, 0}]).

clear(Table_Name, Buffer_Name) ->
    ets:match_delete(Table_Name, #buffer_data{key=buffer_key(Buffer_Name, '_'), data='_'}),
    ets:update_element(Table_Name, Buffer_Name, {#ets_buffer.write_loc, 0}).
    
write(Table_Name, Buffer_Name, Data) ->
    [Type_Num, Max_Loc] = get_buffer_type(Table_Name, Buffer_Name),
    case buffer_type(Type_Num) of
        ring -> Loc = ets:update_counter(Table_Name, Buffer_Name, ring_writer_inc(Max_Loc)),
                ets:insert(Table_Name, buffer_data(Buffer_Name, Loc, Data));
        fifo -> not_implemented_yet;
        lifo -> not_implemented_yet
    end.

%% Currently this function assumes the number of items is not excessive and fetches all in one try.
history_internal(Table_Name, Buffer_Name) ->
    [Type_Num, Max_Loc, Write_Pos] = get_buffer_type_and_pos(Table_Name, Buffer_Name),
    case buffer_type(Type_Num) of
        ring -> history_ring(Table_Name, Buffer_Name, Max_Loc, Write_Pos, Max_Loc);
        fifo -> not_implemented_yet;
        lifo -> not_implemented_yet
    end.

history_internal(Table_Name, Buffer_Name, Num_Items) ->
    [Type_Num, Max_Loc, Write_Pos] = get_buffer_type_and_pos(Table_Name, Buffer_Name),
    case buffer_type(Type_Num) of
        ring -> history_ring(Table_Name, Buffer_Name, Num_Items, Write_Pos, Max_Loc);
        fifo -> not_implemented_yet;
        lifo -> not_implemented_yet
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
