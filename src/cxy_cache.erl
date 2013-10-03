%%%------------------------------------------------------------------------------
%%% @copyright (c) 2013, DuoMark International, Inc.
%%% @author Jay Nelson <jay@duomark.com>
%%% @reference 2013 Development sponsored by TigerText, Inc. [http://tigertext.com/]
%%% @reference The license is based on the template for Modified BSD from
%%%   <a href="http://opensource.org/licenses/BSD-3-Clause">OSI</a>
%%% @doc
%%%   Generational caching using an ets table for each generation of cached
%%%   data. Accesses hit the newer generation first, and migrate from the
%%%   older generation to the newer generation when retrieved from the
%%%   stale table. When a new generation is initally started, the oldest
%%%   table is deleted. This is a form of mass garbage collection which
%%%   avoids using timers and expiration of individual cached elements.
%%% @since 0.9.6
%%% @end
%%%------------------------------------------------------------------------------
-module(cxy_cache).
-author('Jay Nelson <jay@duomark.com>').

-compile({inline, [return_cache_gen1/2, return_cache_gen2/2, return_cache_miss/2,
                   return_and_count_cache/3]}).

%% External interface
-export([
         reserve/2, reserve/3, reserve/4,
         create/1, clear/1, delete/1,
         info/0, info/1,
         replace_check_generation_fun/2,
         delete_item/2, fetch_item/2,
         get_and_clear_counts/1
        ]).

%% Comparator functions called from gen_fun() closure.
-export([new_gen_count_threshold/4, new_gen_time_threshold/4]).

%% Manual generation change functions, must be called from ets owner process.
-export([new_generation/1, maybe_make_new_generation/1]).

-type cache_name()    :: atom().
-type cache_key()     :: any().
-type cache_value()   :: any().
-type thresh_type()   :: count | time.
-type gen_fun()       :: fun((cache_name(), non_neg_integer(), erlang:timestamp()) -> boolean()).
-type gen_fun_opt()   :: none | gen_fun().
-type check_gen_fun() :: gen_fun_opt() | thresh_type().

-export_type([cache_name/0, cache_key/0, cache_value/0,
              gen_fun/0, gen_fun_opt/0, thresh_type/0]).

-record(cxy_cache_meta,
        {
          cache_name                      :: cache_name(),
          started        = os:timestamp() :: erlang:timestamp(),
          fetch_count    = 0              :: non_neg_integer(),
          gen1_hit_count = 0              :: non_neg_integer(),
          gen2_hit_count = 0              :: non_neg_integer(),
          miss_count     = 0              :: non_neg_integer(),
          error_count    = 0              :: non_neg_integer(),
          new_gen_time                    :: erlang:timestamp(),
          old_gen_time                    :: erlang:timestamp(),
          new_gen                         :: ets:tid(),
          old_gen                         :: ets:tid(),
          cache_module                    :: module(),
          new_generation_function = none  :: check_gen_fun(),
          new_generation_thresh   = 0     :: non_neg_integer()
        }).

%% Each cxy_cache behaviour must have a module which defines the Key => Value function.
-callback create_key_value(Key::term()) -> New_Value::term().


%%%------------------------------------------------------------------------------
%%% The cache meta-data is stored as a single record per cache in cxy_cache table.
%%%------------------------------------------------------------------------------

-spec reserve(atom(), atom()) -> atom() | {error, already_exists}.
-spec reserve(atom(), atom(), check_gen_fun()) -> atom() | {error, already_exists}.
-spec reserve(atom(), atom(), thresh_type(), pos_integer()) -> atom() | {error, already_exists}.

-spec create(atom()) -> true.

-spec info() -> proplists:proplist().
-spec info(atom()) -> proplists:proplist().

-spec clear(cache_name()) -> boolean().
-spec delete(cache_name()) -> boolean().
-spec replace_check_generation_fun(cache_name(), check_gen_fun()) -> true.

%% Comparators available to gen_fun().
-spec new_gen_count_threshold(cache_name(), Fetch_Count, Time, Thresh)
                             -> boolean() when Fetch_Count :: non_neg_integer(),
                                               Time        :: erlang:timestamp(),
                                               Thresh      :: non_neg_integer().

-spec new_gen_time_threshold(cache_name(), Fetch_Count, Time, Thresh)
                            -> boolean() when Fetch_Count :: non_neg_integer(),
                                              Time        :: erlang:timestamp(),
                                              Thresh      :: non_neg_integer().

%% @doc Reserve a new cache name and use Mod:create_key_value(Key) to establish items, with no new generations.
reserve(Cache_Name, Cache_Mod)
  when is_atom(Cache_Name), is_atom(Cache_Mod) ->
    reserve(Cache_Name, Cache_Mod, none).

%% @doc Reserve a new cache name and use fun(Name, Count, Time) to decide when new_generations occur.
reserve(Cache_Name, Cache_Mod, New_Gen_Fun)
  when is_atom(Cache_Name), is_atom(Cache_Mod),
       (New_Gen_Fun =:= none orelse is_function(New_Gen_Fun, 3)) ->
    case init_meta_index(Cache_Name, Cache_Mod, New_Gen_Fun) of
        false -> {error, already_exists};
        true  -> Cache_Name
    end.

%% @doc Reserve a new cache with default count or time thresholds for a new generation.
reserve(Cache_Name, Cache_Mod, Threshold_Type, Threshold)
  when is_atom(Cache_Name), is_atom(Cache_Mod), is_integer(Threshold),
       %% New generation per access count can be any integer...
       (Threshold_Type =:= count andalso Threshold > 0
        %% But time-based generations can't be faster than 1 millisecond.
        orelse Threshold_Type =:= time andalso Threshold >= 1000) ->

    case init_meta_index(Cache_Name, Cache_Mod, Threshold_Type, Threshold) of
        false -> {error, already_exists};
        true  -> Cache_Name
    end.


-define(UNLESS_METADATA(__Code), ets:info(?MODULE, named_table) =/= undefined orelse  __Code).
-define(DO_METADATA(__Code),     ets:info(?MODULE, named_table) =/= undefined andalso __Code).
-define(GET_METADATA(__Code), case ets:info(?MODULE, named_table) of undefined -> []; _Found -> __Code end).

%% Meta support functions. The default config var *_thresh can represent either the
%% fetch count or the elapsed microseconds before a new generation is created.
init_meta_index(Cache_Name, Cache_Mod, New_Gen_Fun) ->
    init_meta_ets_table(#cxy_cache_meta{cache_name=Cache_Name,
                                        cache_module=Cache_Mod,
                                        new_generation_function=New_Gen_Fun}).
    
init_meta_index(Cache_Name, Cache_Mod, Threshold_Type, Threshold) ->
    init_meta_ets_table(#cxy_cache_meta{cache_name=Cache_Name,
                                        cache_module=Cache_Mod,
                                        new_generation_function=Threshold_Type,
                                        new_generation_thresh=Threshold}).

%% The singleton metadata table stores records and is indexed on cache_name.
init_meta_ets_table(Initial_Meta_Data) ->
    ?UNLESS_METADATA(ets:new(?MODULE, [named_table, set, public, {keypos, #cxy_cache_meta.cache_name}])),
    ets:insert_new(?MODULE, Initial_Meta_Data).


%% @doc Populate the initial new and old generation caches for a pre-reserved cache_name.    
create(Cache_Name) when is_atom(Cache_Name)->
    ?DO_METADATA(initialize_generation_caches(Cache_Name)).

initialize_generation_caches(Cache_Name) ->
    Now = os:timestamp(),
    ets:update_element(?MODULE, Cache_Name,
                       [{#cxy_cache_meta.old_gen_time, Now},
                        {#cxy_cache_meta.new_gen_time, Now},
                        {#cxy_cache_meta.old_gen, new_cache_gen()},   %% Create oldest first
                        {#cxy_cache_meta.new_gen, new_cache_gen()}]).

%% Cache generations use neither records, nor concurrency, but can be modified by any process.
new_cache_gen() -> ets:new(cache, [set, public]).

%% Comparators available to Gen_Fun closure which provides its own threshold value.
new_gen_count_threshold(_Name,  Fetch_Count, _Time, Thresh) -> Fetch_Count > Thresh.
new_gen_time_threshold (_Name, _Fetch_Count,  Time, Thresh) -> timer:now_diff(os:timestamp(), Time) > Thresh.


%%%------------------------------------------------------------------------------
%%% Cache metadata reporting and update.
%%%------------------------------------------------------------------------------
                              
%% @doc Report metadata about caches.
info() -> ?GET_METADATA([fmt_info(Metadata) || Metadata <- ets:tab2list(?MODULE)]).

%% @doc Report metadata about a single cache.
info(Cache_Name) when is_atom(Cache_Name) -> ?GET_METADATA(report_info(Cache_Name)).

report_info(Cache_Name) ->    
    case ets:lookup(?MODULE, Cache_Name) of
        [] -> [];
        [#cxy_cache_meta{} = Metadata] -> fmt_info(Metadata)
    end.

fmt_info(#cxy_cache_meta{cache_name=Cache_Name, started=Started, fetch_count=Fetches,
                         cache_module=Cache_Mod,
                         new_generation_function=NGF, new_generation_thresh=NGT,
                         new_gen_time=New_Time, new_gen=New, old_gen_time=Old_Time, old_gen=Old,
                         gen1_hit_count=Gen1_Hits, gen2_hit_count=Gen2_Hits,
                         miss_count=Miss_Count, error_count=Error_Count}) ->
    Now = os:timestamp(),
    New_Time_Diff = round(timer:now_diff(Now, New_Time) / 10000 / 60) / 100,
    Old_Time_Diff = round(timer:now_diff(Now, Old_Time) / 10000 / 60) / 100,
    [[New_Count, New_Memory], [Old_Count, Old_Memory]]
        = [[ets:info(Tab, Attr) || Attr <- [size, memory]] || Tab <- [New, Old]],

    %% Fetch count reflects the next request count, so we decrement by 1.
    {Cache_Name, [{started, Started}, {num_accesses_this_generation, Fetches},
                  {cache_module, Cache_Mod},
                  {new_generation_function, NGF}, {new_generation_thresh, NGT},
                  {new_gen_time_minutes, New_Time_Diff}, {new_gen_count, New_Count},
                  {old_gen_time_minutes, Old_Time_Diff}, {old_gen_count, Old_Count},
                  {new_gen_memory, New_Memory}, {old_gen_memory, Old_Memory},
                  {total_memory, New_Memory + Old_Memory},
                  {gen1_hits, Gen1_Hits}, {gen2_hits, Gen2_Hits},
                  {miss_count, Miss_Count}, {error_count, Error_Count}
                 ]}.

%% @doc Clear all items from the generational caches, but don't delete the tables.
clear(Cache_Name) when is_atom(Cache_Name) -> ?DO_METADATA(clear_meta(Cache_Name)).

clear_meta(Cache_Name) ->
    case ets:lookup(?MODULE, Cache_Name) of
        [] -> false;
        [#cxy_cache_meta{new_gen=New, old_gen=Old}] ->
            _ = [try ets:delete_all_objects(Tab) catch error:badarg -> skip end || Tab <- [New, Old]],
            New_Timestamp = os:timestamp(),
            ets:update_element(?MODULE, Cache_Name,
                               [{#cxy_cache_meta.started,      New_Timestamp},
                                {#cxy_cache_meta.fetch_count,     0},
                                {#cxy_cache_meta.gen1_hit_count,  0},
                                {#cxy_cache_meta.gen2_hit_count,  0},
                                {#cxy_cache_meta.miss_count,      0},
                                {#cxy_cache_meta.error_count,     0},
                                {#cxy_cache_meta.new_gen_time, New_Timestamp},
                                {#cxy_cache_meta.old_gen_time, New_Timestamp}])
    end.

%% @doc Delete the cache metadata and all generation tables.
delete(Cache_Name) when is_atom(Cache_Name) -> ?DO_METADATA(delete_meta(Cache_Name)).

delete_meta(Cache_Name) ->
    case ets:lookup(?MODULE, Cache_Name) of
        []    -> false;
        [#cxy_cache_meta{new_gen=New, old_gen=Old}] ->
            _ = [try ets:delete(Tab) catch error:badarg -> skip end || Tab <- [New, Old]],
            ets:delete(?MODULE, Cache_Name)
    end.

%% @doc Replace the existing new generation decision function.
replace_check_generation_fun(Cache_Name, Fun)
  when is_atom(Cache_Name), (Fun =:= none orelse Fun =:= count orelse Fun =:= time orelse is_function(Fun, 3)) ->
    ets:update_element(?MODULE, Cache_Name, {#cxy_cache_meta.new_generation_function, Fun}).
    

%%%------------------------------------------------------------------------------
%%% Generational caches are accessed via the meta-data, newest first.
%%%------------------------------------------------------------------------------

-type gen1_hit_count() :: non_neg_integer().
-type gen2_hit_count() :: non_neg_integer().
-type error_count()    :: non_neg_integer().
-type miss_count()     :: non_neg_integer().

-spec delete_item(cache_name(), cache_key()) -> true.
-spec fetch_item(cache_name(), cache_key()) -> cache_value() | {error, tuple()}.
-spec get_and_clear_counts(cache_name())
                          -> {cache_name(), gen1_hit_count(), gen2_hit_count(),
                              miss_count(), error_count()}.

delete_item(Cache_Name, Key) ->
    [#cxy_cache_meta{new_gen=New_Gen, old_gen=Old_Gen}] = ets:lookup(?MODULE, Cache_Name),
    ets:info(New_Gen, type) =/= undefined andalso ets:delete(New_Gen, Key),
    ets:info(Old_Gen, type) =/= undefined andalso ets:delete(Old_Gen, Key),
    true.
    
fetch_item(Cache_Name, Key) ->
    [#cxy_cache_meta{new_gen=New_Gen, old_gen=Old_Gen, cache_module=Mod}]
        = ets:lookup(?MODULE, Cache_Name),

    %% Return immediately if found in the new generational cache...
    case ets:lookup(New_Gen, Key) of
        [{Key, Value}] -> return_cache_gen1(Cache_Name, Value);

        %% Otherwise migrate the old generational cached value or create a new cached value.
        [] -> copy_old_value_if_found(Cache_Name, New_Gen, Old_Gen, Key, Mod)
    end.

%% Copy needs to be as close to atomic as possible.
%% Someone else (maybe an external admin) may have inserted a value to the
%% new generational table after we checked it, so use the new value when
%% this happens. Also, if an empty generation is created after we have
%% checked new generation, it is possible that old generation is deleted
%% out from under us. In this case, we pretend the key doesn't exist and
%% create a new one. At worst, we are the only user of this new value
%% or the next access copies it to the empty generation.
copy_old_value_if_found(Cache_Name, New_Gen, Old_Gen, Key, Mod) ->
    try ets:lookup(Old_Gen, Key) of

        %% Create a new Mod:create_key_value value if not in old generation...
        [] -> create_new_value(Cache_Name, New_Gen, Mod, Key);

        %% Otherwise, insert the value to the new generation...
        [{Key, Value} = Obj] ->
            insert_to_new_gen(New_Gen, Obj),
            return_cache_gen2(Cache_Name, Value)
    catch
        %% Old generation was probably eliminated by another request, try creating a new value.
        %% Note the value will actually get inserted into 'old_gen' because New_Gen must've been demoted.
        error:badarg -> create_new_value(Cache_Name, New_Gen, Mod, Key)
    end.

%% Always count fetch requests for statistics reporting, even on time-based caches...
return_cache_gen1 (Cache_Name, Value) -> return_and_count_cache(Cache_Name, Value, {#cxy_cache_meta.gen1_hit_count, 1}).
return_cache_gen2 (Cache_Name, Value) -> return_and_count_cache(Cache_Name, Value, {#cxy_cache_meta.gen2_hit_count, 1}).
return_cache_miss (Cache_Name, Value) -> return_and_count_cache(Cache_Name, Value, {#cxy_cache_meta.miss_count,     1}).
return_cache_error(Cache_Name, Value) -> return_and_count_cache(Cache_Name, Value, {#cxy_cache_meta.error_count,    1}).

return_and_count_cache(Cache_Name, Value, Hit_Type_Op) ->
    Inc_Op = [{#cxy_cache_meta.fetch_count, 1}, Hit_Type_Op],
    _ = ets:update_counter(?MODULE, Cache_Name, Inc_Op),
    Value.

%% Retrieve and reset access counters.
get_and_clear_counts(Cache_Name) ->
    Counters = [#cxy_cache_meta.gen1_hit_count, #cxy_cache_meta.gen2_hit_count,
                #cxy_cache_meta.miss_count, #cxy_cache_meta.error_count],
    Counter_Ops = [[{Counter, 0}, {Counter, 0, 0, 0}] || Counter <- Counters],
    [Gen1, 0, Gen2, 0, Miss, 0, Error, 0] = ets:update_counter(?MODULE, Cache_Name, lists:append(Counter_Ops)),
    {Cache_Name, Gen1, Gen2, Miss, Error}.

%% Create a new value from the cache Mod:create_key_value(Key) but watch for errors in generating it.
create_new_value(Cache_Name, New_Gen, Mod, Key) ->
    try Mod:create_key_value(Key) of
        Val -> insert_to_new_gen(New_Gen, {Key, Val}),
               return_cache_miss(Cache_Name, Val)
    catch Type:Class ->
            Error = {error, {Type,Class, {creating_new_value_with, Mod, Key}}},
            return_cache_error(Cache_Name, Error)
    end.

%% Insert our newly generated value, unless someone else beat us to the insert.
insert_to_new_gen(New_Gen, {Key, Val} = Obj) ->
    case ets:insert_new(New_Gen, Obj) of
        true  -> Val;
        false -> [{Key, Beat_Us_To_It_Value}] = ets:lookup(New_Gen, Key),
                 Beat_Us_To_It_Value
    end.

        
%%%------------------------------------------------------------------------------
%%% Generation creation utilities
%%%
%%% These functions must be called by the process that will own the generational
%%% ets tables. The owning process has to stick around longer than the generation
%%% lasts, or the exit of the owner will destroy the ets table prematurely.
%%%------------------------------------------------------------------------------

-spec new_generation(cache_name()) -> ets:tid().
-spec maybe_make_new_generation(cache_name()) -> boolean().

%% @doc Manually cause a new generation to be created, called from new_gen owner process.
new_generation(Cache_Name) ->
    [#cxy_cache_meta{new_gen=New_Gen, new_gen_time=New_Time, old_gen=Old_Gen}] = ets:lookup(?MODULE, Cache_Name),
    new_generation(Cache_Name, New_Gen, New_Time, Old_Gen).

%% @doc Create a new generation if the generation test returns true.
maybe_make_new_generation(Cache_Name) ->
    [Metadata]
        = [#cxy_cache_meta{new_gen_time=New_Time, fetch_count=Fetch_Count, new_generation_function=New_Gen_Fun, new_generation_thresh=Thresh}]
        = ets:lookup(?MODULE, Cache_Name),
    case New_Gen_Fun of
        none      -> Metadata;
        count     -> make_generation_finish(Cache_Name, Metadata, Fetch_Count > Thresh);
        time      -> make_generation_finish(Cache_Name, Metadata, timer:now_diff(os:timestamp(), New_Time) > Thresh);
        _Function -> make_generation_finish(Cache_Name, Metadata, New_Gen_Fun(Cache_Name, Fetch_Count, New_Time))
    end.

make_generation_finish(_Cache_Name, _Metadata, false) -> false;
make_generation_finish( Cache_Name,  Metadata, true ) ->
    #cxy_cache_meta{new_gen=New_Gen, new_gen_time=New_Time, old_gen=Old_Gen} = Metadata,
    _New_Empty_Gen = new_generation(Cache_Name, New_Gen, New_Time, Old_Gen),
    true.


%% Create a new generation cache and update the metadata to reflect its existence.
new_generation(Cache_Name, New_Gen, New_Time, Old_Gen) ->
    Empty_Gen = new_cache_gen(),

    %% Update the metadata record atomically with the new generation info...
    true = ets:update_element(?MODULE, Cache_Name,
                              [
                               %% Reset the fetch count so it can be used for generation measurement...
                               {#cxy_cache_meta.fetch_count,  0},

                               %% Move the empty generation to the new_gen metadata slots...
                               {#cxy_cache_meta.new_gen_time, os:timestamp()},
                               {#cxy_cache_meta.new_gen,      Empty_Gen},

                               %% Move the new generation to the old_gen metadata slots...
                               {#cxy_cache_meta.old_gen_time, New_Time},
                               {#cxy_cache_meta.old_gen,      New_Gen}
                              ]),
    ets:info(Old_Gen, type) =/= undefined andalso ets:delete(Old_Gen),
    Empty_Gen.
