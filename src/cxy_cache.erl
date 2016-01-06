%%%------------------------------------------------------------------------------
%%% @copyright (c) 2013-2015, DuoMark International, Inc.
%%% @author Jay Nelson <jay@duomark.com>
%%% @reference 2013-2015 Development sponsored by TigerText, Inc. [http://tigertext.com/]
%%% @reference The license is based on the template for Modified BSD from
%%%   <a href="http://opensource.org/licenses/BSD-3-Clause">OSI</a>
%%% @doc
%%%   Generational caching using an ets table for each generation of cached
%%%   data. Accesses hit the newer generation first, and migrate from the
%%%   older generation to the newer generation when retrieved from the
%%%   stale table. When a new generation is initally started, the oldest
%%%   table is deleted. This is a form of mass garbage collection which
%%%   avoids using timers and expiration of individual cached elements.
%%%
%%%   Cache names must be reserved before the actual cache can be created.
%%%   This ensures that the metadata is available before the cache storage
%%%   is available, but more importantly reserving also creates the metadata
%%%   ets table if it doesn't already exist. Both reserve and create must
%%%   be called from a process which will outlive any processes which will
%%%   access the caches. Since the cached data and metadata are stored in
%%%   ets tables, the creating process will be the owner and that process
%%%   cannot die or the tables will be automatically deleted.
%%%
%%%   v0.9.8b changes the return value of Mod:create_key_value/1 to account
%%%   for different versions of the cached value. In a race condition, the
%%%   later version wins now instead of the first to store in the ets table.
%%%
%%% @since 0.9.6
%%% @end
%%%------------------------------------------------------------------------------
-module(cxy_cache).
-author('Jay Nelson <jay@duomark.com>').

-compile({inline, [return_cache_gen1/2,     return_cache_gen2/2,
                   return_cache_refresh/2,  return_cache_error/2,
                   return_cache_miss/2,     return_cache_delete/2,
                   return_and_count_cache/4
                  ]}).

%% External interface
-export([
         reserve/2, reserve/3, reserve/4,
         create/1,  clear/1,   delete/1,
         info/0,    info/1,
         replace_check_generation_fun/2,
         delete_item/2,  fetch_item/2,
         refresh_item/2, refresh_item/3,
         fetch_item_version/2,
         is_cached/2,
         get_and_clear_counts/1
        ]).

%% Comparator functions called from gen_fun() closure.
-export([new_gen_count_threshold/4, new_gen_time_threshold/4]).

%% Manual generation change functions, must be called from ets owner process.
-export([new_generation/1, maybe_make_new_generation/1]).

-type cache_name()     :: atom().
-type thresh_type()    :: count | time.
-type gen_fun()        :: fun((cache_name(), non_neg_integer(), erlang:timestamp()) -> boolean()).
-type gen_fun_opt()    :: none | gen_fun().
-type check_gen_fun()  :: gen_fun_opt() | thresh_type().

-export_type([cache_name/0, gen_fun/0, gen_fun_opt/0, thresh_type/0, check_gen_fun/0]).

%% Each cxy_cache behaviour must have a module which defines the Key => {Version, Value} function.
-type cached_key()       :: term().
-type cached_value()     :: term().
-type cached_value_vsn() :: term().
-callback create_key_value(cached_key()) -> {cached_value_vsn(), cached_value()} | no_value_available.

%% Return 'true' if Vsn2 later than Vsn1, otherwise 'false'.
%% -optional_callback is_later_version(Vsn1::cached_value_vsn(), Vsn2::cached_value_vsn()) -> boolean().

-export_type([cached_key/0, cached_value/0, cached_value_vsn/0]).

%% Internal counters are exported for safer metadata spelunking
-type access_count()   :: non_neg_integer().
-type gen1_hit_count() :: access_count().
-type gen2_hit_count() :: access_count().
-type refresh_count()  :: access_count().
-type delete_count()   :: access_count().
-type fetch_count()    :: access_count().
-type error_count()    :: access_count().
-type miss_count()     :: access_count().

-export_type([gen1_hit_count/0, gen2_hit_count/0, delete_count/0, refresh_count/0,
              fetch_count/0, error_count/0, miss_count/0]).

-include("cxy_cache.hrl").


%%%------------------------------------------------------------------------------
%%% The cache meta-data is stored as a single record per cache in cxy_cache table.
%%%------------------------------------------------------------------------------

-spec reserve(Name, module()) -> Name | {error, already_exists} when Name :: cache_name().
-spec reserve(Name, module(), check_gen_fun()) -> Name | {error, already_exists} when Name :: cache_name().
-spec reserve(Name, module(), thresh_type(), pos_integer())
             -> Name | {error, already_exists} when Name :: cache_name().

-spec create(cache_name()) -> boolean().

-spec info()      -> [{Cache, proplists:proplist()}] when Cache :: cache_name().
-spec info(Cache) ->  {Cache, proplists:proplist()}  when Cache :: cache_name().

-spec clear  (cache_name()) -> boolean().
-spec delete (cache_name()) -> boolean().
-spec replace_check_generation_fun(cache_name(), check_gen_fun()) -> boolean().

%% Comparators available to gen_fun().
-spec new_gen_count_threshold(cache_name(), Fetch_Count, Time, Thresh)
                             -> boolean() when Fetch_Count :: fetch_count(),
                                               Time        :: erlang:timestamp(),
                                               Thresh      :: non_neg_integer().

-spec new_gen_time_threshold(cache_name(), Fetch_Count, Time, Thresh)
                            -> boolean() when Fetch_Count :: fetch_count(),
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


-define(UNLESS_METADATA (__Code),       ets:info(?MODULE, named_table) =/= undefined orelse  __Code).
-define(DO_METADATA     (__Code),       ets:info(?MODULE, named_table) =/= undefined andalso __Code).
-define(WHEN_METADATA   (__Code),  case ets:info(?MODULE, named_table)  of undefined -> []; _Found -> __Code end).
-define(GET_METADATA    (__Cache), ?WHEN_METADATA(ets:lookup(?MODULE, __Cache))).

%% Meta support functions. The default config var *_thresh can represent either the
%% fetch count or the elapsed microseconds before a new generation is created.
init_meta_index(Cache_Name, Cache_Mod, New_Gen_Fun) ->
    init_meta_ets_table(#cxy_cache_meta{cache_name              = Cache_Name,
                                        cache_module            = Cache_Mod,
                                        new_generation_function = New_Gen_Fun}).
    
init_meta_index(Cache_Name, Cache_Mod, Threshold_Type, Threshold) ->
    init_meta_ets_table(#cxy_cache_meta{cache_name              = Cache_Name,
                                        cache_module            = Cache_Mod,
                                        new_generation_function = Threshold_Type,
                                        new_generation_thresh   = Threshold}).

%% The singleton metadata table stores records and is indexed on cache_name.
init_meta_ets_table(#cxy_cache_meta{} = Initial_Metadata) ->
    _ = ?UNLESS_METADATA(ets:new(?MODULE, [named_table, set, public, {keypos, #cxy_cache_meta.cache_name}])),
    ?DO_METADATA(ets:insert_new(?MODULE, Initial_Metadata)).


%% @doc Populate the initial new and old generation caches for a pre-reserved cache_name.    
create(Cache_Name)
  when is_atom(Cache_Name)->
    Now          = os:timestamp(),
    New_Metadata = [{#cxy_cache_meta.old_gen_time, Now},
                    {#cxy_cache_meta.new_gen_time, Now},
                    {#cxy_cache_meta.old_gen, new_cache_gen(Cache_Name)},     % Create oldest first
                    {#cxy_cache_meta.new_gen, new_cache_gen(Cache_Name)}],
    ?DO_METADATA(ets:update_element(?MODULE, Cache_Name, New_Metadata)).

%% Cache generations are unnamed tables, use version tagged values and can be modified by any process.
new_cache_gen(Cache_Name) -> ets:new(Cache_Name, [set, public, {keypos, #cxy_cache_value.key}]).

%% Comparators available to Gen_Fun closure which provides its own threshold value.
new_gen_count_threshold(_Name,  Fetch_Count, _Time, Thresh) -> Fetch_Count > Thresh.
new_gen_time_threshold (_Name, _Fetch_Count,  Time, Thresh) -> timer:now_diff(os:timestamp(), Time) > Thresh.


%%%------------------------------------------------------------------------------
%%% Cache metadata reporting and update.
%%%------------------------------------------------------------------------------
                              
%% @doc Report metadata about caches.
info() ->
    ?WHEN_METADATA([fmt_info(Metadata) || Metadata <- ets:tab2list(?MODULE)]).

%% @doc Report metadata about a single cache.
info(Cache_Name)
  when is_atom(Cache_Name) ->
    case ?GET_METADATA(Cache_Name) of
        [] -> {Cache_Name, []};
        [#cxy_cache_meta{} = Metadata] -> fmt_info(Metadata)
    end.

fmt_info(#cxy_cache_meta{cache_name     = Cache_Name,
                         started        = Started,
                         gen1_hit_count = Gen1_Hits,
                         gen2_hit_count = Gen2_Hits,
                         refresh_count  = Refresh_Count,
                         delete_count   = Deletes,
                         fetch_count    = Fetches,
                         error_count    = Error_Count,
                         miss_count     = Miss_Count,
                         new_gen_time   = New_Time, new_gen = New,
                         old_gen_time   = Old_Time, old_gen = Old,
                         cache_module   = Cache_Mod,
                         new_generation_function = NGF,
                         new_generation_thresh   = NGT
                        }) ->
    Now = os:timestamp(),
    New_Time_Diff = case New_Time of
                        undefined -> undefined;
                        New_Time  -> round(timer:now_diff(Now, New_Time) / 10000 / 60) / 100
                    end,
    Old_Time_Diff = case Old_Time of
                        undefined -> undefined;
                        Old_Time  -> round(timer:now_diff(Now, Old_Time) / 10000 / 60) / 100
                    end,
    [[New_Count, New_Memory], [Old_Count, Old_Memory]]
        = [case Tab of
               undefined -> [0, 0];
               _Table_Id -> [ets:info(Tab, Attr) || Attr <- [size, memory]]
           end || Tab <- [New, Old]],

    %% Fetch count reflects the next request count, so we decrement by 1.
    {Cache_Name, [
                  {started,       Started},
                  {cache_module,  Cache_Mod},
                  {total_memory,  New_Memory + Old_Memory},
                  {gen1_hits,     Gen1_Hits},
                  {gen2_hits,     Gen2_Hits},
                  {refresh_count, Refresh_Count},
                  {delete_count,  Deletes},
                  {fetch_count,   Fetches},
                  {error_count,   Error_Count},
                  {miss_count,    Miss_Count},
                  {new_gen_tid,   New},
                  {old_gen_tid,   Old},
                  {new_generation_thresh,        NGT},
                  {new_generation_function,      NGF},
                  {num_accesses_this_generation, Fetches},
                  {new_gen_memory,       New_Memory},      {old_gen_memory,        Old_Memory},
                  {new_gen_count,        New_Count},       {old_gen_count,         Old_Count},
                  {new_gen_time_minutes, New_Time_Diff},   {old_gen_time_minutes,  Old_Time_Diff}
                 ]}.

%% @doc Clear all items from the generational caches, but don't delete the tables.
clear(Cache_Name)
  when is_atom(Cache_Name) ->

    %% Prep locals before accessing metadata to narrow concurrency race window...
    New_Timestamp = os:timestamp(),
    New_Metadata = [{#cxy_cache_meta.started,      New_Timestamp},
                    {#cxy_cache_meta.gen1_hit_count,  0},
                    {#cxy_cache_meta.gen2_hit_count,  0},
                    {#cxy_cache_meta.refresh_count,   0},
                    {#cxy_cache_meta.delete_count,    0},
                    {#cxy_cache_meta.fetch_count,     0},
                    {#cxy_cache_meta.error_count,     0},
                    {#cxy_cache_meta.miss_count,      0},
                    {#cxy_cache_meta.new_gen_time, New_Timestamp},
                    {#cxy_cache_meta.old_gen_time, New_Timestamp}],

    %% Update the metadata if it still exists.
    case ?GET_METADATA(Cache_Name) of
        [] -> false;
        [#cxy_cache_meta{new_gen=New, old_gen=Old}] ->
            %% New accesses that interleave between delete_all_objects and update_element won't be counted.
            _ = [try ets:delete_all_objects(Tab) catch error:badarg -> skip end || Tab <- [New, Old]],
            ?DO_METADATA(ets:update_element(?MODULE, Cache_Name, New_Metadata))
    end.

%% @doc Delete the cache metadata and all generation tables.
delete(Cache_Name)
  when is_atom(Cache_Name) ->
    case ?GET_METADATA(Cache_Name) of
        [] -> false;
        [#cxy_cache_meta{new_gen=New, old_gen=Old}] ->
            _ = ?DO_METADATA(ets:delete(?MODULE, Cache_Name)),
            _ = [try ets:delete(Tab) catch error:badarg -> skip end || Tab <- [New, Old]],
            true
    end.

%% @doc Replace the existing new generation decision function.
replace_check_generation_fun(Cache_Name, Fun)
  when is_atom(Cache_Name), Fun =:= none;
       is_atom(Cache_Name), Fun =:= time;
       is_atom(Cache_Name), Fun =:= count;
       is_atom(Cache_Name), is_function(Fun, 3) ->
    ?DO_METADATA(ets:update_element(?MODULE, Cache_Name, {#cxy_cache_meta.new_generation_function, Fun})).


%%%------------------------------------------------------------------------------
%%% Generational caches are accessed via the meta-data, newest first.
%%%------------------------------------------------------------------------------

-spec delete_item  (cache_name(), cached_key()) -> true.
-spec fetch_item   (cache_name(), cached_key()) -> cached_value() | no_value_available | {error, tuple()}.
-spec refresh_item (cache_name(), cached_key()) -> cached_value() | no_value_available | {error, tuple()}.
-spec refresh_item (cache_name(), cached_key(), {cached_value_vsn(), cached_value()})
                   -> cached_value() | no_value_available | {error, tuple()}.
-spec fetch_item_version (cache_name(), cached_key()) -> cached_value_vsn() | no_value_available | {error, tuple()}.
-spec is_cached(cache_name(), cached_key()) -> boolean().
-spec get_and_clear_counts(cache_name()) -> {cache_name(), proplists:proplist()}.

-define(WHEN_GEN_EXISTS(__Gen_Id, __Code), ets:info(__Gen_Id, type) =:= set andalso __Code).

%% Remove an item from both new and old generations, returns true if at least one value was deleted.
delete_item(Cache_Name, Key) ->
    case ?GET_METADATA(Cache_Name) of
        [] -> return_cache_delete(Cache_Name, false);
        [#cxy_cache_meta{new_gen=New_Gen_Id, old_gen=Old_Gen_Id}] ->
            %% Delete from new generation first is safest during a generation change.
            Deleted_New = ?WHEN_GEN_EXISTS(New_Gen_Id, ets:delete(New_Gen_Id, Key)),
            Deleted_Old = ?WHEN_GEN_EXISTS(Old_Gen_Id, ets:delete(Old_Gen_Id, Key)),
            return_cache_delete(Cache_Name, Deleted_New or Deleted_Old)
    end.

%% Internal support function for consistent error handling when accessing generations.
%% Used for fetch_item/2, refresh_item/2, and refresh_item/3
access_item(Cache_Name, Key, Found_Gen1_Fn, Look_Gen2_Fn, Optional_Object) ->
    case ?GET_METADATA(Cache_Name) of
        [] -> {error, {no_cache_metadata, Cache_Name}};
        [#cxy_cache_meta{new_gen=New_Gen_Id, old_gen=Old_Gen_Id, cache_module=Mod}] ->
            case ?WHEN_GEN_EXISTS(New_Gen_Id, ets:lookup(New_Gen_Id, Key)) of

                %% Gen1 cache is missing, there is an error...
                false -> {error, {no_gen1_cache, Cache_Name}};

                %% Found in the new generational cache...
                [#cxy_cache_value{key=Key, value=Value, version=Vsn}] ->
                    Found_Gen1_Fn(Cache_Name, Value, Vsn, New_Gen_Id, Mod, Key);

                %% Otherwise migrate the old generation cached value or create a new cached value.
                [] -> Look_Gen2_Fn(Cache_Name, New_Gen_Id, Old_Gen_Id, Mod, Key, Optional_Object)
            end
    end.

%% Fetch from Generation 1, migrate from Generation 2, or create a new value and insert to Generation 1.
fetch_item(Cache_Name, Key) ->
    Found_Fn     = fun(Fn_Cache_Name, Fn_Value, _Version, _New_Gen_Id, _Mod, _Key) ->
                           return_cache_gen1(Fn_Cache_Name, Fn_Value)
                   end,
    Not_Found_Fn = fun(Fn_Cache_Name, Fn_New_Gen_Id, Fn_Old_Gen_Id, Fn_Mod, Fn_Key, _Object) ->
                           copy_old_value_if_found(Fn_Cache_Name, Fn_New_Gen_Id, Fn_Old_Gen_Id, Fn_Mod,Fn_Key)
                   end,
    access_item(Cache_Name, Key, Found_Fn, Not_Found_Fn, no_value_available).

%% Fetch from Generation 1 or 2, after updating with a newer key version or leaving existing newest key version.
refresh_item(Cache_Name, Key) ->
    Found_Fn     = fun(Fn_Cache_Name, _Value, _Version, Fn_New_Gen_Id, Fn_Mod, Fn_Key) ->
                           case create_new_value(Fn_Cache_Name, Fn_New_Gen_Id, Fn_Mod,Fn_Key) of
                               no_value_available                  -> return_cache_refresh(Fn_Cache_Name, no_value_available);
                               {_New_Version, Fn_New_Cached_Value} -> return_cache_refresh(Fn_Cache_Name, Fn_New_Cached_Value)
                           end
                   end,
    Not_Found_Fn = fun(Fn_Cache_Name, Fn_New_Gen_Id, Fn_Old_Gen_Id, Fn_Mod, Fn_Key, Fn_Obj) ->
                           Fn_New_Cached_Value = refresh_item(key, Fn_Cache_Name, Fn_New_Gen_Id, Fn_Old_Gen_Id,
                                                              Fn_Mod,Fn_Key,Fn_Obj),
                           return_cache_miss(Fn_Cache_Name, Fn_New_Cached_Value)
                   end,
    access_item(Cache_Name, Key, Found_Fn, Not_Found_Fn, no_value_available).

%% Fetch from Generation 1 or 2, after updating with a newer obj version or leaving existing newest obj version.
refresh_item(Cache_Name, Key, {Possibly_New_Vsn, Possibly_New_Value} = Possibly_New_Object) ->
    Found_Fn     = fun(Fn_Cache_Name, _Value, _Version, Fn_New_Gen_Id, Fn_Mod, Fn_Key) ->
                           {_Cached_Version, Fn_New_Cached_Value}
                               = insert_value_if_newer(Fn_Cache_Name, Fn_New_Gen_Id, Fn_Mod, Fn_Key,
                                                       Possibly_New_Vsn, Possibly_New_Value),
                           return_cache_refresh(Cache_Name, Fn_New_Cached_Value)
                      end,
    Not_Found_Fn = fun(Fn_Cache_Name, Fn_New_Gen_Id, Fn_Old_Gen_Id, Fn_Mod, Fn_Key, Fn_Obj) ->
                           Fn_New_Cached_Value = refresh_item(obj, Fn_Cache_Name, Fn_New_Gen_Id, Fn_Old_Gen_Id,
                                                              Fn_Mod,Fn_Key,Fn_Obj),
                           return_cache_miss(Fn_Cache_Name, Fn_New_Cached_Value)
                   end,
    access_item(Cache_Name, Key, Found_Fn, Not_Found_Fn, Possibly_New_Object).

%% Fetch just the version of an item from the newest generation in which it is present without migrating.
fetch_item_version(Cache_Name, Key) ->
    Found_Fn     = fun(_Cache_Name, _Value, Version, _New_Gen_Id, _Mod, _Key) -> Version end,
    Not_Found_Fn = fun(Fn_Cache_Name, _New_Gen_Id, Fn_Old_Gen_Id, _Mod, Fn_Key, _Obj) ->
                           fetch_gen2_version(Fn_Cache_Name, Fn_Old_Gen_Id, Fn_Key)
                   end,
    access_item(Cache_Name, Key, Found_Fn, Not_Found_Fn, no_value_available).

%% Check if a key is present in the cache, without disturbing the generations or fetching if missing.
is_cached(Cache_Name, Key) ->
    case fetch_item_version(Cache_Name, Key) of
        {error, _}         -> false;
        no_value_available -> false;
        _Version           -> true
    end.

%% Old generation function for getting just the version of a cached value entry.
fetch_gen2_version(Cache_Name, Old_Gen_Id, Key) ->
    case ?WHEN_GEN_EXISTS(Old_Gen_Id, ets:lookup(Old_Gen_Id, Key)) of
        false ->
            {error, {no_gen2_cache, Cache_Name}};
        [#cxy_cache_value{key=Key, version=Version}] ->
            Version;
        [] ->
            no_value_available
    end.

%% Migrate an old value forward to the new generation and then clobber
%% if the key generates a newer version. Otherwise leave the existing
%% version in place, unless there is now no_value_available, in which
%% case the entry is deleted from the cache.
refresh_item(Type, Cache_Name, New_Gen_Id, Old_Gen_Id, Mod, Key, Object) ->
    try ets:lookup(Old_Gen_Id, Key) of

        %% Create a new Mod:create_key_value value if not in old generation...
        [] -> case Type of
                  key -> refresh_now(Type, Cache_Name, New_Gen_Id, Mod, Key, Object);
                  obj -> case refresh_now(Type, Cache_Name, New_Gen_Id, Mod, Key, Object) of
                             no_value_available    -> no_value_available;
                             {_Version, Raw_Value} -> Raw_Value
                         end
              end;

        %% Otherwise, migrate the old value to the new generation...
        [#cxy_cache_value{key=Key} = Old_Obj] ->
            _ = insert_to_new_gen(Cache_Name, New_Gen_Id, Mod, Old_Obj),
            %% Then try to clobber it with a newly created value.
            Cached_Value
                = case {Type, Object} of
                      {key, _} ->
                          create_new_value(Cache_Name, New_Gen_Id, Mod, Key);
                      {obj, no_value_available} ->
                          no_value_available;
                      {obj, _} ->
                          {Possibly_New_Vsn, Possibly_New_Value} = Object,
                          insert_value_if_newer(Cache_Name, New_Gen_Id, Mod,
                                                Key, Possibly_New_Vsn, Possibly_New_Value)
                  end,
            case Cached_Value of
                {_Version, Raw_Value} -> return_cache_refresh(Cache_Name, Raw_Value);
                no_value_available    ->
                    ?WHEN_GEN_EXISTS(New_Gen_Id, ets:delete(New_Gen_Id, Key)),
                    ?WHEN_GEN_EXISTS(Old_Gen_Id, ets:delete(Old_Gen_Id, Key)),
                    return_cache_miss(Cache_Name, no_value_available)
            end
    catch
        %% Old generation was likely eliminated by another request, try creating a new value.
        %% The value will get inserted into 'old_gen' because New_Gen_Id must've been demoted.
        error:badarg -> refresh_now(Type, Cache_Name, New_Gen_Id, Mod, Key, Object)
    end.

refresh_now(key, Cache_Name, New_Gen_Id, Mod, Key, _Object) ->
    cache_miss(Cache_Name, New_Gen_Id, Mod, Key);
refresh_now(obj, Cache_Name, New_Gen_Id, Mod, Key, {Version, Value}) ->
    insert_value_if_newer(Cache_Name, New_Gen_Id, Mod, Key, Version, Value).

%% Copy needs to be as close to atomic as possible. Values are now tagged
%% with a version so that the latest version wins when inserting to the
%% cache. Also, if an empty generation is created after we have checked
%% the new generation, it is possible that the oldest generation is deleted
%% out from under us. In this case, we pretend the key doesn't exist and
%% create a new one. At worst, we are the only user of this new value
%% or the next access copies it to the empty generation.
copy_old_value_if_found(Cache_Name, New_Gen_Id, Old_Gen_Id, Mod, Key) ->
    try ets:lookup(Old_Gen_Id, Key) of

        %% Create a new Mod:create_key_value value if not in old generation...
        [] -> cache_miss(Cache_Name, New_Gen_Id, Mod, Key);

        %% Otherwise, insert the value to the new generation...
        [#cxy_cache_value{key=Key, value=Value} = Obj] ->
            _ = insert_to_new_gen(Cache_Name, New_Gen_Id, Mod, Obj),
            return_cache_gen2(Cache_Name, Value)
    catch
        %% Old generation was probably eliminated by another request, try creating a new value.
        %% Note the value will actually get inserted into 'old_gen' because New_Gen_Id must've been demoted.
        error:badarg -> cache_miss(Cache_Name, New_Gen_Id, Mod, Key)
    end.

cache_miss(Cache_Name, New_Gen_Id, Mod, Key) ->
    case create_new_value(Cache_Name, New_Gen_Id, Mod, Key) of
        {error, _} = Error       -> return_cache_miss(Cache_Name, Error);
        no_value_available       -> return_cache_miss(Cache_Name, no_value_available);
        {_Version, Cached_Value} -> return_cache_miss(Cache_Name, Cached_Value)
    end.

%% Count specific access requests for statistics reporting...
-define(INC(__Name, __Value, __Count_Type, __Count),
        return_and_count_cache(__Name, __Value, __Count_Type, __Count)).
return_cache_gen1    (Cache_Name, Value)->?INC(Cache_Name, Value, gen1,   {#cxy_cache_meta.gen1_hit_count, 1}).
return_cache_gen2    (Cache_Name, Value)->?INC(Cache_Name, Value, gen2,   {#cxy_cache_meta.gen2_hit_count, 1}).
return_cache_refresh (Cache_Name, Value)->?INC(Cache_Name, Value, refresh,{#cxy_cache_meta.refresh_count,  1}).
return_cache_error   (Cache_Name, Value)->?INC(Cache_Name, Value, error,  {#cxy_cache_meta.error_count,    1}).
return_cache_miss    (Cache_Name, Value)->?INC(Cache_Name, Value, miss,   {#cxy_cache_meta.miss_count,     1}).

%% Count whether an item was deleted.
return_cache_delete  (Cache_Name, true )->?INC(Cache_Name, true,  delete, {#cxy_cache_meta.delete_count,   1});
return_cache_delete  (Cache_Name, false)->?INC(Cache_Name, false, delete, {#cxy_cache_meta.delete_count,   0}).

%% Count fetch requests for statistics reporting, even on time-based caches, but not for refresh and delete.
return_and_count_cache(Cache_Name, Value, Count_Type, Hit_Type_Op) ->
    Fetch_Inc = case Count_Type of
                    _Dont_Count_As_Fetch when Count_Type =:= refresh; Count_Type =:= delete -> [];
                    _Count_As_Fetch -> [{#cxy_cache_meta.fetch_count, 1}]
                end,
    Inc_Op = [Hit_Type_Op | Fetch_Inc],
    _ = ?DO_METADATA(ets:update_counter(?MODULE, Cache_Name, Inc_Op)),
    Value.

%% Retrieve and reset access counters.
get_and_clear_counts(Cache_Name) ->
    Counters      = [#cxy_cache_meta.gen1_hit_count, #cxy_cache_meta.gen2_hit_count,
                     #cxy_cache_meta.refresh_count,  #cxy_cache_meta.delete_count,
                     #cxy_cache_meta.error_count,    #cxy_cache_meta.miss_count],
    Read_And_Zero = [[{Counter, 0}, {Counter, 0, 0, 0}] || Counter <- Counters],
    case ?DO_METADATA(ets:update_counter(?MODULE, Cache_Name, lists:append(Read_And_Zero))) of
        false -> false;
        [Gen1_Hits, 0, Gen2_Hits, 0, Refresh_Count, 0, Delete_Count, 0, Error_Count, 0, Miss_Count, 0] ->
            {Cache_Name, [{gen1_hits,     Gen1_Hits},
                          {gen2_hits,     Gen2_Hits},
                          {refresh_count, Refresh_Count},
                          {delete_count,  Delete_Count},
                          {error_count,   Error_Count},
                          {miss_count,    Miss_Count}]}
    end.

%% Create a new value from the cache Mod:create_key_value(Key) but watch for errors in generating it.
create_new_value(Cache_Name, New_Gen_Id, Mod, Key) ->
    try Mod:create_key_value(Key) of
        no_value_available -> no_value_available;
        {Version, Value}   -> insert_value_if_newer(Cache_Name, New_Gen_Id, Mod, Key, Version, Value)
    catch Type:Class ->
            Error = {error, {Type,Class, {creating_new_value_with, Mod, Key}}},
            return_cache_error(Cache_Name, Error)
    end.

insert_value_if_newer(Cache_Name, New_Gen_Id, Mod, Key, Version, Value) ->
    Tagged_Value = #cxy_cache_value{key=Key, version=Version, value=Value},
    insert_to_new_gen(Cache_Name, New_Gen_Id, Mod, Tagged_Value).

%% Insert the new value, IFF a newer version of the data hasn't already beat us to the cache.
insert_to_new_gen(Cache_Name, New_Gen_Id, Mod,
                  #cxy_cache_value{key=Key, version=Insert_Vsn, value=Insert_Value} = Obj) ->
    try ets:insert_new(New_Gen_Id, Obj) of
        true  -> {Insert_Vsn, Insert_Value};
        false ->
            [#cxy_cache_value{key=Key, version=Cached_Vsn, value=Cached_Value}] = ets:lookup(New_Gen_Id, Key),

            %% Check the versions and keep the cached value if it is newer...
            case determine_newest_version(Cache_Name, Mod, Insert_Vsn, Cached_Vsn) of
                cached_is_newer -> {Cached_Vsn, Cached_Value};

                %% Otherwise override by re-inserting on top of the older cached value.
                %% TODO: There is still a race condition between lookup and insert here
                insert_is_newer -> true = ets:insert(New_Gen_Id, Obj),
                                   {Insert_Vsn, Insert_Value}
            end

    %% Somehow the New_Gen_Id cache disappeared unexpectedly.
    catch error:badarg -> {error, {no_gen1_cache, Cache_Name}}
    end.

determine_newest_version(Cache_Name, Mod, Insert_Vsn, Cached_Vsn) ->
    Cached_Is_Later
        = case erlang:function_exported(Mod, is_later_version, 2) of
              false -> Insert_Vsn < Cached_Vsn;
              true  -> try Mod:is_later_version(Insert_Vsn, Cached_Vsn)
                       catch Class:Type -> % The user-supplied function crashed, use the currently cached value.
                               Msg = "~p:is_later_version on cache ~p crashed: {~p:~p} ~p~n",
                               error_logger:error_msg(Msg, [Mod, Cache_Name, Class, Type,
                                                            erlang:get_stacktrace()]),
                               true
                       end
          end,
    case Cached_Is_Later of
        true  -> cached_is_newer;
        false -> insert_is_newer
    end.

        
%%%------------------------------------------------------------------------------
%%% Generation creation utilities
%%%
%%% These functions must be called by the process that will own the generational
%%% ets tables. The owning process has to stick around longer than the generation
%%% lasts, or the exit of the owner will destroy the ets table prematurely. This
%%% should be the same process which reserves the cache names and creates the
%%% caches originally for consistency. This is usually achieved by configuration
%%% data that is read and applied on startup using a dedicated supervisor and/or
%%% gen_fsm for managing the state of caching and all the ets tables. This
%%% library provides cxy_cache_sup and cxy_cache_fsm for exactly that purpose.
%%% cxy_cache_fsm serves as an external signal for new generation notification.
%%%------------------------------------------------------------------------------

-spec new_generation            (cache_name()) -> ets:tid() | {error, {no_cache_metadata, cache_name()}}.
-spec maybe_make_new_generation (cache_name()) -> boolean() | {error, {no_cache_metadata, cache_name()}}.

%% @doc Manually cause a new generation to be created, called from new_gen owner process.
new_generation(Cache_Name) ->
    case ?GET_METADATA(Cache_Name) of
        [] -> {error, {no_cache_metadata, Cache_Name}};
        [#cxy_cache_meta{new_gen=New_Gen_Id, new_gen_time=New_Time, old_gen=Old_Gen_Id}] ->
            new_generation(Cache_Name, New_Gen_Id, New_Time, Old_Gen_Id)
    end.

%% @doc Create a new generation if the generation test returns true.
maybe_make_new_generation(Cache_Name) ->
    case ?GET_METADATA(Cache_Name) of
        [] -> {error, {no_cache_metadata, Cache_Name}};

        [#cxy_cache_meta{new_gen_time            = New_Time,
                         fetch_count             = Fetch_Count,
                         new_generation_thresh   = Thresh,
                         new_generation_function = New_Gen_Fun} = Metadata] ->

            case New_Gen_Fun of
                none -> false;
                time ->
                    Time_Expired = timer:now_diff(os:timestamp(), New_Time) > Thresh,
                    make_generation_finish(Cache_Name, Metadata, Time_Expired);
                count ->
                    Count_Exceeded = Fetch_Count > Thresh,
                    make_generation_finish(Cache_Name, Metadata, Count_Exceeded);
                _Function ->
                    New_Generation_Required
                        = try New_Gen_Fun(Cache_Name, Fetch_Count, New_Time)
                          catch Type:Class ->
                                  Error = {error, {Type,Class,
                                                   {maybe_make_new_generation, New_Gen_Fun,
                                                    [Cache_Name, Fetch_Count, New_Time]}}},
                                  error_logger:error_msg("New generation function failed: ~p~n", [Error]),
                                  return_cache_error(Cache_Name, false)
                          end,
                    make_generation_finish(Cache_Name, Metadata, New_Generation_Required)
            end
    end.

make_generation_finish(_Cache_Name, _Metadata, false) -> false;
make_generation_finish( Cache_Name,  Metadata, true ) ->
    #cxy_cache_meta{new_gen=New_Gen_Id, new_gen_time=New_Time, old_gen=Old_Gen_Id} = Metadata,
    _New_Empty_Gen_Id = new_generation(Cache_Name, New_Gen_Id, New_Time, Old_Gen_Id),
    true.


%% Create a new generation cache and update the metadata to reflect its existence.
new_generation(Cache_Name, New_Gen_Id, New_Time, Old_Gen_Id) ->
    Empty_Gen_Id = new_cache_gen(Cache_Name),
    New_Metadata = [
                    %% Reset the fetch count so it can be used for generation measurement...
                    {#cxy_cache_meta.fetch_count,  0},

                    %% Create an empty generation for the new_gen metadata slot...
                    {#cxy_cache_meta.new_gen_time, os:timestamp()},
                    {#cxy_cache_meta.new_gen,      Empty_Gen_Id},

                    %% Move the previous new generation to the old_gen metadata slots...
                    {#cxy_cache_meta.old_gen_time, New_Time},
                    {#cxy_cache_meta.old_gen,      New_Gen_Id}
                   ],

    %% Update the metadata record atomically with the new generation info...
    _ = ?DO_METADATA(ets:update_element(?MODULE, Cache_Name, New_Metadata)),
    _ = ?WHEN_GEN_EXISTS(Old_Gen_Id, ets:delete(Old_Gen_Id)),
    Empty_Gen_Id.
