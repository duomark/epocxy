%%%------------------------------------------------------------------------------
%%% @copyright (c) 2013-2014, DuoMark International, Inc.
%%% @author Jay Nelson <jay@duomark.com>
%%% @reference 2013-2014 Development sponsored by TigerText, Inc. [http://tigertext.com/]
%%% @reference The license is based on the template for Modified BSD from
%%%   <a href="http://opensource.org/licenses/BSD-3-Clause">OSI</a>
%%% @doc
%%%   Tests for cxy_cache use both common_test and PropEr to check for errors.
%%%   Common_test is the driving framework and is used to validate simple cases
%%%   of calling API functions with pre-canned valid values. The PropEr tests
%%%   are designed to comprehensively generate values which stress the workings
%%%   of the caching API.
%%%
%%%   Simple tests precede PropEr tests in sequence groups so that breakage in
%%%   the basic API are found more quickly without invoking PropEr generators.
%%%
%%% @since 0.9.6
%%% @end
%%%------------------------------------------------------------------------------
-module(cxy_cache_SUITE).
-auth('jay@duomark.com').
-vsn('').

-export([all/0, groups/0,
         init_per_suite/1,    end_per_suite/1,
         init_per_group/1,    end_per_group/1,
         init_per_testcase/2, end_per_testcase/2
        ]).

-export([
         proper_check_create/1,
         vf_check_one_fetch/1, vf_check_many_fetches/1,
         check_fsm_cache/1,

         check_clear_and_delete/1
        ]).

-include("epocxy_common_test.hrl").

-type test_case()  :: atom().
-type test_group() :: atom().

-spec all() -> [test_case() | {group, test_group()}].
all() -> [
          proper_check_create,         % Establish all atoms as valid cache names
          {group, verify_fetch},       % Tests fetching from the cache, even when empty
          check_fsm_cache,             % Certifies the cache supervisor and FSM ets ownership

          check_clear_and_delete
         ].

-spec groups() -> [{test_group(), [sequence], [test_case() | {group, test_group()}]}].
groups() -> [
             {verify_fetch, [sequence], [vf_check_one_fetch, vf_check_many_fetches]}
             ].
    

-type config() :: proplists:proplist().
-spec init_per_suite (config()) -> config().
-spec end_per_suite  (config()) -> config().

init_per_suite (Config) -> Config.
end_per_suite  (Config)  -> Config.

-spec init_per_group (config()) -> config().
-spec end_per_group  (config()) -> config().

init_per_group (Config) -> Config.
end_per_group  (Config) -> Config.

-spec init_per_testcase (atom(), config()) -> config().
-spec end_per_testcase  (atom(), config()) -> config().

init_per_testcase (_Test_Case, Config) -> Config.
end_per_testcase  (_Test_Case, Config) -> Config.

-define(TM, cxy_cache).


%%%------------------------------------------------------------------------------
%%% Unit tests for cxy_cache core
%%%------------------------------------------------------------------------------
                              
-include("cxy_cache.hrl").

%% Validate any atom can be used as a cache_name and info/1 will report properly.
-spec proper_check_create(config()) -> ok.
proper_check_create(_Config) ->
    ct:log("Test using an atom as a cache name"),
    Test_Cache_Name = ?FORALL(Cache_Name, ?SUCHTHAT(Cache_Name, atom(), Cache_Name =/= ''),
                              check_create_test(Cache_Name)),
    true = proper:quickcheck(Test_Cache_Name, ?PQ_NUM(5)),
    ct:comment("Successfully tested atoms as cache_names"),
    ok.

%% Checks that create cache and info reporting are consistent.
check_create_test(Cache_Name) ->
    ct:comment("Testing cache_name: ~p", [Cache_Name]),
    ct:log("Testing cache_name: ~p", [Cache_Name]),
    {Cache_Name, []} = ?TM:info(Cache_Name),

    %% Test invalid args to reserve...
    Cache_Module = list_to_atom(atom_to_list(Cache_Name) ++ "_module"),
    true = try ?TM:reserve(atom_to_list(Cache_Name), Cache_Module) catch error:function_clause -> true end,
    true = try ?TM:reserve(Cache_Name, atom_to_list(Cache_Module)) catch error:function_clause -> true end,

    %% Test that valid args can only reserve once...
    Cache_Name = ?TM:reserve(Cache_Name, Cache_Module),
    {Cache_Name, Cache_Info_Rsrv} = ?TM:info(Cache_Name),
    [undefined, undefined] = [proplists:get_value(Prop, Cache_Info_Rsrv, missing_prop)
                              || Prop <- [new_gen_tid, old_gen_tid]],
    {error, already_exists} = ?TM:reserve(Cache_Name, Cache_Module),
    {error, already_exists} = ?TM:reserve(Cache_Name, any_other_name),

    %% Check that valid info is reported after the cache is created.
    true = ?TM:create(Cache_Name),
    {Cache_Name, Cache_Info} = ?TM:info(Cache_Name),
    true = is_list(Cache_Info),

    %% Verify the info is initialized and an ets table is created for each generation.
    [0, 0] = [proplists:get_value(Prop, Cache_Info) || Prop <- [new_gen_count, old_gen_count]],
    [set, set] = [ets:info(proplists:get_value(Prop, Cache_Info), type)
                  || Prop <- [new_gen_tid, old_gen_tid]],
    eliminate_cache(Cache_Name),
    true.

vf_check_one_fetch(_Config) ->
    validate_create_and_fetch(frogs, frog_obj, frog, "frog-124"),
    eliminate_cache(frogs),
    ok.

vf_check_many_fetches(_Config) ->
    ct:log("Test fetches and new generations"),
    All_Obj_Types = [{fox_obj, fox}, {frog_obj, frog}, {rabbit_obj, rabbit}],
    Test_Generations
        = ?FORALL({Cache_Name, Obj_Type_Pair, Instances},
                  {?SUCHTHAT(Cache_Name, atom(), Cache_Name =/= ''),
                   union(All_Obj_Types),
                   ?SUCHTHAT(Instances, {non_empty(string()), non_empty(string())},
                             element(1,Instances) =/= element(2,Instances))},
                  begin
                      {Instance1,    Instance2} = Instances,
                      {Obj_Type,  Obj_Rec_Type} = Obj_Type_Pair,
                      Result = validate_new_generations(Cache_Name, Obj_Type, Obj_Rec_Type, Instance1, Instance2),
                      eliminate_cache(Cache_Name),
                      Result
                  end),
    true = proper:quickcheck(Test_Generations, ?PQ_NUM(5)),
    ct:comment("Successfully tested new generations"),
    ok.

validate_create_and_fetch(Cache_Name, Cache_Obj_Type, Obj_Record_Type, Obj_Instance_Key) ->
    reserve_and_create_cache(Cache_Name, Cache_Obj_Type),
    [#cxy_cache_meta{new_gen=New, old_gen=Old}] = ets:lookup(?TM, Cache_Name),

    %% First time creates new value (fetch_count always indicates next access count)...
    Before_Obj_Insert = erlang:now(),
    {Obj_Record_Type, Obj_Instance_Key} = ?TM:fetch_item(Cache_Name, Obj_Instance_Key),
    [] = ets:lookup(Old, Obj_Instance_Key),
    [#cxy_cache_value{key=Obj_Instance_Key, version=Obj_Create_Time,
                      value={Obj_Record_Type, Obj_Instance_Key}}] = ets:lookup(New, Obj_Instance_Key),
    [#cxy_cache_meta{fetch_count=1}] = ets:lookup(?TM, Cache_Name),
    true = timer:now_diff(Obj_Create_Time, Before_Obj_Insert) > 0,
    false = ?TM:maybe_make_new_generation(Cache_Name),
    ok.

validate_new_generations(Cache_Name, Cache_Obj_Type, Obj_Record_Type, Obj_Key1, Obj_Key2) ->
    ct:comment("Testing new generations of cache ~p with object type ~p and instances ~p and ~p",
               [Cache_Name, {Cache_Obj_Type, Obj_Record_Type}, Obj_Key1, Obj_Key2]),
    ct:log("Testing new generations of cache ~p with object type ~p and instances ~p and ~p",
           [Cache_Name, {Cache_Obj_Type, Obj_Record_Type}, Obj_Key1, Obj_Key2]),
    ok = validate_create_and_fetch(Cache_Name, Cache_Obj_Type, Obj_Record_Type, Obj_Key1),
    [#cxy_cache_meta{new_gen=New, old_gen=Old}] = ets:lookup(?TM, Cache_Name),

    %% Second time fetches existing value...
    {Obj_Record_Type, Obj_Key1} = ?TM:fetch_item(Cache_Name, Obj_Key1),
    [] = ets:lookup(Old, Obj_Key1),
    [Initial_Obj_Value1] = ets:lookup(New, Obj_Key1),
    [#cxy_cache_meta{fetch_count=2}] = ets:lookup(?TM, Cache_Name),
    false = ?TM:maybe_make_new_generation(Cache_Name),

    %% Retrieve 3 more times still no new generation...
    Exp3 = lists:duplicate(3, {Obj_Record_Type, Obj_Key1}),
    Exp3 = [?TM:fetch_item(Cache_Name, Obj_Key1) || _N <- lists:seq(1,3)],
    [] = ets:lookup(Old, Obj_Key1),
    [Initial_Obj_Value1] = ets:lookup(New, Obj_Key1),
    [#cxy_cache_meta{fetch_count=5}] = ets:lookup(?TM, Cache_Name),
    false = ?TM:maybe_make_new_generation(Cache_Name),

    %% Once more to get a new generation, then use a new key to insert in the new generation only...
    {Obj_Record_Type, Obj_Key1} = ?TM:fetch_item(Cache_Name, Obj_Key1),
    0 = ets:info(Old, size),
    [#cxy_cache_meta{new_gen=New, old_gen=Old}] = ets:lookup(?TM, Cache_Name),
    %% Force check which triggers generation rotation...
    true = ?TM:maybe_make_new_generation(Cache_Name),
    [#cxy_cache_meta{new_gen=New2, old_gen=New}] = ets:lookup(?TM, Cache_Name),
    0 = ets:info(New2, size),
    {Obj_Record_Type, Obj_Key2} = ?TM:fetch_item(Cache_Name, Obj_Key2),
    1 = ets:info(New2, size),
    [] = ets:lookup(New2, Obj_Key1),
    [Initial_Obj_Value2] = ets:lookup(New2, Obj_Key2),
    1 = ets:info(New, size),
    [Initial_Obj_Value1] = ets:lookup(New, Obj_Key1),
    [] = ets:lookup(New, Obj_Key2),
    [#cxy_cache_meta{fetch_count=1}] = ets:lookup(?TM, Cache_Name),

    %% Now check if migration of key Obj_Key1 works properly...
    {Obj_Record_Type, Obj_Key1} = ?TM:fetch_item(Cache_Name, Obj_Key1),
    2 = ets:info(New2, size),
    %% Both objects exist in the newest generation...
    [Initial_Obj_Value1] = ets:lookup(New2, Obj_Key1),
    [Initial_Obj_Value2] = ets:lookup(New2, Obj_Key2),
    %% And the now old generation still has a copy of the first key inserted
    %% because we copy forward without deleting from old generation.
    %% (The old value will have to be deleted in future on migration when we
    %%  want to visit all trashed objects on old generation expiration so that
    %%  we don't garbage collect items that are still active.)
    1 = ets:info(New, size),
    [Initial_Obj_Value1] = ets:lookup(New, Obj_Key1),
    [] = ets:lookup(New, Obj_Key2),
    [#cxy_cache_meta{fetch_count=2}] = ets:lookup(?TM, Cache_Name),

    true.

check_clear_and_delete(_Config) ->
    Cache_Name = frog_cache,
    reserve_and_create_cache(Cache_Name, frog_obj),
    Expected_Frog = {frog, "frog-3127"},
    [Expected_Frog, Expected_Frog, Expected_Frog]
        = [?TM:fetch_item(Cache_Name, "frog-3127") || _N <- lists:seq(1,3)],
    [#cxy_cache_meta{fetch_count=3, started=Started, new_gen_time=NG_Time, old_gen_time=OG_Time}] = ets:tab2list(?TM),
    true = Started =/= NG_Time,

    true = ?TM:clear(Cache_Name),
    [#cxy_cache_meta{fetch_count=0, started=New_Time, new_gen_time=New_Time, old_gen_time=New_Time,
                     new_gen=New_Gen, old_gen=Old_Gen}] = ets:tab2list(?TM),
    true = New_Time > Started andalso New_Time > NG_Time andalso New_Time > OG_Time,
    [set,0] = [ets:info(New_Gen, Attr) || Attr <- [type, size]],
    [set,0] = [ets:info(Old_Gen, Attr) || Attr <- [type, size]],

    false = ?TM:clear(foo),
    false = ?TM:delete(foo),

    eliminate_cache(Cache_Name),
    [0, undefined, undefined] = [ets:info(Tab, size) || Tab <- [?TM, Old_Gen, New_Gen]],
    ok.


%%%------------------------------------------------------------------------------
%%% Thread testing of cxy_cache_sup, cxy_cache_fsm and cxy_cache together.
%%%------------------------------------------------------------------------------

-define(SUP, cxy_cache_sup).
-define(FSM, cxy_cache_fsm).

check_fsm_cache(_Config) ->
    %% Create a simple_one_for_one supervisor...
    {ok, Sup} = ?SUP:start_link(),
    Sup = whereis(?SUP),
    undefined = ets:info(?TM, named_table),

    %% The first cache instance causes the creation of cache ets metadata table.
    %% Make sure that the supervisor owns the metadata ets table 'cxy_cache'...
    {ok, Fox_Cache} = ?SUP:start_cache(fox_cache, fox_obj, time, 1000000),
    [set, true, public, Sup] = [ets:info(?TM, P) || P <- [type, named_table, protection, owner]],
    1 = ets:info(?TM, size),
    {ok, Rabbit_Cache} = ?SUP:start_cache(rabbit_cache, rabbit_obj, time, 1300000),
    2 = ets:info(?TM, size),

    %% Verify the owner of the generational ets tables is the respective FSM instance...
    [#cxy_cache_meta{new_gen=Fox2,    old_gen=Fox1}]    = ets:lookup(?TM, fox_cache),
    [#cxy_cache_meta{new_gen=Rabbit2, old_gen=Rabbit1}] = ets:lookup(?TM, rabbit_cache),
    [Fox_Cache, Fox_Cache, Rabbit_Cache, Rabbit_Cache]
        = [ets:info(Tab, owner) || Tab <- [Fox2, Fox1, Rabbit2, Rabbit1]],
    
    %% Wait for a new generation (1.3 seconds minimum)...
    timer:sleep(1500),     % Additional time for timeout jitter
    [#cxy_cache_meta{new_gen=Fox3,    old_gen=Fox2}]    = ets:lookup(?TM, fox_cache),
    [#cxy_cache_meta{new_gen=Rabbit3, old_gen=Rabbit2}] = ets:lookup(?TM, rabbit_cache),
    true = (Fox3 =/= Fox2 andalso Rabbit3 =/= Rabbit2),
    [Fox_Cache, Fox_Cache, Rabbit_Cache, Rabbit_Cache]
        = [ets:info(Tab, owner) || Tab <- [Fox3, Fox2, Rabbit3, Rabbit2]],
    [undefined, undefined] = [ets:info(Tab) || Tab <- [Fox1, Rabbit1]],

    2 = ets:info(?TM, size),
    true = ?TM:delete(fox_cache),
    1 = ets:info(?TM, size),
    true = ?TM:delete(rabbit_cache),
    0 = ets:info(?TM, size),
    unlink(Sup),
    ok.


%%%------------------------------------------------------------------------------
%%% Support functions
%%%------------------------------------------------------------------------------

%% Functions for triggering new generations.
gen_count_fun (Thresh) -> fun(Name, Count, Time) -> ?TM:new_gen_count_threshold (Name, Count, Time, Thresh) end.
%%gen_time_fun  (Thresh) -> fun(Name, Count, Time) -> ?TM:new_gen_time_threshold  (Name, Count, Time, Thresh) end.

%% Create a new cache (each testcase creates the ets metadata table on first reserve call).
%% Generation logic is to create a new generation every 5 fetches.
reserve_and_create_cache(Cache_Name, Cache_Obj) ->
%%    undefined = ets:info(?TM, named_table),
    Gen_Fun = gen_count_fun(5),
    Cache_Name = ?TM:reserve(Cache_Name, Cache_Obj, Gen_Fun),
    true = validate_cache_metatable(Cache_Name, Cache_Obj, Gen_Fun),
    true = ?TM:create(Cache_Name),
    true = validate_cache_generations(Cache_Name),
    true.

validate_cache_metatable(Cache_Name, Cache_Obj, Gen_Fun) ->
    [Exp1] = ets:tab2list(?TM),
     Exp2  = #cxy_cache_meta{cache_name=Cache_Name, cache_module=Cache_Obj,
                             new_gen=undefined, old_gen=undefined, new_generation_function=Gen_Fun},
    true = metas_match(Exp1, Exp2),
    [set, true, public] = [ets:info(?TM, Prop) || Prop <- [type, named_table, protection]],
    true.
    
validate_cache_generations(Cache_Name) ->
    [Metadata] = ets:lookup(?TM, Cache_Name),
    #cxy_cache_meta{cache_name=Cache_Name, new_gen=Tid1, old_gen=Tid2} = Metadata,
    [set, false, public] = [ets:info(Tid1, Prop) || Prop <- [type, named_table, protection]],
    [set, false, public] = [ets:info(Tid2, Prop) || Prop <- [type, named_table, protection]],
    true.

%% Delete cache and verify that all ets cache meta data is gone.
%% This only works if there is just one (or zero) cache(s) registered.
eliminate_cache(Cache_Name) ->   
    true = ?TM:delete(Cache_Name),
    true = ets:info(?TM, named_table),
    [] = ets:tab2list(?TM).

%% Verify that two metadata records match provided that the 2nd was created later than the 1st.
metas_match(#cxy_cache_meta{
               cache_name=Name, fetch_count=Fetch, gen1_hit_count=Hit_Count1, gen2_hit_count=Hit_Count2,
               miss_count=Miss_Count, error_count=Err_Count, cache_module=Mod, new_gen=New, old_gen=Old,
               new_generation_function=Gen_Fun, new_generation_thresh=Thresh, started=Start1} = _Earlier,
            #cxy_cache_meta{
               cache_name=Name, fetch_count=Fetch, gen1_hit_count=Hit_Count1, gen2_hit_count=Hit_Count2,
               miss_count=Miss_Count, error_count=Err_Count, cache_module=Mod, new_gen=New, old_gen=Old,
               new_generation_function=Gen_Fun, new_generation_thresh=Thresh, started=Start2} = _Later) ->
    Start1 < Start2;

%% Logs and fails if there is any field mismatch.
metas_match(A,B) -> ct:log("~w~n", [A]),
                    ct:log("~w~n", [B]),
                    false.
