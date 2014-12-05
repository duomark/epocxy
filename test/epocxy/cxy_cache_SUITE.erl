-module(cxy_cache_SUITE).
-auth('jay@duomark.com').
-vsn('').

-export([all/0, init_per_suite/1, end_per_suite/1]).
-export([
         check_create/1, check_clear_and_delete/1,
         check_fetching/1, check_info/1,
         check_fsm_cache/1
        ]).

-include_lib("common_test/include/ct.hrl").

-spec all() -> [atom()].

all() -> [
          check_create, check_clear_and_delete,
          check_fetching, check_info, check_fsm_cache
         ].

-type config() :: proplists:proplist().
-spec init_per_suite(config()) -> config().
-spec end_per_suite(config()) -> config().

init_per_suite(Config) -> Config.
end_per_suite(Config)  -> Config.

-define(TM, cxy_cache).


%%%------------------------------------------------------------------------------
%%% Unit tests for cxy_cache core
%%%------------------------------------------------------------------------------
                              
-include("cxy_cache.hrl").

cleanup(Cache_Name) ->     
    true = ?TM:delete(Cache_Name),
    true = ets:info(?TM, named_table),
    [] = ets:tab2list(?TM).

metas_match(#cxy_cache_meta{
               cache_name=Name, fetch_count=Fetch, gen1_hit_count=Hit_Count1, gen2_hit_count=Hit_Count2,
               miss_count=Miss_Count, error_count=Err_Count, cache_module=Mod, new_gen=New, old_gen=Old,
               new_generation_function=Gen_Fun, new_generation_thresh=Thresh, started=Start1} = _Earlier,
            #cxy_cache_meta{
               cache_name=Name, fetch_count=Fetch, gen1_hit_count=Hit_Count1, gen2_hit_count=Hit_Count2,
               miss_count=Miss_Count, error_count=Err_Count, cache_module=Mod, new_gen=New, old_gen=Old,
               new_generation_function=Gen_Fun, new_generation_thresh=Thresh, started=Start2} = _Later) ->
    Start1 < Start2;
metas_match(A,B) -> ct:log("~w~n", [A]),
                    ct:log("~w~n", [B]),
                    false.


do_create(Cache_Name, Cache_Obj) ->
    undefined = ets:info(?TM, named_table),
    Gen_Fun = fun(Name, Count, Time) -> ?TM:new_gen_count_threshold(Name, Count, Time, 5) end,
    Cache_Name = ?TM:reserve(Cache_Name, Cache_Obj, Gen_Fun),
    Exp = ets:tab2list(?TM),
    1 = length(Exp),
    Exp1 = hd(Exp),
    Exp2 = #cxy_cache_meta{cache_name=Cache_Name, cache_module=Cache_Obj, new_gen=undefined, old_gen=undefined,
                           new_generation_function=Gen_Fun},
    true = metas_match(Exp1, Exp2),
    [set, true, public] = [ets:info(?TM, Prop) || Prop <- [type, named_table, protection]],
    true = ?TM:create(Cache_Name),
    [#cxy_cache_meta{cache_name=Cache_Name, fetch_count=0, new_gen=Tid1, old_gen=Tid2}] = ets:tab2list(?TM),
    [set, false, public] = [ets:info(Tid1, Prop) || Prop <- [type, named_table, protection]],
    [set, false, public] = [ets:info(Tid2, Prop) || Prop <- [type, named_table, protection]],
    ok.

check_create(_Config) ->
    Cache_Name = frog_cache,
    do_create(Cache_Name, frog_obj),
    foo = ?TM:reserve(foo, frog_obj, count, 1000),
    Gen_Fun = fun(Name, _Count, _Time) -> Name =:= foo end,
    {error, already_exists} = ?TM:reserve(foo, fake_module, Gen_Fun),
    {error, already_exists} = ?TM:reserve(foo, fake_module, count, 5000),
    ?TM:delete(foo),

    cleanup(Cache_Name),
    ok.

check_clear_and_delete(_Config) ->
    Cache_Name = frog_cache,
    do_create(Cache_Name, frog_obj),
    [{frog, foo}, {frog, foo}, {frog, foo}] = [?TM:fetch_item(Cache_Name, foo) || _N <- lists:seq(1,3)],
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

    cleanup(Cache_Name),
    [0, undefined, undefined] = [ets:info(Tab, size) || Tab <- [?TM, Old_Gen, New_Gen]],
    ok.


check_fetching(_Config) ->
    Cache_Name = frogs,
    do_create(Cache_Name, frog_obj),
    [#cxy_cache_meta{new_gen=New, old_gen=Old}] = ets:lookup(?TM, Cache_Name),

    %% First time creates new value (fetch_count always indicates next access count)...
    {frog, foo} = ?TM:fetch_item(Cache_Name, foo),
    [] = ets:lookup(Old, foo),
    [#cxy_cache_value{key=foo, value={frog, foo}}] = ets:lookup(New, foo),
    [#cxy_cache_meta{fetch_count=1}] = ets:lookup(?TM, Cache_Name),
    false = ?TM:maybe_make_new_generation(Cache_Name),

    %% Second time fetches existing value...
    {frog, foo} = ?TM:fetch_item(Cache_Name, foo),
    [] = ets:lookup(Old, foo),
    [#cxy_cache_value{key=foo, value={frog, foo}}] = ets:lookup(New, foo),
    [#cxy_cache_meta{fetch_count=2}] = ets:lookup(?TM, Cache_Name),
    false = ?TM:maybe_make_new_generation(Cache_Name),

    %% Retrieve 3 more times still no new generation...
    [{frog, foo}, {frog, foo}, {frog, foo}] = [?TM:fetch_item(Cache_Name, foo) || _N <- lists:seq(1,3)],
    [] = ets:lookup(Old, foo),
    [#cxy_cache_value{key=foo, value={frog, foo}}] = ets:lookup(New, foo),
    [#cxy_cache_meta{fetch_count=5}] = ets:lookup(?TM, Cache_Name),
    false = ?TM:maybe_make_new_generation(Cache_Name),

    %% Once more to get a new generation, then use a new key to insert in the new generation only...
    {frog, foo} = ?TM:fetch_item(Cache_Name, foo),
    0 = ets:info(Old, size),
    [#cxy_cache_meta{new_gen=New, old_gen=Old}] = ets:lookup(?TM, Cache_Name),
    true = ?TM:maybe_make_new_generation(Cache_Name),
    [#cxy_cache_meta{new_gen=New2, old_gen=New}] = ets:lookup(?TM, Cache_Name),
    0 = ets:info(New2, size),
    {frog, bar} = ?TM:fetch_item(Cache_Name, bar),
    1 = ets:info(New2, size),
    [] = ets:lookup(New2, foo),
    [#cxy_cache_value{key=bar, value={frog, bar}}] = ets:lookup(New2, bar),
    1 = ets:info(New, size),
    [#cxy_cache_value{key=foo, value={frog, foo}}] = ets:lookup(New, foo),
    [] = ets:lookup(New, bar),
    [#cxy_cache_meta{fetch_count=1}] = ets:lookup(?TM, Cache_Name),

    %% Now check if migration of key 'foo' works properly...
    {frog, foo} = ?TM:fetch_item(Cache_Name, foo),
    2 = ets:info(New2, size),
    [#cxy_cache_value{key=foo, value={frog, foo}}] = ets:lookup(New2, foo),
    [#cxy_cache_value{key=bar, value={frog, bar}}] = ets:lookup(New2, bar),
    1 = ets:info(New, size),
    [#cxy_cache_value{key=foo, value={frog, foo}}] = ets:lookup(New, foo),
    [] = ets:lookup(New, bar),
    [#cxy_cache_meta{fetch_count=2}] = ets:lookup(?TM, Cache_Name),

    cleanup(Cache_Name),
    ok.

check_info(_Config) ->
    Api = api,
    {Api, []} = ?TM:info(Api),
    Api = ?TM:reserve(Api, new_api),
    true = ?TM:create(Api),

    Prod = product,
    {Prod, []} = ?TM:info(Prod),
    Prod = ?TM:reserve(Prod, new_product),
    true = ?TM:create(Prod),

    {Api, Api_Info} = ?TM:info(Api),
    [0, 0] = [proplists:get_value(P, Api_Info)  || P <- [new_gen_count, old_gen_count]],
    {Prod, Prod_Info} = ?TM:info(Prod),
    [0, 0] = [proplists:get_value(P, Prod_Info) || P <- [new_gen_count, old_gen_count]],

    [Api, Prod] = [Cache || {Cache, _Proplist} <- ?TM:info()],
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
        = [ets:info(Tab, owner) || Tab <- [Fox1, Fox2, Rabbit1, Rabbit2]],
    
    %% Wait for a new generation...
    timer:sleep(1500),
    [#cxy_cache_meta{new_gen=Fox3,    old_gen=Fox2}]    = ets:lookup(?TM, fox_cache),
    [#cxy_cache_meta{new_gen=Rabbit3, old_gen=Rabbit2}] = ets:lookup(?TM, rabbit_cache),
    true = (Fox3 =/= Fox2 andalso Rabbit3 =/= Rabbit2),
    [Fox_Cache, Fox_Cache, Rabbit_Cache, Rabbit_Cache]
        = [ets:info(Tab, owner) || Tab <- [Fox3, Fox2, Rabbit3, Rabbit2]],
    [undefined, undefined] = [ets:info(Tab) || Tab <- [Fox1, Rabbit1]],

    unlink(Sup),
    ok.
