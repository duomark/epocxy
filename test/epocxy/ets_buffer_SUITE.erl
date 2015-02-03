%%%------------------------------------------------------------------------------
%%% @copyright (c) 2013-2015, DuoMark International, Inc.
%%% @author Jay Nelson <jay@duomark.com>
%%% @reference 2013-2015 Development sponsored by TigerText, Inc. [http://tigertext.com/]
%%% @reference The license is based on the template for Modified BSD from
%%%   <a href="http://opensource.org/licenses/BSD-3-Clause">OSI</a>
%%% @doc
%%%   Tests for ets_buffer using common test.
%%%
%%% @since 0.9.6
%%% @end
%%%------------------------------------------------------------------------------
-module(ets_buffer_SUITE).
-auth('jay@duomark.com').
-vsn('').

-export([all/0, init_per_suite/1, end_per_suite/1]).
-export([
         check_shared_error/1,      check_dedicated_error/1,
         check_shared_create/1,     check_dedicated_create/1,
         check_shared_history/1,    check_dedicated_history/1,
         check_shared_clear/1,      check_dedicated_clear/1,
         check_shared_read/1,       check_dedicated_read/1,
         check_shared_fifo_edges/1, check_dedicated_fifo_edges/1,
         check_shared_lifo_edges/1, check_dedicated_lifo_edges/1,
         check_shared_ring_edges/1, check_dedicated_ring_edges/1,
         check_shared_read_all/1,   check_dedicated_read_all/1
        ]).

-include_lib("common_test/include/ct.hrl").

-spec all() -> [atom()].

all() -> [
          check_shared_error,      check_dedicated_error,
          check_shared_create,     check_dedicated_create,
          check_shared_history,    check_dedicated_history,
          check_shared_clear,      check_dedicated_clear,
          check_shared_read,       check_dedicated_read,
          check_shared_fifo_edges, check_dedicated_fifo_edges,
          check_shared_lifo_edges, check_dedicated_lifo_edges,
          check_shared_ring_edges, check_dedicated_ring_edges,
          check_shared_read_all,   check_dedicated_read_all
         ].

-type config() :: proplists:proplist().
-spec init_per_suite(config()) -> config().
-spec end_per_suite(config()) -> config().

init_per_suite(Config) -> Config.
end_per_suite(Config)  -> Config.

%% Test Modules is ?TM
-define(TM, ets_buffer).
-define(ATTRS, [name, size, high_water, type, reserve_loc, write_loc, read_loc]).

-spec check_shared_error(proplists:proplist()) -> ok.
check_shared_error(_Config) ->
    Tab1 = foo,
    Tab2 = bar,
    [] = ?TM:list(),
    [] = ?TM:list(Tab1),
    [] = ?TM:list(Tab2),

    ?TM = ?TM:create(Tab1, ring, 20),
    [[Tab1, 20, 0, ring, 0, 0, 0]] = [[proplists:get_value(P, Props) || P <- ?ATTRS]
                                      || Props <- ?TM:list()],
    1 = ?TM:write(Tab1, 15),
    [15] = ?TM:history(Tab1),
    [15] = ?TM:history(Tab1, 5),
    [{Now1, 15}] = ?TM:history_timestamped(Tab1),
    [{Now1, 15}] = ?TM:history_timestamped(Tab1, 5),

    [15] = ?TM:read(Tab1),
    [  ] = ?TM:read_all(Tab1),
       0 = ?TM:num_entries(Tab1),
      20 = ?TM:capacity(Tab1),

    1 = ?TM:write(Tab1, 15),
    [{_Now2, 15}] = ?TM:read_timestamped(Tab1),
    [  ] = ?TM:read_all_timestamped(Tab1),
       0 = ?TM:num_entries(Tab1),
      20 = ?TM:capacity(Tab1),

    1 = ?TM:write(Tab1, 35),
    2 = ?TM:write(Tab1, 45),
    [{_Now3, 35}, {_Now4, 45}] = ?TM:read_all_timestamped(Tab1),
       0 = ?TM:num_entries(Tab1),
      20 = ?TM:capacity(Tab1),

    [] = ?TM:list(Tab2),
    {missing_ets_buffer, Tab2} = ?TM:write(Tab2, 16),
    {missing_ets_buffer, Tab2} = ?TM:history(Tab2),
    {missing_ets_buffer, Tab2} = ?TM:history(Tab2, 5),
    {missing_ets_buffer, Tab2} = ?TM:read(Tab2),
    {missing_ets_buffer, Tab2} = ?TM:read_all(Tab2),
    {missing_ets_buffer, Tab2} = ?TM:num_entries(Tab2),
    {missing_ets_buffer, Tab2} = ?TM:capacity(Tab2),

    true  = ?TM:clear(Tab1),
    true  = ?TM:delete(Tab1),
    false = ?TM:clear(Tab2),
    false = ?TM:delete(Tab2),
    ok.

-spec check_dedicated_error(proplists:proplist()) -> ok.
check_dedicated_error(_Config) ->
    Tab1 = foo,
    Tab2 = bar,
    [] = ?TM:list_dedicated(Tab1),
    [] = ?TM:list_dedicated(Tab2),

    Tab1 = ?TM:create_dedicated(Tab1, ring, 20),
    Props = ?TM:list_dedicated(Tab1),
    [Tab1, 20, 0, ring, 0, 0, 0] = [proplists:get_value(P, Props) || P <- ?ATTRS],
    1 = ?TM:write_dedicated(Tab1, 15),
    [15] = ?TM:history_dedicated(Tab1),
    [15] = ?TM:history_dedicated(Tab1, 5),
    [{Now1, 15}] = ?TM:history_timestamped_dedicated(Tab1),
    [{Now1, 15}] = ?TM:history_timestamped_dedicated(Tab1, 5),

    [15] = ?TM:read_dedicated(Tab1),
    [  ] = ?TM:read_all_dedicated(Tab1),
       0 = ?TM:num_entries_dedicated(Tab1),
      20 = ?TM:capacity_dedicated(Tab1),

    1 = ?TM:write_dedicated(Tab1, 15),
    [{_Now2, 15}] = ?TM:read_timestamped_dedicated(Tab1),
    [  ] = ?TM:read_all_timestamped_dedicated(Tab1),
       0 = ?TM:num_entries_dedicated(Tab1),
      20 = ?TM:capacity_dedicated(Tab1),

    1 = ?TM:write_dedicated(Tab1, 35),
    2 = ?TM:write_dedicated(Tab1, 45),
    [35, 45] = ?TM:read_all_dedicated(Tab1),
       0 = ?TM:num_entries_dedicated(Tab1),
      20 = ?TM:capacity_dedicated(Tab1),

    1 = ?TM:write_dedicated(Tab1, 37),
    2 = ?TM:write_dedicated(Tab1, 47),
    [{_Now3, 37}, {_Now4, 47}] = ?TM:read_all_timestamped_dedicated(Tab1),
       0 = ?TM:num_entries_dedicated(Tab1),
      20 = ?TM:capacity_dedicated(Tab1),

    [] = ?TM:list_dedicated(Tab2),
    {missing_ets_buffer, Tab2} = ?TM:write_dedicated(Tab2, 16),
    {missing_ets_buffer, Tab2} = ?TM:history_dedicated(Tab2),
    {missing_ets_buffer, Tab2} = ?TM:history_dedicated(Tab2, 5),
    {missing_ets_buffer, Tab2} = ?TM:read_dedicated(Tab2),
    {missing_ets_buffer, Tab2} = ?TM:read_all_dedicated(Tab2),
    {missing_ets_buffer, Tab2} = ?TM:num_entries_dedicated(Tab2),
    {missing_ets_buffer, Tab2} = ?TM:capacity_dedicated(Tab2),

    true = ?TM:clear_dedicated(Tab1),
    true = ?TM:delete_dedicated(Tab1),
    {missing_ets_buffer, Tab2} = ?TM:clear_dedicated(Tab2),
    {missing_ets_buffer, Tab2} = ?TM:delete_dedicated(Tab2),
    ok.
    
-spec check_shared_create(proplists:proplist()) -> ok.
check_shared_create(_Config) ->
    Tab1 = ets_tests,
    Tab2 = batches,
    [] = ?TM:list(),
    undefined = ets:info(?TM, name),
    ?TM = ?TM:create(Tab1, ring, 20),
    ?TM = ets:info(?TM, name),
    [[Tab1, 20, 0, ring, 0, 0, 0]]
        = [[proplists:get_value(P, Props) || P <- ?ATTRS]
           || Props <- ?TM:list()],
    ?TM = ?TM:create(Tab2, fifo),
    [[Tab2, 0, 0, fifo, 0, 0, 0], [Tab1, 20, 0, ring, 0, 0, 0]]
        = [[proplists:get_value(P, Props) || P <- ?ATTRS]
           || Props <- ?TM:list()],
    
    Props1 = ?TM:list(Tab1),
    [Tab1, 20, 0, ring, 0, 0, 0] = [proplists:get_value(P, Props1) || P <- ?ATTRS],
    Props2 = ?TM:list(Tab2),
    [Tab2,  0, 0, fifo, 0, 0, 0] = [proplists:get_value(P, Props2) || P <- ?ATTRS],

    true = ?TM:delete(Tab1),
    true = ?TM:delete(Tab2),
    [?TM, 0] = [ets:info(?TM, Prop) || Prop <- [name, size]],
    ok.

-spec check_dedicated_create(proplists:proplist()) -> ok.
check_dedicated_create(_Config) ->
    Tab1 = ets_tests,
    Tab2 = batches,
    [] = ?TM:list(),
    undefined = ets:info(?TM, name),
    undefined = ets:info(Tab1, name),
    Tab1 = ?TM:create_dedicated(Tab1, ring, 20),
    undefined = ets:info(?TM, name),
    Tab1 = ets:info(Tab1, name),
    Props1 = ?TM:list_dedicated(Tab1),
    [Tab1, 20, 0, ring, 0, 0, 0]
        = [proplists:get_value(P, Props1) || P <- ?ATTRS],
    Tab2 = ?TM:create_dedicated(Tab2, fifo),
    Props2 = ?TM:list_dedicated(Tab2),
    [Tab2, 0, 0, fifo, 0, 0, 0]
        = [proplists:get_value(P, Props2) || P <- ?ATTRS],

    true = ?TM:delete_dedicated(Tab1),
    true = ?TM:delete_dedicated(Tab2),
    undefined = ets:info(Tab1, name),
    undefined = ets:info(Tab2, name),
    ok.

-spec check_shared_history(proplists:proplist()) -> ok.
check_shared_history(_Config) ->    
    Tab_Specs = [{Tab1, _Type1}, {Tab2, _Type2, _Size2}, {Tab3, _Type3}]
        = [{beets, fifo}, {apples, ring, 10}, {carrots, lifo}],
    ?TM = ?TM:create(Tab_Specs),
    Data1 = [
            {Tab1, {red, 2}},        {Tab1, {golden, 1}},
            {Tab2, {macintosh, 34}}, {Tab2, {winesap, 18}}, {Tab2, {granny, 8}},
            {Tab3, {purple, 5}},     {Tab3, {hybrid, 7}}
           ],
    Exp1 = [1,2, 1,2,3, true,true],
    Exp1 = [?TM:write(Buffer_Name, Buffer_Data) || {Buffer_Name, Buffer_Data} <- Data1],
    [?TM, 10] = [ets:info(?TM, Prop) || Prop <- [name, size]],  %% 3 meta + 7 data

    [{red, 2},        {golden, 1}               ] = ?TM:history(Tab1),
    [{macintosh, 34}, {winesap, 18}, {granny, 8}] = ?TM:history(Tab2),
    [{hybrid, 7},     {purple, 5}               ] = ?TM:history(Tab3),

    Data2 = [
             {Tab1, {green,  3}}, {Tab1, {orange, 8}},
             {Tab3, {bumpy, 11}}, {Tab3, {smooth, 4}}
             ],
    Exp2 = [3,4, true,true],
    Exp2 = [?TM:write(Buffer_Name, Buffer_Data) || {Buffer_Name, Buffer_Data} <- Data2],
    [?TM, 14] = [ets:info(?TM, Prop) || Prop <- [name, size]],  %% 3 meta + 11 data

    [{red, 2},        {golden, 1},   {green, 3},  {orange, 8}] = ?TM:history(Tab1),
    [{macintosh, 34}, {winesap, 18}, {granny, 8}             ] = ?TM:history(Tab2),
    [{smooth, 4},     {bumpy, 11},   {hybrid, 7}, {purple, 5}] = ?TM:history(Tab3),

    [{red, 2},        {golden, 1},   {green, 3},  {orange, 8}] = ?TM:history(Tab1, 7),
    [{macintosh, 34}, {winesap, 18}, {granny, 8}             ] = ?TM:history(Tab2, 7),
    [{smooth, 4},     {bumpy, 11},   {hybrid, 7}, {purple, 5}] = ?TM:history(Tab3, 7),

    [{red, 2},        {golden, 1},   {green, 3}              ] = ?TM:history(Tab1, 3),
    [{winesap, 18},   {granny, 8}                            ] = ?TM:history(Tab2, 2),
    [{smooth, 4},     {bumpy, 11}                            ] = ?TM:history(Tab3, 2),
    
    [true, true, true] = [?TM:delete(Tab) || Tab <- [Tab1, Tab2, Tab3]],
    ok.

-spec check_dedicated_history(proplists:proplist()) -> ok.
check_dedicated_history(_Config) ->
    Tab_Specs = [{Tab1, _Type1}, {Tab2, _Type2, _Size2}, {Tab3, _Type3}]
        = [{beets, fifo}, {apples, ring, 10}, {carrots, lifo}],
    [Tab1, Tab2, Tab3]
        = [Tab || Spec <- Tab_Specs,
                  begin
                      Tab = case Spec of
                                {Buffer_Name, Buffer_Type}       -> ?TM:create_dedicated(Buffer_Name, Buffer_Type);
                                {Buffer_Name, Buffer_Type, Size} -> ?TM:create_dedicated(Buffer_Name, Buffer_Type, Size)
                            end,
                      true
                  end],
    Data1 = [
            {Tab1, {red, 2}},        {Tab1, {golden, 1}},
            {Tab2, {macintosh, 34}}, {Tab2, {winesap, 18}}, {Tab2, {granny, 8}},
            {Tab3, {purple, 5}},     {Tab3, {hybrid, 7}}
           ],
    Exp1 = [1,2, 1,2,3, true,true],
    Exp1 = [?TM:write_dedicated(Buffer_Name, Buffer_Data) || {Buffer_Name, Buffer_Data} <- Data1],

    [{red, 2},        {golden, 1}               ] = ?TM:history_dedicated(Tab1),
    [{macintosh, 34}, {winesap, 18}, {granny, 8}] = ?TM:history_dedicated(Tab2),
    [{hybrid, 7},     {purple, 5}               ] = ?TM:history_dedicated(Tab3),

    Data2 = [
             {Tab1, {green,  3}}, {Tab1, {orange, 8}},
             {Tab3, {bumpy, 11}}, {Tab3, {smooth, 4}}
             ],
    Exp2 = [3,4, true,true],
    Exp2 = [?TM:write_dedicated(Buffer_Name, Buffer_Data) || {Buffer_Name, Buffer_Data} <- Data2],

    [{red, 2},        {golden, 1},   {green, 3},  {orange, 8}] = ?TM:history_dedicated(Tab1),
    [{macintosh, 34}, {winesap, 18}, {granny, 8}             ] = ?TM:history_dedicated(Tab2),
    [{smooth, 4},     {bumpy, 11},   {hybrid, 7}, {purple, 5}] = ?TM:history_dedicated(Tab3),

    [{red, 2},        {golden, 1},   {green, 3},  {orange, 8}] = ?TM:history_dedicated(Tab1, 7),
    [{macintosh, 34}, {winesap, 18}, {granny, 8}             ] = ?TM:history_dedicated(Tab2, 7),
    [{smooth, 4},     {bumpy, 11},   {hybrid, 7}, {purple, 5}] = ?TM:history_dedicated(Tab3, 7),

    [{red, 2},        {golden, 1},   {green, 3}              ] = ?TM:history_dedicated(Tab1, 3),
    [{winesap, 18},   {granny, 8}                            ] = ?TM:history_dedicated(Tab2, 2),
    [{smooth, 4},     {bumpy, 11}                            ] = ?TM:history_dedicated(Tab3, 2),
    
    [true, true, true] = [?TM:delete_dedicated(Tab) || Tab <- [Tab1, Tab2, Tab3]],
    ok.

-spec check_shared_clear(proplists:proplist()) -> ok.
check_shared_clear(_Config) ->
    Tab_Specs = [{Tab1, Type1}, {Tab2, Type2, Size2}, {Tab3, Type3}]
        = [{beets, fifo}, {apples, ring, 10}, {carrots, lifo}],
    [] = ?TM:list(),
    [undefined, undefined, undefined, undefined]
        = [ets:info(Tab, name) || Tab <- [?TM] ++ [element(1,Spec) || Spec <- Tab_Specs]],
    ?TM = ?TM:create(Tab_Specs),
    [?TM, undefined, undefined, undefined]
        = [ets:info(Tab, name) || Tab <- [?TM] ++ [element(1,Spec) || Spec <- Tab_Specs]],

    %% Entries are sorted by table name.
    [[Tab2, Size2, 0, Type2, 0, 0, 0], [Tab1, 0, 0, Type1, 0, 0, 0], [Tab3, 0, 0, Type3, 0, 0, 0]]
        = [[proplists:get_value(P, Props) || P <- ?ATTRS]
           || Props <- ?TM:list()],

    Data = [{Tab1, {red, 2}}, {Tab1, {golden, 1}}, {Tab2, {macintosh, 34}}, {Tab3, {purple, 5}}],
    [1,2, 1, true] = [?TM:write(Buffer_Name, Buffer_Data) || {Buffer_Name, Buffer_Data} <- Data],
    [?TM, 7] = [ets:info(?TM, Prop) || Prop <- [name, size]],  %% 3 meta + 4 data

    %% Entries are sorted by table name.
    [[Tab2, Size2, 1, Type2, 1, 1, 0], [Tab1, 0, 2, Type1, 2, 2, 0], [Tab3, 0, 0, Type3, -1, -1, -1]]
        = [[proplists:get_value(P, Props) || P <- ?ATTRS]
           || Props <- ?TM:list()],
    
    [true, true, true] = [?TM:clear(Tab) || Tab <- [Tab1, Tab2, Tab3]],
    [?TM, 3] = [ets:info(?TM, Prop) || Prop <- [name, size]],  %% 3 meta

    %% High water mark remains after a clear
    [[Tab2, Size2, 1, Type2, 0, 0, 0], [Tab1, 0, 2, Type1, 0, 0, 0], [Tab3, 0, 0, Type3, 0, 0, 0]]
        = [[proplists:get_value(P, Props) || P <- ?ATTRS]
           || Props <- ?TM:list()],
    [true, true, true] = [?TM:clear_high_water(Tab) || Tab <- [Tab1, Tab2, Tab3]],
    [[Tab2, Size2, 0, Type2, 0, 0, 0], [Tab1, 0, 0, Type1, 0, 0, 0], [Tab3, 0, 0, Type3, 0, 0, 0]]
        = [[proplists:get_value(P, Props) || P <- ?ATTRS]
           || Props <- ?TM:list()],
    
    [true, true, true] = [?TM:delete(Tab) || Tab <- [Tab1, Tab2, Tab3]],
    [?TM, 0] = [ets:info(?TM, Prop) || Prop <- [name, size]],  %% no meta
    ok.

-spec check_dedicated_clear(proplists:proplist()) -> ok.
check_dedicated_clear(_Config) ->
    Tab_Specs = [{Tab1, Type1}, {Tab2, Type2, Size2}, {Tab3, Type3}]
        = [{beets, fifo}, {apples, ring, 10}, {carrots, lifo}],
    [[], [], []] = [?TM:list_dedicated(Tab) || Tab <- [Tab1, Tab2, Tab3]],
    [undefined, undefined, undefined, undefined]
        = [ets:info(Tab, name) || Tab <- [?TM, Tab1, Tab2, Tab3]],
    [Tab1, Tab2, Tab3] = [Result || Spec <- Tab_Specs,
                                    begin
                                        Result = case Spec of
                                                     {Tab, Type}       -> ?TM:create_dedicated(Tab, Type);
                                                     {Tab, Type, Size} -> ?TM:create_dedicated(Tab, Type, Size)
                                                 end,
                                        true
                                    end],
    [undefined, Tab1, Tab2, Tab3]
        = [ets:info(Tab, name) || Tab <- [?TM, Tab1, Tab2, Tab3]],

    [[Tab1, 0, 0, Type1, 0, 0, 0], [Tab2, Size2, 0, Type2, 0, 0, 0], [Tab3, 0, 0, Type3, 0, 0, 0]]
        = [[proplists:get_value(P, Props) || P <- ?ATTRS]
           || Props <- [?TM:list_dedicated(T) || T <- [Tab1, Tab2, Tab3]]],

    Data = [{Tab1, {red, 2}}, {Tab1, {golden, 1}}, {Tab2, {macintosh, 34}}, {Tab3, {purple, 5}}],
    [1,2, 1, true] = [?TM:write_dedicated(Buffer_Name, Buffer_Data) || {Buffer_Name, Buffer_Data} <- Data],
    [[undefined, undefined], [Tab1, 3], [Tab2, 2], [Tab3, 2]]
        = [[ets:info(Tab, Prop) || Prop <- [name, size]] || Tab <- [?TM, Tab1, Tab2, Tab3]],

    [[Tab1, 0, 2, Type1, 2, 2, 0], [Tab2, Size2, 1, Type2, 1, 1, 0], [Tab3, 0, 0, Type3, -1, -1, -1]]
        = [[proplists:get_value(P, Props) || P <- ?ATTRS]
           || Props <- [?TM:list_dedicated(T) || T <- [Tab1, Tab2, Tab3]]],
    
    [true, true, true] = [?TM:clear_dedicated(Tab) || Tab <- [Tab1, Tab2, Tab3]],
    [[undefined, undefined], [Tab1, 1], [Tab2, 1], [Tab3, 1]]
        = [[ets:info(Tab, Prop) || Prop <- [name, size]] || Tab <- [?TM, Tab1, Tab2, Tab3]],

    %% High water mark remains after a clear
    [[Tab1, 0, 2, Type1, 0, 0, 0], [Tab2, Size2, 1, Type2, 0, 0, 0], [Tab3, 0, 0, Type3, 0, 0, 0]]
        = [[proplists:get_value(P, Props) || P <- ?ATTRS]
           || Props <- [?TM:list_dedicated(T) || T <- [Tab1, Tab2, Tab3]]],
    [true, true, true] = [?TM:clear_high_water_dedicated(Tab) || Tab <- [Tab1, Tab2, Tab3]],
    [[Tab1, 0, 0, Type1, 0, 0, 0], [Tab2, Size2, 0, Type2, 0, 0, 0], [Tab3, 0, 0, Type3, 0, 0, 0]]
        = [[proplists:get_value(P, Props) || P <- ?ATTRS]
           || Props <- [?TM:list_dedicated(T) || T <- [Tab1, Tab2, Tab3]]],

    [true, true, true] = [?TM:delete_dedicated(Tab) || Tab <- [Tab1, Tab2, Tab3]],
    [undefined, undefined, undefined, undefined] = [ets:info(Tab, name) || Tab <- [?TM, Tab1, Tab2, Tab3]],
    ok.

-spec check_shared_read(proplists:proplist()) -> ok.
check_shared_read(_Config) ->    
    Tab_Specs = [{Tab1, _Type1}, {Tab2, _Type2, _Size2}, {Tab3, _Type3}]
        = [{beets, fifo}, {apples, ring, 10}, {carrots, lifo}],
    ?TM = ?TM:create(Tab_Specs),
    Data1 = [
             {Tab1, {red,        2}},
             {Tab2, {macintosh, 34}}, {Tab2, {winesap, 18}}, {Tab2, {granny, 8}},
             {Tab3, {purple,     5}}, {Tab3, {hybrid,   7}}
           ],
    Exp1 = [1, 1,2,3, true,true],
    Exp1 = [?TM:write(Buffer_Name, Buffer_Data) || {Buffer_Name, Buffer_Data} <- Data1],
    [?TM, 9] = [ets:info(?TM, Prop) || Prop <- [name, size]],  %% 3 meta + 6 data

    not_supported = ?TM:read(Tab3, 5),

    [{red,        2}] = ?TM:read(Tab1),
    [{macintosh, 34}] = ?TM:read(Tab2),
    [{hybrid,     7}] = ?TM:read(Tab3),

    Data2 = [
             {Tab1, {golden,  1}}, {Tab1, {green,  3}}, {Tab1, {orange, 8}},
             {Tab3, {bumpy,  11}}, {Tab3, {smooth, 4}}
            ],
    Exp2 = [1,2,3, true,true],
    Exp2 = [?TM:write(Buffer_Name, Buffer_Data) || {Buffer_Name, Buffer_Data} <- Data2],
    [?TM, 12] = [ets:info(?TM, Prop) || Prop <- [name, size]],  %% 3 meta + 9 remaining data

    [{golden,   1}, {green,  3}] = ?TM:read(Tab1, 2),
    [{winesap, 18}, {granny, 8}] = ?TM:read(Tab2, 2),

    [{smooth,   4}] = ?TM:read(Tab3),
    [{bumpy,   11}] = ?TM:read(Tab3, 1),

    [{orange, 8}] = ?TM:read(Tab1, 5),
    [           ] = ?TM:read(Tab2, 5),
    [{purple, 5}] = ?TM:read(Tab3),

    [true, true, true] = [?TM:delete(Tab) || Tab <- [Tab1, Tab2, Tab3]],
    ok.

-spec check_dedicated_read(proplists:proplist()) -> ok.
check_dedicated_read(_Config) ->
    Tab_Specs = [{Tab1, _Type1}, {Tab2, _Type2, _Size2}, {Tab3, _Type3}]
        = [{beets, fifo}, {apples, ring, 10}, {carrots, lifo}],
    [Tab1, Tab2, Tab3] = [Result || Spec <- Tab_Specs,
                                    begin
                                        Result = case Spec of
                                                     {Tab, Type}       -> ?TM:create_dedicated(Tab, Type);
                                                     {Tab, Type, Size} -> ?TM:create_dedicated(Tab, Type, Size)
                                                 end,
                                        true
                                    end],
    Data1 = [
             {Tab1, {red,        2}}, {Tab1, {golden,   1}},
             {Tab2, {macintosh, 34}}, {Tab2, {winesap, 18}}, {Tab2, {granny, 8}},
             {Tab3, {purple,     5}}, {Tab3, {hybrid,   7}}
           ],
    Exp1 = [1,2, 1,2,3, true,true],
    Exp1 = [?TM:write_dedicated(Buffer_Name, Buffer_Data) || {Buffer_Name, Buffer_Data} <- Data1],

    not_supported = ?TM:read_dedicated(Tab3, 5),

    [{red,        2}] = ?TM:read_dedicated(Tab1),
    [{macintosh, 34}] = ?TM:read_dedicated(Tab2),
    [{hybrid,     7}] = ?TM:read_dedicated(Tab3),

    Data2 = [
              {Tab1, {green,  3}}, {Tab1, {orange, 8}},
              {Tab3, {bumpy, 11}}, {Tab3, {smooth, 4}}
            ],
    Exp2 = [2,3, true,true],
    Exp2 = [?TM:write_dedicated(Buffer_Name, Buffer_Data) || {Buffer_Name, Buffer_Data} <- Data2],

    [{golden,   1}, {green,  3}] = ?TM:read_dedicated(Tab1, 2),
    [{winesap, 18}, {granny, 8}] = ?TM:read_dedicated(Tab2, 2),
    [{smooth,   4}] = ?TM:read_dedicated(Tab3, 1),
    [{bumpy,   11}] = ?TM:read_dedicated(Tab3),

    [{orange, 8}] = ?TM:read_dedicated(Tab1, 5),
    [           ] = ?TM:read_dedicated(Tab2, 5),
    [{purple, 5}] = ?TM:read_dedicated(Tab3),

    [true, true, true] = [?TM:delete_dedicated(Tab) || Tab <- [Tab1, Tab2, Tab3]],
    ok.

-spec check_shared_fifo_edges(proplists:proplist()) -> ok.
check_shared_fifo_edges(_Config) ->    
    Tabs = [Tab1, Tab2] = [beets, apples],
    ?TM = ?TM:create([{T, fifo} || T <- Tabs]),
    
    %% Read brand new empty buffer...
    [] = ?TM:read(Tab1),
    [] = ?TM:read(Tab1, 10),
    [] = ?TM:history(Tab1),
    [] = ?TM:history(Tab1, 10),

    Data1 = [{Tab1, {red, 2}}, {Tab2, {winesap, 3}}],
    _ = [?TM:write(Buffer_Name, Buffer_Data) || {Buffer_Name, Buffer_Data} <- Data1],
    [{red,     2}] = ?TM:history(Tab1),
    [{red,     2}] = ?TM:history(Tab1, 10),
    [{red,     2}] = ?TM:read(Tab1),
    [{winesap, 3}] = ?TM:read(Tab2, 5),
    [] = ?TM:history(Tab1),
    [] = ?TM:history(Tab1, 10),

    Data2 = [{Tab1, {golden, 1}}, {Tab1, {green, 3}}, {Tab1, {orange, 8}}],
    _ = [?TM:write(Buffer_Name, Buffer_Data) || {Buffer_Name, Buffer_Data} <- Data2],
    [{golden, 1}, {green, 3}] = ?TM:read(Tab1, 2),
    [{orange, 8}            ] = ?TM:read(Tab1, 1),
    [                       ] = ?TM:read(Tab1, 3),

    [true, true] = [?TM:delete(T) || T <- Tabs],
    ok.

-spec check_dedicated_fifo_edges(proplists:proplist()) -> ok.
check_dedicated_fifo_edges(_Config) ->    
    Tabs = [Tab1, Tab2] = [beets, apples],
    Tabs = [?TM:create_dedicated(T, fifo) || T <- Tabs],
    
    %% Read brand new empty buffer...
    [] = ?TM:read_dedicated(Tab1),
    [] = ?TM:read_dedicated(Tab1, 10),
    [] = ?TM:history_dedicated(Tab1),
    [] = ?TM:history_dedicated(Tab1, 10),

    Data1 = [{Tab1, {red, 2}}, {Tab2, {winesap, 3}}],
    _ = [?TM:write_dedicated(Buffer_Name, Buffer_Data) || {Buffer_Name, Buffer_Data} <- Data1],
    [{red,     2}] = ?TM:history_dedicated(Tab1),
    [{red,     2}] = ?TM:history_dedicated(Tab1, 10),
    [{red,     2}] = ?TM:read_dedicated(Tab1),
    [{winesap, 3}] = ?TM:read_dedicated(Tab2, 5),
    [] = ?TM:history_dedicated(Tab1),
    [] = ?TM:history_dedicated(Tab1, 10),

    Data2 = [{Tab1, {golden, 1}}, {Tab1, {green, 3}}, {Tab1, {orange, 8}}],
    _ = [?TM:write_dedicated(Buffer_Name, Buffer_Data) || {Buffer_Name, Buffer_Data} <- Data2],
    [{golden, 1}, {green, 3}] = ?TM:read_dedicated(Tab1, 2),
    [{orange, 8}            ] = ?TM:read_dedicated(Tab1, 1),
    [                       ] = ?TM:read_dedicated(Tab1, 3),

    [true, true] = [?TM:delete_dedicated(T) || T <- Tabs],
    ok.

%% LIFO read is only supported 1 record at a time, but maybe in the future...
-spec check_shared_lifo_edges(proplists:proplist()) -> ok.
check_shared_lifo_edges(_Config) ->    
    Tabs = [Tab1, Tab2] = [beets, apples],
    ?TM = ?TM:create([{T, lifo} || T <- Tabs]),
    
    %% Read brand new empty buffer...
    [] = ?TM:read(Tab1),
 %% [] = ?TM:read(Tab1, 10),
    [] = ?TM:history(Tab1),
    [] = ?TM:history(Tab1, 10),

    Data1 = [{Tab1, {red, 2}}, {Tab2, {winesap, 3}}],
    _ = [?TM:write(Buffer_Name, Buffer_Data) || {Buffer_Name, Buffer_Data} <- Data1],
    [{red,     2}] = ?TM:history(Tab1),
    [{red,     2}] = ?TM:history(Tab1, 10),
    [{red,     2}] = ?TM:read(Tab1),
 %% [{winesap, 3}] = ?TM:read(Tab2, 5),
    [] = ?TM:history(Tab1),
    [] = ?TM:history(Tab1, 10),

    Data2 = [{Tab1, {golden, 1}}, {Tab1, {green, 3}}, {Tab1, {orange, 8}}],
    _ = [?TM:write(Buffer_Name, Buffer_Data) || {Buffer_Name, Buffer_Data} <- Data2],
    [[{orange, 8}], [{green, 3}], [{golden, 1}], []]
        = [?TM:read(Tab1) || _N <- lists:seq(1,4)],

    [true, true] = [?TM:delete(T) || T <- Tabs],
    ok.

-spec check_dedicated_lifo_edges(proplists:proplist()) -> ok.
check_dedicated_lifo_edges(_Config) ->    
    Tabs = [Tab1, Tab2] = [beets, apples],
    Tabs = [?TM:create_dedicated(T, lifo) || T <- Tabs],
    
    %% Read brand new empty buffer...
    [] = ?TM:read_dedicated(Tab1),
 %% [] = ?TM:read_dedicated(Tab1, 10),
    [] = ?TM:history_dedicated(Tab1),
    [] = ?TM:history_dedicated(Tab1, 10),

    Data1 = [{Tab1, {red, 2}}, {Tab2, {winesap, 3}}],
    _ = [?TM:write_dedicated(Buffer_Name, Buffer_Data) || {Buffer_Name, Buffer_Data} <- Data1],
    [{red,     2}] = ?TM:history_dedicated(Tab1),
    [{red,     2}] = ?TM:history_dedicated(Tab1, 10),
    [{red,     2}] = ?TM:read_dedicated(Tab1),
 %% [{winesap, 3}] = ?TM:read_dedicated(Tab2, 5),
    [] = ?TM:history_dedicated(Tab1),
    [] = ?TM:history_dedicated(Tab1, 10),

    Data2 = [{Tab1, {golden, 1}}, {Tab1, {green, 3}}, {Tab1, {orange, 8}}],
    _ = [?TM:write_dedicated(Buffer_Name, Buffer_Data) || {Buffer_Name, Buffer_Data} <- Data2],
    [[{orange, 8}], [{green, 3}], [{golden, 1}], []]
        = [?TM:read_dedicated(Tab1) || _N <- lists:seq(1,4)],

    [true, true] = [?TM:delete_dedicated(T) || T <- Tabs],
    ok.

-spec check_shared_ring_edges(proplists:proplist()) -> ok.
check_shared_ring_edges(_Config) ->    
    Tabs = [Tab1, Tab2] = [beets, apples],
    ?TM = ?TM:create([{T, ring, 5} || T <- Tabs]),
    
    %% Read brand new empty buffer...
    [] = ?TM:read(Tab1),
    [] = ?TM:read(Tab1, 3),
    [] = ?TM:read(Tab1, 10),
    [] = ?TM:history(Tab1),
    [] = ?TM:history(Tab1, 3),
    [] = ?TM:history(Tab1, 10),

    Data1 = [{Tab1, {red, 2}}, {Tab1, {granny, 5}}, {Tab2, {winesap, 3}}],
    _ = [?TM:write(Buffer_Name, Buffer_Data) || {Buffer_Name, Buffer_Data} <- Data1],
    [{red, 2}, {granny, 5}] = ?TM:history(Tab1),
    [{red, 2}, {granny, 5}] = ?TM:history(Tab1, 3),
    [{red, 2}, {granny, 5}] = ?TM:history(Tab1, 8),
    2 = ?TM:num_entries(Tab1),
    [{red, 2}] = ?TM:read(Tab1),
    [{red, 2}, {granny, 5}] = ?TM:history(Tab1),
    [{red, 2}, {granny, 5}] = ?TM:history(Tab1, 3),
    [{red, 2}, {granny, 5}] = ?TM:history(Tab1, 8),
    1 = ?TM:num_entries(Tab1),
    [{granny, 5}] = ?TM:read(Tab1),
    [{red, 2}, {granny, 5}] = ?TM:history(Tab1),
    [{red, 2}, {granny, 5}] = ?TM:history(Tab1, 3),
    [{red, 2}, {granny, 5}] = ?TM:history(Tab1, 8),
    0 = ?TM:num_entries(Tab1),

    [{winesap, 3}] = ?TM:history(Tab2),
    [{winesap, 3}] = ?TM:history(Tab2, 3),
    [{winesap, 3}] = ?TM:history(Tab2, 8),
    [{winesap, 3}] = ?TM:read(Tab2),
    [] = ?TM:read(Tab2),
    [{winesap, 3}] = ?TM:history(Tab2),
    [{winesap, 3}] = ?TM:history(Tab2, 3),
    [{winesap, 3}] = ?TM:history(Tab2, 8),

    Data2 = [{Tab1, {golden, 1}}, {Tab1, {green, 3}}, {Tab1, {orange, 8}}],
    _ = [?TM:write(Buffer_Name, Buffer_Data) || {Buffer_Name, Buffer_Data} <- Data2],
    [[{golden, 1}], [{green, 3}], [{orange, 8}], []]
        = [?TM:read(Tab1) || _N <- lists:seq(1,4)],
    [{red, 2}, {granny, 5}, {golden, 1}, {green, 3}, {orange, 8}] = ?TM:history(Tab1),
    [{golden, 1}, {green, 3}, {orange, 8}] = ?TM:history(Tab1, 3),
    [{red, 2}, {granny, 5}, {golden, 1}, {green, 3}, {orange, 8}] = ?TM:history(Tab1, 8),

    Data3 = [{Tab1, {pink, 13}}, {Tab1, {brown, 7}}, {Tab1, {sugar, 12}}],
    _ = [?TM:write(Buffer_Name, Buffer_Data) || {Buffer_Name, Buffer_Data} <- Data3],
    [{pink, 13}, {brown, 7}] = ?TM:read(Tab1, 2),
    [{sugar, 12}] = ?TM:read(Tab1, 4),
    [{green, 3}, {orange, 8}, {pink, 13}, {brown, 7}, {sugar, 12}] = ?TM:history(Tab1),
    [{pink, 13}, {brown, 7}, {sugar, 12}] = ?TM:history(Tab1, 3),
    [{green, 3}, {orange, 8}, {pink, 13}, {brown, 7}, {sugar, 12}] = ?TM:history(Tab1, 8),

    true = ?TM:clear(Tab1),
    [] = ?TM:read(Tab1),
    [] = ?TM:read(Tab1, 4),
    [] = ?TM:read(Tab1, 9),
    [] = ?TM:history(Tab1),

    [true, true] = [?TM:delete(T) || T <- Tabs],
    ok.

-spec check_dedicated_ring_edges(proplists:proplist()) -> ok.
check_dedicated_ring_edges(_Config) ->    
    Tabs = [Tab1, Tab2] = [beets, apples],
    [Tab1, Tab2] = [?TM:create_dedicated(T, ring, 5) || T <- Tabs],
    
    %% Read brand new empty buffer...
    [] = ?TM:read_dedicated(Tab1),
    [] = ?TM:read_dedicated(Tab1, 3),
    [] = ?TM:read_dedicated(Tab1, 10),
    [] = ?TM:history_dedicated(Tab1),
    [] = ?TM:history_dedicated(Tab1, 3),
    [] = ?TM:history_dedicated(Tab1, 10),

    Data1 = [{Tab1, {red, 2}}, {Tab1, {granny, 5}}, {Tab2, {winesap, 3}}],
    _ = [?TM:write_dedicated(Buffer_Name, Buffer_Data) || {Buffer_Name, Buffer_Data} <- Data1],
    [{red, 2}, {granny, 5}] = ?TM:history_dedicated(Tab1),
    [{red, 2}, {granny, 5}] = ?TM:history_dedicated(Tab1, 3),
    [{red, 2}, {granny, 5}] = ?TM:history_dedicated(Tab1, 8),
    [{red, 2}] = ?TM:read_dedicated(Tab1),
    [{red, 2}, {granny, 5}] = ?TM:history_dedicated(Tab1),
    [{red, 2}, {granny, 5}] = ?TM:history_dedicated(Tab1, 3),
    [{red, 2}, {granny, 5}] = ?TM:history_dedicated(Tab1, 8),
    [{granny, 5}] = ?TM:read_dedicated(Tab1),
    [{red, 2}, {granny, 5}] = ?TM:history_dedicated(Tab1),
    [{red, 2}, {granny, 5}] = ?TM:history_dedicated(Tab1, 3),
    [{red, 2}, {granny, 5}] = ?TM:history_dedicated(Tab1, 8),

    [{winesap, 3}] = ?TM:history_dedicated(Tab2),
    [{winesap, 3}] = ?TM:history_dedicated(Tab2, 3),
    [{winesap, 3}] = ?TM:history_dedicated(Tab2, 8),
    [{winesap, 3}] = ?TM:read_dedicated(Tab2),
    [] = ?TM:read_dedicated(Tab2),
    [{winesap, 3}] = ?TM:history_dedicated(Tab2),
    [{winesap, 3}] = ?TM:history_dedicated(Tab2, 3),
    [{winesap, 3}] = ?TM:history_dedicated(Tab2, 8),

    Data2 = [{Tab1, {golden, 1}}, {Tab1, {green, 3}}, {Tab1, {orange, 8}}],
    _ = [?TM:write_dedicated(Buffer_Name, Buffer_Data) || {Buffer_Name, Buffer_Data} <- Data2],
    [[{golden, 1}], [{green, 3}], [{orange, 8}], []]
        = [?TM:read_dedicated(Tab1) || _N <- lists:seq(1,4)],
    [{red, 2}, {granny, 5}, {golden, 1}, {green, 3}, {orange, 8}] = ?TM:history_dedicated(Tab1),
    [{golden, 1}, {green, 3}, {orange, 8}] = ?TM:history_dedicated(Tab1, 3),
    [{red, 2}, {granny, 5}, {golden, 1}, {green, 3}, {orange, 8}] = ?TM:history_dedicated(Tab1, 8),

    Data3 = [{Tab1, {pink, 13}}, {Tab1, {brown, 7}}, {Tab1, {sugar, 12}}],
    _ = [?TM:write_dedicated(Buffer_Name, Buffer_Data) || {Buffer_Name, Buffer_Data} <- Data3],
    [{pink, 13}, {brown, 7}] = ?TM:read_dedicated(Tab1, 2),
    [{sugar, 12}] = ?TM:read_dedicated(Tab1, 4),
    [{green, 3}, {orange, 8}, {pink, 13}, {brown, 7}, {sugar, 12}] = ?TM:history_dedicated(Tab1),
    [{pink, 13}, {brown, 7}, {sugar, 12}] = ?TM:history_dedicated(Tab1, 3),
    [{green, 3}, {orange, 8}, {pink, 13}, {brown, 7}, {sugar, 12}] = ?TM:history_dedicated(Tab1, 8),

    true = ?TM:clear_dedicated(Tab1),
    [] = ?TM:read_dedicated(Tab1),
    [] = ?TM:read_dedicated(Tab1, 4),
    [] = ?TM:read_dedicated(Tab1, 9),
    [] = ?TM:history_dedicated(Tab1),

    [true, true] = [?TM:delete_dedicated(T) || T <- Tabs],
    ok.

-spec check_shared_read_all(proplists:proplist()) -> ok.
check_shared_read_all(_Config) ->    
    Tab_Specs = [{Tab1, _Type1}, {Tab2, _Type2, _Size2}, {Tab3, _Type3}]
        = [{beets, fifo}, {apples, ring, 10}, {carrots, lifo}],
    ?TM = ?TM:create(Tab_Specs),
    [unlimited, 10, unlimited] = [?TM:capacity(Buffer) || Buffer <- [Tab1, Tab2, Tab3]],
    [0, 0, not_supported] = [?TM:num_entries(Buffer) || Buffer <- [Tab1, Tab2, Tab3]],

    Data1 = [
             {Tab1, {red,        2}},
             {Tab2, {macintosh, 34}}, {Tab2, {winesap, 18}}, {Tab2, {granny, 8}},
             {Tab3, {purple,     5}}, {Tab3, {hybrid,   7}}
           ],
    Exp1 = [1, 1,2,3, true,true],
    Exp1 = [?TM:write(Buffer_Name, Buffer_Data) || {Buffer_Name, Buffer_Data} <- Data1],

    [1, 3, not_supported] = [?TM:num_entries(Buffer) || Buffer <- [Tab1, Tab2, Tab3]],
    [{red,        2}] = ?TM:read_all(Tab1),
    [{macintosh, 34}, {winesap, 18}, {granny, 8}] = ?TM:read_all(Tab2),
    not_supported = ?TM:read_all(Tab3),
    [0, 0, not_supported] = [?TM:num_entries(Buffer) || Buffer <- [Tab1, Tab2, Tab3]],

    Data2 = [
             {Tab1, {golden,  1}}, {Tab1, {green,  3}}, {Tab1, {orange, 8}},
             {Tab3, {bumpy,  11}}, {Tab3, {smooth, 4}}
            ],
    Exp2 = [1,2,3, true,true],
    Exp2 = [?TM:write(Buffer_Name, Buffer_Data) || {Buffer_Name, Buffer_Data} <- Data2],

    [3, 0, not_supported] = [?TM:num_entries(Buffer) || Buffer <- [Tab1, Tab2, Tab3]],
    [{golden, 1}, {green, 3}, {orange, 8}] = ?TM:read_all(Tab1),
    [] = ?TM:read_all(Tab2),
    [0, 0, not_supported] = [?TM:num_entries(Buffer) || Buffer <- [Tab1, Tab2, Tab3]],

    [true, true, true] = [?TM:delete(Tab) || Tab <- [Tab1, Tab2, Tab3]],
    ok.

-spec check_dedicated_read_all(proplists:proplist()) -> ok.
check_dedicated_read_all(_Config) ->
    Tab_Specs = [{Tab1, _Type1}, {Tab2, _Type2, _Size2}, {Tab3, _Type3}]
        = [{beets, fifo}, {apples, ring, 10}, {carrots, lifo}],
    [Tab1, Tab2, Tab3] = [Result || Spec <- Tab_Specs,
                                    begin
                                        Result = case Spec of
                                                     {Tab, Type}       -> ?TM:create_dedicated(Tab, Type);
                                                     {Tab, Type, Size} -> ?TM:create_dedicated(Tab, Type, Size)
                                                 end,
                                        true
                                    end],
    [unlimited, 10, unlimited] = [?TM:capacity_dedicated(Buffer) || Buffer <- [Tab1, Tab2, Tab3]],
    [0, 0, not_supported] = [?TM:num_entries_dedicated(Buffer) || Buffer <- [Tab1, Tab2, Tab3]],

    Data1 = [
             {Tab1, {red,        2}}, {Tab1, {golden,   1}},
             {Tab2, {macintosh, 34}}, {Tab2, {winesap, 18}}, {Tab2, {granny, 8}},
             {Tab3, {purple,     5}}, {Tab3, {hybrid,   7}}
           ],
    Exp1 = [1,2, 1,2,3, true,true],
    Exp1 = [?TM:write_dedicated(Buffer_Name, Buffer_Data) || {Buffer_Name, Buffer_Data} <- Data1],

    [2, 3, not_supported] = [?TM:num_entries_dedicated(Buffer) || Buffer <- [Tab1, Tab2, Tab3]],
    [{red,        2}, {golden, 1}] = ?TM:read_all_dedicated(Tab1),
    [{macintosh, 34}, {winesap, 18}, {granny, 8}] = ?TM:read_all_dedicated(Tab2),
    not_supported = ?TM:read_all_dedicated(Tab3),
    [0, 0, not_supported] = [?TM:num_entries_dedicated(Buffer) || Buffer <- [Tab1, Tab2, Tab3]],

    Data2 = [
              {Tab1, {green,  3}}, {Tab1, {orange, 8}},
              {Tab3, {bumpy, 11}}, {Tab3, {smooth, 4}}
            ],
    Exp2 = [1,2, true,true],
    Exp2 = [?TM:write_dedicated(Buffer_Name, Buffer_Data) || {Buffer_Name, Buffer_Data} <- Data2],

    [2, 0, not_supported] = [?TM:num_entries_dedicated(Buffer) || Buffer <- [Tab1, Tab2, Tab3]],
    [{green, 3}, {orange, 8}] = ?TM:read_all_dedicated(Tab1),
    [] = ?TM:read_all_dedicated(Tab2),
    [0, 0, not_supported] = [?TM:num_entries_dedicated(Buffer) || Buffer <- [Tab1, Tab2, Tab3]],

    [true, true, true] = [?TM:delete_dedicated(Tab) || Tab <- [Tab1, Tab2, Tab3]],
    ok.
