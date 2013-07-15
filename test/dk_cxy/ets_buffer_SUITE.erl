-module(ets_buffer_SUITE).
-auth('jay@duomark.com').
-vsn('').

-export([all/0, init_per_suite/1, end_per_suite/1]).
-export([
         check_shared_create/1,  check_dedicated_create/1,
         check_shared_history/1, check_dedicated_history/1,
         check_shared_clear/1,   check_dedicated_clear/1,
         check_shared_read/1,    check_dedicated_read/1
        ]).

-include_lib("common_test/include/ct.hrl").

-spec all() -> [atom()].

all() -> [
          check_shared_create,  check_dedicated_create,
          check_shared_history, check_dedicated_history,
          check_shared_clear,   check_dedicated_clear,
          check_shared_read,    check_dedicated_read
         ].

-type config() :: proplists:proplist().
-spec init_per_suite(config()) -> config().
-spec end_per_suite(config()) -> config().

init_per_suite(Config) -> Config.
end_per_suite(Config)  -> Config.

%% Test Modules is ?TM
-define(TM, ets_buffer).

check_shared_create(_Config) ->
    Tab1 = ets_tests,
    Tab2 = batches,
    Attrs = [name, size, type, write_loc, read_loc],
    [] = ?TM:list(),
    undefined = ets:info(?TM, name),
    ?TM = ?TM:create(Tab1, ring, 20),
    ?TM = ets:info(?TM, name),
    [[Tab1, 20, ring, 0, 0]]
        = [[proplists:get_value(P, Props) || P <- Attrs]
           || Props <- ?TM:list()],
    ?TM = ?TM:create(Tab2, fifo),
    [[Tab2, 0, fifo, 0, 0], [Tab1, 20, ring, 0, 0]]
        = [[proplists:get_value(P, Props) || P <- Attrs]
           || Props <- ?TM:list()],
    
    Props1 = ?TM:list(Tab1),
    [Tab1, 20, ring, 0, 0] = [proplists:get_value(P, Props1) || P <- Attrs],
    Props2 = ?TM:list(Tab2),
    [Tab2,  0, fifo, 0, 0] = [proplists:get_value(P, Props2) || P <- Attrs],

    true = ?TM:delete(Tab1),
    true = ?TM:delete(Tab2),
    [?TM, 0] = [ets:info(?TM, Prop) || Prop <- [name, size]],
    ok.

check_dedicated_create(_Config) ->
    Tab1 = ets_tests,
    Tab2 = batches,
    Attrs = [name, size, type, write_loc, read_loc],
    [] = ?TM:list(),
    undefined = ets:info(?TM, name),
    undefined = ets:info(Tab1, name),
    Tab1 = ?TM:create_dedicated(Tab1, ring, 20),
    undefined = ets:info(?TM, name),
    Tab1 = ets:info(Tab1, name),
    Props1 = ?TM:list_dedicated(Tab1),
    [Tab1, 20, ring, 0, 0]
        = [proplists:get_value(P, Props1) || P <- Attrs],
    Tab2 = ?TM:create_dedicated(Tab2, fifo),
    Props2 = ?TM:list_dedicated(Tab2),
    [Tab2, 0, fifo, 0, 0]
        = [proplists:get_value(P, Props2) || P <- Attrs],

    true = ?TM:delete_dedicated(Tab1),
    true = ?TM:delete_dedicated(Tab2),
    undefined = ets:info(Tab1, name),
    undefined = ets:info(Tab2, name),
    ok.

check_shared_history(_Config) ->    
    Tab_Specs = [{Tab1, _Type1}, {Tab2, _Type2, _Size2}, {Tab3, _Type3}]
        = [{beets, fifo}, {apples, ring, 10}, {carrots, lifo}],
    ?TM = ?TM:create(Tab_Specs),
    Data1 = [
            {Tab1, {red, 2}},        {Tab1, {golden, 1}},
            {Tab2, {macintosh, 34}}, {Tab2, {winesap, 18}}, {Tab2, {granny, 8}},
            {Tab3, {purple, 5}},     {Tab3, {hybrid, 7}}
           ],
    Exp1 = lists:duplicate(length(Data1), true),
    Exp1 = [?TM:write(Buffer_Name, Buffer_Data) || {Buffer_Name, Buffer_Data} <- Data1],
    [?TM, 10] = [ets:info(?TM, Prop) || Prop <- [name, size]],  %% 3 meta + 7 data

    [{red, 2},        {golden, 1}               ] = ?TM:history(Tab1),
    [{macintosh, 34}, {winesap, 18}, {granny, 8}] = ?TM:history(Tab2),
    [{hybrid, 7},     {purple, 5}               ] = ?TM:history(Tab3),

    Data2 = [
             {Tab1, {green,  3}}, {Tab1, {orange, 8}},
             {Tab3, {bumpy, 11}}, {Tab3, {smooth, 4}}
             ],
    Exp2 = lists:duplicate(length(Data2), true),
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
    Exp1 = lists:duplicate(length(Data1), true),
    Exp1 = [?TM:write_dedicated(Buffer_Name, Buffer_Data) || {Buffer_Name, Buffer_Data} <- Data1],

    [{red, 2},        {golden, 1}               ] = ?TM:history_dedicated(Tab1),
    [{macintosh, 34}, {winesap, 18}, {granny, 8}] = ?TM:history_dedicated(Tab2),
    [{hybrid, 7},     {purple, 5}               ] = ?TM:history_dedicated(Tab3),

    Data2 = [
             {Tab1, {green,  3}}, {Tab1, {orange, 8}},
             {Tab3, {bumpy, 11}}, {Tab3, {smooth, 4}}
             ],
    Exp2 = lists:duplicate(length(Data2), true),
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

check_shared_clear(_Config) ->
    Attrs = [name, size, type, write_loc, read_loc],
    Tab_Specs = [{Tab1, Type1}, {Tab2, Type2, Size2}, {Tab3, Type3}]
        = [{beets, fifo}, {apples, ring, 10}, {carrots, lifo}],
    [] = ?TM:list(),
    [undefined, undefined, undefined, undefined]
        = [ets:info(Tab, name) || Tab <- [?TM] ++ [element(1,Spec) || Spec <- Tab_Specs]],
    ?TM = ?TM:create(Tab_Specs),
    [?TM, undefined, undefined, undefined]
        = [ets:info(Tab, name) || Tab <- [?TM] ++ [element(1,Spec) || Spec <- Tab_Specs]],

    %% Entries are sorted by table name.
    [[Tab2, Size2, Type2, 0, 0], [Tab1, 0, Type1, 0, 0], [Tab3, 0, Type3, 0, 0]]
        = [[proplists:get_value(P, Props) || P <- Attrs]
           || Props <- ?TM:list()],

    Data = [{Tab1, {red, 2}}, {Tab1, {golden, 1}}, {Tab2, {macintosh, 34}}, {Tab3, {purple, 5}}],
    [true, true, true, true] = [?TM:write(Buffer_Name, Buffer_Data) || {Buffer_Name, Buffer_Data} <- Data],
    [?TM, 7] = [ets:info(?TM, Prop) || Prop <- [name, size]],  %% 3 meta + 4 data

    %% Entries are sorted by table name.
    [[Tab2, Size2, Type2, 1, 0], [Tab1, 0, Type1, 2, 0], [Tab3, 0, Type3, -1, -1]]
        = [[proplists:get_value(P, Props) || P <- Attrs]
           || Props <- ?TM:list()],
    
    [true, true, true] = [?TM:clear(Tab) || Tab <- [Tab1, Tab2, Tab3]],
    [?TM, 3] = [ets:info(?TM, Prop) || Prop <- [name, size]],  %% 3 meta
    [true, true, true] = [?TM:delete(Tab) || Tab <- [Tab1, Tab2, Tab3]],
    [?TM, 0] = [ets:info(?TM, Prop) || Prop <- [name, size]],  %% no meta
    ok.

check_dedicated_clear(_Config) ->
    Attrs = [name, size, type, write_loc, read_loc],
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

    [[Tab1, 0, Type1, 0, 0], [Tab2, Size2, Type2, 0, 0], [Tab3, 0, Type3, 0, 0]]
        = [[proplists:get_value(P, Props) || P <- Attrs]
           || Props <- [?TM:list_dedicated(T) || T <- [Tab1, Tab2, Tab3]]],

    Data = [{Tab1, {red, 2}}, {Tab1, {golden, 1}}, {Tab2, {macintosh, 34}}, {Tab3, {purple, 5}}],
    [true, true, true, true] = [?TM:write_dedicated(Buffer_Name, Buffer_Data) || {Buffer_Name, Buffer_Data} <- Data],
    [[undefined, undefined], [Tab1, 3], [Tab2, 2], [Tab3, 2]]
        = [[ets:info(Tab, Prop) || Prop <- [name, size]] || Tab <- [?TM, Tab1, Tab2, Tab3]],

    [[Tab1, 0, Type1, 2, 0], [Tab2, Size2, Type2, 1, 0], [Tab3, 0, Type3, -1, -1]]
        = [[proplists:get_value(P, Props) || P <- Attrs]
           || Props <- [?TM:list_dedicated(T) || T <- [Tab1, Tab2, Tab3]]],
    
    [true, true, true] = [?TM:clear_dedicated(Tab) || Tab <- [Tab1, Tab2, Tab3]],
    [[undefined, undefined], [Tab1, 1], [Tab2, 1], [Tab3, 1]]
        = [[ets:info(Tab, Prop) || Prop <- [name, size]] || Tab <- [?TM, Tab1, Tab2, Tab3]],

    [true, true, true] = [?TM:delete_dedicated(Tab) || Tab <- [Tab1, Tab2, Tab3]],
    [undefined, undefined, undefined, undefined] = [ets:info(Tab, name) || Tab <- [?TM, Tab1, Tab2, Tab3]],
    ok.

check_shared_read(_Config) ->    
    Tab_Specs = [{Tab1, _Type1}, {Tab2, _Type2, _Size2}, {Tab3, _Type3}]
        = [{beets, fifo}, {apples, ring, 10}, {carrots, lifo}],
    ?TM = ?TM:create(Tab_Specs),
    Data1 = [
             {Tab1, {red,        2}},
             {Tab2, {macintosh, 34}}, {Tab2, {winesap, 18}}, {Tab2, {granny, 8}},
             {Tab3, {purple,     5}}, {Tab3, {hybrid,   7}}
           ],
    Exp1 = lists:duplicate(length(Data1), true),
    Exp1 = [?TM:write(Buffer_Name, Buffer_Data) || {Buffer_Name, Buffer_Data} <- Data1],
    [?TM, 9] = [ets:info(?TM, Prop) || Prop <- [name, size]],  %% 3 meta + 6 data

    not_supported = ?TM:read(Tab3, 5),

    [{red,        2}] = ?TM:read(Tab1),
    [{macintosh, 34}] = ?TM:read(Tab2),
    [{hybrid,     7}] = ?TM:read(Tab3),

    Data2 = [
             {Tab1, {golden,   1}}, {Tab1, {green,  3}}, {Tab1, {orange, 8}},
             {Tab3, {bumpy, 11}}, {Tab3, {smooth, 4}}
            ],
    Exp2 = lists:duplicate(length(Data2), true),
    Exp2 = [?TM:write(Buffer_Name, Buffer_Data) || {Buffer_Name, Buffer_Data} <- Data2],
    [?TM, 11] = [ets:info(?TM, Prop) || Prop <- [name, size]],  %% 3 meta + 8 remaining data

    [{golden,   1}, {green,  3}] = ?TM:read(Tab1, 2),
    [{winesap, 18}, {granny, 8}] = ?TM:read(Tab2, 2),

    [{smooth,   4}] = ?TM:read(Tab3),
    [{bumpy,   11}] = ?TM:read(Tab3, 1),

    [{orange, 8}] = ?TM:read(Tab1, 5),
    [           ] = ?TM:read(Tab2, 5),
    [{purple, 5}] = ?TM:read(Tab3),

    [true, true, true] = [?TM:delete(Tab) || Tab <- [Tab1, Tab2, Tab3]],
    ok.

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
    Exp1 = lists:duplicate(length(Data1), true),
    Exp1 = [?TM:write_dedicated(Buffer_Name, Buffer_Data) || {Buffer_Name, Buffer_Data} <- Data1],

    not_supported = ?TM:read_dedicated(Tab3, 5),

    [{red,        2}] = ?TM:read_dedicated(Tab1),
    [{macintosh, 34}] = ?TM:read_dedicated(Tab2),
    [{hybrid,     7}] = ?TM:read_dedicated(Tab3),

    Data2 = [
              {Tab1, {green,  3}}, {Tab1, {orange, 8}},
              {Tab3, {bumpy, 11}}, {Tab3, {smooth, 4}}
            ],
    Exp2 = lists:duplicate(length(Data2), true),
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
