%%%------------------------------------------------------------------------------
%%% @copyright (c) 2015, DuoMark International, Inc.
%%% @author Jay Nelson <jay@duomark.com>
%%% @reference 2015 Development sponsored by TigerText, Inc. [http://tigertext.com/]
%%% @reference The license is based on the template for Modified BSD from
%%%   <a href="http://opensource.org/licenses/BSD-3-Clause">OSI</a>
%%% @doc
%%%   Validation of batch_feeder using common test and PropEr.
%%%
%%% @since 0.9.9
%%% @end
%%%------------------------------------------------------------------------------
-module(batch_feeder_SUITE).
-auth('jay@duomark.com').
-vsn('').

-behaviour(batch_feeder).

-export([first_batch/1, prep_batch/3, exec_batch/3]).

%%% Common_test exports
-export([all/0,
         init_per_suite/1,    end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2
        ]).

%%% Test case exports
-export([check_processing/1]).

-include("epocxy_common_test.hrl").


%%%===================================================================
%%% Test cases
%%%===================================================================

-type test_case()  :: atom().
-type test_group() :: atom().

-spec all() -> [test_case() | {group, test_group()}].
all() -> [check_processing].

-type config() :: proplists:proplist().
-spec init_per_suite (config()) -> config().
-spec end_per_suite  (config()) -> config().

init_per_suite (Config) -> Config.
end_per_suite  (Config)  -> Config.

-spec init_per_testcase (atom(), config()) -> config().
-spec end_per_testcase  (atom(), config()) -> config().

init_per_testcase (_Test_Case, Config) -> Config.
end_per_testcase  (_Test_Case, Config) -> Config.

%% Test Modules is ?TM
-define(TM, batch_feeder).

%%%===================================================================
%%% check_processing/1
%%%===================================================================
-spec check_processing(config()) -> ok.
check_processing(_Config) ->
    Test_Start = "Check that continuation-based processing visits each step",
    ct:comment(Test_Start), ct:log(Test_Start),
    Test_Fn
        = ?FORALL({Num_Ids, Batch_Size}, {range(10,100), range(1,10)},
                  begin
                      Ids   =  {all_ids, lists:seq(1, Num_Ids)},
                      Props = [{sum, 0}, {batch_size, Batch_Size}, {collector, self()}, Ids],
                      ct:log("Testing with context ~p", [Props]),

                      done  = ?TM:process_data({?MODULE, Props}),
                      %% ct:log("Message queue:~n~p~n", [process_info(self(), messages)]),
                      Iters = round((Num_Ids / Batch_Size) + 0.49),
                      receive_processed(Iters, Batch_Size, Batch_Size, 1, Num_Ids)
                  end),
    true = proper:quickcheck(Test_Fn, ?PQ_NUM(5)),

    Test_Complete = "Continuation functions worked",
    ct:comment(Test_Complete), ct:log(Test_Complete),
    ok.

receive_processed(    0,           _,         _,    _, Max_Pos) -> receive_sum(Max_Pos);
receive_processed(Iters,    Num_Msgs,  Curr_Msg,  Pos, Max_Pos) ->
    ct:log("NM: ~p  I: ~p   CM: ~p  P: ~p", [Num_Msgs, Iters, Curr_Msg, Pos]),
    receive
        Msg -> 
            {processed, Iteration, {Iteration, Pos}} = Msg,
            New_Pos = Pos + 1,
            case {Curr_Msg - 1, New_Pos =< Max_Pos} of
                {0,     _} -> Pos > 1 andalso New_Pos < Max_Pos andalso receive_sum(Pos),
                              receive_processed(Iters-1, Num_Msgs, Num_Msgs, New_Pos, Max_Pos);
                {_, false} -> receive_processed(Iters-1, Num_Msgs, Num_Msgs, New_Pos, Max_Pos);
                {N,  true} -> receive_processed(Iters,   Num_Msgs,        N, New_Pos, Max_Pos)
            end
    after 100 -> timeout
    end.

receive_sum(High_Water) ->
    Sum = lists:sum(lists:seq(1, High_Water)),
    receive Msg -> ct:log("Received sum message ~p", [Msg]),
                   {sum, Sum} = Msg, ct:log("Received sum ~p", [Sum]), true
    after 100   -> ct:log("Timeout waiting for sum ~p", [High_Water]), timeout
    end.


%%%===================================================================
%%% batch_feeder behaviour implementation
%%%===================================================================

%%% This behaviour implementation uses {?MODULE, proplists:proplist()}
%%% to define the context of batch generation. The initial set of
%%% integers is a list contained on the 'all_ids' property. A 2nd
%%% property 'batch_size' determines how many integers to slice off
%%% for each batch. In this implementation, the batch_size never
%%% varies.

%%% prep_batch pairs {Batch_Num, Id} and adds a sum of Ids seen
%%% to the proplist context pushing it on the front to shadow
%%% older sums (but leaving them there for debugging inspection
%%% if necessary for the test suite).

%%% exec_batch wraps {processed, Batch_Num, Elem} where Elem is
%%% the pair from prep_batch. It takes a timestamp and puts that
%%% on the context and messages the sum to a collector.

%%% This is only a demonstration to show how side-effects can
%%% be maintained in the context, and concurrency or messaging
%%% can be embedded into the iteration phases.

first_batch({_Module, Env} = Context) ->
    Num_Items     = proplists:get_value(batch_size, Env),
    All_Ids       = proplists:get_value(all_ids,    Env),
    {Batch, Rest} = ?TM:get_next_batch_list(Num_Items, All_Ids),
    {{Batch, Context}, make_continuation_fn(Rest)}.

prep_batch(Iteration, Batch, {Module, Env} = _Context) ->
    Sum = lists:sum(Batch) + proplists:get_value(sum, Env),
    {[{Iteration, Elem} || Elem <- Batch], {Module, [{sum, Sum} | Env]}}.

exec_batch(Iteration, Batch, {Module, Env} = _Context) ->
    Pid = proplists:get_value(collector, Env),
    [Pid ! {processed, Iteration, Elem} || Elem <- Batch],
    {Module, New_Env} = New_Context = {Module, [{timestamp, os:timestamp()} | Env]},
    New_Sum = proplists:get_value(sum, New_Env),
    ct:log("Context: ~p  Sum: ~p  Processing: ~p", [New_Context, New_Sum, Batch]),
    Pid ! {sum, New_Sum},
    {ok, New_Context}.


%%%------------------------------------------------------------------------------
%%% Support functions
%%%------------------------------------------------------------------------------

make_continuation_fn([]) ->
    fun(_Iteration, _Context) -> done end;
make_continuation_fn(Batch_Remaining) ->
    fun(_Iteration, {_Module, Env} = Context) ->
            Num_Items          = proplists:get_value(batch_size, Env),
            {Next_Batch, More} = ?TM:get_next_batch_list(Num_Items, Batch_Remaining),
            {{Next_Batch, Context}, make_continuation_fn(More)}
    end.
