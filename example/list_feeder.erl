%%%------------------------------------------------------------------------------
%%% @copyright (c) 2015, DuoMark International, Inc.
%%% @author Jay Nelson <jay@duomark.com>
%%% @reference 2015 Development sponsored by TigerText, Inc. [http://tigertext.com/]
%%% @reference The license is based on the template for Modified BSD from
%%%   <a href="http://opensource.org/licenses/BSD-3-Clause">OSI</a>
%%% @doc
%%%   An example of a batch_feeder behaviour implementation to process a
%%%   list of integers.
%%% @since 0.9.9
%%% @end
%%%------------------------------------------------------------------------------
-module(list_feeder).
-author('Jay Nelson <jay@duomark.com>').

-behaviour(batch_feeder).

-export([
         first_batch/1,
         prep_batch/3,
         exec_batch/3
        ]).

%%% Localized narrower batch_feeder dialyzer datatypes.
-type context()        :: {module(), batch_feeder:context(proplists:proplist())}.
-type batch_type()     :: [pos_integer()].
-type batch_chunk()    :: batch_feeder:batch_chunk(batch_type()).
-type batch_continue() :: batch_feeder:batch_continue(batch_type(), context()).
-type batch_done()     :: batch_done().
-type thunk_return()   :: batch_feeder:thunk_return(batch_type()).

-spec first_batch(context())               -> thunk_return().
-spec prep_batch(pos_integer(), batch_chunk(), context()) -> batch_continue().
-spec exec_batch(pos_integer(), batch_chunk(), context()) -> {ok, context()} | {error, any()}.


%%% Model implementation details...
all_ids()         ->  [1,2,3,4,5,6,7,8,9,10].
batch_size_prop() -> batch_size.


%%% batch_feeder behaviour implementation.
first_batch({_Module, Env} = Context) ->
    Num_Items     = proplists:get_value(batch_size_prop(), Env),
    {Batch, Rest} = batch_feeder:get_next_batch_list(Num_Items, all_ids()),
    {{Batch, Context}, make_continuation_fn(Rest)}.

prep_batch(Iteration, Batch, Context) ->
    {[{{pass, Iteration}, Elem} || Elem <- Batch], Context}.

exec_batch(Iteration, Batch, Context) ->
    _ = [io:format("Iteration ~p: ~p~n", [Iteration, {processed, Elem}]) || Elem <- Batch],
    {ok, Context}.


%%%------------------------------------------------------------------------------
%%% Support functions
%%%------------------------------------------------------------------------------

make_continuation_fn([]) ->
    fun(_Iteration, _Context) -> done end;
make_continuation_fn(Batch_Remaining) ->
    fun(_Iteration, {_Module, Env} = Context) ->
            Num_Items          = proplists:get_value(batch_size_prop(), Env),
            {Next_Batch, More} = batch_feeder:get_next_batch_list(Num_Items, Batch_Remaining),
            {{Next_Batch, Context}, make_continuation_fn(More)}
    end.

get_next_batch(Num_Items_Per_Batch, Items) ->
    case length(Items) of
        N when N =< Num_Items_Per_Batch ->
            {Items, []};
        Len ->
            lists:split(lists:min([Num_Items_Per_Batch, Len]), Items)
    end.
