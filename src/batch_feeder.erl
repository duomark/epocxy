%%%------------------------------------------------------------------------------
%%% @copyright (c) 2015, DuoMark International, Inc.
%%% @author Jay Nelson <jay@duomark.com>
%%% @reference 2015 Development sponsored by TigerText, Inc. [http://tigertext.com/]
%%% @reference The license is based on the template for Modified BSD from
%%%   <a href="http://opensource.org/licenses/BSD-3-Clause">OSI</a>
%%% @doc
%%%   Batches are often used as a way to increase throughput or to handle a
%%%   large amount of data in bite-size chunks. The key features of this
%%%   module are the ability to control the size and pace of the batches
%%%   and to adapatively modify the amount of processing in each step. A
%%%   common pattern is to pair a batch_feeder with a cxy_fount or cxy_ctl
%%%   to incrementally process a large set of data using concurrency in a
%%%   controlled way to avoid overloading the CPU and memory.
%%% @since 0.9.9
%%% @end
%%%------------------------------------------------------------------------------
-module(batch_feeder).
-author('Jay Nelson <jay@duomark.com>').

-export([process_data/1]).


%% Source of work is consumed using continuation thunks.
%% Each unit of work is handled independently, but the
%% pace from one unit to the next is controlled.

-type iteration()                   :: pos_integer().
-type context(Ctxt)                 :: {module, Ctxt}.
-type batch_chunk(Type)             :: Type.
-type batch_done()                  :: done.
-type batch_continue(Type, Context) :: {batch_chunk(Type), Context} | batch_done().

-type thunk(Type, Context) :: fun((pos_integer(), Context) -> batch_continue(Type, Context)).
-type thunk_return(Type, Context) :: {batch_continue(Type, Context), thunk(Type, Context)}.

-export_type([batch_chunk/1, context/1, batch_continue/2, batch_done/0,
              thunk/2, thunk_return/2]).


%% The first batch is generated from only a contextual configuration. 
%% It returns a batch plus a thunk for the next batch.
-callback first_batch(context(Ctxt1)) ->
    thunk_return(Type, context(Ctxt2))
        when Type::any(), Ctxt1 :: any(), Ctxt2 :: any().

%% %% Future batches return further thunks or 'done'.
%% -callback next_batch(Iteration, thunk(Type, context(Ctxt1)), context(Ctxt1)) ->
%%     batch_continue(Type, context(Ctxt2))
%%         when Iteration :: pos_integer(), Type :: any(), Ctxt1 :: any(), Ctxt2 :: any().

%% Reformat the raw batch in preparation for processing (can be just a noop()).
-callback prep_batch(iteration(), batch_chunk(Type), context(Ctxt1)) ->
    batch_continue(Type, context(Ctxt2))
        when Type :: any(), Ctxt1 :: any(), Ctxt2 :: any().

%% Perform the downstream task on the batch chunk.
-callback exec_batch(iteration(), batch_chunk(Type), context(Ctxt1)) ->
    {ok, context(Ctxt2)} | {error, Reason}
        when Type :: any(), Ctxt1 :: any(), Ctxt2 :: any(), Reason :: any().

-spec process_data(context(Ctxt)) -> done | {error, tuple()} when Ctxt :: any().
process_data({Module, _Env} = Context) ->
    try Module:first_batch(Context) of
        done -> done;
        {{First_Batch, Context2}, Continuation_Fn} ->
            process_batch(1, First_Batch, Context2, Continuation_Fn)
    catch Error:Type -> Args = [?MODULE, Error, Type, erlang:get_stacktrace()],
                        error_logger:error_msg("~p:process_data error {~p,~p}~n~99999p", Args),
                        {error, {first_batch, {1, Error, Type}}}
    end.


%% todo: report progress, elapsed time.
process_batch(Iteration, This_Batch, {Module, _Env} = Context, Continuation_Fn) ->
    try
        {Prepped_Batch, Context2} = Module:prep_batch(Iteration, This_Batch, Context),
        {Reply, Context3} = case Module:exec_batch(Iteration, Prepped_Batch, Context2) of
                                {error, Reason} = Err -> {Err, Context2};
                                {ok, Context2a}       -> {ok, Context2a}
                            end,
        Next_Iteration = Iteration+1,
        case Continuation_Fn(Next_Iteration, Context3) of
            done -> done;
            {{Next_Batch, Context4}, Next_Continuation_Fn} ->
                process_batch(Iteration+1, Next_Batch, Context4, Next_Continuation_Fn)
        end
         
    catch Error:Type -> Args = [?MODULE, Error, Type, erlang:get_stacktrace()],
                        error_logger:error_msg("~p:process_batch error {~p,~p}~n~99999p", Args),
                        {error, {prepped_batch, {Iteration, Error, Type}}}
    end.
