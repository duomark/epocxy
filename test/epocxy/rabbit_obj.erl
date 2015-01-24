%%%------------------------------------------------------------------------------
%%% @copyright (c) 2013-2015, DuoMark International, Inc.
%%% @author Jay Nelson <jay@duomark.com>
%%% @reference 2013-2015 Development sponsored by TigerText, Inc. [http://tigertext.com/]
%%% @reference The license is based on the template for Modified BSD from
%%%   <a href="http://opensource.org/licenses/BSD-3-Clause">OSI</a>
%%% @doc
%%%   Stub object for testing cxy_cache.
%%%
%%% @since 0.9.6
%%% @end
%%%------------------------------------------------------------------------------
-module(rabbit_obj).
-auth('jay@duomark.com').
-vsn('').

-behaviour(cxy_cache).

-export([create_key_value/1]).

-spec create_key_value(cxy_cache:cached_key()) -> {cxy_cache:cached_value_vsn(), cxy_cache:cached_value()}.
create_key_value(Key) -> {erlang:now(), new_rabbit(Key)}.

new_rabbit(Name) -> {rabbit, Name}.
