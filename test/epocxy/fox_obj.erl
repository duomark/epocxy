-module(fox_obj).
-auth('jay@duomark.com').
-vsn('').

-behaviour(cxy_cache).

-export([create_key_value/1]).

-spec create_key_value(cxy_cache:cached_key()) -> {cxy_cache:cached_vsn(), cxy_cache:cached_value()}.
create_key_value(Key) -> {erlang:now(), new_fox(Key)}.

new_fox(Name) -> {fox, Name}.

