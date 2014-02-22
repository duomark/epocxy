-module(rabbit_obj).
-auth('jay@duomark.com').
-vsn('').

-behaviour(cxy_cache).

-export([create_key_value/1]).

-spec create_key_value(term()) -> term().
create_key_value(Key) -> new_rabbit(Key).

new_rabbit(Name) -> {rabbit, Name}.
