-module(fox_obj).
-auth('jay@duomark.com').
-vsn('').

-behaviour(cxy_cache).

-export([create_key_value/1]).

-spec create_key_value(term()) -> term().
create_key_value(Key) -> new_fox(Key).

new_fox(Name) -> {fox, Name}.

