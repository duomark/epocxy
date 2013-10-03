-module(frog_obj).
-auth('jay@duomark.com').
-vsn('').

-behaviour(cxy_cache).

-export([create_key_value/1]).

-spec create_key_value(term()) -> term().
create_key_value(Key) -> new_frog(Key).

new_frog(Name) -> {frog, Name}.

