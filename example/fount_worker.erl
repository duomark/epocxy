-module(fount_worker).

-behaviour(cxy_fount).

-export([start_pid/1, send_msg/3]).

-spec start_pid(cxy_fount:fount_ref()) -> pid().
start_pid(Fount_Ref) ->
    Pid = spawn(fun() -> receive {From, hello} -> From ! goodbye
                         end
                end),
    link(Fount_Ref),
    Pid.

-spec send_msg(cxy_fount:fount_ref(), module, tuple()) -> pid().
send_msg(_Fount_Ref, _Module, _Tuple) ->
    spawn_link(fun() -> receive {From, hello} -> From ! goodbye
                        after 3000 -> timeout
                        end
               end).
