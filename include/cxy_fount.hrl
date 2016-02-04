-define(START_FOUNT_PID(__Fount, __Fun),
        start_pid(__Fount) -> spawn(fun() -> link(Fount), __Fun() end)).

-define(START_FOUNT_PID_WITH_ARGS(__Fount, __Fun, __Args),
        start_pid(__Fount) -> spawn(fun() -> link(Fount), apply(__Fun, __Args) end).

-spec start_pid (cxy_fount:fount_ref()) -> pid()  | {error, Reason::any()}.
-spec send_msg  (Worker, any())         -> Worker | {error, Reason::any()}
                                               when Worker :: pid().
