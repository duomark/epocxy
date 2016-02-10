-spec start_pid (cxy_fount:fount_ref()) -> pid()  | {error, Reason::any()}.
-spec send_msg  (Worker, any())         -> Worker | {error, Reason::any()}
                                               when Worker :: pid().
