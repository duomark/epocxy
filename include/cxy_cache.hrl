
-record(cxy_cache_meta,
        {
          cache_name                      :: cxy_cache:cache_name(),
          started        = os:timestamp() :: erlang:timestamp(),
          gen1_hit_count = 0              :: cxy_cache:gen1_hit_count(),
          gen2_hit_count = 0              :: cxy_cache:gen2_hit_count(),
          refresh_count  = 0              :: cxy_cache:refresh_count(),
          delete_count   = 0              :: cxy_cache:delete_count(),
          fetch_count    = 0              :: cxy_cache:fetch_count(),
          error_count    = 0              :: cxy_cache:error_count(),
          miss_count     = 0              :: cxy_cache:miss_count(),
          new_gen_time                    :: erlang:timestamp(),
          old_gen_time                    :: erlang:timestamp(),
          new_gen                         :: ets:tid(),
          old_gen                         :: ets:tid(),
          cache_module                    :: module(),
          new_generation_function = none  :: cxy_cache:check_gen_fun(),
          new_generation_thresh   = 0     :: non_neg_integer()
        }).

-record(cxy_cache_value,
        {
          key      :: cxy_cache:cached_key(),
          value    :: cxy_cache:cached_value(),
          version  :: cxy_cache:cached_value_vsn()
        }).

