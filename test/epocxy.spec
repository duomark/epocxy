{alias, epocxy, "./epocxy/"}.
{include, ["../include"]}.
{logdir, "./epocxy/logs/"}.
{cover, "./epocxy.coverspec"}.
{suites, epocxy, [
                  ets_buffer_SUITE,
                  cxy_ctl_SUITE,
                  cxy_cache_SUITE
                 ]}.
