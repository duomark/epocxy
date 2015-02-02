{alias, epocxy, "./epocxy/"}.
{include, ["../include"]}.
{logdir, "./epocxy/logs/"}.
{cover, "./epocxy.coverspec"}.
{suites, epocxy, [
                  batch_feeder_SUITE,
                  epocxy_sup_SUITE,
                  epocxy_ets_fsm_SUITE,
                  ets_ring_buffer_ro_SUITE,
                  ets_buffer_SUITE,
                  cxy_ctl_SUITE,
                  cxy_cache_SUITE,
                  cxy_fount_SUITE
                 ]}.
