{alias, dk_cxy, "./dk_cxy/"}.
{include, ["../include"]}.
{logdir, "./dk_cxy/logs/"}.
{cover, "./dk_cxy.coverspec"}.
{suites, dk_cxy, [ets_buffer_SUITE, cxy_ctl_SUITE]}.
