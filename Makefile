PROJECT = epocxy

CT_SUITES = ets_buffer cxy_ctl cxy_cache
TEST_ERLC_OPTS := -I include/

include erlang.mk
