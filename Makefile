PROJECT = epocxy
V = 0

DEPS = proper
dep_proper = git https://github.com/manopapad/proper master

ERLC_OPTS := +debug_info +"{cover_enabled, true}"
TEST_ERLC_OPTS := -I include/ $(ERLC_OPTS)

CT_OPTS := -cover test/epocxy.coverspec
CT_SUITES = ets_buffer cxy_ctl cxy_cache

DIALYZER_OPTS := -I include test/epocxy -Werror_handling -Wrace_conditions -Wunmatched_returns

include erlang.mk
