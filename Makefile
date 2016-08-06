PROJECT = epocxy
V = 0

DEPS = proper
## DEPS = proper eper  # when debugging

dep_proper = git https://github.com/manopapad/proper master

ERLC_OPTS := +debug_info
# +\"{cover_enabled, true}\"

TEST_ERLC_OPTS := +debug_info -I include -I test/epocxy
# +\"{cover_enabled, true}\"

CT_OPTS := -cover test/epocxy.coverspec
CT_SUITES = cxy_regulator cxy_fount batch_feeder ets_buffer cxy_ctl cxy_cache

DIALYZER_OPTS := -I include -Werror_handling -Wrace_conditions -Wunmatched_returns

include erlang.mk

run:
	erl -pa ebin -pa deps/*/ebin -smp enable -name epocxy -boot start_sasl
