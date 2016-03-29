REBAR = $(shell which rebar3 || echo ./rebar3)

## Common variables
CONFIG ?= test/test.config
DEFAULT_PATH = ./_build/default
DEFAULT_BUILD_PATH = $(DEFAULT_PATH)/lib/*/ebin

## CT
CT_PATH = ./_build/test
CT_BUILD_PATH = $(CT_PATH)/lib/*/ebin
CT_SUITES = task_SUITE local_SUITE dist_SUITE
CT_OPTS = -cover test/cover.spec -erl_args -config ${CONFIG}

.PHONY: all compile clean distclean dialyze tests shell doc

all: compile

compile:
	$(REBAR) compile

clean:
	rm -rf ebin/* test/*.beam logs log
	$(REBAR) clean

distclean: clean
	$(REBAR) clean --all
	rm -rf _build logs log edoc *.dump c_src/*.o priv/*.so

dialyze:
	$(REBAR) dialyzer

tests:
	$(REBAR) as test compile
	mkdir -p $(CT_PATH)/logs
	ct_run -dir test -suite $(CT_SUITES) -pa $(CT_BUILD_PATH) -logdir $(CT_PATH)/logs $(CT_OPTS)
	rm -rf test/*.beam

shell: compile
	erl -pa $(DEFAULT_BUILD_PATH) -s shards -config ${CONFIG}

edoc:
	$(REBAR) edoc
