REBAR = $(shell which rebar3)

EPMD_PROC_NUM = $(shell ps -ef | grep epmd | grep -v "grep")

.PHONY: all check_rebar compile clean distclean dialyzer test shell doc

all: check_rebar compile

check_rebar:
ifeq ($(REBAR),)
ifeq ($(wildcard rebar3),)
	curl -O https://s3.amazonaws.com/rebar3/rebar3
	chmod a+x rebar3
	./rebar3 update
	$(eval REBAR=./rebar3)
else
	$(eval REBAR=./rebar3)
endif
endif

compile: check_rebar
	$(REBAR) compile

clean: check_rebar
	rm -rf ebin/* test/*.beam logs log
	$(REBAR) clean

distclean: clean
	$(REBAR) clean --all
	rm -rf _build logs log doc *.dump *_plt *.crashdump priv

dialyzer: check_rebar
	$(REBAR) dialyzer

check_epmd:
ifeq ($(EPMD_PROC_NUM),)
	epmd&
	@echo " ---> Started epmd!"
endif

test: check_rebar check_epmd
	$(REBAR) do ct --name ct@127.0.0.1, cover
	rm -rf test/*.beam

local_test: check_rebar check_epmd
	$(REBAR) do ct --suite=test/shards_local_SUITE,test/shards_state_SUITE,test/shards_lib_SUITE,test/shards_task_SUITE, cover
	rm -rf test/*.beam

dist_test: check_rebar check_epmd
	$(REBAR) do ct --name ct@127.0.0.1 --suite=test/shards_dist_SUITE, cover
	rm -rf test/*.beam

shell: check_rebar
	$(REBAR) shell

doc: check_rebar
	$(REBAR) edoc
