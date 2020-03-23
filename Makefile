REBAR = $(shell which rebar3)

EPMD_PROC_NUM = $(shell ps -ef | grep epmd | grep -v "grep")

LOCAL_SUITES = "test/shards_local_SUITE,test/shards_state_SUITE,test/shards_lib_SUITE,test/shards_task_SUITE"

.PHONY: all check_rebar compile clean distclean dialyzer test shell doc

all: check_rebar compile

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

xref: check_rebar
	$(REBAR) xref

ci: check_rebar check_epmd check_plt xref
	$(REBAR) do ct, cover

test: check_rebar check_epmd check_plt
	$(REBAR) do ct, cover

local_test: check_rebar check_epmd
	$(REBAR) do ct --suite=$(LOCAL_SUITES), cover

dist_test: check_rebar check_epmd
	$(REBAR) do ct --suite=test/shards_dist_SUITE, cover

shell: check_rebar
	$(REBAR) shell

doc: check_rebar
	$(REBAR) edoc

check_rebar:
ifeq ($(REBAR),)
ifeq ($(wildcard rebar3),)
	$(call get_rebar)
else
	$(eval REBAR=./rebar3)
endif
endif

check_plt:
ifeq (,$(wildcard ./*_plt))
	@echo " ---> Running dialyzer ..."
	$(REBAR) dialyzer
endif

check_epmd:
ifeq ($(EPMD_PROC_NUM),)
	epmd -daemon
	@echo " ---> Started epmd!"
endif

define get_rebar
	curl -O https://s3.amazonaws.com/rebar3/rebar3
	chmod a+x rebar3
	./rebar3 update
	$(eval REBAR=./rebar3)
endef
