REBAR = $(shell which rebar3)

EPMD_PROC_NUM = $(shell ps -ef | grep epmd | grep -v "grep")

.PHONY: all compile clean distclean test qc test_suite covertool dialyzer xref check shell docs

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

test: check_rebar
	$(REBAR) do proper, ct, cover

qc: check_rebar
	$(REBAR) do proper

test_suite: check_rebar
	$(REBAR) do ct --suite=test/$(SUITE)_SUITE, cover

covertool: check_rebar
	$(REBAR) as test covertool generate

docs: check_rebar
	$(REBAR) edoc

check: test covertool dialyzer xref docs
	@echo "OK!"

shell: check_rebar
	$(REBAR) shell

check_rebar:
ifeq ($(REBAR),)
ifeq ($(wildcard rebar3),)
	$(call get_rebar)
else
	$(eval REBAR=./rebar3)
endif
endif

define get_rebar
	curl -O https://s3.amazonaws.com/rebar3/rebar3
	chmod a+x rebar3
	./rebar3 update
	$(eval REBAR=./rebar3)
endef
