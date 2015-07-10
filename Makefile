REBAR = $(shell pwd)/rebar3

.PHONY: rel stagedevrel package version all


all: cp-hooks compile

cp-hooks:
	cp hooks/* .git/hooks

version:
	@echo "$(shell git symbolic-ref HEAD 2> /dev/null | cut -b 12-)-$(shell git log --pretty=format:'%h, %ad' -1)" > sniffle.version

version_header: version
	@echo "-define(VERSION, <<\"$(shell cat sniffle.version)\">>)." > apps/sniffle_version/include/sniffle_version.hrl

compile:
	$(REBAR) compile
clean:
	$(REBAR) clean
	make -C rel/pkg clean

long-test:
	$(REBAR) eunit -DEQC_LONG_TESTS -v
eunit: 
	$(REBAR) compile
	$(REBAR) eunit -v

test: eunit
	$(REBAR) xref

quick-xref:
	$(REBAR) xref

quick-test:
	$(REBAR) -DEQC_SHORT_TEST eunit -v

rel:
	$(REBAR) as prod compile
	sh generate_zabbix_template.sh
	$(REBAR) release

package:
	make -C rel/pkg package

###
### Docs
###
docs:
	$(REBAR) doc

##
## Developer targets
##

xref: all
	$(REBAR) xref

##
## Dialyzer
##
APPS = kernel stdlib sasl erts ssl tools os_mon runtime_tools crypto inets \
       xmerl webtool snmp public_key mnesia eunit syntax_tools compiler edoc

COMBO_PLT = $(HOME)/.sniffle_combo_dialyzer_plt

# DIALYZER_IGNORE="^\(riak_core\|leexinc.hrl\|pokemon_pb.erl\|meck_cover.erl\|meck.erl\|supervisor_pre_r14b04.erl\|webmachine_resource.erl\|uuid.erl\|gen_server2.erl\|folsom_vm_metrics.erl\|protobuffs_compile.erl\)"

check_plt: deps compile
	dialyzer --check_plt --plt $(COMBO_PLT) --apps $(APPS) \
		deps/*/ebin apps/*/ebin

build_plt: deps compile
	dialyzer --build_plt --output_plt $(COMBO_PLT) --apps $(APPS) \
		deps/*/ebin apps/*/ebin

dialyzer: deps compile
	@echo
	@echo Use "'make check_plt'" to check PLT prior to using this target.
	@echo Use "'make build_plt'" to build PLT prior to using this target.
	@echo
	@sleep 1
	dialyzer -Wno_return --plt $(COMBO_PLT) deps/*/ebin apps/*/ebin | grep -v -f dialyzer.mittigate

dialyzer-gui: deps compile
	dialyzer --gui -Wno_return --plt $(COMBO_PLT) deps/*/ebin apps/*/ebin
typer:
	typer --plt $(COMBO_PLT) deps/*/ebin apps/*/ebin

cleanplt:
	@echo
	@echo "Are you sure?  It takes about 1/2 hour to re-build."
	@echo Deleting $(COMBO_PLT) in 5 seconds.
	@echo
	sleep 5
	rm $(COMBO_PLT)

tags:
	find . -name "*.[he]rl" -print | etags -
