REBAR = $(shell pwd)/rebar

.PHONY: deps rel stagedevrel package version all

all: deps compile

version:
	echo "$(shell git symbolic-ref HEAD 2> /dev/null | cut -b 12-)-$(shell git log --pretty=format:'%h, %ad' -1)" > sniffle.version

version_header: version
	echo "-define(VERSION, <<\"$(shell cat sniffle.version)\">>)." > apps/sniffle/src/sniffle_version.hrl

compile: version_header
	erlc -oapps/sniffle/priv/mibs/ apps/sniffle/mibs/SNIFFLE-MIB.mib
	$(REBAR) compile

deps:
	$(REBAR) get-deps

clean:
	$(REBAR) clean
	make -C rel/pkg clean

distclean: clean devclean relclean
	$(REBAR) delete-deps

test: xref
	$(REBAR) skip_deps=true eunit

rel: all zabbix
	$(REBAR) generate

relclean:
	rm -rf rel/sniffle

devrel: dev1 dev2 dev3 dev4

package: rel
	make -C rel/pkg package

zabbix:
	./generate_zabbix_template.sh

###
### Docs
###
docs:
	$(REBAR) skip_deps=true doc

##
## Developer targets
##

xref: all
	$(REBAR) xref skip_deps=true

stage : rel
	$(foreach dep,$(wildcard deps/* wildcard apps/*), rm -rf rel/sniffle/lib/$(shell basename $(dep))-* && ln -sf $(abspath $(dep)) rel/sniffle/lib;)


stagedevrel: dev1 dev2 dev3 dev4
	mkdir -p dev/dev{1,2,3}/data/{ipranges,datasets,packages,ring}
	$(foreach dev,$^,\
	  $(foreach dep,$(wildcard deps/* wildcard apps/*), rm -rf dev/$(dev)/lib/$(shell basename $(dep))-* && ln -sf $(abspath $(dep)) dev/$(dev)/lib;))

devrel: dev1 dev2 dev3 dev4


devclean:
	rm -rf dev

dev1 dev2 dev3 dev4: all
	mkdir -p dev
	(cd rel && $(REBAR) generate target_dir=../dev/$@ overlay_vars=vars/$@.config)


##
## Dialyzer
##
APPS = kernel stdlib sasl erts ssl tools os_mon runtime_tools crypto inets \
	xmerl webtool snmp public_key mnesia eunit syntax_tools compiler

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

typer:
	typer --plt $(COMBO_PLT) deps/*/ebin apps/*/ebin

cleanplt:
	@echo
	@echo "Are you sure?  It takes about 1/2 hour to re-build."
	@echo Deleting $(COMBO_PLT) in 5 seconds.
	@echo
	sleep 5
	rm $(COMBO_PLT)
