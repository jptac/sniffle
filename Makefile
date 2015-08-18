REBAR = $(shell pwd)/rebar3

.PHONY: rel stagedevrel package version all tree

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
	$(REBAR) as eqc,long eunit 

eunit: 
	$(REBAR) compile
	$(REBAR) eunit -v

test: eunit
	$(REBAR) xref

quick-xref:
	$(REBAR) xref

quick-test:
	$(REBAR) as eqc,short eunit -v

update:
	$(REBAR) update

rel: update
	$(REBAR) as prod compile
	sh generate_zabbix_template.sh
	$(REBAR) as prod release

package: rel
	make -C rel/pkg package

###
### Docs
###
docs:
	$(REBAR) edoc

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

# DIALYZER_IGNORE="^\(riak_core\|leexinc.hrl\|pokemon_pb.erl\|meck_cover.erl\|meck.erl\|supervisor_pre_r14b04.erl\|webmachine_resource.erl\|uuid.erl\|gen_server2.erl\|folsom_vm_metrics.erl\|protobuffs_compile.erl\)"

dialyzer: deps compile
	$(REBAR) dialyzer -Wno_return | grep -v -f dialyzer.mittigate

typer:
	typer --plt ./_build/default/rebar3_*_plt _build/default/lib/*/ebin

tree:
	rebar3 tree | grep '|' | sed 's/ (.*//' > tree
