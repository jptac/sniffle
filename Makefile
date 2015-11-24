REBAR = $(shell pwd)/rebar3
ELVIS = $(shell pwd)/elvis

.PHONY: rel stagedevrel package version all tree

all: version_header compile

include fifo.mk

version:
	@echo "$(shell git symbolic-ref HEAD 2> /dev/null | cut -b 12-)-$(shell git log --pretty=format:'%h, %ad' -1)" > sniffle.version

version_header: version
	@echo "-define(VERSION, <<\"$(shell cat sniffle.version)\">>)." > apps/sniffle_version/include/sniffle_version.hrl

clean:
	$(REBAR) clean
	-rm -rf apps/sniffle/.eunit
	-rm -rf apps/sniffle/ebin/
	make -C rel/pkg clean

long-test:
	$(REBAR) as eqc,long eunit

update:
	$(REBAR) update

rel: update
	$(REBAR) as prod compile
	sh generate_zabbix_template.sh
	$(REBAR) as prod release

package: rel
	make -C rel/pkg package

typer:
	typer --plt ./_build/default/rebar3_*_plt _build/default/lib/*/ebin
