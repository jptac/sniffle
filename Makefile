REBAR = $(shell pwd)/rebar3
ELVIS = $(shell pwd)/elvis
APP=sniffle

.PHONY: rel stagedevrel package version all tree

all: version_header compile

include fifo.mk

version:
	@echo "$(shell git symbolic-ref HEAD 2> /dev/null | cut -b 12-)-$(shell git log --pretty=format:'%h, %ad' -1)" > sniffle.version

version_header: version
	@echo "-define(VERSION, <<\"$(shell cat sniffle.version)\">>)." > apps/sniffle_version/include/sniffle_version.hrl

clean:
	$(REBAR) clean
	$(MAKE) -C rel/pkg clean

long-test:
	$(REBAR) as eqc,long eunit

rel: update
	$(REBAR) as prod compile
	sh generate_zabbix_template.sh
	$(REBAR) as prod release

package: rel
	$(MAKE) -C rel/pkg package
