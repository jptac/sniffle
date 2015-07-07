VERSION=0.6.2
COMPONENT_INTERNAL=sniffle
COMPONENT=fifo-sniffle
DEPS="erlang>=16.1.2" "coreutils>=8.22nb1" "sudo>=1.7.10p9"

include ../../deps/fifo_utils/priv/pkg.mk

.PHONY: package prepare clean

prepare:
	-rm -r $(STAGE_DIR)/$(COMPONENT)
	cp -r ../../_build/default/rel/$(COMPONENT_INTERNAL) $(STAGE_DIR)/$(COMPONENT)

package: prepare $(FILE).tgz

clean: clean-pkg
	-rm *.tgz
