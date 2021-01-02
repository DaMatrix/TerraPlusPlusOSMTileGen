export SHELL		:=	/bin/bash

export NPROC		:=	$(shell nproc)

export TARGET		:=	$(shell basename $(CURDIR))
export TOPDIR		:=	$(CURDIR)
export TOOLCHAINS	:=	$(CURDIR)/toolchain
export COMMONSRC	:=	$(CURDIR)/src/main/native

#export LDFLAGS		:=	$(CFLAGS) -Wl,--gc-sections
export LDFLAGS		:=	$(CFLAGS) -shared

ifndef NATIVES_DEBUG
#export CFLAGS		:=	-shared -Ofast -ffast-math -fPIC -ffunction-sections -fdata-sections
export CFLAGS		:=	-Ofast -ffast-math -fPIC
export BUILD_TYPE	:=	release
else
export CFLAGS		:=	-fPIC -DNATIVES_DEBUG
export BUILD_TYPE	:=	debug
endif
export CXXFLAGS		:=	$(CFLAGS)
$(info natives: building for $(BUILD_TYPE))

export INCLUDES		:=	$(JAVA_HOME)include $(JAVA_HOME)include/linux

export ARCHS		:=	x86_64-linux-gnu x86_64-w64-mingw32
export ARCH_TASKS	:=	$(foreach arch,$(ARCHS),build.$(arch))

export MODULES		:=	native

export LIB_URL_BASE	:=	https://cloud.daporkchop.net/programs/source/
export LIBS			:=	libosmium-2.15.6.tar.gz

.PHONY: build clean .FORCE

build: $(ARCH_TASKS) $(LIBS)

build.%: .FORCE $(foreach module,$(MODULES),%,$(module).lib)
	@echo Built libraries for $(shell echo '$@' | perl -n -e '/build\.(.+)/ && print $$1')!

%.lib: .FORCE $(LIBS)
	@_PRJ_NAME=$(shell echo "$@" | perl -n -e '/,(.*?)\.lib$$/ && print $$1') && \
		_ARCH=$(shell echo "$@" | perl -n -e '/^([^,]*?),.*?\.lib$$/ && print $$1') && \
		$(MAKE) --no-print-directory -C $(TOPDIR)/$$_PRJ_NAME BUILD=$$_ARCH PROJDIR=$(TOPDIR)/$$_PRJ_NAME $$_ARCH && \
		echo Built $$_PRJ_NAME for target $$_ARCH!

clean:
	@for f in $(MODULES); do $(MAKE) -C $(TOPDIR)/$$f clean; done

%.tar.gz:
	@echo "Downloading source for $@"
	@curl -o $@ $(LIB_URL_BASE)$@

.FORCE: