#prevent make from using default rules for compiling .o files, which is stupid
MAKEFLAGS += --no-builtin-rules
MAKEFLAGS += --no-builtin-variables
.SUFFIXES:

#this seems to make it possible to use functions to compute prerequisites
.SECONDEXPANSION:

export SHELL		:=	/bin/bash

export NPROC		:=	$(shell nproc)

export TARGET		:=	$(shell basename $(CURDIR))
export TOPDIR		:=	$(CURDIR)
export TOOLCHAINS	:=	$(CURDIR)/toolchain
export COMMONSRC	:=	$(CURDIR)/src/main/native

export CP			:=	cp --reflink=auto

#export CC			:=	gcc
#export CXX			:=	g++
#export LD			:=	g++
export CC			:=	clang
export CXX			:=	clang++
export LD			:=	clang++

ifndef NATIVES_DEBUG
export CFLAGS		:=	-O2 -ffast-math
export BUILD_TYPE	:=	release
else
export CFLAGS		:=	-O0 -DNATIVES_DEBUG
export BUILD_TYPE	:=	debug
endif
$(info natives: building for $(BUILD_TYPE))

export CFLAGS		:=	$(CFLAGS) -fPIC
export CFLAGS		:=	$(CFLAGS) -MMD
#export CFLAGS		:=	$(CFLAGS) -DROCKSDB_LITE

export CXXFLAGS		:= $(CFLAGS) $(CXXFLAGS) -std=c++20
export CXXFLAGS		:= $(CXXFLAGS) -fno-implement-inlines -fvisibility=hidden -fvisibility-inlines-hidden
export CXXFLAGS		:= $(CXXFLAGS) -I$(JAVA_HOME)/include -I$(JAVA_HOME)/include/linux
export CXXFLAGS		:= $(CXXFLAGS) -I.

#for proper linking with rocksdb
export CXXFLAGS		:= $(CXXFLAGS) -D_GLIBCXX_USE_CXX11_ABI=0
export CXXFLAGS		:= $(CXXFLAGS) -fno-rtti
export CXXFLAGS		:= $(CXXFLAGS) -fsized-deallocation

export CXXFLAGS		:= $(CXXFLAGS) -ffunction-sections -fdata-sections
#export CXXFLAGS		:= $(CXXFLAGS) -fno-function-sections -fno-data-sections

export LDFLAGS		:= $(LDFLAGS) -shared

ifndef NATIVES_DEBUG
export LDFLAGS		:= $(LDFLAGS) -Wl,--gc-sections
export LDFLAGS		:= $(LDFLAGS) -Wl,-s
export LDFLAGS		:= $(LDFLAGS) -Wl,-O3 -Wl,--icf=all
endif

export LDFLAGS		:= $(LDFLAGS) -fuse-ld=lld
export LDFLAGS		:= $(LDFLAGS) -Wl,-x
#export LDFLAGS		:= $(LDFLAGS) -static-libstdc++

export LDFLAGS		:= $(LDFLAGS) -Wl,--warn-common
#export LDFLAGS		:= $(LDFLAGS) -Wl,--print-map
#export LDFLAGS		:= $(LDFLAGS) -Wl,--print-gc-sections

export LDFLAGS		:=	$(LDFLAGS) -Wl,-L$(TOPDIR)/build/native-deps/shared -Wl,-l:librocksdbjni-linux64.so

export CXXFLAGS		:= $(CXXFLAGS) -fopenmp
export LDFLAGS		:= $(LDFLAGS) -fopenmp

export INCLUDES		:=	$(JAVA_HOME)include $(JAVA_HOME)include/linux

export ARCHS		:=	x86_64-linux-gnu
export ARCH_TASKS	:=	$(foreach arch,$(ARCHS),build.$(arch))

export MODULES		:=	native

export LIB_URL_BASE	:=	https://cloud.daporkchop.net/programs/source/
export LIBS			:=	libosmium-2.15.6.tar.gz rocksdb-7.7.3.tar.gz

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
