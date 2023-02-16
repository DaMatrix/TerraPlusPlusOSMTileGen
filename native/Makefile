#this seems to make it possible to use functions to compute prerequisites
.SECONDEXPANSION:

ifneq ($(BUILD),$(notdir $(CURDIR)))
.PHONY: $(BUILD) clean

export OUTDIR	:=	$(CURDIR)/../src/main/resources/net/daporkchop/tpposmtilegen/natives
export OUTDIR2	:=	$(CURDIR)/../out/production/resources/net/daporkchop/tpposmtilegen/natives
BUILDDIR		:=	../build/native/$(BUILD_TYPE)/$(BUILD)

clean:
	@if [ -d ../build/native/ ]; then rm -rf ../build/native/; fi
	@rm -rf $(foreach arch,$(ARCHS),$(OUTDIR)/$(arch).*)

$(BUILD):
	@[ -d $(BUILDDIR) ] || mkdir -p $(BUILDDIR)
	@$(MAKE) --no-print-directory -C $(BUILDDIR) -f $(CURDIR)/Makefile BUILD=$(BUILD) build

else
.PHONY: build

-include $(TOOLCHAINS)/$(BUILD)

MAKEFILES		:=	$(TOPDIR)/Makefile $(TOPDIR)/native/Makefile

SOURCES			:=	$(PROJDIR)/../src/main/native/common $(PROJDIR)/../src/main/native/$(BUILD)
SOURCES			:=	$(abspath $(SOURCES))

CFILES			:=	$(foreach dir,$(SOURCES),$(wildcard $(dir)/*.c))
CPPFILES		:=	$(foreach dir,$(SOURCES),$(wildcard $(dir)/*.cpp))
OFILES			:=	$(addsuffix .o,$(subst /,__,$(CFILES)) $(subst /,__,$(CPPFILES)))

INCLUDES		:=	$(INCLUDES) $(CURDIR)/lib-libosmium/include $(CURDIR)/lib-rocksdb/include/
INCLUDE			:=	$(addprefix -I,$(INCLUDES) $(CURDIR))

LIB_DEPS		:=
COMPILE_DEPS	:=	$(LIB_DEPS) $(MAKEFILES) $(CURDIR)/lib-libosmium/README.md $(CURDIR)/lib-rocksdb/README.md

build: tpposmtilegen.$(EXT)
	@{ [ -d $(OUTDIR) ] || mkdir $(OUTDIR); } && \
		$(CP) -v tpposmtilegen.$(EXT) $(OUTDIR)/$(BUILD).$(EXT)
	@{ [ -d $(OUTDIR2) ] || mkdir $(OUTDIR2); } && \
		$(CP) -v tpposmtilegen.$(EXT) $(OUTDIR2)/$(BUILD).$(EXT)

tpposmtilegen.$(EXT): $(OFILES) $(COMPILE_DEPS)
	@echo "Linking $@..."
	@$(LD) $(LDFLAGS) $(INCLUDE) -o $@ $(OFILES)

%.c.o: $$(subst __,/,%.c) $(COMPILE_DEPS)
	@echo "Building $<"
	$(CC) $(CFLAGS) $(INCLUDE) -c $< -o $@

%.cpp.o: $$(subst __,/,%.cpp) $(COMPILE_DEPS)
	@echo "Building $<"
	$(CXX) $(CXXFLAGS) $(INCLUDE) -c $< -o $@

$(CURDIR)/lib-libosmium/README.md: $(TOPDIR)/libosmium-2.15.6.tar.gz $(LIB_DEPS)
	@echo "Extracting '$<' to '$@'"
	@[ ! -d lib-libosmium/ ] || rm -rf lib-libosmium/
	@tar zxf $(TOPDIR)/libosmium-2.15.6.tar.gz
	@mv libosmium-2.15.6/ lib-libosmium/
	@touch -m lib-libosmium/README.md

$(CURDIR)/lib-rocksdb/README.md: $(TOPDIR)/rocksdb-7.7.3.tar.gz $(LIB_DEPS)
	@echo "Extracting '$<' to '$@'"
	@[ ! -d lib-rocksdb/ ] || rm -rf lib-rocksdb/
	@tar zxf $(TOPDIR)/rocksdb-7.7.3.tar.gz
	@touch -m rocksdb-7.7.3/README.md
	@mv rocksdb-7.7.3/ lib-rocksdb/

-include $(patsubst %.o,%.d,$(OFILES))

endif
