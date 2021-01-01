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

include $(TOOLCHAINS)/$(BUILD)

SOURCES			:=	$(PROJDIR)/../src/main/native/common $(PROJDIR)/../src/main/native/$(BUILD)
SOURCES			:=	$(abspath $(SOURCES))

CFILES			:=	$(foreach dir,$(SOURCES),$(wildcard $(dir)/*.c))
CPPFILES		:=	$(foreach dir,$(SOURCES),$(wildcard $(dir)/*.cpp))
OFILES			:=	$(addsuffix .o,$(subst /,__,$(CFILES)) $(subst /,__,$(CPPFILES)))

INCLUDES		:=	$(INCLUDES) $(CURDIR)/lib-libosmium/include
INCLUDE			:=	$(addprefix -I,$(INCLUDES) $(CURDIR))

build: tpposmtilegen.$(EXT)
	@echo "Copying tpposmtilegen.$(EXT) to $(OUTDIR)/..."
	@[ -d $(OUTDIR) ] || mkdir -p $(OUTDIR)
	@cp tpposmtilegen.$(EXT) $(OUTDIR)/$(BUILD).$(EXT)
	@echo "Copying tpposmtilegen.$(EXT) to $(OUTDIR2)/..."
	@[ -d $(OUTDIR2) ] || mkdir -p $(OUTDIR2)
	@cp tpposmtilegen.$(EXT) $(OUTDIR2)/$(BUILD).$(EXT)

tpposmtilegen.$(EXT): $(CFILES) $(CPPFILES) $(OFILES) $(CURDIR)/lib-libosmium/README.md
	@echo "Linking $@..."
	@$(LD) $(LDFLAGS) $(INCLUDE) -o $@ $(OFILES)
	@echo "Stripping $@..."
	@$(STRIP) $@

%.c.o: $(CFILES) $(CURDIR)/lib-libosmium/README.md
	@echo "Building $(subst .o,,$(subst __,/,$@))..."
	@$(CC) $(CFLAGS) $(INCLUDE) -c $(subst .o,,$(subst __,/,$@)) -o $@

%.cpp.o: $(CPPFILES) $(CURDIR)/lib-libosmium/README.md
	@echo "Building $(subst .o,,$(subst __,/,$@))..."
	$(CXX) $(CXXFLAGS) $(INCLUDE) -c $(subst .o,,$(subst __,/,$@)) -o $@

$(CURDIR)/lib-libosmium/README.md: $(TOPDIR)/libosmium-2.15.6.tar.gz
	@[ ! -d lib-libosmium/ ] || rm -rf lib-libosmium/
	@tar zxf $(TOPDIR)/libosmium-2.15.6.tar.gz
	@mv libosmium-2.15.6/ lib-libosmium/

endif
