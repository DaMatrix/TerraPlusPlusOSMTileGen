#!/bin/bash

#i use this script simply so that i can add the natives compilation as a run configuration in intellij

export NATIVES_DEBUG="true"

[ -f src/main/resources/x86_64-linux-gnu/tpposmtilegen.so ] && (rm -v src/main/resources/x86_64-linux-gnu/tpposmtilegen.so || exit 1)

#make clean && \
#make -j$( nproc ) || exit 1

make -j$( nproc ) build.x86_64-linux-gnu || exit 1

#[ -d test_out2 ] && (rm -rfv test_out2 || exit 1)
#cp -rv test_out test_out2 || exit 1