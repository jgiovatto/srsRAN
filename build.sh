#!/bin/bash

# set options to supress errors due to dyn_array boundry issues
# see lib/include/srsran/asn1/asn1_utils.h
cmake .. -DCMAKE_CXX_FLAGS="-Warray-bounds=0 -Wstringop-overflow=0 -Wno-stringop-overread" && make -j8
