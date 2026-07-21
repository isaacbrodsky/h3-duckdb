#!/usr/bin/env bash

set -euo pipefail

AR="$1"
RANLIB="$2"
EXTENSION_LIB="$3"
H3_LIB="$4"

d=$(mktemp -d)
pushd "$d"

"$AR" x "$H3_LIB"
"$AR" q "$EXTENSION_LIB" *.o
"$RANLIB" "$EXTENSION_LIB"

popd
echo "$d" left dangling
#rm -rf "$d"
