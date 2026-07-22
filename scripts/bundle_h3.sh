#!/usr/bin/env bash

set -euo pipefail

AR="$1"
RANLIB="$2"
EXTENSION_LIB="$3"
H3_LIB="$4"

if "$AR" t "$EXTENSION_LIB" | grep -qxF "latLngToCell"; then
  echo "$EXTENSION_LIB contains $H3_LIB already"
  exit 0
fi

temp_dir="$(mktemp -d)"
trap 'rm -rf "temp_dir"' EXIT

pushd "$temp_dir"

"$AR" x "$H3_LIB"
"$AR" q "$EXTENSION_LIB" *.o
"$RANLIB" "$EXTENSION_LIB"

popd
