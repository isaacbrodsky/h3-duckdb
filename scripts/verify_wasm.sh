#!/usr/bin/env bash

set -euox pipefail

ARTIFACT_DIR="$1"

mapfile -t wasms < <(find "$ARTIFACT_DIR" -type f -name '*.wasm' | sort)
if [ "${#wasms[@]}" -eq 0 ]; then
  echo "Error: No wasm artifacts for verify step"
  exit 1
fi

for f in "${wasms[@]}"; do
  echo "check: $f"
  wasm-objdump -j Import -x "$f" | grep latLngToCell
  echo "ok"
done
