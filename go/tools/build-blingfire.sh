#!/usr/bin/env bash
set -euo pipefail

# Requires emsdk 4.0.23 on PATH. Supply a BlingFire source checkout as argument 1.
package_root=$(cd "$(dirname "$0")/.." && pwd)
source_directory=$(cd "${1:?Supply a BlingFire checkout}" && pwd)
test "$(git -C "$source_directory" rev-parse HEAD)" = 18e9a19e586095fb60d629fa850fb610d5bca605
test -z "$(git -C "$source_directory" status --porcelain)" || { echo 'Use an unchanged BlingFire checkout.' >&2; exit 1; }
em++ --version | head -1 | grep -F '4.0.23' >/dev/null
em++ "$source_directory/blingfiretools/blingfiretokdll/blingfiretokdll.cpp" \
  "$source_directory/blingfiretools/blingfiretokdll/"*.cxx \
  "$source_directory/blingfireclient.library/src/"*.cpp \
  -sSTANDALONE_WASM=1 '-sEXPORTED_FUNCTIONS=["_TextToSentences","_malloc","_free"]' \
  -sALLOW_MEMORY_GROWTH=1 -sMAXIMUM_MEMORY=67108864 -sDISABLE_EXCEPTION_CATCHING=1 \
  -I"$source_directory/blingfireclient.library/inc" \
  -I"$source_directory/blingfirecompile.library/inc" \
  -DHAVE_ICONV_LIB -DHAVE_NO_SPECSTRINGS -D_VERBOSE -DBLING_FIRE_NOAP \
  -DBLING_FIRE_NOWINDOWS -DNDEBUG -O3 --std=c++11 --no-entry \
  -o "$package_root/internal/sentences/blingfire.wasm"
chmod 644 "$package_root/internal/sentences/blingfire.wasm"
shasum -a 256 "$package_root/internal/sentences/blingfire.wasm"
