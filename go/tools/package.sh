#!/usr/bin/env bash
set -euo pipefail
package_root=$(cd "$(dirname "$0")/.." && pwd)
output=${1:?Supply an absolute output .tar.gz path}
case "$output" in /*) ;; *) echo 'Output path must be absolute.' >&2; exit 1 ;; esac
cd "$package_root"
COPYFILE_DISABLE=1 tar -czf "$output" go.mod go.sum ./*.go LICENSE README.md CHANGELOG.md version.txt \
  internal testdata examples tools
