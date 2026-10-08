#!/usr/bin/env bash
set -euo pipefail

# Run from any directory. Consumers do not need these build tools.
package_root=$(cd "$(dirname "$0")/.." && pwd)
build_directory=$(mktemp -d)
trap 'rm -rf "$build_directory"' EXIT
test "$(protoc --version)" = 'libprotoc 35.1' || { echo 'Use protoc 35.1.' >&2; exit 1; }
npm pack @rimelabs/api@0.1.0 --pack-destination "$build_directory" \
  --@rimelabs:registry=https://registry.npmjs.org --loglevel=error
tar -xzf "$build_directory/rimelabs-api-0.1.0.tgz" -C "$build_directory"
schema="$build_directory/package/schema"
expected=6a5707f207c95fcb32557f4b811e30fb67c01df8fc4dc60ec823e592cc477e5e
actual=$(shasum -a 256 "$schema/rime/text_to_speech.proto" | cut -d ' ' -f1)
test "$actual" = "$expected" || { echo 'Unexpected TTS schema hash.' >&2; exit 1; }
GOBIN="$build_directory/bin" go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.11
GOBIN="$build_directory/bin" go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.6.1
mkdir "$build_directory/generated"
PATH="$build_directory/bin:$PATH" protoc -I "$schema" \
  --go_out="$build_directory/generated" --go_opt=paths=source_relative \
  --go_opt=Mrime/text_to_speech.proto=github.com/rimelabs/rime-sdk/go/internal/proto \
  --go-grpc_out="$build_directory/generated" --go-grpc_opt=paths=source_relative \
  --go-grpc_opt=Mrime/text_to_speech.proto=github.com/rimelabs/rime-sdk/go/internal/proto \
  rime/text_to_speech.proto
cp "$build_directory/generated/rime/"*.go "$package_root/internal/proto/"
cp "$build_directory/package/"{LICENSE,NOTICE,SOURCE.json} "$package_root/internal/proto/"
