#!/usr/bin/env bash
# Verify a tagged release through the public Go proxy, without local replacements.
set -euo pipefail
tag=${1:?Supply the Go release tag, such as go/v0.1.0-alpha.2}
if [[ ! "$tag" =~ ^go/v[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?$ ]]; then
  echo "Invalid Go release tag: $tag" >&2
  exit 1
fi
version=${tag#go/}
module=github.com/rimelabs/rime-sdk/go
work=$(mktemp -d)
# Downloaded modules contain read-only directories.
trap 'chmod -R u+w "$work"; rm -rf "$work"' EXIT

# Ignore developer settings, private credentials, workspaces, and cached modules.
export GOENV=off GOWORK=off GOFLAGS= GOTOOLCHAIN=auto
export GOPROXY=https://proxy.golang.org GOSUMDB=sum.golang.org
export GOPRIVATE= GONOPROXY= GONOSUMDB=
export GOPATH="$work/gopath" GOMODCACHE="$work/modules"
export GIT_TERMINAL_PROMPT=0
cd "$work"
go mod init example.com/rime-release-check

# A new Git tag can take time to appear in the public proxy.
for attempt in {1..8}; do
  if go mod download "$module@$version"; then
    break
  fi
  if [ "$attempt" -eq 8 ]; then
    echo "Public download failed for $module@$version. Check repository visibility and the tag." >&2
    exit 1
  fi
  echo "Waiting for the public Go proxy (attempt $attempt of 8)." >&2
  sleep 15
done
go get "$module@$version"
cat > main.go <<'GO'
package main

import rime "github.com/rimelabs/rime-sdk/go"

func main() {
    client, err := rime.NewClient(rime.Config{APIKey: "release-check"})
    if err != nil { panic(err) }
    if err := client.Close(); err != nil { panic(err) }
}
GO
go mod tidy
CGO_ENABLED=0 go run .
go list -m "$module"
