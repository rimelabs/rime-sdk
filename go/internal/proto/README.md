# Generated TTS protocol

These files come from the exact TTS schema in `@rimelabs/api` 0.1.0.
`SOURCE.json` records the schema hash. The included Apache 2.0 license applies.

Run `bash tools/generate.sh` from the Go module to regenerate. It checks the
schema hash and pins protoc 35.1, protoc-gen-go 1.36.11, and
protoc-gen-go-grpc 1.6.1. It uses import mappings without changing the schema.

The generated files are internal so SDK users do not depend on protobuf types.
They ship with the module. Users need neither npm nor protoc. This avoids a
dependency on a Go protocol module that Rime has not published yet. The canonical
schema remains in `rime-api`; do not edit generated files here.
