# Sentence detector

`blingfire.wasm` is a standalone build of Microsoft/BlingFire commit
`18e9a19e586095fb60d629fa850fb610d5bca605`. The included MIT license applies.
Emscripten 4.0.23 builds the binary with `../../tools/build-blingfire.sh`.
Its SHA-256 is `e6d73f99dd1e80ce8951feb25222ad47e0a435da5d0f42c4e488c22c32550a65`.
The Emscripten, musl, libc++, and libc++abi license files cover linked runtime code.

The build exports sentence detection, allocation, and cleanup. It disables C++
exception catching. A native failure traps and becomes a Go error. It uses WASI
and one memory-growth notification. The host does not retain memory views between
calls, so that notification needs no action.

Each stream has its own WASM instance, limited to 64 MiB. The process shares only
the compiled-code cache. wazero runs the detector without cgo, external libraries,
or runtime downloads. The SDK closes the instance when text input ends or fails.

Tests compare sentence output with the Python and TypeScript fixtures. Buffer
scans count Unicode code points. Sentence limits count UTF-8 bytes. Original text,
including whitespace and directional marks, is preserved.
