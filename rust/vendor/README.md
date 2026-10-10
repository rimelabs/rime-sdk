# BlingFire sentence detection

`blingfire.wasm` is identical to `go/internal/sentences/blingfire.wasm`. It uses
Microsoft/BlingFire commit `18e9a19e586095fb60d629fa850fb610d5bca605` and Emscripten
4.0.23. Rebuild it with `go/tools/build-blingfire.sh`, then copy the binary here.
CI compares both copies.

SHA-256: `e6d73f99dd1e80ce8951feb25222ad47e0a435da5d0f42c4e488c22c32550a65`.

Wasmi runs the module with no filesystem, environment, or network access.
Each text stream owns a separate instance with a 64 MiB memory limit. Instances
share the compiled module. Fuel limits bound each scan. Detection runs on Tokio's
blocking pool, with input split into at most 1,024 Unicode scalar values per call.

The included license files cover BlingFire and its linked runtime code.
