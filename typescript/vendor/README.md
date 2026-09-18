# BlingFire runtime

The WASM binary and generated runtime come from Microsoft/BlingFire commit
`18e9a19e586095fb60d629fa850fb610d5bca605`, under the included MIT license.

`blingfire.cjs` loads the local WASM bytes, exposes initialization as a promise,
and removes the generated global process exception handlers. It otherwise uses
the upstream runtime. No code or model is downloaded at runtime.

The SDK copies output before freeing WASM memory. It maps normalized detector
output back to source text, so synthesis preserves the original text.
