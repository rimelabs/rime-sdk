# Pinned BlingFire sources

The vendor directory contains unchanged files from Microsoft/BlingFire revision
`18e9a19e586095fb60d629fa850fb610d5bca605`, the revision used by the original Node
package. Only the client library, compile headers, tokenizer library, and license
are included. Cargo builds these sources with `cc`; no runtime download occurs.
The upstream MIT license is at `vendor/LICENSE`.
