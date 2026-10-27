# Changelog

## [0.1.0-alpha.14](https://github.com/rimelabs/rime-sdk/compare/go/v0.1.0-alpha.13...go/v0.1.0-alpha.14) (2026-10-27)


### Bug Fixes

* preserve Prism handshake error categories and diagnostics ([#110](https://github.com/rimelabs/rime-sdk/issues/110)) ([75568b2](https://github.com/rimelabs/rime-sdk/commit/75568b278afddd265a0a4f2c26c716ccb6b29514))

## [0.1.0-alpha.13](https://github.com/rimelabs/rime-sdk/compare/go/v0.1.0-alpha.12...go/v0.1.0-alpha.13) (2026-10-24)


### ⚠ BREAKING CHANGES

* Rust SessionInfo struct literals must add truncation: None. The optional field reports the policy published by the server.
* Go Prism service faults now match ErrRealtime instead of ErrStream. Rust service faults now use ErrorKind::Realtime instead of Stream. Keep fault inspection for code, scope, and retry classification.

### Features

* add Prism conversation history operations ([#108](https://github.com/rimelabs/rime-sdk/issues/108)) ([1f33051](https://github.com/rimelabs/rime-sdk/commit/1f330517163cd521bd03321a0728295f84da0316))
* add Prism history truncation controls ([#109](https://github.com/rimelabs/rime-sdk/issues/109)) ([b8b1aa1](https://github.com/rimelabs/rime-sdk/commit/b8b1aa172e69df650bfeda274bb91329f7e5bc52))


### Bug Fixes

* distinguish Prism faults and correct SDK release guidance ([#102](https://github.com/rimelabs/rime-sdk/issues/102)) ([b152abb](https://github.com/rimelabs/rime-sdk/commit/b152abb1ac4e5dabb92e747c04a6bc70aa72b60e))

## [0.1.0-alpha.12](https://github.com/rimelabs/rime-sdk/compare/go/v0.1.0-alpha.11...go/v0.1.0-alpha.12) (2026-10-23)


### ⚠ BREAKING CHANGES

* Rust RealtimeSession::info returns an owned SessionInfo snapshot instead of a reference so cloned handles can read current settings.

### Features

* expose Prism error retry classification ([#99](https://github.com/rimelabs/rime-sdk/issues/99)) ([c04309c](https://github.com/rimelabs/rime-sdk/commit/c04309c174c2eed82df3b676581c45cab2a95db6))
* **realtime:** request ids, reply labels, and the items a reply answers ([#95](https://github.com/rimelabs/rime-sdk/issues/95)) ([f33950b](https://github.com/rimelabs/rime-sdk/commit/f33950b2d0fdb960f4701c1a1ec208c0d539d5ec))
* support mid-session Prism settings updates in all SDKs ([#101](https://github.com/rimelabs/rime-sdk/issues/101)) ([83f5347](https://github.com/rimelabs/rime-sdk/commit/83f534761418217292fbbce8dbdc47454ad5ddb8))

## [0.1.0-alpha.11](https://github.com/rimelabs/rime-sdk/compare/go/v0.1.0-alpha.10...go/v0.1.0-alpha.11) (2026-10-23)


### Features

* add Prism realtime support to Rust and Go ([#96](https://github.com/rimelabs/rime-sdk/issues/96)) ([ab5a360](https://github.com/rimelabs/rime-sdk/commit/ab5a3608280b83dd8d01b343bd9aee6a4e89adf7))

## [0.1.0-alpha.10](https://github.com/rimelabs/rime-sdk/compare/go/v0.1.0-alpha.9...go/v0.1.0-alpha.10) (2026-10-21)


### ⚠ BREAKING CHANGES

* Rust TranscriptionUpdate and TypeScript TranscriptionUpdate include Delta/delta; exhaustive handlers must handle the new variant.

### Features

* add append-fragment STT streaming across SDKs ([369f04d](https://github.com/rimelabs/rime-sdk/commit/369f04dadfea9d3d8593d14283d64e3a1f36e4d1))

## [0.1.0-alpha.9](https://github.com/rimelabs/rime-sdk/compare/go/v0.1.0-alpha.8...go/v0.1.0-alpha.9) (2026-10-16)


### Features

* add unary STT transcription to all SDKs ([#80](https://github.com/rimelabs/rime-sdk/issues/80)) ([39427eb](https://github.com/rimelabs/rime-sdk/commit/39427eba7738e4f2fe0514a2cddaa219137dcc4d))

## [0.1.0-alpha.8](https://github.com/rimelabs/rime-sdk/compare/go/v0.1.0-alpha.7...go/v0.1.0-alpha.8) (2026-10-12)


### Features

* **tts:** add sample rates, time-scale control, and buffered audio output ([1856d0d](https://github.com/rimelabs/rime-sdk/commit/1856d0d6c00a46d7917b61d73bc7f2a8526c8bb0))

## [0.1.0-alpha.7](https://github.com/rimelabs/rime-sdk/compare/go/v0.1.0-alpha.6...go/v0.1.0-alpha.7) (2026-10-11)


### Bug Fixes

* **tts:** remove complete-text synthesis option ([#61](https://github.com/rimelabs/rime-sdk/issues/61)) ([4e3c133](https://github.com/rimelabs/rime-sdk/commit/4e3c1338dc9b129a0201a462f12fb414eeb520d1))

## [0.1.0-alpha.6](https://github.com/rimelabs/rime-sdk/compare/go/v0.1.0-alpha.5...go/v0.1.0-alpha.6) (2026-10-10)


### ⚠ BREAKING CHANGES

* Use synthesize for complete text and stream for incremental text in all SDKs. Go StreamSource becomes Stream. Rust synthesis moves under Client::tts(). Both methods return streaming audio.

### Features

* align TTS APIs and SDK example workflows ([#49](https://github.com/rimelabs/rime-sdk/issues/49)) ([bc2be77](https://github.com/rimelabs/rime-sdk/commit/bc2be779f7a845d51850fbe068a4cf6d95eab666))


### Bug Fixes

* **deps:** bump github.com/rimelabs/rime-api/go in /go ([#53](https://github.com/rimelabs/rime-sdk/issues/53)) ([866f2c6](https://github.com/rimelabs/rime-sdk/commit/866f2c66c43fb42fa049697de6f8ba60537c9480))

## [0.1.0-alpha.5](https://github.com/rimelabs/rime-sdk/compare/go/v0.1.0-alpha.4...go/v0.1.0-alpha.5) (2026-10-09)


### Features

* add streaming STT to Python, Node.js, and Go ([#39](https://github.com/rimelabs/rime-sdk/issues/39)) ([79977ef](https://github.com/rimelabs/rime-sdk/commit/79977ef019bc474d799881e60223c6868af104d0))


### Bug Fixes

* **deps:** bump google.golang.org/protobuf in /go ([#29](https://github.com/rimelabs/rime-sdk/issues/29)) ([641acca](https://github.com/rimelabs/rime-sdk/commit/641acca528ff56223dbf68b17a2a918edccea06f))

## [0.1.0-alpha.4](https://github.com/rimelabs/rime-sdk/compare/go/v0.1.0-alpha.3...go/v0.1.0-alpha.4) (2026-10-09)


### Bug Fixes

* **go:** preserve context cause during stream cancellation ([#36](https://github.com/rimelabs/rime-sdk/issues/36)) ([d8617cf](https://github.com/rimelabs/rime-sdk/commit/d8617cf8c5858959dfdd9b9b3e1bedea80e0c863))

## [0.1.0-alpha.3](https://github.com/rimelabs/rime-sdk/compare/go/v0.1.0-alpha.2...go/v0.1.0-alpha.3) (2026-10-08)


### Bug Fixes

* **deps:** use versioned API packages across SDKs ([#34](https://github.com/rimelabs/rime-sdk/issues/34)) ([bde6ecb](https://github.com/rimelabs/rime-sdk/commit/bde6ecbb50028d9f920883d5c82732e3d14fb2ed))

## [0.1.0-alpha.2](https://github.com/rimelabs/rime-sdk/compare/go/v0.1.0-alpha.1...go/v0.1.0-alpha.2) (2026-10-08)


### Features

* add Go TTS SDK and public module releases ([#26](https://github.com/rimelabs/rime-sdk/issues/26)) ([dc03e89](https://github.com/rimelabs/rime-sdk/commit/dc03e8999797d9a3af84c512e9b774d6e4f8c770))

## 0.1.0-alpha.1

- Add Go TTS support for Coda and Mist v3.
- Support complete and incremental text, PCM and mu-law output, discovery,
  cancellation, deadlines, and service request IDs.
