# Changelog

## [0.1.0-alpha.3](https://github.com/rimelabs/rime-sdk/compare/rust-v0.1.0-alpha.2...rust-v0.1.0-alpha.3) (2026-10-10)


### ⚠ BREAKING CHANGES

* Use synthesize for complete text and stream for incremental text in all SDKs. Go StreamSource becomes Stream. Rust synthesis moves under Client::tts(). Both methods return streaming audio.

### Features

* align TTS APIs and SDK example workflows ([#49](https://github.com/rimelabs/rime-sdk/issues/49)) ([bc2be77](https://github.com/rimelabs/rime-sdk/commit/bc2be779f7a845d51850fbe068a4cf6d95eab666))


### Bug Fixes

* **deps:** bump rimelabs-api from 0.3.0 to 0.4.0 in /rust ([#52](https://github.com/rimelabs/rime-sdk/issues/52)) ([2141c1d](https://github.com/rimelabs/rime-sdk/commit/2141c1d1434edc584ac055241a4b50eb2f37f5c4))
* **deps:** bump sha2 from 0.10.9 to 0.11.0 in /rust ([#48](https://github.com/rimelabs/rime-sdk/issues/48)) ([db7976c](https://github.com/rimelabs/rime-sdk/commit/db7976c276a3a64999d8e1f608ff887c1adb5c5f))

## [0.1.0-alpha.2](https://github.com/rimelabs/rime-sdk/compare/rust-v0.1.0-alpha.1...rust-v0.1.0-alpha.2) (2026-10-10)


### Features

* add Rust streaming TTS SDK ([#44](https://github.com/rimelabs/rime-sdk/issues/44)) ([76cb112](https://github.com/rimelabs/rime-sdk/commit/76cb112e8f122145bb1e1fcffa2bffe927546f04))

## 0.1.0-alpha.1

- Add asynchronous Coda and Mist v3 TTS, voice and language discovery, and raw
  PCM24/mu-law8 audio streaming.
