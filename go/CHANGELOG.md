# Changelog

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
