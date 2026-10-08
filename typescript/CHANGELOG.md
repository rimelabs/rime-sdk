# Changelog

## [0.1.0-alpha.9](https://github.com/rimelabs/rime-sdk/compare/typescript-v0.1.0-alpha.8...typescript-v0.1.0-alpha.9) (2026-10-08)


### Bug Fixes

* make package quickstarts usable without repository access ([#24](https://github.com/rimelabs/rime-sdk/issues/24)) ([3a30b83](https://github.com/rimelabs/rime-sdk/commit/3a30b83ecaad1740c84d1e8c005520522cf56d26))

## [0.1.0-alpha.8](https://github.com/rimelabs/rime-sdk/compare/typescript-v0.1.0-alpha.7...typescript-v0.1.0-alpha.8) (2026-10-08)


### Features

* add voice examples and automate example dependency updates ([#22](https://github.com/rimelabs/rime-sdk/issues/22)) ([dea7044](https://github.com/rimelabs/rime-sdk/commit/dea7044831058ebee9ae536e8d822a4d7fabbd3d))

## [0.1.0-alpha.7](https://github.com/rimelabs/rime-sdk/compare/typescript-v0.1.0-alpha.6...typescript-v0.1.0-alpha.7) (2026-10-07)


### Bug Fixes

* publish npm releases under the latest tag ([#20](https://github.com/rimelabs/rime-sdk/issues/20)) ([40cf8a4](https://github.com/rimelabs/rime-sdk/commit/40cf8a44720be24cd42acc5f9f97f1269b786e3e))

## [0.1.0-alpha.6](https://github.com/rimelabs/rime-sdk/compare/typescript-v0.1.0-alpha.5...typescript-v0.1.0-alpha.6) (2026-10-07)


### Features

* add Prism realtime support to Python and TypeScript ([#18](https://github.com/rimelabs/rime-sdk/issues/18)) ([af56a96](https://github.com/rimelabs/rime-sdk/commit/af56a96f909b53ca1818b3cbc942f8d0812092ea))

## [0.1.0-alpha.5](https://github.com/rimelabs/rime-sdk/compare/typescript-v0.1.0-alpha.4...typescript-v0.1.0-alpha.5) (2026-09-29)


### Bug Fixes

* support API 0.1.0 in Python and TypeScript SDKs ([#16](https://github.com/rimelabs/rime-sdk/issues/16)) ([af0f646](https://github.com/rimelabs/rime-sdk/commit/af0f6467ffe662673e13a4f943351d4c8b88e0fa))

## [0.1.0-alpha.4](https://github.com/rimelabs/rime-sdk/compare/typescript-v0.1.0-alpha.3...typescript-v0.1.0-alpha.4) (2026-09-28)


### Bug Fixes

* **deps:** bump @rimelabs/api from 0.0.1 to 0.0.2 in /typescript ([8a57747](https://github.com/rimelabs/rime-sdk/commit/8a57747e6e07edfa7ec4f44238fb7074b7e010ab))

## [0.1.0-alpha.3](https://github.com/rimelabs/rime-sdk/compare/typescript-v0.1.0-alpha.2...typescript-v0.1.0-alpha.3) (2026-09-28)


### Bug Fixes

* match ITU mu-law quantization for negative PCM ([#10](https://github.com/rimelabs/rime-sdk/issues/10)) ([e03dcb3](https://github.com/rimelabs/rime-sdk/commit/e03dcb3424cab5a9b7d6bc9b1c37b3b72540e3a6))

## [0.1.0-alpha.2](https://github.com/rimelabs/rime-sdk/compare/typescript-v0.1.0-alpha.1...typescript-v0.1.0-alpha.2) (2026-09-28)

- Add `model: "mistv3"` for Mist v3 speech, voice discovery, and language discovery.
  The model selects its standard endpoint and default voice. Custom endpoints
  retain the model's default voice. Coda remains the default model.
- Reorganize the usage guide around examples and option tables. Clarify audio
  output, timeout defaults, error handling, and cancellation.

## 0.1.0-alpha.1

Initial public alpha for Node.js 22 and later, with ESM and TypeScript types:

- Stream Coda speech from a string or an async text source with API-key authentication.
- Discover voices and languages, and select a custom deployment endpoint.
- Receive raw 24 kHz PCM or 8 kHz mu-law audio with sentence buffering.
- Set operation deadlines, cancel streams, and handle typed errors with request IDs.
- Preserve original text during sentence detection, including zero-width spaces
  and byte order marks.
- Report server errors even when no audio metadata arrives or input writes fail.
