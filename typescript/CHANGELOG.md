# Changelog

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
