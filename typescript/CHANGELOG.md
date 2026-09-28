# Changelog

## Unreleased

Add `model: "mistv3"` for Mist v3 streaming speech and discovery. Model selection
sets the standard endpoint and default voice. Custom endpoints retain the selected
model's voice default. Coda remains the default model.

## 0.1.0-alpha.1

Initial local alpha for Node.js 22+ and ESM. Adds Coda synthesis, discovery,
API-key authentication, sentence buffering, PCM and mu-law profiles, typed errors,
deadlines, and cancellation. The Themis wire contract is assumed and needs
deployment validation.

Review fixes preserve zero-width spaces and BOM characters during sentence
mapping, distinguish token-exchange rate limits and service failures from
rejected credentials, and cancel failed HTTP response bodies.

Further review fixes stop pending reads after cancellation, preserve errors from
responses without audio metadata, handle responses with only trailers, and
collect discovery request IDs from headers and trailers.
