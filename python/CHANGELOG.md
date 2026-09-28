# Changelog

## Unreleased

Add `model="mistv3"` for Mist v3 streaming speech and discovery. Model selection
sets the standard endpoint and default voice. Custom endpoints retain the selected
model's voice default. Coda remains the default model.

## 0.1.0a1

Initial local alpha. Adds async Coda synthesis, discovery, API-key authentication,
sentence buffering, PCM and mu-law profiles, typed errors, deadlines, and cancellation.
The Themis wire contract is assumed and needs deployment validation.

Review fixes preserve server errors when writes reach a closed RPC, use native
sentence offsets for Unicode text, and distinguish token-exchange rate limits
and service failures from rejected credentials.

Further review fixes preserve errors from responses without audio metadata,
collect request IDs from headers and trailers, and release completed credential
refresh tasks at shutdown.
