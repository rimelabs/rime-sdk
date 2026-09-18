# Changelog

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
