# Authentication assumption

The public API accepts `api_key` in Python and `apiKey` in Node.js. Both read
`RIME_API_KEY` when the caller omits the key. Empty explicit keys fail.

The implementation proceeds on the requested assumption that Themis is complete.
The actual Themis wire contract was not supplied. The private adapters use this
provisional contract for implementation and local tests:

- POST `https://themis.api.rime.ai/v1/token` over verified TLS.
- Header `Authorization: Api-Key <api key>`.
- JSON request `{"audience":"coda.api.rime.ai"}`.
- JSON response with `access_token`, positive `expires_in` seconds, and the same `audience`.
- Use the returned token as Bearer metadata for each new gRPC operation.

This URL and response shape are assumptions, not confirmed deployment details.
No real credentials were sent to this URL during validation. Confirm the private
adapter against the deployed Themis contract before running production synthesis.
This does not require a public SDK API change.

Each client caches its own credential. Concurrent operations share one refresh.
Cancelling one waiter leaves the shared refresh active. Client shutdown cancels
refresh and clears credentials. Refresh does not restart active synthesis.
The SDK rejects redirects and oversized responses. Errors exclude HTTP response
bodies and secret-bearing HTTP exception chains.
HTTP 429 produces a resource-limit error. HTTP 5xx produces an unavailable error.
HTTP 401 and 403 remain authentication and permission errors. Node.js cancels
failed response bodies before it returns an error, including unfinished bodies.
