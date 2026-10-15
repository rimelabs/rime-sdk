# TTS compatibility contract — T0

This directory freezes the union of the official Python and Node LiveKit Rime
plugin contracts. It is the input to subsequent SDK implementation stages.
**Passing T0 validates this corpus, not SDK transport or plugin compatibility.**
All four SDK languages must eventually execute each applicable expectation using
their public APIs. Every case's `sdk_verification` remains `not_run` in T0.

Shared behavioral decisions, architecture and stage dependencies live in the
[shared plan](../../docs/livekit-tts-sdk-plan.md). Language-specific APIs and
lifecycle tests belong in its four linked implementation plans. T0 is one PR
across all four SDKs; no runtime minimum, dependency or synthesis behavior changes.

## Frozen baselines

The user selected the current published plugins on October 15, 2026; older
plugin releases are outside this migration's qualification scope.

| Plugin artifact | Framework baseline | Runtime floor |
| --- | --- | --- |
| `livekit-plugins-rime==1.8.8` | `livekit-agents==1.8.8`; plugin declares `>=1.8.8` | Python 3.10; Agents declares `<3.15` |
| `@livekit/agents-plugin-rime@1.9.3` | `@livekit/agents@1.9.3`; exact peer | Node 20 documented; exact patch/dependencies pending Q6 |

`contract.json` records registry artifact URLs, hashes/integrity, declared
dependencies, full Git revisions, and SHA-256 hashes of ten implementation files.
The downloaded artifact checksums matched registry metadata; those ten files
matched the pinned release source byte-for-byte. No live synthesis or migrated
adapter qualification is implied. Future main-branch changes do not silently
move these baselines. Changes require a reviewed contract update.

## Files and interpretation

- `contract.json`: baseline manifest, sources, C01–C14 families, Q gates,
  public argument/method inventory and compatibility cases.
- `schema.json`: JSON Schema for the manifest and case structure.
- `invalid.json`: 15 shared JSON-pointer replacement mutations that every
  language's validator must reject.

Each case has a stable ID, plugin baseline (or `sdk` for shared requirements),
transport, capability families, source references and explicit evidence type:

| Evidence | Meaning |
| --- | --- |
| `source` | Expected behavior derived from the pinned plugin source; not a live service result |
| `design` | Required SDK behavior from the shared plan; not implemented by T0 |
| `requires_live` | Unresolved qualification question with an open Q gate; may also need package/runtime experiments |

`specified` means an expectation has been recorded. It does not mean it passed.
`unresolved` cases contain only `pending` expectations and name an open gate.
The other expectation operations are `equal`, `absent`, `reject` and `ordered`.
`target` names a semantic observation to be bound to a language's public API,
recording peer or resource spy in a later stage. Error descriptions specify
meaning, not exact language-specific exception text.

The `given` scripts are language-neutral inputs, not executable SDK calls:

- `options`/`overrides` use `{ "state": "omitted" }`,
  `{ "state": "clear" }` or `{ "state": "value", "value": ... }`.
  False, zero, null and empty collections are explicit values when present;
  route validation may still reject them. Omission must never silently become false.
- `entrypoints` expands each argument case into constructor and/or update checks.
  For an update check, construct the same transport/model context first and update
  the named argument. Do not interpret these cases as allowing transport switches;
  separate rejection cases cover those. Node's additional-field inventory expands
  `[key:string]` into the supplied extension mapping, not a literal wire key.
- `parameterize` expands independent examples. Rate/codec qualification candidates
  are not a claim that every Cartesian pair is supported. A `{ "repeat": "x",
  "count": 4097 }` input describes a long text unit without a large literal.
- `steps`, `updates`, labels and resource references describe observable sequences.
  Credentials, sessions and tokenizers are supplied by a local harness. There are
  no usable credentials here; example URLs are not contacted by T0.
- Route-specific cases imply selection of that route even when the corresponding
  constructor selector is omitted from the abbreviated input. Default cases retain
  the exact omission recorded. `all` means applicable routes, not unsupported
  option/transport combinations.

Both plugins retain their tokenizers. Their recorded token output/space-appending
behavior is separate from SDK text preservation. An empty token sequence and a
whitespace-only SDK text unit are distinct cases. Local tokenizer flush does not
imply a wire flush or a V1 synthesis end.

## Coverage and open gates

The inventory contains 103 public argument/method entries: 44 Python and 59 Node.
All have case references. The 126 cases comprise 40 Python, 50 Node and 36 shared
cases; evidence comprises 91 source observations, 27 SDK requirements and 8
unresolved qualification cases. A case can cover several capability families.

| Family | Cases | Unresolved cases |
| --- | ---: | ---: |
| C01 Options | 16 | 0 |
| C02 Runtime compatibility | 3 | 2 |
| C03 Transport | 12 | 0 |
| C04 Endpoints and sessions | 22 | 1 |
| C05 Models | 15 | 1 |
| C06 Mist controls | 11 | 0 |
| C07 Legacy Coda controls | 11 | 0 |
| C08 Provider extensions | 8 | 1 |
| C09 Speed | 11 | 0 |
| C10 Output | 11 | 1 |
| C11 Text policy | 16 | 0 |
| C12 Timings | 4 | 1 |
| C13 Preparation and timeouts | 11 | 1 |
| C14 Errors and ownership | 17 | 0 |

Q1 is resolved by the baseline decision. Q2–Q8 remain explicit later-stage gates:
direct Mist v2/custom service availability, actual audio format/rate pairs,
timestamp units/scaling, deployed legacy-option effects, runtime/resource edges,
measured regression budgets and finite SDK upgrade ranges. Source forwarding
does not establish engine interpretation. The owning stage must attach evidence
before reporting these cases as verified. Numeric case counts measure inventory
coverage, not the completeness of future behavioral test suites.

## Validation and maintenance

Python and Node apply the full JSON Schema plus cross-reference, presence and
coverage checks. Go and Rust validate loading and the shared integrity invariants
without adding dependencies. All four reject the same 15 invalid mutations,
including invented SDK success, uncovered arguments, missing families and
unresolved cases with asserted answers.

Run the focused checks from each language directory:

```sh
# python/
uv run pytest tests/test_tts_compat_contract.py
# typescript/
node --test tests/tts-compat-contract.test.mjs
# go/
go test -run TestTTSCompatContract ./...
# rust/
cargo test --locked tts_compat_contract
```

Edit the canonical JSON here and copy all three JSON files byte-for-byte to
`go/testdata/tts-compat/` and `rust/testdata/tts-compat/`. Existing CI runs all four
language jobs for shared fixture changes and compares the packaged copies.
Go/Rust tests load their copies so exported modules/crates can run the checks.
Later stages add public-API behavioral runners and separate execution results;
do not turn corpus-validation success into an SDK conformance result.
