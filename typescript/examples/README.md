# TypeScript examples

From the repository root:

```sh
npm ci
npm --prefix typescript run build
export RIME_API_KEY="your-api-key"
npm --prefix typescript/examples run tts:save
```

The npm workspace links `@rimelabs/sdk` to the local TypeScript package. Rebuild
after changes to SDK source. See the [example guide](../../docs/examples.md) for
playback and Prism configuration.

Append-fragment STT in this checkout requires the unreleased `outputContract`
option. Published `@rimelabs/sdk` versions through `0.1.0-alpha.17` do not support
it, even though they satisfy the current example dependency range. Run append
examples inside the workspace until support is released; older SDKs may ignore
the option and return revised hypotheses instead. The first release containing
append support must become the copied examples' minimum dependency version.

To use published packages for the other examples, copy this whole directory outside the repository:

```sh
cd path/to/copied-examples
npm install
npm run check
npm run tts:save
```

For recorded Prism input, pass `-- --input /absolute/path/to/recording.wav` after
the npm script name. The recording must be mono PCM16 at 16 kHz. The default
fixture is only available inside the repository. Copied examples need a published
SDK version that meets the requirement in `package.json`.

The example tests use repository test helpers and fixtures. Run them inside the
repository with `npm test --workspace rime-sdk-examples`.
