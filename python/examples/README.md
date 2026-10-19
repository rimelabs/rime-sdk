# Python examples

From the repository root:

```sh
uv sync --project python/examples --locked
export RIME_API_KEY="your-api-key"
uv run --directory python/examples --locked -m tts.save
```

The workspace installs the SDK from `python/src/` in editable mode. For audio
playback, add `--extra audio` to `uv sync` and `uv run`. See the
[example guide](../../docs/examples.md) for device setup and Prism configuration.

For direct WebSocket TTS, run `uv run -m tts.coda_ws` from this directory.
Add `--model mistv3` to connect to `wss://mist.api.rime.ai/ws`; the default is
Coda at `wss://coda.api.rime.ai/ws`. Playback requires SoX, or use `--no-playback`
to save audio only.

To use published packages, copy this whole directory outside the repository:

```sh
cd path/to/copied-examples
uv sync
uv run -m tts.save
uv run --extra audio -m tts.play
```

Run Python examples as modules from this directory so their shared imports work.
For recorded Prism input, pass `--input /absolute/path/to/recording.wav`.
The recording must be mono PCM16 at 16 kHz. The default fixture is only available
inside the repository. Copied examples need a published SDK version that meets
the requirement in `pyproject.toml`.
