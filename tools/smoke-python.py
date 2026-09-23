"""Run with the Python interpreter from a wheel-only environment."""

import asyncio

from rimelabs_sdk import AudioFormat, Rime, RimeCancelledError, RimeInputError, _native

assert not hasattr(_native.NativeClient, "testing"), (
    "test hooks leaked into a release wheel"
)
assert _native.SentenceBuffer(65536).feed("Hello. World.", True) == ["Hello.", " World."]


async def main():
    async with Rime(api_key="package-test") as client:
        stream = client.tts.stream("Hello.")
        assert stream.format is AudioFormat.PCM_24000
        await stream.cancel()
        try:
            await anext(stream)
        except RimeCancelledError:
            pass
        else:
            raise AssertionError("cancelled stream yielded")
    try:
        Rime(api_key="test", endpoint="https://host")
    except RimeInputError:
        pass
    else:
        raise AssertionError("invalid endpoint accepted")


asyncio.run(main())
