"""Play streamed text as speech. Press Ctrl-C to stop generation and playback."""

import asyncio

from audio_devices import Speaker

from rimelabs_sdk import Rime


async def text():
    # Replace this generator with text deltas from your application's LLM.
    for sentence in (
        "Your appointment is confirmed. ",
        "It is tomorrow at ten in the morning. ",
        "Please arrive fifteen minutes early.",
    ):
        yield sentence
        await asyncio.sleep(0.5)


async def main():
    with Speaker() as speaker:
        playback = speaker.begin()
        async with Rime(timeout=60) as client, client.tts.stream(text()) as audio:
            async for chunk in audio:
                speaker.write(playback, chunk)
            speaker.end(playback)
            await playback.done


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Stopped speech and playback.")
