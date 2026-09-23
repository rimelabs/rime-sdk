"""Run against either installed SDK version; emits JSON for comparison."""

import json
import time
from rimelabs_sdk._sentences import SentenceBuffer
from rimelabs_sdk._audio import Converter, AudioFormat

text = "Hello world. This is a test of sentence boundaries. " * 100
pcm = b"\x01\x00" * 24000
start = time.perf_counter()
for _ in range(20):
    buffer = SentenceBuffer(65536)
    for offset in range(0, len(text), 7):
        list(buffer.feed(text[offset : offset + 7]))
    list(buffer.feed("", final=True))
sentences = (time.perf_counter() - start) * 1000 / 20
start = time.perf_counter()
for _ in range(20):
    converter = Converter(AudioFormat.MULAW_8000)
    converter.process(pcm, final=True)
print(
    json.dumps(
        {
            "sentence_ms": sentences,
            "audio_ms": (time.perf_counter() - start) * 1000 / 20,
        }
    )
)
