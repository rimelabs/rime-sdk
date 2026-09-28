import hashlib
import json
import struct
from pathlib import Path

import pytest

from rimelabs_sdk._audio import AudioFormat, Converter, _mulaw

FIXTURE = json.loads((Path(__file__).resolve().parents[2] / "conformance/mulaw.json").read_text())


def test_mulaw_all_signed_16_bit_inputs_match_itu():
    encoded = bytes(_mulaw(sample) for sample in range(-32768, 32768))
    assert hashlib.sha256(encoded).hexdigest() == FIXTURE["all_inputs_sha256"]


@pytest.mark.parametrize("case", FIXTURE["samples"], ids=lambda case: str(case["pcm"]))
def test_mulaw_quantization_boundaries(case):
    assert _mulaw(case["pcm"]) == case["mulaw"]


def test_mulaw_streaming_constant_samples_match_itu():
    for case in FIXTURE["samples"]:
        converter = Converter(AudioFormat.MULAW_8000)
        pcm = struct.pack("<h", case["pcm"]) * 96
        encoded = converter.process(pcm) + converter.process(b"", final=True)
        assert len(encoded) == 32
        # These output samples have all 63 FIR taps inside the constant input.
        assert encoded[11:22] == bytes([case["mulaw"]]) * 11, case["pcm"]
