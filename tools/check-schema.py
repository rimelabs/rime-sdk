"""Verify the pinned descriptor and used wire types against both published APIs."""

import base64
import hashlib
import json
from pathlib import Path
import re
from google.protobuf.descriptor_pb2 import FileDescriptorProto, FileDescriptorSet
from rime_api import text_to_speech_pb2

root = Path(__file__).resolve().parents[1]
folder = root / "crates/sdk-protocol"
raw = (folder / "schema.bin").read_bytes()
assert (
    hashlib.sha256(raw).hexdigest()
    == json.loads((folder / "provenance.json").read_text())["sha256"]
)
pinned = FileDescriptorSet.FromString(raw).file[0]
python = FileDescriptorProto.FromString(text_to_speech_pb2.DESCRIPTOR.serialized_pb)
js = (
    root / "typescript/node_modules/@rimelabs/api/esm/rime/text_to_speech_pb.js"
).read_text()
encoded = re.search(r'fileDesc\("([^"]+)"', js).group(1)
node = FileDescriptorProto.FromString(
    base64.b64decode(encoded + "=" * (-len(encoded) % 4))
)
used = {
    "StreamingSynthesisRequest": ["header", "text_chunk"],
    "SynthesisRequest": ["speaker", "language", "audio_parameters"],
    "AudioParameters": ["audio_format", "sampling_rate"],
    "SynthesisResponseStream": ["audio"],
    "GetSupportedLanguagesRequest": [],
    "GetSupportedLanguagesResponse": ["languages"],
    "GetSupportedSpeakersRequest": ["language"],
    "GetSupportedSpeakersResponse": ["speakers"],
}


def signature(descriptor):
    messages = {m.name: m for m in descriptor.message_type}
    return {
        name: {
            f.name: (f.number, f.type, f.type_name, f.label, f.proto3_optional)
            for f in messages[name].field
            if f.name in fields
        }
        for name, fields in used.items()
    }


assert signature(pinned) == signature(python) == signature(node)
print(
    "Pinned Python and Node wire contracts agree for all SDK request and response fields."
)
