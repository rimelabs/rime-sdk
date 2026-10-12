"""Completed PCM16 WAV files and bounded synthesis collection."""

import struct

from .._errors import RimeAudioFormatError

MAX_BUFFER_BYTES = 64 * 1024 * 1024


def finalize_wav(data: bytearray, sample_rate: int) -> None:
    """Validate mono PCM16 WAV and replace unknown streaming chunk lengths."""

    def invalid():
        return RimeAudioFormatError(
            "Expected a complete mono PCM16 WAV at the requested sample rate"
        )

    if len(data) < 12 or data[:4] != b"RIFF" or data[8:12] != b"WAVE":
        raise invalid()
    if len(data) - 8 > 0xFFFFFFFF:
        raise RimeAudioFormatError("Audio exceeds the WAV size limit")
    if struct.unpack_from("<I", data, 4)[0] not in (0xFFFFFFFF, len(data) - 8):
        raise invalid()
    offset = 12
    found_format = False
    data_size_offset = None
    data_size = 0
    while offset < len(data):
        if len(data) - offset < 8:
            raise invalid()
        name = data[offset : offset + 4]
        size = struct.unpack_from("<I", data, offset + 4)[0]
        start = offset + 8
        if name == b"data" and size == 0xFFFFFFFF:
            size = len(data) - start
        end = start + size
        if end > len(data):
            raise invalid()
        if name == b"fmt ":
            if found_format or size < 16:
                raise invalid()
            if struct.unpack_from("<HHIIHH", data, start) != (
                1,
                1,
                sample_rate,
                sample_rate * 2,
                2,
                16,
            ):
                raise invalid()
            found_format = True
        elif name == b"data":
            if not found_format or data_size_offset is not None or size % 2:
                raise invalid()
            data_size_offset = offset + 4
            data_size = size
        offset = end + size % 2
    if offset != len(data) or data_size_offset is None:
        raise invalid()
    struct.pack_into("<I", data, 4, len(data) - 8)
    struct.pack_into("<I", data, data_size_offset, data_size)
