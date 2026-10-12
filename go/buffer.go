package rime

import (
	"context"
	"encoding/binary"
	"io"
)

const maxBufferBytes = 64 * 1024 * 1024

// SynthesizeToBuffer waits for complete audio and finalizes WAV lengths.
// Errors return nil audio, including errors after audio has arrived. Output is
// limited to 64 MiB (67,108,864 bytes); exceeding it returns ErrResourceLimit.
// Use Synthesize for larger outputs or timestamps.
func (t *TTSService) SynthesizeToBuffer(ctx context.Context, text string, options SynthesisOptions) ([]byte, error) {
	stream, err := t.Synthesize(ctx, text, options)
	if err != nil {
		return nil, err
	}
	defer stream.Close()
	data := make([]byte, 0)
	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		if len(chunk) > maxBufferBytes-len(data) {
			return nil, operationError(failure(ErrResourceLimit, "audio exceeds the 64 MiB buffer limit"), stream.RequestID())
		}
		data = append(data, chunk...)
	}
	if stream.Format() == WAV {
		if err := finalizeWAV(data, stream.SampleRate()); err != nil {
			return nil, operationError(err, stream.RequestID())
		}
	}
	return data, nil
}

// finalizeWAV accepts complete mono PCM16 WAV, including streaming size sentinels.
func finalizeWAV(data []byte, sampleRate int) error {
	invalid := func() error {
		return failure(ErrAudioFormat, "expected a complete mono PCM16 WAV at the requested sample rate")
	}
	if len(data) < 12 || string(data[:4]) != "RIFF" || string(data[8:12]) != "WAVE" {
		return invalid()
	}
	if uint64(len(data)-8) > 0xffffffff {
		return failure(ErrAudioFormat, "audio exceeds the WAV size limit")
	}
	riffSize := binary.LittleEndian.Uint32(data[4:8])
	if riffSize != 0xffffffff && uint64(riffSize) != uint64(len(data)-8) {
		return invalid()
	}
	offset, dataSizeOffset, dataSize := 12, -1, 0
	foundFormat := false
	for offset < len(data) {
		if len(data)-offset < 8 {
			return invalid()
		}
		name := string(data[offset : offset+4])
		size := uint64(binary.LittleEndian.Uint32(data[offset+4 : offset+8]))
		start := offset + 8
		if name == "data" && size == 0xffffffff {
			size = uint64(len(data) - start)
		}
		if size > uint64(len(data)-start) {
			return invalid()
		}
		end := start + int(size)
		switch name {
		case "fmt ":
			if foundFormat || size < 16 {
				return invalid()
			}
			if binary.LittleEndian.Uint16(data[start:]) != 1 || binary.LittleEndian.Uint16(data[start+2:]) != 1 || binary.LittleEndian.Uint32(data[start+4:]) != uint32(sampleRate) || binary.LittleEndian.Uint32(data[start+8:]) != uint32(sampleRate*2) || binary.LittleEndian.Uint16(data[start+12:]) != 2 || binary.LittleEndian.Uint16(data[start+14:]) != 16 {
				return invalid()
			}
			foundFormat = true
		case "data":
			if !foundFormat || dataSizeOffset >= 0 || size%2 != 0 {
				return invalid()
			}
			dataSizeOffset, dataSize = offset+4, int(size)
		}
		offset = end + int(size%2)
	}
	if offset != len(data) || dataSizeOffset < 0 {
		return invalid()
	}
	binary.LittleEndian.PutUint32(data[4:8], uint32(len(data)-8))
	binary.LittleEndian.PutUint32(data[dataSizeOffset:], uint32(dataSize))
	return nil
}
