package rime

import (
	"context"
	"encoding/binary"
)

func resolvePCMFormat(format PCMFormat) (PCMFormat, error) {
	if format.SampleRate == 0 {
		format.SampleRate = 16000
	}
	if format.Channels == 0 {
		format.Channels = 1
	}
	if (format.SampleRate != 8000 && format.SampleRate != 16000 && format.SampleRate != 24000 && format.SampleRate != 48000) || (format.Channels != 1 && format.Channels != 2) {
		return format, failure(ErrAudioFormat, "use PCM16 at 8, 16, 24 or 48 kHz, with one or two channels")
	}
	return format, nil
}

// PCM input uses streaming linear interpolation and floor rounding. State and
// incomplete frames persist across arbitrary source chunk boundaries.
type pcmInput struct {
	format          PCMFormat
	phase, previous int
	pending         []byte
}

func newPCMInput(format PCMFormat) *pcmInput { return &pcmInput{format: format, phase: -16000} }
func floorDivide(value, divisor int) int {
	quotient := value / divisor
	if value < 0 && value%divisor != 0 {
		quotient--
	}
	return quotient
}
func (input *pcmInput) feed(ctx context.Context, data []byte, emit func([]byte) error) error {
	const portionBytes = 16384
	frameBytes := input.format.Channels * 2
	for len(data) > 0 {
		if ctx.Err() != nil {
			return context.Cause(ctx)
		}
		count := min(len(data), portionBytes)
		input.pending = append(input.pending, data[:count]...)
		data = data[count:]
		complete := len(input.pending) / frameBytes * frameBytes
		outputSamples := (complete/frameBytes*16000 + input.format.SampleRate - 1) / input.format.SampleRate
		output := make([]byte, 0, outputSamples*2)
		for offset := 0; offset < complete; offset += frameBytes {
			current := int(int16(binary.LittleEndian.Uint16(input.pending[offset:])))
			if input.format.Channels == 2 {
				current = floorDivide(current+int(int16(binary.LittleEndian.Uint16(input.pending[offset+2:]))), 2)
			}
			input.phase += 16000
			for input.phase >= 0 {
				sample := floorDivide(input.previous*input.phase+current*(16000-input.phase), 16000)
				output = binary.LittleEndian.AppendUint16(output, uint16(int16(sample)))
				input.phase -= input.format.SampleRate
			}
			input.previous = current
		}
		remaining := copy(input.pending, input.pending[complete:])
		input.pending = input.pending[:remaining]
		if len(output) != 0 {
			if err := emit(output); err != nil {
				return err
			}
		}
	}
	return nil
}
func (input *pcmInput) finish() error {
	if len(input.pending) != 0 {
		return failure(ErrAudioFormat, "audio ended with an incomplete PCM frame")
	}
	return nil
}
