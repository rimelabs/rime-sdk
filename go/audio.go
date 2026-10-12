package rime

import (
	"encoding/binary"
	"math"
	"math/bits"
)

// AudioFormat selects a mono encoding or a fixed-rate compatibility preset.
type AudioFormat uint8

const (
	// PCM is signed little-endian PCM16. SampleRate defaults to 24000.
	PCM AudioFormat = iota
	MULAW8000
	PCM24000
	WAV
	MP3
)

func (f AudioFormat) Encoding() string {
	if f == PCM || f == PCM24000 {
		return "pcm_s16le"
	}
	if f == MULAW8000 {
		return "mulaw"
	}
	if f == WAV {
		return "wav"
	}
	if f == MP3 {
		return "mp3"
	}
	return ""
}

// SampleRate is the fixed preset rate, or zero for configurable encodings.
// Use AudioStream.SampleRate for the rate selected for an operation.
func (f AudioFormat) SampleRate() int {
	if f == PCM24000 {
		return 24000
	}
	if f == MULAW8000 {
		return 8000
	}
	return 0
}
func (f AudioFormat) Channels() int {
	if f <= MP3 {
		return 1
	}
	return 0
}

func (f AudioFormat) wireFormat() string {
	if f == WAV {
		return "audio/wav"
	}
	if f == MP3 {
		return "audio/mpeg"
	}
	return "audio/pcm"
}

func resolveAudio(options SynthesisOptions) (AudioFormat, int, *float32, error) {
	format, rate := options.AudioFormat, options.SampleRate
	if format > MP3 {
		return 0, 0, nil, failure(ErrAudioFormat, "select a named AudioFormat")
	}
	if rate == 0 {
		rate = format.SampleRate()
		if rate == 0 {
			rate = 24000
		}
	}
	if rate != 8000 && rate != 16000 && rate != 24000 {
		return 0, 0, nil, failure(ErrAudioFormat, "SampleRate must be 8000, 16000, or 24000")
	}
	if format.SampleRate() != 0 && rate != format.SampleRate() {
		return 0, 0, nil, failure(ErrAudioFormat, "SampleRate conflicts with the fixed AudioFormat preset")
	}
	var factor *float32
	if options.TimeScaleFactor != nil {
		value := *options.TimeScaleFactor
		if math.IsNaN(value) || math.IsInf(value, 0) || value < 0.4 || value > 2.5 {
			return 0, 0, nil, failure(ErrInput, "TimeScaleFactor must be a finite number from 0.4 to 2.5")
		}
		converted := float32(value)
		factor = &converted
	}
	if format == PCM && rate == 24000 {
		format = PCM24000
	}
	return format, rate, factor, nil
}

var coefficients = func() [63]float64 {
	var result [63]float64
	total := 0.0
	for i := range result {
		value := 2.0 * 3400 / 24000
		if i != 31 {
			value = math.Sin(2*math.Pi*3400/24000*float64(i-31)) / (math.Pi * float64(i-31))
		}
		result[i] = value * (0.54 - 0.46*math.Cos(2*math.Pi*float64(i)/62))
		total += result[i]
	}
	for i := range result {
		result[i] /= total
	}
	return result
}()

func mulaw(sample int) byte {
	sample = max(-32768, min(32767, sample))
	sign := 0
	if sample < 0 {
		sign = 128
		sample = ^sample
	}
	magnitude := min(sample, 32635) + 132
	exponent := max(0, bits.Len(uint(magnitude))-8)
	return byte(^(sign | exponent<<4 | (magnitude>>(exponent+3))&15))
}

type converter struct {
	format                      AudioFormat
	tail                        []byte
	samples                     [63]float64
	seen, emitted, inputSamples int
}

func (c *converter) sample(sample float64) (byte, bool) {
	copy(c.samples[1:], c.samples[:62])
	c.samples[0] = sample
	position := c.seen - 31
	c.seen++
	if position >= 0 && position%3 == 0 {
		value := 0.0
		for i, coefficient := range coefficients {
			value += coefficient * c.samples[i]
		}
		c.emitted++
		return mulaw(int(math.Floor(value + 0.5))), true
	}
	return 0, false
}

func (c *converter) process(input []byte, final bool) ([]byte, error) {
	if c.format == WAV || c.format == MP3 {
		return input, nil
	}
	data := input
	if len(c.tail) != 0 {
		data = append(c.tail, input...)
	}
	size := len(data) / 2 * 2
	c.tail = append([]byte(nil), data[size:]...)
	if final && len(c.tail) != 0 {
		return nil, failure(ErrAudioFormat, "incomplete final PCM sample frame")
	}
	if c.format == PCM || c.format == PCM24000 {
		return data[:size], nil
	}
	output := make([]byte, 0, size/6+11)
	for i := 0; i < size; i += 2 {
		c.inputSamples++
		if value, ok := c.sample(float64(int16(binary.LittleEndian.Uint16(data[i:])))); ok {
			output = append(output, value)
		}
	}
	if final {
		for c.emitted < (c.inputSamples+2)/3 {
			if value, ok := c.sample(0); ok {
				output = append(output, value)
			}
		}
	}
	return output, nil
}
