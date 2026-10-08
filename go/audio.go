package rime

import (
	"encoding/binary"
	"math"
	"math/bits"
)

// AudioFormat selects raw mono audio. Neither profile includes a file header.
type AudioFormat uint8

const (
	PCM24000 AudioFormat = iota
	MULAW8000
)

func (f AudioFormat) Encoding() string {
	if f == PCM24000 {
		return "pcm_s16le"
	}
	if f == MULAW8000 {
		return "mulaw"
	}
	return ""
}
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
	if f <= MULAW8000 {
		return 1
	}
	return 0
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
	data := input
	if len(c.tail) != 0 {
		data = append(c.tail, input...)
	}
	size := len(data) / 2 * 2
	c.tail = append([]byte(nil), data[size:]...)
	if final && len(c.tail) != 0 {
		return nil, failure(ErrAudioFormat, "incomplete final PCM sample frame")
	}
	if c.format == PCM24000 {
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
