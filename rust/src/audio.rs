use crate::{Error, ErrorKind};
use std::sync::LazyLock;

/// Raw mono audio profile. Neither profile contains a file header.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum AudioFormat {
    /// Signed 16-bit little-endian PCM, 24,000 samples per second.
    #[default]
    Pcm24000,
    /// G.711 mu-law, 8,000 samples per second.
    Mulaw8000,
}

impl AudioFormat {
    /// Samples per second.
    pub fn sample_rate(self) -> u32 {
        match self {
            Self::Pcm24000 => 24_000,
            Self::Mulaw8000 => 8_000,
        }
    }
    /// Number of audio channels.
    pub fn channels(self) -> u16 {
        1
    }
    /// Encoding identifier used by the other Rime SDKs.
    pub fn encoding(self) -> &'static str {
        match self {
            Self::Pcm24000 => "pcm_s16le",
            Self::Mulaw8000 => "mulaw",
        }
    }
}

static COEFFICIENTS: LazyLock<[f64; 63]> = LazyLock::new(|| {
    let mut coefficients = [0.0; 63];
    for (index, coefficient) in coefficients.iter_mut().enumerate() {
        let offset = index as f64 - 31.0;
        let value = if index == 31 {
            2.0 * 3400.0 / 24000.0
        } else {
            (2.0 * std::f64::consts::PI * 3400.0 / 24000.0 * offset).sin()
                / (std::f64::consts::PI * offset)
        };
        *coefficient =
            value * (0.54 - 0.46 * (2.0 * std::f64::consts::PI * index as f64 / 62.0).cos());
    }
    let total: f64 = coefficients.iter().sum();
    for coefficient in &mut coefficients {
        *coefficient /= total;
    }
    coefficients
});

fn mulaw(sample: i32) -> u8 {
    let mut sample = sample.clamp(-32768, 32767);
    let sign = if sample < 0 {
        sample = !sample;
        128
    } else {
        0
    };
    let magnitude = sample.min(32635) + 132;
    let exponent = (32 - magnitude.leading_zeros()).saturating_sub(8);
    !(sign | (exponent as i32) << 4 | (magnitude >> (exponent + 3)) & 15) as u8
}

pub(crate) struct Converter {
    format: AudioFormat,
    tail: Option<u8>,
    samples: [f64; 63],
    seen: u64,
    emitted: u64,
    input_samples: u64,
}

impl Converter {
    pub(crate) fn new(format: AudioFormat) -> Self {
        Self {
            format,
            tail: None,
            samples: [0.0; 63],
            seen: 0,
            emitted: 0,
            input_samples: 0,
        }
    }
    fn sample(&mut self, sample: f64, output: &mut Vec<u8>) {
        self.samples.copy_within(..62, 1);
        self.samples[0] = sample;
        let position = self.seen as i64 - 31;
        self.seen += 1;
        if position >= 0 && position % 3 == 0 {
            let value: f64 = COEFFICIENTS
                .iter()
                .zip(self.samples)
                .map(|(coefficient, sample)| coefficient * sample)
                .sum();
            output.push(mulaw((value + 0.5).floor() as i32));
            self.emitted += 1;
        }
    }
    pub(crate) fn process(&mut self, input: &[u8], final_chunk: bool) -> Result<Vec<u8>, Error> {
        let mut data = Vec::with_capacity(input.len() + 1);
        data.extend(self.tail.take());
        data.extend_from_slice(input);
        if data.len() % 2 != 0 {
            self.tail = data.pop();
        }
        if final_chunk && self.tail.is_some() {
            return Err(Error::new(
                ErrorKind::AudioFormat,
                "incomplete final PCM sample frame",
            ));
        }
        if self.format == AudioFormat::Pcm24000 {
            return Ok(data);
        }
        let mut output = Vec::with_capacity(data.len() / 6 + 11);
        for sample in data.as_chunks::<2>().0 {
            self.input_samples += 1;
            self.sample(i16::from_le_bytes(*sample) as f64, &mut output);
        }
        if final_chunk {
            while self.emitted < self.input_samples.div_ceil(3) {
                self.sample(0.0, &mut output);
            }
        }
        Ok(output)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sha2::{Digest, Sha256};

    #[test]
    fn shared_audio_vectors_across_chunk_boundaries() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../testdata/audio.json")).unwrap();
        let input = hex::decode(fixture["pcm_hex"].as_str().unwrap()).unwrap();
        for format in [AudioFormat::Pcm24000, AudioFormat::Mulaw8000] {
            for size in fixture["chunk_bytes"].as_array().unwrap() {
                let mut converter = Converter::new(format);
                let mut output = Vec::new();
                for chunk in input.chunks(size.as_u64().unwrap() as usize) {
                    output.extend(converter.process(chunk, false).unwrap());
                }
                output.extend(converter.process(&[], true).unwrap());
                let expected = if format == AudioFormat::Pcm24000 {
                    "pcm_hex"
                } else {
                    "mulaw_hex"
                };
                assert_eq!(hex::encode(output), fixture[expected].as_str().unwrap());
            }
        }
    }

    #[test]
    fn all_mulaw_inputs_match_reference() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../testdata/mulaw.json")).unwrap();
        let bytes: Vec<_> = (-32768..=32767).map(mulaw).collect();
        assert_eq!(
            hex::encode(Sha256::digest(bytes)),
            fixture["all_inputs_sha256"].as_str().unwrap()
        );
    }

    #[test]
    fn incomplete_sample_is_an_error() {
        for format in [AudioFormat::Pcm24000, AudioFormat::Mulaw8000] {
            assert_eq!(
                Converter::new(format)
                    .process(&[1], true)
                    .unwrap_err()
                    .kind(),
                ErrorKind::AudioFormat
            );
        }
    }
}
