use crate::error::{CoreError, Result};
use std::sync::LazyLock;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AudioProfile {
    Pcm24000,
    Mulaw8000,
}
impl AudioProfile {
    pub fn parse(value: &str) -> Result<Self> {
        match value {
            "PCM_24000" => Ok(Self::Pcm24000),
            "MULAW_8000" => Ok(Self::Mulaw8000),
            _ => Err(CoreError::new(
                "AudioFormat",
                "Select a named AudioFormat profile",
            )),
        }
    }
}
static COEFFICIENTS: LazyLock<[f64; 63]> = LazyLock::new(|| {
    let mut result = [0.; 63];
    for (i, c) in result.iter_mut().enumerate() {
        let x = i as f64 - 31.;
        *c = (if i == 31 {
            2. * 3400. / 24000.
        } else {
            (2. * std::f64::consts::PI * 3400. / 24000. * x).sin() / (std::f64::consts::PI * x)
        }) * (0.54 - 0.46 * (2. * std::f64::consts::PI * i as f64 / 62.).cos());
    }
    let total: f64 = result.iter().sum();
    result.iter_mut().for_each(|v| *v /= total);
    result
});
fn mulaw(sample: i32) -> u8 {
    let sample = sample.clamp(-32768, 32767);
    let sign = if sample < 0 { 128 } else { 0 };
    let magnitude = sample.unsigned_abs().min(32635) + 132;
    let exponent = (32 - magnitude.leading_zeros()).saturating_sub(8);
    !(sign | (exponent << 4) | ((magnitude >> (exponent + 3)) & 15)) as u8
}
pub struct Converter {
    profile: AudioProfile,
    tail: Option<u8>,
    samples: [f64; 63],
    seen: usize,
    emitted: usize,
    input_samples: usize,
}
impl Converter {
    pub fn new(profile: AudioProfile) -> Self {
        Self {
            profile,
            tail: None,
            samples: [0.; 63],
            seen: 0,
            emitted: 0,
            input_samples: 0,
        }
    }
    fn sample(&mut self, sample: i16) -> Option<u8> {
        self.samples.rotate_right(1);
        self.samples[0] = sample as f64;
        let position = self.seen as i64 - 31;
        self.seen += 1;
        if position >= 0 && position % 3 == 0 {
            let filtered: f64 = COEFFICIENTS
                .iter()
                .zip(self.samples)
                .map(|(a, b)| a * b)
                .sum();
            self.emitted += 1;
            Some(mulaw((filtered + 0.5).floor() as i32))
        } else {
            None
        }
    }
    pub fn process(&mut self, input: &[u8], final_input: bool) -> Result<Vec<u8>> {
        let mut data = Vec::with_capacity(input.len() + 1);
        if let Some(tail) = self.tail.take() {
            data.push(tail);
        }
        data.extend_from_slice(input);
        if data.len() % 2 == 1 {
            self.tail = data.pop();
        }
        if final_input && self.tail.is_some() {
            return Err(CoreError::new(
                "AudioFormat",
                "Incomplete final PCM sample frame",
            ));
        }
        if self.profile == AudioProfile::Pcm24000 {
            return Ok(data);
        }
        let mut output = Vec::with_capacity(data.len() / 6 + 16);
        for pair in data.chunks_exact(2) {
            self.input_samples += 1;
            if let Some(value) = self.sample(i16::from_le_bytes([pair[0], pair[1]])) {
                output.push(value);
            }
        }
        if final_input {
            while self.emitted < self.input_samples.div_ceil(3) {
                if let Some(value) = self.sample(0) {
                    output.push(value);
                }
            }
        }
        Ok(output)
    }
}
