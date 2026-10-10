use crate::{Error, ErrorKind};

/// Headerless signed little-endian PCM16 input, fixed for an utterance.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PcmFormat {
    /// Frames per second: 8000, 16000, 24000, or 48000.
    pub sample_rate: u32,
    /// One for mono, two for interleaved stereo.
    pub channels: u16,
}
impl Default for PcmFormat {
    fn default() -> Self {
        Self {
            sample_rate: 16_000,
            channels: 1,
        }
    }
}
impl PcmFormat {
    pub(super) fn validate(self) -> Result<(), Error> {
        if !matches!(self.sample_rate, 8000 | 16000 | 24000 | 48000)
            || !matches!(self.channels, 1 | 2)
        {
            return Err(Error::new(
                ErrorKind::AudioFormat,
                "use PCM16 at 8, 16, 24 or 48 kHz, with one or two channels",
            ));
        }
        Ok(())
    }
}

// Matches the shared audioop vectors, including floor rounding for negative values.
// Feed at most SOURCE_BYTES at a time so conversion allocations stay bounded.
pub(super) const SOURCE_BYTES: usize = 16_384;
pub(super) struct InputAudio {
    format: PcmFormat,
    pending: Vec<u8>,
    phase: i32,
    previous: i32,
}
impl InputAudio {
    pub fn new(format: PcmFormat) -> Self {
        Self {
            format,
            pending: Vec::new(),
            phase: -16_000,
            previous: 0,
        }
    }
    pub fn feed(&mut self, data: &[u8]) -> Vec<u8> {
        debug_assert!(data.len() <= SOURCE_BYTES);
        self.pending.extend_from_slice(data);
        let frame = self.format.channels as usize * 2;
        let complete = self.pending.len() / frame * frame;
        let mut output = Vec::new();
        for sample in self.pending[..complete].chunks_exact(frame) {
            let mut current = i16::from_le_bytes([sample[0], sample[1]]) as i32;
            if frame == 4 {
                current =
                    (current + i16::from_le_bytes([sample[2], sample[3]]) as i32).div_euclid(2);
            }
            self.phase += 16_000;
            while self.phase >= 0 {
                let value = (self.previous * self.phase + current * (16_000 - self.phase))
                    .div_euclid(16_000);
                output.extend_from_slice(&(value as i16).to_le_bytes());
                self.phase -= self.format.sample_rate as i32;
            }
            self.previous = current;
        }
        self.pending.drain(..complete);
        output
    }
    pub fn finish(&self) -> Result<(), Error> {
        if !self.pending.is_empty() {
            return Err(Error::new(
                ErrorKind::AudioFormat,
                "audio ended with an incomplete PCM16 frame",
            ));
        }
        Ok(())
    }
}
