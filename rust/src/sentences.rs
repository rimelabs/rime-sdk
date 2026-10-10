//! Private sentence detector. All text offsets are UTF-8 byte offsets; scan
//! intervals and retained context count Unicode scalar values.
use crate::{Error, ErrorKind};
use std::sync::{Arc, OnceLock};
use wasmi::{Config, Engine, Instance, Linker, Module, Store, StoreLimits, StoreLimitsBuilder};
use wasmi_wasi::{WasiCtx, WasiCtxBuilder};

const FUEL: u64 = 100_000_000;
const SENTENCE_LIMIT: usize = 65_536;
const TRIGGERS: &str = "\r\n.!?\u{01c3}\u{06d4}\u{061f}\u{2024}\u{2026}\u{203c}\u{203d}\u{2048}\u{2049}\u{2404}\u{fe52}\u{ff0e}\u{ff61}\u{3002}\u{ff1f}\u{ff01}\u{2028}\u{2029}\u{00bf}\u{00a1}";

struct Host {
    wasi: WasiCtx,
    limits: StoreLimits,
}
struct Detector {
    store: Store<Host>,
    instance: Instance,
}

fn detector_error(error: impl std::fmt::Display) -> Error {
    Error::new(ErrorKind::Input, "sentence detection failed")
        .caused_by(std::io::Error::other(error.to_string()))
}

impl Detector {
    fn new() -> Result<Self, Error> {
        static COMPILED: OnceLock<Result<(Engine, Arc<Module>), Error>> = OnceLock::new();
        let (engine, module) = COMPILED
            .get_or_init(|| {
                let mut configuration = Config::default();
                configuration.consume_fuel(true);
                let engine = Engine::new(&configuration);
                let module = Module::new(&engine, include_bytes!("../vendor/blingfire.wasm"))
                    .map_err(detector_error)?;
                Ok((engine, Arc::new(module)))
            })
            .as_ref()
            .map_err(Clone::clone)?;
        let mut linker = Linker::<Host>::new(engine);
        wasmi_wasi::add_to_linker(&mut linker, |host| &mut host.wasi).map_err(detector_error)?;
        linker
            .func_wrap("env", "emscripten_notify_memory_growth", |_: i32| {})
            .map_err(detector_error)?;
        let mut store = Store::new(
            engine,
            Host {
                wasi: WasiCtxBuilder::new().build(),
                limits: StoreLimitsBuilder::new()
                    .memory_size(64 * 1024 * 1024)
                    .build(),
            },
        );
        store.limiter(|host| &mut host.limits);
        store.set_fuel(FUEL).map_err(detector_error)?;
        let instance = linker
            .instantiate_and_start(&mut store, module)
            .map_err(detector_error)?;
        instance
            .get_typed_func::<(), ()>(&store, "_initialize")
            .map_err(detector_error)?
            .call(&mut store, ())
            .map_err(detector_error)?;
        Ok(Self { store, instance })
    }

    fn ends(&mut self, text: &str) -> Result<Vec<usize>, Error> {
        if text.trim().is_empty() {
            return Ok(Vec::new());
        }
        self.store.set_fuel(FUEL).map_err(detector_error)?;
        let allocate = self
            .instance
            .get_typed_func::<i32, i32>(&self.store, "malloc")
            .map_err(detector_error)?;
        let free = self
            .instance
            .get_typed_func::<i32, ()>(&self.store, "free")
            .map_err(detector_error)?;
        let detect = self
            .instance
            .get_typed_func::<(i32, i32, i32, i32), i32>(&self.store, "TextToSentences")
            .map_err(detector_error)?;
        let memory = self
            .instance
            .get_memory(&self.store, "memory")
            .ok_or_else(|| detector_error("missing WASM memory"))?;
        let capacity = text.len() * 3 + 16;
        let source = allocate
            .call(&mut self.store, (text.len() + 1) as i32)
            .map_err(detector_error)?;
        let output = match allocate.call(&mut self.store, capacity as i32) {
            Ok(output) => output,
            Err(error) => {
                let _ = free.call(&mut self.store, source);
                return Err(detector_error(error));
            }
        };
        let result = (|| {
            if source <= 0 || output <= 0 {
                return Err(detector_error("WASM allocation failed"));
            }
            memory
                .write(&mut self.store, source as usize, text.as_bytes())
                .map_err(detector_error)?;
            memory
                .write(&mut self.store, source as usize + text.len(), &[0])
                .map_err(detector_error)?;
            let length = detect
                .call(
                    &mut self.store,
                    (source, text.len() as i32, output, capacity as i32),
                )
                .map_err(detector_error)?;
            if length < 0 || length as usize > capacity {
                return Err(detector_error("invalid sentence output length"));
            }
            let mut bytes = vec![0; length as usize];
            memory
                .read(&self.store, output as usize, &mut bytes)
                .map_err(detector_error)?;
            let normalized = std::str::from_utf8(&bytes)
                .map_err(detector_error)?
                .trim_end_matches('\0');
            let mut position = 0;
            let mut ends = Vec::new();
            for sentence in normalized
                .split('\n')
                .filter(|sentence| !sentence.trim().is_empty())
            {
                for character in sentence
                    .chars()
                    .filter(|character| !character.is_whitespace())
                {
                    while let Some(original) = text[position..].chars().next() {
                        if original == character
                            || !(original.is_whitespace()
                                || "\u{200b}\u{200e}\u{200f}\u{feff}".contains(original))
                        {
                            break;
                        }
                        position += original.len_utf8();
                    }
                    if !text[position..].starts_with(character) {
                        return Err(detector_error("could not preserve source offsets"));
                    }
                    position += character.len_utf8();
                }
                ends.push(position);
            }
            Ok(ends)
        })();
        let _ = free.call(&mut self.store, source);
        let _ = free.call(&mut self.store, output);
        result
    }
}

pub(crate) struct Buffer {
    detector: Detector,
    pending: String,
    committed: usize,
    until_scan: usize,
    since_punctuation: usize,
}

impl Buffer {
    pub(crate) fn new() -> Result<Self, Error> {
        Ok(Self {
            detector: Detector::new()?,
            pending: String::new(),
            committed: 0,
            until_scan: 1024,
            since_punctuation: 16,
        })
    }

    pub(crate) fn feed(&mut self, fragment: &str, final_chunk: bool) -> Result<Vec<String>, Error> {
        let mut sentences = Vec::new();
        for character in fragment.chars() {
            self.pending.push(character);
            self.until_scan -= 1;
            self.since_punctuation = (self.since_punctuation + 1).min(16);
            if TRIGGERS.contains(character) {
                self.since_punctuation = 0;
                self.until_scan = self.until_scan.min(16);
            }
            if self.until_scan == 0 {
                self.scan(false, &mut sentences)?;
            }
        }
        if final_chunk {
            self.scan(true, &mut sentences)?;
        }
        Ok(sentences)
    }

    fn scan(&mut self, final_chunk: bool, sentences: &mut Vec<String>) -> Result<(), Error> {
        if self.pending.is_empty() {
            return Ok(());
        }
        let mut ends = self.detector.ends(&self.pending)?;
        if final_chunk {
            if let Some(end) = ends.last_mut() {
                *end = self.pending.len();
            }
        }
        let mut start = self.committed;
        for end in ends
            .iter()
            .copied()
            .chain(std::iter::once(self.pending.len()))
        {
            if end <= start {
                continue;
            }
            if end - start > SENTENCE_LIMIT {
                return Err(Error::new(
                    ErrorKind::ResourceLimit,
                    "sentence exceeds the supported byte limit",
                ));
            }
            start = end;
        }
        for (index, end) in ends.iter().copied().enumerate() {
            if !final_chunk
                && (index == ends.len() - 1 || self.pending[end..].trim().chars().count() < 16)
            {
                continue;
            }
            if end <= self.committed {
                continue;
            }
            let sentence = &self.pending[self.committed..end];
            if !sentence.trim().is_empty() {
                sentences.push(sentence.to_owned());
            }
            self.committed = end;
        }
        if final_chunk {
            let remaining = &self.pending[self.committed..];
            if !remaining.trim().is_empty() {
                sentences.push(remaining.to_owned());
            }
            self.pending.clear();
            self.committed = 0;
            self.until_scan = 1024;
            self.since_punctuation = 16;
            return Ok(());
        }
        self.until_scan = if self.since_punctuation < 16 {
            16
        } else {
            1024
        };
        for end in ends {
            if self.committed < end && end < self.pending.len() {
                self.until_scan = self.until_scan.min(
                    16_usize
                        .saturating_sub(self.pending[end..].trim_start().chars().count())
                        .max(1),
                );
                break;
            }
        }
        let context_start = self.pending[..self.committed]
            .char_indices()
            .rev()
            .nth(127)
            .map_or(0, |(index, _)| index);
        self.pending = self.pending[context_start..].to_owned();
        self.committed -= context_start;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shared_sentences_preserve_text_at_all_chunk_sizes() {
        let fixtures: serde_json::Value =
            serde_json::from_str(include_str!("../testdata/sentences.json")).unwrap();
        let contract: serde_json::Value =
            serde_json::from_str(include_str!("../testdata/contract.json")).unwrap();
        for fixture in fixtures.as_array().unwrap() {
            for size in contract["chunk_sizes"].as_array().unwrap() {
                let mut buffer = Buffer::new().unwrap();
                let characters: Vec<_> = fixture["text"].as_str().unwrap().chars().collect();
                let mut sentences = Vec::new();
                for chunk in characters.chunks(size.as_u64().unwrap() as usize) {
                    sentences.extend(
                        buffer
                            .feed(&chunk.iter().collect::<String>(), false)
                            .unwrap(),
                    );
                }
                sentences.extend(buffer.feed("", true).unwrap());
                assert_eq!(
                    serde_json::json!(sentences),
                    fixture["sentences"],
                    "{} chunk {size}",
                    fixture["id"]
                );
            }
        }
    }

    #[test]
    fn sentence_limits_count_utf8_bytes() {
        let mut buffer = Buffer::new().unwrap();
        assert_eq!(
            buffer.feed(&"界".repeat(22_000), true).unwrap_err().kind(),
            ErrorKind::ResourceLimit
        );
    }
}
