use crate::error::{CoreError, Result};

const TRIGGERS: &str = "\r\n.!?\u{01c3}\u{06d4}\u{061f}\u{2024}\u{2026}\u{203c}\u{203d}\u{2048}\u{2049}\u{2404}\u{fe52}\u{ff0e}\u{ff61}\u{3002}\u{ff1f}\u{ff01}\u{2028}\u{2029}\u{00bf}\u{00a1}";
fn whitespace(c: char) -> bool {
    c.is_whitespace() || c == '\u{feff}'
}
fn trim(text: &str) -> &str {
    text.trim_matches(whitespace)
}
fn ends(text: &str) -> Result<Vec<usize>> {
    if trim(text).is_empty() {
        return Ok(vec![]);
    }
    blingfire_sys::sentence_ends(text).map_err(CoreError::input)
}
pub struct SentenceBuffer {
    pending: String,
    scans: usize,
    high: Option<u16>,
    committed: usize,
    until_scan: usize,
    since_punctuation: usize,
    limit: usize,
}
impl SentenceBuffer {
    pub fn new(limit: usize) -> Self {
        Self {
            pending: String::new(),
            scans: 0,
            high: None,
            committed: 0,
            until_scan: 1024,
            since_punctuation: 16,
            limit,
        }
    }
    pub fn feed(&mut self, text: &str, final_input: bool) -> Result<Vec<String>> {
        let mut output = vec![];
        for c in text.chars() {
            self.pending.push(c);
            self.until_scan -= 1;
            self.since_punctuation = (self.since_punctuation + 1).min(16);
            if TRIGGERS.contains(c) {
                self.since_punctuation = 0;
                self.until_scan = self.until_scan.min(16);
            }
            if self.until_scan == 0 {
                self.scan(false, &mut output)?;
            }
        }
        if final_input {
            self.scan(true, &mut output)?;
        }
        Ok(output)
    }
    fn scan(&mut self, final_input: bool, output: &mut Vec<String>) -> Result<()> {
        if self.pending.is_empty() {
            return Ok(());
        }
        self.scans += 1;
        let mut offsets = ends(&self.pending)?;
        if final_input && let Some(last) = offsets.last_mut() {
            *last = self.pending.len();
        }
        let mut start = self.committed;
        for end in offsets
            .iter()
            .copied()
            .chain(std::iter::once(self.pending.len()))
        {
            if end <= start {
                continue;
            }
            if end - start > self.limit {
                return Err(CoreError::new(
                    "ResourceLimit",
                    "Sentence exceeds the supported byte limit",
                ));
            }
            start = end;
        }
        for (i, &end) in offsets.iter().enumerate() {
            if end <= self.committed {
                continue;
            }
            if !final_input
                && (i + 1 == offsets.len() || trim(&self.pending[end..]).chars().count() < 16)
            {
                continue;
            }
            let sentence = &self.pending[self.committed..end];
            self.committed = end;
            if !trim(sentence).is_empty() {
                output.push(sentence.to_owned());
            }
        }
        if final_input {
            let residual = &self.pending[self.committed..];
            if !trim(residual).is_empty() {
                output.push(residual.to_owned());
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
        for end in offsets {
            if self.committed < end && end < self.pending.len() {
                let needed = 16usize
                    .saturating_sub(
                        self.pending[end..]
                            .trim_start_matches(whitespace)
                            .chars()
                            .count(),
                    )
                    .max(1);
                self.until_scan = self.until_scan.min(needed);
                break;
            }
        }
        let keep = self.pending[..self.committed]
            .char_indices()
            .rev()
            .nth(127)
            .map_or(0, |(i, _)| i);
        self.pending.drain(..keep);
        self.committed -= keep;
        Ok(())
    }
    pub fn scans(&self) -> usize {
        self.scans
    }
    pub fn feed_utf16(&mut self, mut units: Vec<u16>, final_input: bool) -> Result<Vec<String>> {
        if let Some(high) = self.high.take() {
            units.insert(0, high);
        }
        if !final_input && units.last().is_some_and(|c| (0xd800..=0xdbff).contains(c)) {
            self.high = units.pop();
        }
        let text = String::from_utf16(&units)
            .map_err(|_| CoreError::input("Text contains an unpaired UTF-16 surrogate"))?;
        self.feed(&text, final_input)
    }
    pub fn retained_bytes(&self) -> usize {
        self.pending.len()
    }
}
