/// A word or phrase and its space-separated X-SAMPA pronunciation.
///
/// The service validates pronunciations, entry limits, and model/language support.
/// Entries are passed unchanged, including their order, case, and whitespace.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PronunciationEntry {
    /// The word or phrase to override.
    pub spelling: String,
    /// Its space-separated X-SAMPA pronunciation.
    pub pronunciation: String,
}

impl PronunciationEntry {
    /// Create an override. Rust strings guarantee valid UTF-8; linguistic
    /// validation belongs to the service.
    pub fn new(spelling: impl Into<String>, pronunciation: impl Into<String>) -> Self {
        Self {
            spelling: spelling.into(),
            pronunciation: pronunciation.into(),
        }
    }
}
