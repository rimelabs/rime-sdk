package rime

import "unicode/utf8"

// PronunciationEntry overrides a word or phrase with space-separated X-SAMPA.
// The service validates pronunciations and model/language support.
type PronunciationEntry struct {
	Spelling      string
	Pronunciation string
}

func snapshotLexicon(entries []PronunciationEntry) ([]PronunciationEntry, error) {
	result := append([]PronunciationEntry(nil), entries...)
	for _, entry := range result {
		if !utf8.ValidString(entry.Spelling) || !utf8.ValidString(entry.Pronunciation) {
			return nil, failure(ErrInput, "lexicon spelling and pronunciation must be valid UTF-8")
		}
	}
	return result, nil
}
