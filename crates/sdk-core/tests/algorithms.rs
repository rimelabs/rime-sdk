use sdk_core::{
    audio::{AudioProfile, Converter},
    sentences::SentenceBuffer,
};

fn unhex(text: &str) -> Vec<u8> {
    (0..text.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&text[i..i + 2], 16).unwrap())
        .collect()
}

#[test]
fn shared_sentence_contract() {
    let cases: serde_json::Value =
        serde_json::from_str(include_str!("../../../conformance/sentences.json")).unwrap();
    for case in cases.as_array().unwrap() {
        let text = case["text"].as_str().unwrap();
        let expected: Vec<String> = serde_json::from_value(case["sentences"].clone()).unwrap();
        for size in [1, 2, 7, 17, 1024] {
            let mut buffer = SentenceBuffer::new(65536);
            let mut actual = vec![];
            for chunk in text.chars().collect::<Vec<_>>().chunks(size) {
                actual.extend(
                    buffer
                        .feed(&chunk.iter().collect::<String>(), false)
                        .unwrap(),
                );
            }
            actual.extend(buffer.feed("", true).unwrap());
            assert_eq!(actual, expected, "{} / {size}", case["id"]);
        }
    }
}
#[test]
fn shared_audio_contract() {
    let case: serde_json::Value =
        serde_json::from_str(include_str!("../../../conformance/audio.json")).unwrap();
    let pcm = unhex(case["pcm_hex"].as_str().unwrap());
    let expected = unhex(case["mulaw_hex"].as_str().unwrap());
    for size in [1, 7, 32, 511, 4096] {
        let mut converter = Converter::new(AudioProfile::Mulaw8000);
        let mut actual = vec![];
        for chunk in pcm.chunks(size) {
            actual.extend(converter.process(chunk, false).unwrap());
        }
        actual.extend(converter.process(&[], true).unwrap());
        assert_eq!(actual, expected, "chunk {size}");
    }
}
