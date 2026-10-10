use crate::{
    stt::audio::{InputAudio, SOURCE_BYTES},
    ErrorKind, PcmFormat,
};
use serde_json::Value;

#[test]
fn shared_pcm_vectors_at_every_byte_split() {
    let fixtures: Value =
        serde_json::from_str(include_str!("../../../testdata/pcm-input.json")).unwrap();
    for vector in fixtures["vectors"].as_array().unwrap() {
        let format = PcmFormat {
            sample_rate: vector["sampleRate"].as_u64().unwrap() as u32,
            channels: vector["channels"].as_u64().unwrap() as u16,
        };
        let bytes = |field: &str| -> Vec<u8> {
            vector[field]
                .as_array()
                .unwrap()
                .iter()
                .flat_map(|v| (v.as_i64().unwrap() as i16).to_le_bytes())
                .collect()
        };
        let input = bytes("input");
        for size in 1..=input.len() {
            let mut converter = InputAudio::new(format);
            let output: Vec<_> = input
                .chunks(size)
                .flat_map(|part| converter.feed(part))
                .collect();
            converter.finish().unwrap();
            assert_eq!(output, bytes("output"), "{format:?}, chunk size {size}");
        }
    }
}

#[test]
fn pcm_conversion_bounds_and_truncated_frames() {
    for channels in [1, 2] {
        let mut converter = InputAudio::new(PcmFormat {
            sample_rate: 8000,
            channels,
        });
        for _ in 0..10 {
            let output = converter.feed(&vec![0; SOURCE_BYTES]);
            assert!(output.len() <= 65_536);
        }
        converter.finish().unwrap();
        converter.feed(&[0]);
        assert_eq!(
            converter.finish().unwrap_err().kind(),
            ErrorKind::AudioFormat
        );
    }
}
