//! A separate test process ensures that the workers race on the first detector call.
//! Run with ThreadSanitizer for both Rust and the vendored C++ to check publication.
use std::sync::{Arc, Barrier};

#[test]
fn concurrent_first_calls() {
    let barrier = Arc::new(Barrier::new(16));
    let workers: Vec<_> = (0..16)
        .map(|_| {
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                barrier.wait();
                for _ in 0..100 {
                    let text = "Hello. World.";
                    assert_eq!(blingfire_sys::sentence_ends(text).unwrap(), [6, text.len()]);
                }
            })
        })
        .collect();
    for worker in workers {
        worker.join().unwrap();
    }
}
