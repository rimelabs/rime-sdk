//! The only unsafe detector seam. Caller buffers are owned and length-checked.
use std::ffi::{c_char, c_int};
use std::sync::Once;

static INITIALIZE: Once = Once::new();

unsafe extern "C" {
    fn TextToSentencesWithOffsets(
        input: *const c_char,
        length: c_int,
        output: *mut c_char,
        start_offsets: *mut c_int,
        end_offsets: *mut c_int,
        capacity: c_int,
    ) -> c_int;
}

/// Exclusive UTF-8 byte ends in the original text, before detector normalization.
pub fn sentence_ends(text: &str) -> Result<Vec<usize>, &'static str> {
    let capacity = text
        .len()
        .checked_mul(3)
        .and_then(|n| n.checked_add(16))
        .filter(|n| *n <= c_int::MAX as usize)
        .ok_or("Sentence input is too large")?;
    INITIALIZE.call_once(|| {
        // The vendored detector's double-checked initialization uses a non-atomic
        // flag. All calls must pass this Once before reading that flag or model.
        // SAFETY: a zero-length call initializes the model, then returns before
        // accessing input or output buffers in the pinned implementation.
        unsafe {
            TextToSentencesWithOffsets(
                c"".as_ptr(),
                0,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                0,
            );
        }
    });
    let mut output = vec![0u8; capacity];
    // BlingFire initializes capacity integers even when it finds fewer sentences.
    let mut ends = vec![0 as c_int; capacity];
    // SAFETY: buffers stay alive for this synchronous call and lengths fit c_int.
    // Output has capacity bytes and ends has capacity integers. Start offsets
    // are optional; BlingFire checks for null before writing them.
    let size = unsafe {
        TextToSentencesWithOffsets(
            text.as_ptr().cast(),
            text.len() as c_int,
            output.as_mut_ptr().cast(),
            std::ptr::null_mut(),
            ends.as_mut_ptr(),
            capacity as c_int,
        )
    };
    if size < 0 || size as usize > capacity {
        return Err("Sentence detection failed");
    }
    output.truncate(size as usize);
    while output.last() == Some(&0) {
        output.pop();
    }
    // The return value is the output byte count, not the sentence count.
    // BlingFire separates sentences with newlines and replaces embedded newlines.
    let count = if output.is_empty() {
        0
    } else {
        output.iter().filter(|&&b| b == b'\n').count() + 1
    };
    let mut result = Vec::with_capacity(count);
    let mut previous = 0;
    for &inclusive in &ends[..count] {
        let end = usize::try_from(inclusive)
            .ok()
            .and_then(|n| n.checked_add(1))
            .filter(|&n| n > previous && text.is_char_boundary(n))
            .ok_or("Sentence detector returned invalid source offsets")?;
        result.push(end);
        previous = end;
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::sentence_ends;

    #[test]
    fn offsets_use_original_utf8_bytes() {
        for text in ["A", "é", "🚀"] {
            assert_eq!(sentence_ends(text).unwrap(), vec![text.len()]);
        }
        for text in ["Hello. \u{2060}Next.", "Hello. \0Next."] {
            assert_eq!(sentence_ends(text).unwrap(), vec![6, text.len()]);
        }
    }

    #[test]
    fn empty_and_whitespace_have_no_sentences() {
        for text in ["", " ", "\n\t\r"] {
            assert!(sentence_ends(text).unwrap().is_empty());
        }
    }
}
