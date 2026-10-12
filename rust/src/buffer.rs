use crate::{Error, ErrorKind};

pub(crate) const MAX_BUFFER_BYTES: usize = 64 * 1024 * 1024;

/// Validate mono PCM16 WAV and replace unknown streaming chunk lengths.
pub(crate) fn finalize_wav(data: &mut [u8], sample_rate: u32) -> Result<(), Error> {
    let invalid = || {
        Error::new(
            ErrorKind::AudioFormat,
            "expected a complete mono PCM16 WAV at the requested sample rate",
        )
    };
    if data.len() < 12 || &data[..4] != b"RIFF" || &data[8..12] != b"WAVE" {
        return Err(invalid());
    }
    let riff_size = u32::try_from(data.len() - 8)
        .map_err(|_| Error::new(ErrorKind::AudioFormat, "audio exceeds the WAV size limit"))?;
    if ![u32::MAX, riff_size].contains(&read_u32(data, 4)) {
        return Err(invalid());
    }
    let mut offset = 12;
    let mut found_format = false;
    let mut data_size_field = None;
    while offset < data.len() {
        if data.len() - offset < 8 {
            return Err(invalid());
        }
        let name = &data[offset..offset + 4];
        let mut size = read_u32(data, offset + 4) as usize;
        let start = offset + 8;
        if name == b"data" && size == u32::MAX as usize {
            size = data.len() - start;
        }
        if size > data.len() - start {
            return Err(invalid());
        }
        let end = start + size;
        if name == b"fmt " {
            if found_format || size < 16 {
                return Err(invalid());
            }
            if read_u16(data, start) != 1
                || read_u16(data, start + 2) != 1
                || read_u32(data, start + 4) != sample_rate
                || read_u32(data, start + 8) != sample_rate * 2
                || read_u16(data, start + 12) != 2
                || read_u16(data, start + 14) != 16
            {
                return Err(invalid());
            }
            found_format = true;
        } else if name == b"data" {
            if !found_format || data_size_field.is_some() || !size.is_multiple_of(2) {
                return Err(invalid());
            }
            data_size_field = Some((offset + 4, size as u32));
        }
        offset = end + size % 2;
    }
    if offset != data.len() {
        return Err(invalid());
    }
    let (data_size_offset, data_size) = data_size_field.ok_or_else(invalid)?;
    data[4..8].copy_from_slice(&riff_size.to_le_bytes());
    data[data_size_offset..data_size_offset + 4].copy_from_slice(&data_size.to_le_bytes());
    Ok(())
}

fn read_u16(data: &[u8], offset: usize) -> u16 {
    u16::from_le_bytes(
        data[offset..offset + 2]
            .try_into()
            .expect("validated WAV field"),
    )
}
fn read_u32(data: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes(
        data[offset..offset + 4]
            .try_into()
            .expect("validated WAV field"),
    )
}
