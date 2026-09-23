use crate::error::{CoreError, Result};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct Policy {
    pub sentence_bytes: usize,
    pub source_chars: usize,
    pub output_bytes: usize,
    pub output_chunk_bytes: usize,
    pub receive_bytes: usize,
    pub auth_timeout: f64,
    pub connection_timeout: f64,
    pub first_audio_timeout: f64,
    pub progress_timeout: f64,
    pub discovery_timeout: f64,
    pub cleanup_timeout: f64,
}
impl Default for Policy {
    fn default() -> Self {
        Self {
            sentence_bytes: 65536,
            source_chars: 1024,
            output_bytes: 96000,
            output_chunk_bytes: 9600,
            receive_bytes: 4194304,
            auth_timeout: 10.,
            connection_timeout: 10.,
            first_audio_timeout: 30.,
            progress_timeout: 60.,
            discovery_timeout: 10.,
            cleanup_timeout: 2.,
        }
    }
}
pub fn timeout(value: Option<f64>) -> Result<Option<f64>> {
    if value.is_some_and(|v| !v.is_finite() || v <= 0.) {
        return Err(CoreError::input(
            "timeout must be finite positive seconds or null",
        ));
    }
    Ok(value)
}
pub fn endpoint(model: &str, value: Option<&str>) -> Result<(String, String)> {
    if model != "coda" {
        return Err(CoreError::input("This SDK supports model='coda'"));
    }
    let value = value.unwrap_or("coda.api.rime.ai");
    let error = || {
        CoreError::input(
            "endpoint must be a hostname with an optional port (1-65535), without a scheme or path",
        )
    };
    let mut parts = value.split(':');
    let host = parts.next().unwrap_or_default();
    let port = parts.next();
    if parts.next().is_some()
        || host.is_empty()
        || host.len() > 253
        || host.split('.').any(|label| {
            label.is_empty()
                || label.len() > 63
                || !label.as_bytes()[0].is_ascii_alphanumeric()
                || !label.as_bytes()[label.len() - 1].is_ascii_alphanumeric()
                || !label
                    .bytes()
                    .all(|c| c.is_ascii_alphanumeric() || c == b'-')
        })
    {
        return Err(error());
    }
    let port = match port {
        None => 443,
        Some(p) if !p.is_empty() && p.len() <= 5 && p.bytes().all(|b| b.is_ascii_digit()) => {
            p.parse::<u16>().ok().filter(|p| *p > 0).ok_or_else(error)?
        }
        _ => return Err(error()),
    };
    Ok((
        format!("{}:{port}", host.to_ascii_lowercase()),
        host.to_ascii_lowercase(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn endpoints_preserve_model_and_tls_authority_rules() {
        assert_eq!(
            endpoint("coda", Some("Customer.Example")).unwrap(),
            ("customer.example:443".into(), "customer.example".into())
        );
        assert_eq!(
            endpoint("coda", Some("Customer.Example:8443")).unwrap().0,
            "customer.example:8443"
        );
        assert!(endpoint("mist", Some("customer.example")).is_err());
        for host in [
            "",
            "https://host",
            "host/path",
            "host:0",
            "host:65536",
            "host:1:2",
            "host..name",
            "host\n",
        ] {
            assert!(endpoint("coda", Some(host)).is_err());
        }
    }
}
