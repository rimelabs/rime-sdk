//! HTTP CONNECT support with the same lowercase proxy variables as gRPC hosts.
use base64::Engine;
use std::io;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

#[derive(Clone)]
pub(crate) struct Proxy {
    address: String,
    authorization: Option<String>,
}
impl Proxy {
    pub(crate) fn from_env(target: &str) -> Option<Self> {
        let bypass = std::env::var("no_grpc_proxy")
            .or_else(|_| std::env::var("no_proxy"))
            .unwrap_or_default();
        if bypasses_proxy(target, &bypass) {
            return None;
        }
        let value = ["grpc_proxy", "https_proxy", "http_proxy"]
            .iter()
            .find_map(|name| std::env::var(name).ok().filter(|v| !v.is_empty()))?;
        Self::parse(&value)
    }
    fn parse(value: &str) -> Option<Self> {
        let url = reqwest::Url::parse(value).ok()?;
        if url.scheme() != "http" {
            return None;
        }
        let address = format!("{}:{}", url.host_str()?, url.port().unwrap_or(80));
        let authorization = if url.username().is_empty() {
            None
        } else {
            let encoded = format!("{}:{}", url.username(), url.password().unwrap_or_default());
            let raw = percent_encoding::percent_decode_str(&encoded)
                .decode_utf8()
                .ok()?;
            Some(base64::engine::general_purpose::STANDARD.encode(raw.as_bytes()))
        };
        Some(Self {
            address,
            authorization,
        })
    }
    pub(crate) async fn connect(&self, target: &str) -> io::Result<TcpStream> {
        let mut stream = TcpStream::connect(&self.address).await?;
        let mut request = format!("CONNECT {target} HTTP/1.1\r\nHost: {target}\r\n");
        if let Some(auth) = &self.authorization {
            request.push_str(&format!("Proxy-Authorization: Basic {auth}\r\n"));
        }
        request.push_str("\r\n");
        stream.write_all(request.as_bytes()).await?;
        // Do not consume bytes from the TLS stream following the HTTP response.
        let mut header = Vec::new();
        while !header.ends_with(b"\r\n\r\n") {
            if header.len() >= 16384 {
                return Err(io::Error::other("Proxy response exceeds limit"));
            }
            header.push(stream.read_u8().await?);
        }
        let line =
            std::str::from_utf8(&header).map_err(|_| io::Error::other("Invalid proxy response"))?;
        if line
            .lines()
            .next()
            .and_then(|l| l.split_whitespace().nth(1))
            != Some("200")
        {
            return Err(io::Error::other("Proxy connection rejected"));
        }
        Ok(stream)
    }
}

fn bypasses_proxy(target: &str, bypass: &str) -> bool {
    let host = target.split(':').next().unwrap_or(target);
    let ip = host.parse::<std::net::IpAddr>().ok();
    bypass.split(',').any(|item| {
        let item = item.trim().trim_start_matches('.');
        !item.is_empty()
            && (item == "*"
                || host == item
                || host.ends_with(&format!(".{item}"))
                || ip.is_some_and(|ip| {
                    item.parse::<ipnet::IpNet>()
                        .is_ok_and(|network| network.contains(&ip))
                }))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn proxy_exclusions_match_subnets_and_hostnames() {
        for (target, bypass, expected) in [
            ("127.0.0.1:443", "127.0.0.0/8", true),
            ("128.0.0.1:443", "127.0.0.0/8", false),
            ("10.1.2.0:443", "10.1.2.0/24", true),
            ("10.1.2.255:443", "10.1.2.0/24", true),
            ("10.1.3.0:443", "10.1.2.0/24", false),
            ("127.0.0.1:443", "0.0.0.0/0", true),
            ("127.0.0.1:443", "127.0.0.1/32", true),
            ("127.0.0.2:443", "127.0.0.1/32", false),
            ("127.0.0.1:443", "127.0.0.1/33", false),
            ("127.0.0.1:443", "127.0.0.1/bad", false),
            ("localhost:443", "127.0.0.0/8", false),
            ("127.0.0.1:443", "example.com, 127.0.0.0/8", true),
            ("api.example.com:443", ".example.com", true),
            ("example.com:443", "example.com", true),
            ("otherexample.com:443", "example.com", false),
            ("127.0.0.1:443", "127.0.0.1", true),
            ("api.example.com:443", "*", true),
            ("api.example.com:443", " , ", false),
        ] {
            assert_eq!(
                bypasses_proxy(target, bypass),
                expected,
                "{target}, {bypass}"
            );
        }
    }

    #[tokio::test]
    async fn connect_preserves_tunnel_bytes_and_encodes_credentials() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let proxy = Proxy::parse(&format!(
            "http://user:pass@{}",
            listener.local_addr().unwrap()
        ))
        .unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut header = vec![];
            while !header.ends_with(b"\r\n\r\n") {
                header.push(socket.read_u8().await.unwrap());
            }
            let header = String::from_utf8(header).unwrap();
            assert!(header.starts_with("CONNECT service:443 HTTP/1.1"));
            assert!(header.contains("Basic dXNlcjpwYXNz"));
            socket
                .write_all(b"HTTP/1.1 200 Connection established\r\n\r\ntunnel")
                .await
                .unwrap();
        });
        let mut socket = proxy.connect("service:443").await.unwrap();
        let mut data = vec![];
        socket.read_to_end(&mut data).await.unwrap();
        assert_eq!(data, b"tunnel");
        server.await.unwrap();
    }
}
