use crate::{Client, ErrorKind, RealtimeConnectOptions};
use std::time::Duration;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

#[tokio::test]
async fn http_refusals_preserve_category_and_diagnostics() {
    for (status, body, kind, detail) in [
        (
            401,
            b"Authentication failed".to_vec(),
            ErrorKind::Authentication,
            Some("Authentication failed"),
        ),
        (
            403,
            b"Forbidden".to_vec(),
            ErrorKind::Authentication,
            Some("Forbidden"),
        ),
        (
            429,
            b"Currently at websocket limit".to_vec(),
            ErrorKind::ResourceLimit,
            Some("websocket limit"),
        ),
        (
            429,
            vec![],
            ErrorKind::ResourceLimit,
            Some("connection or rate limit reached"),
        ),
        (
            503,
            b"conversation capacity reached".to_vec(),
            ErrorKind::Unavailable,
            Some("capacity reached"),
        ),
        (
            503,
            b"engine is draining".to_vec(),
            ErrorKind::Unavailable,
            Some("engine is draining"),
        ),
        (
            503,
            br#"{"error":{"message":"graph is not ready"}}"#.to_vec(),
            ErrorKind::Unavailable,
            Some("graph is not ready"),
        ),
        (
            503,
            b"<html>proxy error</html>".to_vec(),
            ErrorKind::Unavailable,
            None,
        ),
        (503, br#"{"error":"#.to_vec(), ErrorKind::Unavailable, None),
        (
            503,
            br#"{"error":null}"#.to_vec(),
            ErrorKind::Unavailable,
            None,
        ),
        (503, vec![255], ErrorKind::Unavailable, None),
        (
            503,
            b"bad\x00message".to_vec(),
            ErrorKind::Unavailable,
            None,
        ),
        (429, vec![b'x'; 2048], ErrorKind::ResourceLimit, None),
    ] {
        check(status, body, kind, detail, false).await;
    }
}

#[tokio::test]
async fn unfinished_body_keeps_status_and_closes_without_waiting() {
    check(429, vec![], ErrorKind::ResourceLimit, None, true).await;
}

async fn check(
    status: u16,
    body: Vec<u8>,
    kind: ErrorKind,
    detail: Option<&str>,
    unfinished: bool,
) {
    tokio::time::timeout(Duration::from_secs(5), async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("ws://{}/v1/realtime", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            for _ in 0..2 {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut request = Vec::new();
                while !request.ends_with(b"\r\n\r\n") {
                    request.push(socket.read_u8().await.unwrap());
                }
                let length = if unfinished { 100 } else { body.len() };
                let mut response = format!("HTTP/1.1 {status} Refused\r\nContent-Length: {length}\r\nx-request-id: server-request\r\n\r\n").into_bytes();
                response.extend_from_slice(&body);
                socket.write_all(&response).await.unwrap();
                // The SDK must release the connection, even if the peer waits.
                let result = socket.read(&mut [0]).await;
                assert!(matches!(result, Ok(0)) || result.is_err());
            }
        });
        let client = Client::builder().api_key("test").build().unwrap();
        for _ in 0..2 {
            let error = client.realtime().connect(RealtimeConnectOptions::default().endpoint(&endpoint)).await.err().expect("connection must fail");
            assert_eq!(error.kind(), kind);
            assert_eq!(error.request_id(), Some("server-request"));
            assert!(error.message().contains(&format!("HTTP {status}")));
            if let Some(detail) = detail {
                assert!(error.message().contains(detail), "{}", error.message());
            } else {
                assert!(!error.message().contains(": "), "{}", error.message());
            }
        }
        client.close().await;
        server.await.unwrap();
    }).await.unwrap();
}
