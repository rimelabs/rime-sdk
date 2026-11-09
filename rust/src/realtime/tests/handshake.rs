use crate::{Client, ErrorKind, RealtimeConnectOptions};
use std::time::Duration;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

#[tokio::test]
async fn http_refusals_preserve_category_and_diagnostics() {
    for (status, body, kind, detail, server_kind) in [
        (401, b"Authentication failed".to_vec(), ErrorKind::Authentication, Some("Authentication failed"), None),
        (403, b"Forbidden".to_vec(), ErrorKind::Permission, Some("Forbidden"), None),
        (402, vec![], ErrorKind::Billing, Some("available credits"), None),
        (402, br#"{"error":{"kind":"insufficient_credits","message":"No credits remain"}}"#.to_vec(), ErrorKind::Billing, Some("No credits remain"), Some("insufficient_credits")),
        (403, br#"{"error":{"kind":"billing_inactive","message":"Billing is inactive"}}"#.to_vec(), ErrorKind::Permission, Some("Billing is inactive"), Some("billing_inactive")),
        (402, br#"{"error":{"kind":42,"message":"No credits remain"}}"#.to_vec(), ErrorKind::Billing, Some("No credits remain"), None),
        (403, br#"{ "error": { "kind": "future_reason", "message": "Unavailable for this account" } }"#.to_vec(), ErrorKind::Permission, Some("Unavailable for this account"), Some("future_reason")),
        (429, b"Currently at websocket limit".to_vec(), ErrorKind::ResourceLimit, Some("websocket limit"), None),
        (429, vec![], ErrorKind::ResourceLimit, Some("connection or rate limit reached"), None),
        (503, b"conversation capacity reached".to_vec(), ErrorKind::Unavailable, Some("capacity reached"), None),
        (503, b"engine is draining".to_vec(), ErrorKind::Unavailable, Some("engine is draining"), None),
        (503, br#"{"error":{"message":"graph is not ready"}}"#.to_vec(), ErrorKind::Unavailable, Some("graph is not ready"), None),
        (503, b"<html>proxy error</html>".to_vec(), ErrorKind::Unavailable, None, None),
        (503, br#"{"error":"#.to_vec(), ErrorKind::Unavailable, None, None),
        (503, br#"{"error":null}"#.to_vec(), ErrorKind::Unavailable, None, None),
        (503, vec![255], ErrorKind::Unavailable, None, None),
        (503, b"bad\x00message".to_vec(), ErrorKind::Unavailable, None, None),
        (429, vec![b'x'; 2048], ErrorKind::ResourceLimit, None, None),
    ] {
        check(status, body, kind, detail, false, None, server_kind).await;
    }
}

#[tokio::test]
async fn unfinished_body_keeps_status_and_closes_without_waiting() {
    check(
        429,
        vec![],
        ErrorKind::ResourceLimit,
        None,
        true,
        None,
        None,
    )
    .await;
}

async fn check(
    status: u16,
    body: Vec<u8>,
    kind: ErrorKind,
    detail: Option<&str>,
    unfinished: bool,
    header_kind: Option<&'static str>,
    expected_server_kind: Option<&str>,
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
                let header = header_kind.map(|kind| format!("x-rime-error-kind: {kind}\r\n")).unwrap_or_default();
                let mut response = format!("HTTP/1.1 {status} Refused\r\nContent-Length: {length}\r\nx-request-id: server-request\r\n{header}\r\n").into_bytes();
                response.extend_from_slice(&body);
                socket.write_all(&response).await.unwrap();
                // The SDK must release the connection, even if the peer waits.
                let mut byte = [0u8; 1];
                let result = socket.read(&mut byte).await;
                assert!(matches!(result, Ok(0)) || result.is_err());
            }
        });
        let client = Client::builder().api_key("test").build().unwrap();
        for _ in 0..2 {
            let error = client.realtime().connect(RealtimeConnectOptions::default().endpoint(&endpoint)).await.err().expect("connection must fail");
            assert_eq!(error.kind(), kind);
            assert_eq!(error.request_id(), Some("server-request"));
            assert_eq!(error.status_code(), Some(status));
            assert_eq!(error.server_kind(), expected_server_kind);
            assert_eq!(error.close_code(), None);
            assert_eq!(error.retry_after(), None);
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

#[tokio::test]
async fn retry_after_is_optional_and_bounded() {
    let future_date =
        httpdate::fmt_http_date(std::time::SystemTime::now() + Duration::from_secs(30));
    for (value, expected) in [
        ("3", Some(3)),
        ("0", Some(0)),
        ("-1", None),
        ("1.5", None),
        ("nan", None),
        ("inf", None),
        ("bad date", None),
        ("1\r\nRetry-After: 2", None),
        ("Wed, 21 Oct 2015 07:28:00 GMT", Some(0)),
        (future_date.as_str(), Some(30)),
    ] {
        tokio::time::timeout(Duration::from_secs(3), async {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let endpoint = format!("ws://{}/v1/realtime", listener.local_addr().unwrap());
            let value = if expected == Some(30) {
                httpdate::fmt_http_date(std::time::SystemTime::now() + Duration::from_secs(30))
            } else { value.to_owned() };
            let response = format!("HTTP/1.1 503 Refused\r\nContent-Length: 0\r\nRetry-After: {value}\r\nConnection: close\r\n\r\n");
            let server = tokio::spawn(async move {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut buffer = vec![];
                while !buffer.ends_with(b"\r\n\r\n") {buffer.push(socket.read_u8().await.unwrap());}
                socket.write_all(response.as_bytes()).await.unwrap();
            });
            let client = Client::builder().api_key("test").build().unwrap();
            let error = client.realtime().connect(RealtimeConnectOptions::default().endpoint(endpoint)).await.err().unwrap();
            assert_eq!(error.status_code(), Some(503));
            if expected == Some(30) {
                let delay = error.retry_after().unwrap();
                assert!(delay > Duration::from_secs(28) && delay <= Duration::from_secs(30));
            } else {assert_eq!(error.retry_after(), expected.map(Duration::from_secs));}
            client.close().await; server.await.unwrap();
        }).await.unwrap();
    }
}

#[tokio::test]
async fn received_close_code_survives_initialization_failure() {
    use futures_util::SinkExt;
    use tokio_tungstenite::{
        accept_async,
        tungstenite::{protocol::CloseFrame, Message},
    };
    for code in [1000u16, 1001, 1008, 1013, 0] {
        tokio::time::timeout(Duration::from_secs(3), async {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let endpoint = format!("ws://{}/v1/realtime", listener.local_addr().unwrap());
            let server = tokio::spawn(async move {
                let (socket, _) = listener.accept().await.unwrap();
                let mut websocket = accept_async(socket).await.unwrap();
                if code == 0 {
                    return;
                }
                websocket
                    .send(Message::Close(Some(CloseFrame {
                        code: code.into(),
                        reason: "".into(),
                    })))
                    .await
                    .unwrap();
            });
            let client = Client::builder().api_key("test").build().unwrap();
            let error = client
                .realtime()
                .connect(RealtimeConnectOptions::default().endpoint(endpoint))
                .await
                .err()
                .unwrap();
            assert_eq!(error.kind(), ErrorKind::Stream);
            assert_eq!(
                error.close_code(),
                if code == 0 { None } else { Some(code) }
            );
            assert_eq!(error.status_code(), None);
            client.close().await;
            server.await.unwrap();
        })
        .await
        .unwrap();
    }
}

#[tokio::test]
async fn http_reason_header_survives_missing_or_unusable_body() {
    for (body, detail, unfinished, server_kind) in [
        (
            vec![],
            Some("available credits"),
            false,
            Some("header_kind"),
        ),
        (br#"{"error":"#.to_vec(), None, false, Some("header_kind")),
        (vec![b'x'; 2048], None, false, Some("header_kind")),
        (vec![], Some("available credits"), true, Some("header_kind")),
        (
            br#"{"error":{"kind":"insufficient_credits","message":"No credits remain"}}"#.to_vec(),
            Some("No credits remain"),
            false,
            Some("insufficient_credits"),
        ),
    ] {
        check(
            402,
            body,
            ErrorKind::Billing,
            detail,
            unfinished,
            Some("header_kind"),
            server_kind,
        )
        .await;
    }
}
