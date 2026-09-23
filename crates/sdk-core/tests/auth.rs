use sdk_core::auth::{Credentials, exchange};
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
async fn server(
    status: u16,
    body: String,
    stall: bool,
) -> (String, Arc<AtomicUsize>, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/token", listener.local_addr().unwrap());
    let calls = Arc::new(AtomicUsize::new(0));
    let count = calls.clone();
    let task = tokio::spawn(async move {
        loop {
            let (mut socket, _) = listener.accept().await.unwrap();
            let body = body.clone();
            count.fetch_add(1, Ordering::SeqCst);
            tokio::spawn(async move {
                let mut data = vec![0; 8192];
                let n = socket.read(&mut data).await.unwrap();
                let text = String::from_utf8_lossy(&data[..n]);
                assert!(
                    text.to_ascii_lowercase()
                        .contains("authorization: api-key secret")
                );
                let headers = format!(
                    "HTTP/1.1 {status} Result\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    if stall { 999999 } else { body.len() }
                );
                socket.write_all(headers.as_bytes()).await.unwrap();
                if stall {
                    tokio::time::sleep(Duration::from_secs(2)).await;
                } else {
                    socket.write_all(body.as_bytes()).await.unwrap();
                }
            });
        }
    });
    (url, calls, task)
}
#[tokio::test]
async fn exchange_status_and_invalid_bodies_are_safe() {
    for (status, body, kind) in [
        (401, "secret", "Authentication"),
        (403, "secret", "Permission"),
        (429, "secret", "ResourceLimit"),
        (500, "secret", "Unavailable"),
        (502, "secret", "Unavailable"),
        (503, "secret", "Unavailable"),
        (504, "secret", "Unavailable"),
        (
            200,
            r#"{"access_token":"x","expires_in":true,"audience":"host"}"#,
            "Authentication",
        ),
        (
            200,
            r#"{"access_token":"x","expires_in":0,"audience":"host"}"#,
            "Authentication",
        ),
        (
            200,
            r#"{"access_token":"x","expires_in":60,"audience":"wrong"}"#,
            "Authentication",
        ),
    ] {
        let (url, _, server) = server(status, body.into(), false).await;
        let error = exchange("secret", "host", &url, Duration::from_secs(1))
            .await
            .err()
            .unwrap();
        assert_eq!(error.kind, kind);
        assert!(!error.message.contains("secret"));
        server.abort();
    }
    let (url, _, server) = server(200, "x".repeat(65537), false).await;
    assert_eq!(
        exchange("secret", "host", &url, Duration::from_secs(1))
            .await
            .err()
            .unwrap()
            .kind,
        "Authentication"
    );
    server.abort();
}
#[tokio::test]
async fn rejection_does_not_wait_for_body_and_success_has_deadline() {
    for status in [401, 403, 429, 500, 502, 503, 504, 200] {
        let (url, _, server) = server(status, "".into(), true).await;
        let error = exchange("secret", "host", &url, Duration::from_millis(500))
            .await
            .err()
            .unwrap();
        assert_eq!(error.kind == "Timeout", status == 200);
        server.abort();
    }
}
#[tokio::test]
async fn refresh_is_singleflight_and_close_releases_cached_token() {
    let (url, calls, server) = server(
        200,
        r#"{"access_token":"token","expires_in":60,"audience":"host"}"#.into(),
        false,
    )
    .await;
    let credentials = Credentials::new("secret".into(), "host".into(), url);
    let (a, b) = tokio::join!(credentials.token(), credentials.token());
    let a = a.unwrap();
    let b = b.unwrap();
    assert!(Arc::ptr_eq(&a, &b));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let weak = Arc::downgrade(&a);
    drop(a);
    drop(b);
    credentials.close().await;
    assert!(weak.upgrade().is_none());
    assert!(credentials.token().await.is_err());
    server.abort();
}
