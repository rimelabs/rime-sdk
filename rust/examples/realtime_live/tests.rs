use super::*;
use futures_util::{SinkExt, StreamExt};
use rimelabs_sdk::{ErrorKind, RealtimeSession};
use std::{future::Future, time::Duration};
use tokio::{net::TcpListener, task::JoinHandle};
use tokio_tungstenite::{accept_async, tungstenite::Message};

async fn session() -> (Client, RealtimeSession, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("ws://{}/v1/realtime", listener.local_addr().unwrap());
    let peer = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut socket = accept_async(socket).await.unwrap();
        socket
            .send(Message::Text(
                json!({"type":"session.created","session":{"id":"session-1"}})
                    .to_string()
                    .into(),
            ))
            .await
            .unwrap();
        let message = socket.next().await.unwrap().unwrap();
        let request: Value = serde_json::from_str(message.to_text().unwrap()).unwrap();
        assert_eq!(request["type"], "session.update");
        socket
            .send(Message::Text(
                json!({"type":"session.updated","prism_request_event_id":request["event_id"],"session":{"id":"session-1","voice":"test"}})
                    .to_string().into(),
            ))
            .await.unwrap();
        while let Some(message) = socket.next().await {
            if matches!(message, Ok(Message::Close(_)) | Err(_)) {
                break;
            }
        }
    });
    let client = Client::builder().api_key("test").build().unwrap();
    let session = client
        .realtime()
        .connect(RealtimeConnectOptions::default().endpoint(endpoint))
        .await
        .unwrap();
    (client, session, peer)
}

struct ObservedSource {
    source: Source,
    waiting: CancellationToken,
    force_eof_branch: bool,
}
impl ConversationSource for ObservedSource {
    async fn read(&mut self, stop: CancellationToken) -> Result<Option<AudioChunk>, Error> {
        let read = async {
            if self.force_eof_branch {
                // Select the closed-channel outcome deterministically. This is
                // the Ok(None) branch in Source::read when both branches wake.
                assert!(self.source.audio.recv().await.is_none());
                Ok(None)
            } else {
                self.source.read(stop).await
            }
        };
        tokio::pin!(read);
        std::future::poll_fn(|context| {
            let result = read.as_mut().poll(context);
            if result.is_pending() {
                self.waiting.cancel();
            }
            result
        })
        .await
    }
}

async fn disconnect_while_reading(force_eof_branch: bool) {
    let (client, session, peer) = session().await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let remote = TcpStream::connect(listener.local_addr().unwrap())
        .await
        .unwrap();
    let (connection, _) = listener.accept().await.unwrap();
    let (read, _write) = connection.into_split();
    let (send, _outgoing) = mpsc::channel(8);
    let (audio, capture) = mpsc::channel(8);
    let shared = Arc::new(Shared {
        send,
        receipts: Mutex::new(HashMap::new()),
        stopped: CancellationToken::new(),
        failed: CancellationToken::new(),
    });
    let reader = tokio::spawn(read_bridge(read, audio, shared.clone()));
    let waiting = CancellationToken::new();
    let source = ObservedSource {
        source: Source {
            audio: capture,
            shared: shared.clone(),
        },
        waiting: waiting.clone(),
        force_eof_branch,
    };
    let run = tokio::spawn(
        ConversationRunner::new(session, source, Playback(shared.clone()))
            .run(CancellationToken::new()),
    );
    tokio::time::timeout(Duration::from_secs(2), waiting.cancelled())
        .await
        .unwrap();
    // The real bridge reader publishes failure and drops the audio sender in
    // the same task poll, so Source::read wakes with both conditions ready.
    drop(remote);
    let error = tokio::time::timeout(Duration::from_secs(2), run)
        .await
        .expect("runner did not stop after bridge disconnect")
        .unwrap()
        .unwrap_err();
    assert!(shared.failed.is_cancelled());
    assert_eq!(error.kind(), ErrorKind::Input);
    assert_eq!(
        std::error::Error::source(&error).unwrap().to_string(),
        "audio bridge failed"
    );
    reader.await.unwrap();
    client.close().await;
    tokio::time::timeout(Duration::from_secs(2), peer)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn bridge_disconnect_while_source_waits_reaches_runner() {
    disconnect_while_reading(false).await;
}

#[tokio::test]
async fn bridge_disconnect_is_not_lost_when_source_selects_eof() {
    disconnect_while_reading(true).await;
}
