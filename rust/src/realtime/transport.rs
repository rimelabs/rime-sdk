use crate::{Error, ErrorKind};
use futures_util::{Sink, SinkExt, StreamExt};
use std::{
    sync::{
        atomic::{AtomicU8, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::sync::mpsc;
use tokio_tungstenite::{
    connect_async_with_config,
    tungstenite::{client::IntoClientRequest, protocol::WebSocketConfig, Message},
};
use tokio_util::sync::CancellationToken;

fn handshake_error(error: tokio_tungstenite::tungstenite::Error) -> Error {
    let tokio_tungstenite::tungstenite::Error::Http(response) = error else {
        return Error::new(ErrorKind::Unavailable, "realtime handshake failed");
    };
    let status = response.status().as_u16();
    let (kind, reason) = match status {
        401 | 403 => (
            ErrorKind::Authentication,
            "realtime endpoint rejected credentials",
        ),
        429 => (
            ErrorKind::ResourceLimit,
            "realtime connection or rate limit reached",
        ),
        503 => (ErrorKind::Unavailable, "realtime endpoint is unavailable"),
        _ => (
            ErrorKind::Unavailable,
            "realtime endpoint rejected the connection",
        ),
    };
    let mut message = format!("{reason} (HTTP {status})");
    // Tungstenite retains only bytes received with the headers. Use a complete,
    // bounded body; partial or chunked bodies cannot provide a reliable message.
    if let Some(body) = response.body().as_ref().filter(|body| {
        body.len() < 1024
            && !response.headers().contains_key("transfer-encoding")
            && response
                .headers()
                .get("content-length")
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.parse::<usize>().ok())
                == Some(body.len())
    }) {
        if let Ok(text) = std::str::from_utf8(body) {
            let text = text.trim();
            let json = if text.starts_with(['{', '[']) {
                serde_json::from_str::<serde_json::Value>(text).ok()
            } else {
                None
            };
            let detail = if text.starts_with(['{', '[']) {
                json.as_ref()
                    .and_then(|v| v.get("error")?.get("message")?.as_str())
            } else {
                Some(text)
            };
            if let Some(detail) = detail.filter(|s| {
                !s.trim_start().starts_with('<')
                    && s.chars()
                        .all(|c| c >= ' ' || matches!(c, '\r' | '\n' | '\t'))
            }) {
                let detail = detail.split_whitespace().collect::<Vec<_>>().join(" ");
                if !detail.is_empty() {
                    message.push_str(&format!(": {detail}"));
                }
            }
        }
    }
    Error::new(kind, message).with_request_id(
        response
            .headers()
            .get("x-request-id")
            .and_then(|v| v.to_str().ok()),
    )
}

pub(super) struct Write {
    pub id: u64,
    pub frames: Vec<String>,
    pub status: Arc<AtomicU8>,
    pub cancelled: CancellationToken,
    pub audio: bool,
}
pub(super) struct Written {
    pub id: u64,
    pub skipped: bool,
    pub result: Result<(), Error>,
}
pub(super) struct Transport {
    pub reads: mpsc::Receiver<Result<String, Error>>,
    pub writes: mpsc::Sender<Write>,
    pub written: mpsc::Receiver<Written>,
    reader: tokio::task::JoinHandle<()>,
    writer: tokio::task::JoinHandle<()>,
}
impl Transport {
    pub async fn connect(
        address: &str,
        authorization: &str,
        request_id: Option<&str>,
        request_timeout: Duration,
        stop: CancellationToken,
    ) -> Result<Self, Error> {
        let mut request = address
            .into_client_request()
            .map_err(|_| Error::new(ErrorKind::Input, "invalid realtime endpoint"))?;
        let mut header = authorization
            .parse()
            .map_err(|_| Error::new(ErrorKind::Authentication, "invalid credentials"))?;
        request.headers_mut().insert("Authorization", {
            let h: &mut tokio_tungstenite::tungstenite::http::HeaderValue = &mut header;
            h.set_sensitive(true);
            header
        });
        if let Some(id) = request_id {
            let id = id
                .parse()
                .map_err(|_| Error::new(ErrorKind::Input, "invalid request ID"))?;
            request.headers_mut().insert("x-request-id", id);
        }
        let config = WebSocketConfig::default()
            .max_message_size(Some(1024 * 1024))
            .max_frame_size(Some(1024 * 1024));
        let (socket, _) = connect_async_with_config(request, Some(config), false)
            .await
            .map_err(handshake_error)?;
        let (mut sink, mut stream) = socket.split();
        let (read_tx, reads) = mpsc::channel(1);
        let (writes, mut write_rx) = mpsc::channel::<Write>(1);
        let (written_tx, written) = mpsc::channel(1);
        let read_stop = stop.clone();
        let reader = tokio::spawn(async move {
            loop {
                let next =
                    tokio::select! {biased;_=read_stop.cancelled()=>break,v=stream.next()=>v};
                let result = match next {
                    Some(Ok(Message::Text(s))) => Ok(s.to_string()),
                    Some(Ok(Message::Ping(_) | Message::Pong(_))) => continue,
                    _ => Err(Error::new(
                        ErrorKind::Stream,
                        "invalid event or lost realtime connection",
                    )),
                };
                let failed = result.is_err();
                tokio::select! {_=read_stop.cancelled()=>break,r=read_tx.send(result)=>if r.is_err(){break}};
                if failed {
                    break;
                }
            }
        });
        let writer = tokio::spawn(async move {
            loop {
                let job = tokio::select! {biased;_=stop.cancelled()=>break,j=write_rx.recv()=>match j{Some(j)=>j,None=>break}};
                let result = tokio::select! {
                    biased;
                    _ = stop.cancelled() => break,
                    result = write_frames(&mut sink, job, request_timeout) => result,
                };
                tokio::select! {_=stop.cancelled()=>break,r=written_tx.send(result)=>if r.is_err(){break}}
            }
        });
        Ok(Self {
            reads,
            writes,
            written,
            reader,
            writer,
        })
    }
    pub async fn close(self) {
        self.reader.abort();
        self.writer.abort();
        let _ = self.reader.await;
        let _ = self.writer.await;
    }
}

// This private seam also permits controlled writes in cancellation tests.
pub(super) async fn write_frames<S>(sink: &mut S, job: Write, budget: Duration) -> Written
where
    S: Sink<Message> + Unpin,
{
    // This check commits the writer to submission. A later future drop retains
    // matching until a known write outcome or session failure.
    let skipped = job.status.load(Ordering::Acquire) == 2;
    let result = if skipped || job.frames.is_empty() {
        Ok(())
    } else {
        let write = async {
            for frame in job.frames {
                if job.audio && job.cancelled.is_cancelled() {
                    return Err(Error::new(
                        ErrorKind::Stream,
                        "audio submission cancelled; outcome unknown",
                    ));
                }
                sink.send(Message::Text(frame.into())).await.map_err(|_| {
                    Error::new(ErrorKind::Stream, "realtime write failed; outcome unknown")
                })?;
            }
            if job.audio && job.cancelled.is_cancelled() {
                return Err(Error::new(
                    ErrorKind::Stream,
                    "audio submission cancelled; outcome unknown",
                ));
            }
            Ok(())
        };
        tokio::select! {
            biased;
            _ = job.cancelled.cancelled(), if job.audio => {
                Err(Error::new(ErrorKind::Stream, "audio submission cancelled; outcome unknown"))
            },
            result = tokio::time::timeout(budget, write) => result.unwrap_or_else(|_| {
                Err(Error::new(ErrorKind::Timeout, "realtime write timed out; outcome unknown"))
            }),
        }
    };
    Written {
        id: job.id,
        skipped,
        result,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        pin::Pin,
        task::{Context, Poll},
    };

    struct ControlledSink {
        started: CancellationToken,
        release: CancellationToken,
        writes: usize,
    }
    impl Sink<Message> for ControlledSink {
        type Error = ();
        fn poll_ready(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), ()>> {
            Poll::Ready(Ok(()))
        }
        fn start_send(mut self: Pin<&mut Self>, _: Message) -> Result<(), ()> {
            self.writes += 1;
            self.started.cancel();
            Ok(())
        }
        fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), ()>> {
            if self.release.is_cancelled() {
                Poll::Ready(Ok(()))
            } else {
                // Tests release through cancellation, which wakes the outer select.
                let _ = cx;
                Poll::Pending
            }
        }
        fn poll_close(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), ()>> {
            Poll::Ready(Ok(()))
        }
    }
    fn job(audio: bool) -> Write {
        Write {
            id: 1,
            frames: vec!["first".into(), "second".into()],
            status: Arc::new(AtomicU8::new(0)),
            cancelled: CancellationToken::new(),
            audio,
        }
    }
    #[tokio::test]
    async fn cancelled_queued_audio_does_not_write_or_block_other_work() {
        let mut sink = ControlledSink {
            started: CancellationToken::new(),
            release: CancellationToken::new(),
            writes: 0,
        };
        let queued = job(true);
        queued.status.store(2, Ordering::Release);
        queued.cancelled.cancel();
        assert!(
            write_frames(&mut sink, queued, Duration::from_secs(1))
                .await
                .skipped
        );
        assert_eq!(sink.writes, 0);
        sink.release.cancel();
        let result = write_frames(&mut sink, job(false), Duration::from_secs(1)).await;
        assert!(result.result.is_ok());
        assert_eq!(sink.writes, 2);
    }
    #[tokio::test]
    async fn cancellation_during_append_stops_remaining_frames() {
        let mut sink = ControlledSink {
            started: CancellationToken::new(),
            release: CancellationToken::new(),
            writes: 0,
        };
        let started = sink.started.clone();
        let active = job(true);
        let cancel = active.cancelled.clone();
        let result = tokio::spawn(async move {
            let result = write_frames(&mut sink, active, Duration::from_secs(1)).await;
            (result, sink.writes)
        });
        started.cancelled().await;
        cancel.cancel();
        let (result, writes) = result.await.unwrap();
        assert!(!result.skipped);
        assert_eq!(result.result.unwrap_err().kind(), ErrorKind::Stream);
        assert_eq!(writes, 1);
    }
}

#[cfg(test)]
mod shutdown_tests {
    use super::*;
    use tokio::net::TcpListener;
    use tokio_tungstenite::accept_async;

    #[tokio::test]
    async fn cancellation_releases_full_read_queue_without_worker_drain() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = format!("ws://{}/v1/realtime", listener.local_addr().unwrap());
        let sent = CancellationToken::new();
        let ready = sent.clone();
        let peer = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut ws = accept_async(socket).await.unwrap();
            for _ in 0..3 {
                ws.send(Message::Text("{}".into())).await.unwrap();
            }
            ready.cancel();
            while ws.next().await.is_some() {}
        });
        let stop = CancellationToken::new();
        let transport = Transport::connect(
            &address,
            "Bearer test",
            None,
            Duration::from_secs(2),
            stop.clone(),
        )
        .await
        .unwrap();
        sent.cancelled().await;
        tokio::time::timeout(Duration::from_secs(2), async {
            while transport.reads.len() != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        // Give the reader a chance to block sending the second message.
        tokio::task::yield_now().await;
        stop.cancel();
        tokio::time::timeout(Duration::from_secs(2), async {
            while !transport.reader.is_finished() || !transport.writer.is_finished() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        // Both tasks exit from the shared token, before close aborts or joins them.
        assert_eq!(transport.reads.len(), 1);
        transport.close().await;
        tokio::time::timeout(Duration::from_secs(2), peer)
            .await
            .unwrap()
            .unwrap();
    }
}
