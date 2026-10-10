use super::*;
use crate::ErrorKind;
use futures_util::FutureExt;

struct Dropped(Arc<AtomicBool>);
impl Drop for Dropped {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}
fn stuck_source(flag: Arc<AtomicBool>) -> impl Stream<Item = Result<Bytes, Error>> {
    let guard = Dropped(flag);
    futures_util::stream::once(async move {
        let _guard = guard;
        std::future::pending().await
    })
}
#[tokio::test]
async fn cancellation_releases_started_and_unstarted_sources() {
    let h = Harness::new(Mode::Echo).await;
    let dropped = Arc::new(AtomicBool::new(false));
    let mut started = h.stream(stuck_source(dropped.clone()));
    assert!(started.next().now_or_never().is_none());
    let lazy_dropped = Arc::new(AtomicBool::new(false));
    let mut lazy = h.stream(stuck_source(lazy_dropped.clone()));
    h.client.close().await;
    assert!(dropped.load(Ordering::SeqCst));
    assert!(lazy_dropped.load(Ordering::SeqCst));
    assert_eq!(
        next(&mut started).await.unwrap().unwrap_err().kind(),
        ErrorKind::Cancelled
    );
    assert_eq!(
        next(&mut lazy).await.unwrap().unwrap_err().kind(),
        ErrorKind::Cancelled
    );
}

#[tokio::test]
async fn cancelling_one_stream_preserves_siblings_and_client_reuse() {
    let h = Harness::new(Mode::Echo).await;
    let dropped = Arc::new(AtomicBool::new(false));
    let mut cancelled = h.stream(stuck_source(dropped.clone()));
    assert!(cancelled.next().now_or_never().is_none());
    cancelled.cancel();
    assert_eq!(
        next(&mut cancelled).await.unwrap().unwrap_err().kind(),
        ErrorKind::Cancelled
    );
    cancelled.close().await;
    assert!(dropped.load(Ordering::SeqCst));
    for _ in 0..2 {
        let mut stream = h.stream(futures_util::stream::empty());
        assert_eq!(
            collect(&mut stream).await.unwrap(),
            [TranscriptionUpdate::Final {
                text: "".into(),
                language: "en".into()
            }]
        );
    }
    h.client.close().await;
}

#[tokio::test]
async fn overall_timeout_applies_to_paused_sources() {
    let h = Harness::new(Mode::Echo).await;
    let dropped = Arc::new(AtomicBool::new(false));
    let mut stream = h
        .client
        .stt()
        .stream(
            stuck_source(dropped.clone()),
            TranscriptionOptions::new("en").timeout(Some(Duration::from_millis(100))),
        )
        .unwrap();
    assert_eq!(
        next(&mut stream).await.unwrap().unwrap_err().kind(),
        ErrorKind::Timeout
    );
    stream.close().await;
    assert!(dropped.load(Ordering::SeqCst));
    h.client.close().await;
}

#[tokio::test]
async fn queued_final_stays_subject_to_timeout_and_cancellation() {
    for cancel in [false, true] {
        let h = Harness::new(Mode::Echo).await;
        let mut stream = h
            .client
            .stt()
            .stream(
                futures_util::stream::empty(),
                TranscriptionOptions::new("en").timeout(Some(Duration::from_millis(150))),
            )
            .unwrap();
        assert!(stream.next().now_or_never().is_none());
        tokio::time::timeout(Duration::from_secs(2), h.service.completed.notified())
            .await
            .unwrap();
        // Allow trailers to reach the worker without consuming its final.
        tokio::time::sleep(Duration::from_millis(30)).await;
        if cancel {
            stream.cancel();
        } else {
            tokio::time::sleep(Duration::from_millis(160)).await;
        }
        let expected = if cancel {
            ErrorKind::Cancelled
        } else {
            ErrorKind::Timeout
        };
        assert_eq!(
            next(&mut stream).await.unwrap().unwrap_err().kind(),
            expected
        );
        h.client.close().await;
    }
}

#[tokio::test]
async fn completion_timer_stops_after_transport_success() {
    let h = Harness::new(Mode::Echo).await;
    let mut stream = h.stream(futures_util::stream::empty());
    assert!(stream.next().now_or_never().is_none());
    tokio::time::timeout(Duration::from_secs(2), h.service.completed.notified())
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(matches!(
        next(&mut stream).await.unwrap().unwrap(),
        TranscriptionUpdate::Final { .. }
    ));
    stream.cancel();
    assert!(next(&mut stream).await.is_none());
    h.client.close().await;
}

#[tokio::test]
async fn slow_consumer_can_cancel_a_full_queue() {
    let h = Harness::new(Mode::Flood).await;
    let mut stream = h.stream(futures_util::stream::pending());
    assert!(matches!(
        next(&mut stream).await.unwrap().unwrap(),
        TranscriptionUpdate::Partial { .. }
    ));
    tokio::time::sleep(Duration::from_millis(50)).await;
    stream.cancel();
    assert_eq!(
        next(&mut stream).await.unwrap().unwrap_err().kind(),
        ErrorKind::Cancelled
    );
    tokio::time::timeout(Duration::from_secs(2), stream.close())
        .await
        .unwrap();
    h.client.close().await;
}

#[tokio::test]
async fn source_panic_becomes_stream_error() {
    let h = Harness::new(Mode::Echo).await;
    let mut stream = h.stream(futures_util::stream::once(async {
        panic!("test audio source panic");
        #[allow(unreachable_code)]
        Ok(Bytes::new())
    }));
    assert_eq!(
        next(&mut stream).await.unwrap().unwrap_err().kind(),
        ErrorKind::Stream
    );
    stream.close().await;
    assert!(!h.service.eof.load(Ordering::SeqCst));
    h.client.close().await;
}

#[tokio::test]
async fn drop_stream_releases_source_and_rpc() {
    let h = Harness::new(Mode::Echo).await;
    let dropped = Arc::new(AtomicBool::new(false));
    let mut stream = h.stream(
        futures_util::stream::iter([Ok(Bytes::from_static(&[0, 0]))])
            .chain(stuck_source(dropped.clone())),
    );
    assert!(matches!(
        next(&mut stream).await.unwrap().unwrap(),
        TranscriptionUpdate::Partial { .. }
    ));
    drop(stream);
    tokio::time::timeout(Duration::from_secs(2), h.service.cancelled.notified())
        .await
        .unwrap();
    assert!(dropped.load(Ordering::SeqCst));
    h.client.close().await;
}

#[tokio::test]
async fn overall_timeout_discards_full_transcript_queue() {
    let h = Harness::new(Mode::Flood).await;
    let mut stream = h
        .client
        .stt()
        .stream(
            futures_util::stream::pending(),
            TranscriptionOptions::new("en").timeout(Some(Duration::from_millis(150))),
        )
        .unwrap();
    assert!(matches!(
        next(&mut stream).await.unwrap().unwrap(),
        TranscriptionUpdate::Partial { .. }
    ));
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(
        next(&mut stream).await.unwrap().unwrap_err().kind(),
        ErrorKind::Timeout
    );
    stream.close().await;
    h.client.close().await;
}
