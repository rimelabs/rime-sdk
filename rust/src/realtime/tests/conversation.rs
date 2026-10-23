use super::peer::Peer;
use crate::{
    realtime::{
        AudioChunk, ConversationOutput, ConversationPlayback, ConversationRunner,
        ConversationSource, OutputRef,
    },
    Error, ErrorKind,
};
use serde_json::json;
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
struct Source(Arc<AtomicBool>);
impl ConversationSource for Source {
    async fn read(&mut self, stop: CancellationToken) -> Result<Option<AudioChunk>, Error> {
        stop.cancelled().await;
        self.0.store(true, Ordering::SeqCst);
        Err(Error::new(ErrorKind::Cancelled, "capture stopped"))
    }
}
struct Output {
    reference: OutputRef,
    done: CancellationToken,
    value: AtomicU64,
    writes: AtomicUsize,
}
impl Output {
    fn finish(&self, value: f64) {
        self.value.store(value.to_bits(), Ordering::SeqCst);
        self.done.cancel()
    }
}
impl ConversationOutput for Output {
    fn write(&self, _: AudioChunk) -> Result<(), Error> {
        self.writes.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
    fn end(&self) -> Result<(), Error> {
        Ok(())
    }
    async fn completion(&self, stop: CancellationToken) -> Result<f64, Error> {
        tokio::select! {_=stop.cancelled()=>Err(Error::new(ErrorKind::Cancelled,"receipt stopped")),_=self.done.cancelled()=>Ok(f64::from_bits(self.value.load(Ordering::SeqCst)))}
    }
}
struct Playback {
    opened: mpsc::Sender<Arc<Output>>,
    outputs: Arc<Mutex<HashMap<OutputRef, Arc<Output>>>>,
    stopped: Arc<AtomicBool>,
}
impl ConversationPlayback for Playback {
    type Output = Output;
    fn open(&mut self, r: &OutputRef) -> Result<Arc<Output>, Error> {
        let o = Arc::new(Output {
            reference: r.clone(),
            done: CancellationToken::new(),
            value: AtomicU64::new(0),
            writes: AtomicUsize::new(0),
        });
        self.outputs.lock().unwrap().insert(r.clone(), o.clone());
        self.opened.try_send(o.clone()).unwrap();
        Ok(o)
    }
    fn interrupt(&mut self, refs: &[OutputRef]) -> Result<(), Error> {
        for r in refs {
            self.outputs.lock().unwrap()[r].finish(12.0)
        }
        Ok(())
    }
    async fn stop(&mut self, _: CancellationToken) -> Result<(), Error> {
        self.stopped.store(true, Ordering::SeqCst);
        Ok(())
    }
}
async fn emit(p: &Peer, response: &str, item: &str) {
    p.send(json!({"type":"response.output_item.added","response_id":response,"output_index":0,"item":{"type":"message","id":item}})).await;
    p.send(json!({"type":"response.audio.delta","response_id":response,"item_id":item,"output_index":0,"content_index":0,"delta":"AAA="})).await;
}
#[tokio::test]
async fn measured_playback_order_and_confirmed_shutdown() {
    let mut p = Peer::new().await;
    let (c, s) = p.setup().await;
    let (capture, stopped) = (
        Arc::new(AtomicBool::new(false)),
        Arc::new(AtomicBool::new(false)),
    );
    let (opened, mut outputs) = mpsc::channel(4);
    let playback = Playback {
        opened,
        outputs: Arc::new(Mutex::new(HashMap::new())),
        stopped: stopped.clone(),
    };
    let stop = CancellationToken::new();
    let task = tokio::spawn(
        ConversationRunner::new(s, Source(capture.clone()), playback).run(stop.clone()),
    );
    p.send(json!({"type":"response.created","response":{"id":"r1"}}))
        .await;
    emit(&p, "r1", "i1").await;
    let o1 = outputs.recv().await.unwrap();
    assert_eq!(o1.reference.item_id(), "i1");
    p.ended("r1").await;
    p.send(json!({"type":"response.created","response":{"id":"r2"}}))
        .await;
    emit(&p, "r2", "i2").await;
    let o2 = outputs.recv().await.unwrap();
    p.ended("r2").await;
    o2.finish(20.0);
    p.no_request().await;
    o1.finish(10.0);
    for id in ["r1", "r2"] {
        let q = p.next("prism.playback.drained").await;
        assert_eq!(q["response_id"], id)
    }
    stop.cancel();
    let result = tokio::time::timeout(Duration::from_secs(3), task)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(result.unwrap_err().kind(), ErrorKind::Cancelled);
    assert!(stopped.load(Ordering::SeqCst));
    assert!(capture.load(Ordering::SeqCst));
    c.close().await;
}
#[tokio::test]
async fn interruption_rejects_late_audio_and_cancels_dependent_reply() {
    let mut p = Peer::new().await;
    let (c, s) = p.setup().await;
    let (opened, mut outputs) = mpsc::channel(4);
    let playback = Playback {
        opened,
        outputs: Arc::new(Mutex::new(HashMap::new())),
        stopped: Arc::new(AtomicBool::new(false)),
    };
    let stop = CancellationToken::new();
    let task = tokio::spawn(
        ConversationRunner::new(
            s.clone(),
            Source(Arc::new(AtomicBool::new(false))),
            playback,
        )
        .run(stop.clone()),
    );
    p.send(json!({"type":"response.created","response":{"id":"r1"}}))
        .await;
    emit(&p, "r1", "i1").await;
    let output = outputs.recv().await.unwrap();
    let clone = s.clone();
    let update = tokio::spawn(async move {
        clone
            .update(crate::RealtimeUpdateOptions::default().interrupt_on_speech(false))
            .await
    });
    let q = p.next("session.update").await;
    p.send(json!({"type":"session.updated", "prism_request_event_id":q["event_id"], "session":{"id":"session-1","voice":"test","prism_effective_interrupt_response":false,"prism_settings_revision":2}})).await;
    update.await.unwrap().unwrap();
    p.send(
        json!({"type":"input_audio_buffer.speech_started","item_id":"speech","audio_start_ms":0}),
    )
    .await;
    let q = p.next("conversation.item.truncate").await;
    assert_eq!(q["audio_end_ms"], 12);
    p.send(json!({"type":"conversation.item.truncated","item_id":"i1","content_index":0}))
        .await;
    p.send(json!({"type":"response.audio.delta","response_id":"r1","item_id":"i1","output_index":0,"content_index":0,"delta":"AAA="})).await;
    p.send(json!({"type":"response.created","response":{"id":"child","metadata":{"prism_cause":"tool_continuation","prism_parent_response_id":"r1"}}})).await;
    let q = p.next("response.cancel").await;
    assert_eq!(q["response_id"], "child");
    p.ended("child").await;
    assert_eq!(output.writes.load(Ordering::SeqCst), 1);
    stop.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(3), task)
        .await
        .unwrap();
    c.close().await;
}

struct FailingReleaseSource;
impl ConversationSource for FailingReleaseSource {
    async fn read(&mut self, stop: CancellationToken) -> Result<Option<AudioChunk>, Error> {
        stop.cancelled().await;
        Err(Error::input(std::io::Error::other(
            "capture release failed",
        )))
    }
}
#[tokio::test]
async fn capture_cleanup_failure_is_preserved() {
    let p = Peer::new().await;
    let (c, s) = p.setup().await;
    let (opened, _) = mpsc::channel(4);
    let playback = Playback {
        opened,
        outputs: Arc::new(Mutex::new(HashMap::new())),
        stopped: Arc::new(AtomicBool::new(false)),
    };
    let task = tokio::spawn(
        ConversationRunner::new(s.clone(), FailingReleaseSource, playback)
            .run(CancellationToken::new()),
    );
    s.close().await;
    let error = tokio::time::timeout(Duration::from_secs(3), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Input);
    c.close().await;
}
#[tokio::test]
async fn normal_close_with_pending_interruption_report() {
    let mut p = Peer::new().await;
    let (c, s) = p.setup().await;
    let (opened, mut outputs) = mpsc::channel(4);
    let playback = Playback {
        opened,
        outputs: Arc::new(Mutex::new(HashMap::new())),
        stopped: Arc::new(AtomicBool::new(false)),
    };
    let task = tokio::spawn(
        ConversationRunner::new(
            s.clone(),
            Source(Arc::new(AtomicBool::new(false))),
            playback,
        )
        .run(CancellationToken::new()),
    );
    p.send(json!({"type":"response.created","response":{"id":"r1"}}))
        .await;
    emit(&p, "r1", "i1").await;
    outputs.recv().await.unwrap();
    p.send(
        json!({"type":"input_audio_buffer.speech_started","item_id":"speech","audio_start_ms":0}),
    )
    .await;
    p.next("conversation.item.truncate").await;
    s.close().await;
    tokio::time::timeout(Duration::from_secs(3), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    c.close().await;
}

struct ReleasedAudioSource {
    started: CancellationToken,
    release: CancellationToken,
}
impl ConversationSource for ReleasedAudioSource {
    async fn read(&mut self, stop: CancellationToken) -> Result<Option<AudioChunk>, Error> {
        self.started.cancel();
        tokio::select! {
            _=stop.cancelled()=>Err(Error::cancelled()),
            _=self.release.cancelled()=>Ok(Some(AudioChunk { data: vec![0;1280].into(), format: Default::default() })),
        }
    }
}
#[tokio::test]
async fn disconnect_racing_capture_send_remains_a_failure() {
    for failed_before_capture in [false, true] {
        let p = Peer::new().await;
        let (client, session) = p.setup().await;
        let started = CancellationToken::new();
        let release = CancellationToken::new();
        let (opened, _) = mpsc::channel(4);
        let stopped = Arc::new(AtomicBool::new(false));
        let playback = Playback {
            opened,
            outputs: Arc::new(Mutex::new(HashMap::new())),
            stopped: stopped.clone(),
        };
        if failed_before_capture {
            p.disconnect();
            assert_eq!(
                session
                    .add_message("user", "wait for disconnect")
                    .await
                    .unwrap_err()
                    .kind(),
                ErrorKind::Stream
            );
            release.cancel();
        }
        let task = tokio::spawn(
            ConversationRunner::new(
                session,
                ReleasedAudioSource {
                    started: started.clone(),
                    release: release.clone(),
                },
                playback,
            )
            .run(CancellationToken::new()),
        );
        if !failed_before_capture {
            started.cancelled().await;
            p.disconnect();
            release.cancel();
        }
        let error = tokio::time::timeout(Duration::from_secs(3), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Stream);
        assert!(stopped.load(Ordering::SeqCst));
        client.close().await;
    }
}

struct DelayedStop {
    playback: Playback,
    entered: CancellationToken,
    release: CancellationToken,
    dropped: CancellationToken,
}
impl Drop for DelayedStop {
    fn drop(&mut self) {
        self.dropped.cancel();
    }
}
impl ConversationPlayback for DelayedStop {
    type Output = Output;
    fn open(&mut self, reference: &OutputRef) -> Result<Arc<Output>, Error> {
        self.playback.open(reference)
    }
    fn interrupt(&mut self, refs: &[OutputRef]) -> Result<(), Error> {
        self.playback.interrupt(refs)
    }
    async fn stop(&mut self, stop: CancellationToken) -> Result<(), Error> {
        self.entered.cancel();
        tokio::select! {
            _=self.release.cancelled()=>self.playback.stop(stop).await,
            _=stop.cancelled()=>Err(Error::cancelled()),
        }
    }
}
#[tokio::test]
async fn dropped_run_future_keeps_cleanup_alive() {
    for during_stop in [false, true] {
        let p = Peer::new().await;
        let (client, session) = p.setup().await;
        let (opened, mut outputs) = mpsc::channel(4);
        let capture = Arc::new(AtomicBool::new(false));
        let stopped = Arc::new(AtomicBool::new(false));
        let entered = CancellationToken::new();
        let release = CancellationToken::new();
        let dropped = CancellationToken::new();
        let playback = DelayedStop {
            playback: Playback {
                opened,
                outputs: Arc::new(Mutex::new(HashMap::new())),
                stopped: stopped.clone(),
            },
            entered: entered.clone(),
            release: release.clone(),
            dropped: dropped.clone(),
        };
        let cancel = CancellationToken::new();
        let run = tokio::spawn(
            ConversationRunner::new(session.clone(), Source(capture.clone()), playback)
                .run(cancel.clone()),
        );
        p.send(json!({"type":"response.created","response":{"id":"active"}}))
            .await;
        emit(&p, "active", "output").await;
        let output = outputs.recv().await.unwrap();
        // Playback remains active until cleanup receives device confirmation.
        assert!(!output.done.is_cancelled());
        if during_stop {
            cancel.cancel();
            tokio::time::timeout(Duration::from_secs(2), entered.cancelled())
                .await
                .unwrap();
        }
        run.abort();
        assert!(run.await.unwrap_err().is_cancelled());
        tokio::time::timeout(Duration::from_secs(2), entered.cancelled())
            .await
            .unwrap();
        assert!(!stopped.load(Ordering::SeqCst));
        assert!(!dropped.is_cancelled());
        release.cancel();
        tokio::time::timeout(Duration::from_secs(2), dropped.cancelled())
            .await
            .unwrap();
        assert!(stopped.load(Ordering::SeqCst));
        assert!(capture.load(Ordering::SeqCst));
        assert!(session
            .send_text("after cleanup")
            .await
            .unwrap_err()
            .is_realtime_closed());
        client.close().await;
    }
}
