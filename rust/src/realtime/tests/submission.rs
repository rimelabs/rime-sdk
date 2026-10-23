use super::*;
use crate::realtime::transport::write_frames;
use futures_util::{future::BoxFuture, FutureExt, Sink};
use std::{
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};
use tokio_tungstenite::tungstenite::Message;

fn worker() -> (
    Worker,
    mpsc::Sender<Write>,
    mpsc::Receiver<Write>,
    mpsc::Receiver<SessionEvent>,
) {
    let (_, commands) = mpsc::channel(128);
    let (events, receiver) = mpsc::channel(16);
    let (writes, jobs) = mpsc::channel(1);
    (
        Worker {
            epoch: 0,
            commands,
            events,
            pending: vec![],
            responses: HashMap::new(),
            order: VecDeque::new(),
            ready: true,
            latest: String::new(),
            id: "session-1".into(),
            sequence: 0,
            converter: None,
            timeouts: RealtimeTimeouts::default(),
            interrupt: true,
            initialized: None,
        },
        writes,
        jobs,
        receiver,
    )
}
fn queue_text(worker: &mut Worker) -> CallGuard {
    let mut command = Command::new(
        Kind::Text,
        json!({"response":{"prism_input_text":"hello","metadata":{"prism_cause":"user_text"}}}),
    );
    command.deadline = Instant::now() + Duration::from_secs(1);
    command.admission_deadline = command.deadline;
    let guard = CallGuard {
        status: command.status.clone(),
        wake: Arc::new(Notify::new()),
        cancelled: command.cancelled.clone(),
    };
    worker.pending.push(command);
    guard
}
struct Gate {
    sent: mpsc::Sender<Message>,
    release: BoxFuture<'static, ()>,
}
impl Sink<Message> for Gate {
    type Error = ();
    fn poll_ready(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), ()>> {
        Poll::Ready(Ok(()))
    }
    fn start_send(self: Pin<&mut Self>, item: Message) -> Result<(), ()> {
        self.sent.try_send(item).map_err(|_| ())
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), ()>> {
        self.release.as_mut().poll(cx).map(|()| Ok(()))
    }
    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), ()>> {
        self.poll_flush(cx)
    }
}
fn gate() -> (Gate, mpsc::Receiver<Message>, CancellationToken) {
    let (sent, receiver) = mpsc::channel(4);
    let release = CancellationToken::new();
    (
        Gate {
            sent,
            release: release.clone().cancelled_owned().boxed(),
        },
        receiver,
        release,
    )
}
fn dispatch(worker: &mut Worker, event: Value) {
    let event = protocol::decode(&event.to_string(), "session-1")
        .unwrap()
        .unwrap();
    worker.dispatch(event).unwrap();
}
#[tokio::test]
async fn cancellation_before_writer_entry_waits_for_confirmed_skip() {
    let (mut worker, writes, mut jobs, _) = worker();
    let guard = queue_text(&mut worker);
    worker.advance(&writes).unwrap();
    let job = jobs.recv().await.unwrap();
    drop(guard);
    worker.advance(&writes).unwrap();
    assert!(!worker.ready);
    assert!(!worker.pending[0].finished);
    let (mut sink, mut sent, _) = gate();
    let result = write_frames(&mut sink, job, Duration::from_secs(1)).await;
    assert!(result.skipped);
    assert!(sent.try_recv().is_err());
    worker.written(result).unwrap();
    assert!(worker.ready);
    let _next = queue_text(&mut worker);
    worker.advance(&writes).unwrap();
    assert!(jobs.try_recv().is_ok());
}
#[tokio::test]
async fn early_acknowledgment_and_future_drop_cancel_exactly_once() {
    for cancel_first in [false, true] {
        let (mut worker, writes, mut jobs, mut events) = worker();
        let mut guard = Some(queue_text(&mut worker));
        worker.advance(&writes).unwrap();
        let job = jobs.recv().await.unwrap();
        let id = job.id;
        let (mut sink, mut sent, release) = gate();
        let writer =
            tokio::spawn(async move { write_frames(&mut sink, job, Duration::from_secs(1)).await });
        sent.recv().await.unwrap();
        if cancel_first {
            drop(guard.take());
            worker.advance(&writes).unwrap();
        }
        dispatch(
            &mut worker,
            json!({"type":"response.created","prism_request_event_id":format!("evt_{id}"),"response":{"id":"accepted"}}),
        );
        assert!(worker.pending[0].ack.is_some());
        assert!(!worker.pending[0].finished);
        drop(guard.take());
        worker.advance(&writes).unwrap();
        assert!(worker.responses["accepted"].abandoned);
        release.cancel();
        worker.written(writer.await.unwrap()).unwrap();
        worker.advance(&writes).unwrap();
        let cancellation = jobs.recv().await.unwrap();
        assert_eq!(cancellation.frames.len(), 1);
        let frame: Value = serde_json::from_str(&cancellation.frames[0]).unwrap();
        assert_eq!(frame["type"], "response.cancel");
        assert_eq!(frame["response_id"], "accepted");
        worker
            .written(Written {
                id: cancellation.id,
                skipped: false,
                result: Ok(()),
            })
            .unwrap();
        dispatch(
            &mut worker,
            json!({"type":"response.done","response":{"id":"accepted","status":"cancelled"}}),
        );
        worker.advance(&writes).unwrap();
        assert!(jobs.try_recv().is_err());
        let mut abandoned = 0;
        while let Ok(event) = events.try_recv() {
            if matches!(event.payload, RealtimeEvent::ResponseAbandoned { .. }) {
                abandoned += 1
            }
        }
        assert_eq!(abandoned, 1);
    }
}
