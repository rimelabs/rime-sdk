// Bounded, atomic transcript snapshots. Failure discards unread updates;
// consuming the final result makes successful completion irreversible.
use super::TranscriptionUpdate;
use crate::Error;
use futures_util::task::AtomicWaker;
use std::{
    collections::VecDeque,
    sync::{Mutex, OnceLock},
    task::{Context, Poll},
};
use tokio::sync::Notify;

const QUEUED_UPDATES: usize = 16;

#[derive(Default)]
struct Queue {
    items: VecDeque<TranscriptionUpdate>,
    error: Option<Error>,
    final_consumed: bool,
}
#[derive(Default)]
pub(super) struct TranscriptQueue {
    queue: Mutex<Queue>,
    reader: AtomicWaker,
    space: Notify,
    consumed: Notify,
    request_id: OnceLock<String>,
}
impl TranscriptQueue {
    pub(super) fn request_id(&self) -> Option<&str> {
        self.request_id.get().map(String::as_str)
    }
    pub(super) fn set_request_id(&self, id: &str) {
        let _ = self.request_id.set(id.to_owned());
    }
    pub(super) async fn wait_consumed(&self) {
        self.consumed.notified().await;
    }
    pub(super) fn poll_next(
        &self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<TranscriptionUpdate, Error>> {
        self.reader.register(cx.waker());
        let mut queue = self.queue.lock().expect("transcript queue poisoned");
        if let Some(error) = &queue.error {
            return Poll::Ready(Err(error.clone()));
        }
        if let Some(update) = queue.items.pop_front() {
            if matches!(update, TranscriptionUpdate::Final { .. }) {
                queue.final_consumed = true;
                self.consumed.notify_one();
            }
            self.space.notify_one();
            return Poll::Ready(Ok(update));
        }
        Poll::Pending
    }
    pub(super) fn fail(&self, error: Error) {
        if let Some(id) = error.request_id() {
            let _ = self.request_id.set(id.to_owned());
        }
        let mut queue = self.queue.lock().expect("transcript queue poisoned");
        if queue.final_consumed || queue.error.is_some() {
            return;
        }
        queue.items.clear();
        queue.error = Some(error.with_request_id(self.request_id.get().map(String::as_str)));
        drop(queue);
        self.reader.wake();
    }
    pub(super) async fn put(&self, update: TranscriptionUpdate) -> Result<(), Error> {
        loop {
            {
                let mut queue = self.queue.lock().expect("transcript queue poisoned");
                if let Some(error) = &queue.error {
                    return Err(error.clone());
                }
                if queue.items.len() < QUEUED_UPDATES {
                    queue.items.push_back(update);
                    drop(queue);
                    self.reader.wake();
                    return Ok(());
                }
            }
            self.space.notified().await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ErrorKind;
    use futures_util::FutureExt;

    #[tokio::test]
    async fn queue_bounds_snapshots_and_wakes_blocked_writer() {
        let shared = TranscriptQueue::default();
        for n in 0..QUEUED_UPDATES {
            shared
                .put(TranscriptionUpdate::Partial {
                    text: n.to_string(),
                })
                .await
                .unwrap();
        }
        let mut blocked = Box::pin(shared.put(TranscriptionUpdate::Partial {
            text: "next".into(),
        }));
        assert!(blocked.as_mut().now_or_never().is_none());
        {
            let mut queue = shared.queue.lock().unwrap();
            assert_eq!(queue.items.len(), QUEUED_UPDATES);
            assert_eq!(
                queue.items.pop_front(),
                Some(TranscriptionUpdate::Partial { text: "0".into() })
            );
        }
        shared.space.notify_one();
        blocked.await.unwrap();
        assert_eq!(shared.queue.lock().unwrap().items.len(), QUEUED_UPDATES);
        shared.fail(Error::new(ErrorKind::Cancelled, "cancelled"));
        let queue = shared.queue.lock().unwrap();
        assert!(queue.items.is_empty());
        assert_eq!(queue.error.as_ref().unwrap().kind(), ErrorKind::Cancelled);
    }
}
