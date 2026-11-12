use super::types::{Review, ReviewDecision, ReviewRequest};
use crate::{Error, ErrorKind};
use futures_util::FutureExt;
use std::{
    collections::{HashMap, VecDeque},
    panic::AssertUnwindSafe,
    time::Duration,
};
use tokio::{
    task::{AbortHandle, JoinSet},
    time::Instant,
};

const LIMIT: usize = 128;

pub(super) struct Completed {
    pub request: ReviewRequest,
    pub deadline: Instant,
    pub decision: Option<Result<ReviewDecision, Error>>,
}

struct Active {
    request: ReviewRequest,
    abort: AbortHandle,
}

/// Session-owned callbacks, including replies that have not been announced yet.
pub(super) struct Reviews {
    config: Option<Review>,
    jobs: JoinSet<Completed>,
    active: HashMap<String, Active>,
    recent: VecDeque<String>,
    stopped: HashMap<String, (Option<u64>, Instant)>,
}

impl Reviews {
    pub fn new(config: Option<Review>) -> Self {
        Self {
            config,
            jobs: JoinSet::new(),
            active: HashMap::new(),
            recent: VecDeque::new(),
            stopped: HashMap::new(),
        }
    }

    pub fn has_jobs(&self) -> bool {
        !self.jobs.is_empty()
    }

    pub fn allows(&self, request: &ReviewRequest) -> bool {
        self.stopped
            .get(&request.response_id)
            .is_none_or(|(index, deadline)| {
                *deadline <= Instant::now() || index.is_some_and(|index| request.index <= index)
            })
    }

    pub fn start(&mut self, request: ReviewRequest) -> Result<(), Error> {
        let Some(config) = &self.config else {
            return Err(Error::new(
                ErrorKind::Stream,
                "review request arrived without a configured callback",
            ));
        };
        if self.active.contains_key(&request.review_id)
            || self.recent.contains(&request.review_id)
            || !self.allows(&request)
        {
            return Ok(());
        }
        if self.jobs.len() >= LIMIT {
            return Err(Error::new(
                ErrorKind::ResourceLimit,
                "too many pending speech reviews",
            ));
        }
        self.stopped
            .retain(|_, (_, deadline)| *deadline > Instant::now());
        let callback = config.callback.clone();
        let deadline = Instant::now() + Duration::from_millis(config.settings.timeout_ms.into());
        let task_request = request.clone();
        let abort = self.jobs.spawn(async move {
            let result = tokio::time::timeout_at(
                deadline,
                AssertUnwindSafe(async { callback(task_request.clone()).await }).catch_unwind(),
            )
            .await;
            let decision = match result {
                Err(_) => None,
                Ok(Err(_)) => Some(Err(Error::new(
                    ErrorKind::Stream,
                    "speech review callback panicked",
                ))),
                Ok(Ok(Err(error))) => Some(Err(Error::new(
                    ErrorKind::Stream,
                    "speech review callback failed",
                )
                .caused_by(error))),
                Ok(Ok(Ok(value))) => Some(Ok(value)),
            };
            Completed {
                request: task_request,
                deadline,
                decision,
            }
        });
        self.recent.push_back(request.review_id.clone());
        if self.recent.len() > LIMIT * 2 {
            self.recent.pop_front();
        }
        self.active
            .insert(request.review_id.clone(), Active { request, abort });
        Ok(())
    }

    pub async fn next(&mut self) -> Result<Option<Completed>, Error> {
        let Some(joined) = self.jobs.join_next().await else {
            return Ok(None);
        };
        let result = match joined {
            Ok(result) => result,
            Err(error) => {
                self.active
                    .retain(|_, active| active.abort.id() != error.id());
                return if error.is_cancelled() {
                    Ok(None)
                } else {
                    Err(Error::new(ErrorKind::Stream, "speech review task failed").caused_by(error))
                };
            }
        };
        if self.active.remove(&result.request.review_id).is_none() {
            return Ok(None);
        }
        if !self.allows(&result.request) || Instant::now() >= result.deadline {
            return Ok(None);
        }
        Ok(Some(result))
    }

    /// Earlier chunks still need their own decisions before the server reaches this one.
    pub fn stop_after(&mut self, response_id: &str, index: Option<u64>) {
        let existing = self.stopped.get(response_id).map(|v| v.0);
        let index = match (existing, index) {
            (Some(None), _) | (_, None) => None,
            (Some(Some(previous)), Some(next)) => Some(previous.min(next)),
            (None, next) => next,
        };
        self.stopped.insert(
            response_id.into(),
            (index, Instant::now() + Duration::from_secs(5)),
        );
        if self.stopped.len() > LIMIT * 2 {
            if let Some(oldest) = self
                .stopped
                .iter()
                .min_by_key(|(_, v)| v.1)
                .map(|(key, _)| key.clone())
            {
                self.stopped.remove(&oldest);
            }
        }
        self.active.retain(|_, active| {
            let remove = active.request.response_id == response_id
                && index.is_none_or(|i| active.request.index > i);
            if remove {
                active.abort.abort();
            }
            !remove
        });
    }

    pub async fn close(&mut self) {
        self.jobs.abort_all();
        while self.jobs.join_next().await.is_some() {}
        self.active.clear();
    }
}
