//! Dormant Themis exchange. Production still sends the API key as a bearer token.
use crate::error::{CoreError, Result};
use serde::Deserialize;
use std::time::Duration;

pub struct Token {
    pub value: String,
    pub refresh_at: tokio::time::Instant,
}
#[derive(Deserialize)]
struct Response {
    access_token: String,
    expires_in: f64,
    audience: String,
}
pub async fn exchange(key: &str, audience: &str, url: &str, timeout: Duration) -> Result<Token> {
    let operation = async {
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|_| CoreError::new("Authentication", "Credential exchange failed"))?;
        let mut response = client
            .post(url)
            .header("Authorization", format!("Api-Key {key}"))
            .json(&serde_json::json!({"audience":audience}))
            .send()
            .await
            .map_err(|_| CoreError::new("Authentication", "Credential exchange failed"))?;
        let kind = match response.status().as_u16() {
            200 => None,
            403 => Some("Permission"),
            429 => Some("ResourceLimit"),
            500..=599 => Some("Unavailable"),
            _ => Some("Authentication"),
        };
        if let Some(kind) = kind {
            return Err(CoreError::new(
                kind,
                "Credential exchange rejected the request",
            ));
        }
        let mut data = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|_| CoreError::new("Authentication", "Credential exchange failed"))?
        {
            if data.len() + chunk.len() > 65536 {
                return Err(CoreError::new(
                    "Authentication",
                    "Credential response exceeds the size limit",
                ));
            }
            data.extend_from_slice(&chunk);
        }
        let body: Response = serde_json::from_slice(&data).map_err(|_| {
            CoreError::new(
                "Authentication",
                "Credential exchange returned an invalid token",
            )
        })?;
        if body.access_token.is_empty()
            || !body.access_token.bytes().all(|b| (33..=126).contains(&b))
            || body.audience != audience
            || !body.expires_in.is_finite()
            || body.expires_in <= 0.
            || body.expires_in > 315360000.
        {
            return Err(CoreError::new(
                "Authentication",
                "Credential exchange returned an invalid token",
            ));
        }
        Ok(Token {
            value: body.access_token,
            refresh_at: tokio::time::Instant::now()
                + Duration::from_secs_f64(body.expires_in - 30f64.min(body.expires_in / 10.)),
        })
    };
    tokio::time::timeout(timeout, operation)
        .await
        .unwrap_or_else(|_| Err(CoreError::new("Timeout", "Credential exchange timed out")))
}

/// A refresh is shared by callers and lives until it completes or the owner closes.
/// Cancelling one waiter cannot cancel another waiter's refresh.
use futures_util::{
    FutureExt,
    future::{BoxFuture, Shared},
};
use std::sync::Arc;
type Refresh = Shared<BoxFuture<'static, Result<Arc<Token>>>>;
#[derive(Default)]
struct Cache {
    token: Option<Arc<Token>>,
    refresh: Option<Refresh>,
}
pub struct Credentials {
    key: std::sync::Mutex<String>,
    audience: String,
    url: String,
    state: tokio::sync::Mutex<Cache>,
    closed: tokio_util::sync::CancellationToken,
}
impl Credentials {
    pub fn new(key: String, audience: String, url: String) -> Arc<Self> {
        Arc::new(Self {
            key: std::sync::Mutex::new(key),
            audience,
            url,
            state: tokio::sync::Mutex::new(Cache::default()),
            closed: tokio_util::sync::CancellationToken::new(),
        })
    }
    pub async fn token(self: &Arc<Self>) -> Result<Arc<Token>> {
        if self.closed.is_cancelled() {
            return Err(CoreError::new("Authentication", "Credentials are closed"));
        }
        let refresh = {
            let mut cache = self.state.lock().await;
            if let Some(token) = cache
                .token
                .as_ref()
                .filter(|t| t.refresh_at > tokio::time::Instant::now())
            {
                return Ok(token.clone());
            }
            cache.refresh.get_or_insert_with(|| {
                let owner=self.clone();
                let task=tokio::spawn(async move {
                    let key=owner.key.lock().unwrap().clone();
                    let result=tokio::select! { biased;
                        _=owner.closed.cancelled()=>Err(CoreError::new("Authentication","Credentials are closed")),
                        result=exchange(&key,&owner.audience,&owner.url,Duration::from_secs(10))=>result.map(Arc::new),
                    };
                    let mut cache=owner.state.lock().await;
                    if !owner.closed.is_cancelled() {cache.token=result.as_ref().ok().cloned();}
                    cache.refresh=None;
                    result
                });
                async move {task.await.map_err(|_|CoreError::new("Authentication","Credential acquisition failed"))?}.boxed().shared()
            }).clone()
        };
        refresh.await
    }
    pub async fn close(&self) {
        self.closed.cancel();
        self.key.lock().unwrap().clear();
        let mut cache = self.state.lock().await;
        cache.token = None;
        cache.refresh = None;
    }
}
