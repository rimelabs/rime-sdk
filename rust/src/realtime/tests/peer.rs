use crate::{
    realtime::{RealtimeConnectOptions, RealtimeSession, RealtimeTimeouts, ResponseRef},
    Client,
};
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{net::TcpListener, sync::mpsc, task::JoinHandle};
use tokio_tungstenite::{
    accept_hdr_async,
    tungstenite::{
        handshake::server::{Request, Response},
        Message,
    },
};

pub(super) struct Peer {
    pub endpoint: String,
    send: mpsc::Sender<Value>,
    recv: mpsc::Receiver<Value>,
    /// Each session.update's settings.
    updates: mpsc::Receiver<Value>,
    /// The upgrade's x-request-id.
    request_id: Arc<Mutex<Option<String>>>,
    task: JoinHandle<()>,
}
impl Drop for Peer {
    fn drop(&mut self) {
        self.task.abort()
    }
}
impl Peer {
    #[allow(clippy::result_large_err)] // The WebSocket handshake callback fixes this error type.
    pub async fn new() -> Self {
        Self::with_paused_reads(false).await
    }
    #[allow(clippy::result_large_err)] // The WebSocket library fixes this callback type.
    pub async fn with_paused_reads(pause: bool) -> Self {
        super::schema::prepare();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("ws://{}/v1/realtime", listener.local_addr().unwrap());
        let (send, mut incoming) = mpsc::channel::<Value>(512);
        let (out, recv) = mpsc::channel(512);
        let (update, updates) = mpsc::channel(16);
        let request_id = Arc::new(Mutex::new(None));
        let header = request_id.clone();
        let task = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut ws = accept_hdr_async(socket, |r: &Request, response: Response| {
                assert_eq!(r.headers()["authorization"], "Bearer test");
                *header.lock().unwrap() = r
                    .headers()
                    .get("x-request-id")
                    .map(|v| v.to_str().unwrap().to_owned());
                Ok(response)
            })
            .await
            .unwrap();
            ws.send(Message::Text(
                json!({"type":"session.created","session":{"id":"session-1"}})
                    .to_string()
                    .into(),
            ))
            .await
            .unwrap();
            let used = header
                .lock()
                .unwrap()
                .clone()
                .unwrap_or_else(|| "server-made".into());
            let mut initialized = false;
            loop {
                tokio::select! {
                 v=incoming.recv()=>match v{Some(v)=>if ws.send(Message::Text(v.to_string().into())).await.is_err(){break},None=>break},
                 message=ws.next(), if !(pause && initialized)=>{let Some(Ok(Message::Text(message)))=message else{break};let v:Value=serde_json::from_str(&message).unwrap();super::schema::validate_request(&v);if v["type"]=="session.update"{initialized=true;let mut session=json!({"id":"session-1","voice":"test","request_id":used});if let Some(labels)=v["session"].get("prism_response_metadata"){session["prism_response_metadata"]=labels.clone()}let _=update.try_send(v["session"].clone());if ws.send(Message::Text(json!({"type":"session.updated","prism_request_event_id":v["event_id"],"session":session}).to_string().into())).await.is_err(){break}}else if out.send(v).await.is_err(){break}}
                }
            }
        });
        Self {
            endpoint,
            send,
            recv,
            updates,
            request_id,
            task,
        }
    }
    pub async fn setup(&self) -> (Client, RealtimeSession) {
        self.setup_with(RealtimeConnectOptions::default()).await
    }
    pub async fn setup_with(&self, options: RealtimeConnectOptions) -> (Client, RealtimeSession) {
        let client = Client::builder()
            .api_key("test")
            .timeout(Duration::from_nanos(1))
            .build()
            .unwrap();
        let session = client
            .realtime()
            .connect(options.endpoint(&self.endpoint).timeouts(RealtimeTimeouts {
                connect: Duration::from_secs(2),
                ready: Duration::from_secs(1),
                request: Duration::from_secs(1),
            }))
            .await
            .unwrap();
        (client, session)
    }
    pub fn request_id(&self) -> Option<String> {
        self.request_id.lock().unwrap().clone()
    }
    pub async fn update(&mut self) -> Value {
        tokio::time::timeout(Duration::from_secs(3), self.updates.recv())
            .await
            .expect("no session.update")
            .unwrap()
    }
    pub async fn send(&self, v: Value) {
        self.send.send(v).await.unwrap()
    }
    pub async fn next(&mut self, kind: &str) -> Value {
        let v = tokio::time::timeout(Duration::from_secs(3), self.recv.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(v["type"], kind, "{v}");
        v
    }
    pub async fn accept(&self, q: &Value, id: &str) {
        self.send(json!({"type":"response.created","prism_request_event_id":q["event_id"],"response":{"id":id}})).await
    }
    pub async fn ended(&self, id: &str) {
        self.send(json!({"type":"response.done","response":{"id":id,"status":"completed","metadata":{"prism_drain_token":format!("token-{id}")}}})).await
    }
    pub async fn turn(&mut self, s: &RealtimeSession, id: &str) -> ResponseRef {
        self.send(json!({"type":"prism.typed_input.ready"})).await;
        let session = s.clone();
        let result = tokio::spawn(async move { session.send_text("hello").await });
        let q = self.next("response.create").await;
        self.accept(&q, id).await;
        result.await.unwrap().unwrap()
    }
    pub async fn no_request(&mut self) {
        assert!(
            tokio::time::timeout(Duration::from_millis(40), self.recv.recv())
                .await
                .is_err()
        )
    }
    pub async fn closed_without_request(&mut self) {
        assert!(
            tokio::time::timeout(Duration::from_secs(3), self.recv.recv())
                .await
                .expect("session did not close")
                .is_none(),
            "unexpected request before closure"
        );
    }
    pub fn disconnect(&self) {
        self.task.abort()
    }
}
pub(super) async fn event(
    events: &mut crate::realtime::RealtimeEvents,
) -> crate::realtime::SessionEvent {
    tokio::time::timeout(Duration::from_secs(3), events.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap()
}
