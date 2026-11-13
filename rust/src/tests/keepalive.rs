use super::*;
use crate::Model;
use std::sync::atomic::AtomicBool;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::task::JoinSet;
use tokio_util::task::AbortOnDropHandle;

struct Probe {
    target: String,
    backend: String,
    stalled: Arc<AtomicBool>,
    connections: Arc<AtomicUsize>,
    pings: mpsc::UnboundedReceiver<()>,
    closed: mpsc::UnboundedReceiver<()>,
    _task: AbortOnDropHandle<()>,
}

impl Probe {
    async fn start(backend: String) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let target = listener.local_addr().unwrap().to_string();
        let stalled = Arc::new(AtomicBool::new(false));
        let connections = Arc::new(AtomicUsize::new(0));
        let (ping, pings) = mpsc::unbounded_channel();
        let (close, closed) = mpsc::unbounded_channel();
        let blocked = stalled.clone();
        let count = connections.clone();
        let backend_target = backend.clone();
        let task = tokio::spawn(async move {
            let mut tasks = JoinSet::new();
            loop {
                tokio::select! {
                    accepted = listener.accept() => {
                        let (client, _) = accepted.unwrap();
                        count.fetch_add(1, Ordering::SeqCst);
                        let backend = backend.clone();
                        let blocked = blocked.clone();
                        let ping = ping.clone();
                        let close = close.clone();
                        tasks.spawn(async move {
                            let server = tokio::net::TcpStream::connect(backend).await.unwrap();
                            let (mut client_read, mut client_write) = client.into_split();
                            let (mut server_read, mut server_write) = server.into_split();
                            let outgoing = async {
                                let mut preface = [0; 24];
                                client_read.read_exact(&mut preface).await?;
                                server_write.write_all(&preface).await?;
                                loop {
                                    let mut header = [0; 9];
                                    if let Err(error) = client_read.read_exact(&mut header).await {
                                        break Err::<(), std::io::Error>(error);
                                    }
                                    let size = u32::from_be_bytes([0, header[0], header[1], header[2]]) as usize;
                                    let mut payload = vec![0; size];
                                    client_read.read_exact(&mut payload).await?;
                                    if header[3] == 6 && header[4] & 1 == 0 {
                                        let _ = ping.send(());
                                    }
                                    if !blocked.load(Ordering::SeqCst) {
                                        server_write.write_all(&header).await?;
                                        server_write.write_all(&payload).await?;
                                    }
                                }
                            };
                            let incoming = async {
                                let mut buffer = [0; 65536];
                                loop {
                                    let size = server_read.read(&mut buffer).await?;
                                    if size == 0 { return Ok::<(), std::io::Error>(()); }
                                    if !blocked.load(Ordering::SeqCst) {
                                        client_write.write_all(&buffer[..size]).await?;
                                    }
                                }
                            };
                            tokio::select! { _ = outgoing => {}, _ = incoming => {} }
                            let _ = close.send(());
                        });
                    }
                    _ = tasks.join_next(), if !tasks.is_empty() => {}
                }
            }
        });
        Self {
            target,
            backend: backend_target,
            stalled,
            connections,
            pings,
            closed,
            _task: AbortOnDropHandle::new(task),
        }
    }
}

async fn elapse(duration: Duration) {
    tokio::time::pause();
    tokio::time::advance(duration).await;
    tokio::time::resume();
    tokio::task::yield_now().await;
}

async fn setup(model: Model, enabled: bool) -> (Harness, Probe) {
    let client = Client::builder()
        .api_key("test-key")
        .model(model)
        .enable_tts_keepalive(enabled)
        .build()
        .unwrap();
    let mut harness = Harness::with_client(Mode::Echo, client).await;
    harness.service.discoveries.store(2, Ordering::SeqCst);
    let backend = harness
        .client
        .inner
        .endpoint
        .uri()
        .authority()
        .unwrap()
        .to_string();
    let probe = Probe::start(backend).await;
    Arc::get_mut(&mut harness.client.inner).unwrap().endpoint =
        Endpoint::from_shared(format!("http://{}", probe.target)).unwrap();
    assert_eq!(harness.client.languages().await.unwrap(), ["en"]);
    (harness, probe)
}

#[test]
fn keepalive_scope() {
    for (model, endpoint, enabled, expected) in [
        (Model::Coda, None, false, false),
        (Model::MistV3, None, false, false),
        (Model::Coda, None, true, true),
        (Model::MistV3, None, true, true),
        (Model::Coda, Some("coda.api.rime.ai:443"), true, true),
        (Model::Coda, Some("mist.api.rime.ai"), true, true),
        (Model::Coda, Some("CODA.API.RIME.AI:00443"), true, true),
        (Model::MistV3, Some("coda.api.rime.ai"), true, true),
        (Model::Coda, Some("private.example:443"), true, false),
        (Model::Coda, Some("coda.api.rime.ai.example"), true, false),
        (Model::Coda, Some("mist.api.rime.ai:8443"), true, false),
        (Model::Coda, Some("coda.api.rime.ai:443"), false, false),
        (Model::Coda, Some("mist.api.rime.ai:443"), false, false),
    ] {
        let mut builder = Client::builder()
            .api_key("test-key")
            .model(model)
            .enable_tts_keepalive(enabled);
        if let Some(endpoint) = endpoint {
            builder = builder.endpoint(endpoint);
        }
        let client = builder.build().unwrap();
        assert_eq!(
            client.inner.tts_keepalive_enabled, expected,
            "{model:?} {endpoint:?}"
        );
    }
}

#[tokio::test]
async fn idle_ping_reuses_transport_and_close_releases_it() {
    let (mut harness, mut probe) = setup(Model::Coda, true).await;
    let backend = probe.backend.clone();
    let mut recognition_probe = Probe::start(backend).await;
    Arc::get_mut(&mut harness.client.inner)
        .unwrap()
        .stt_endpoint =
        Endpoint::from_shared(format!("http://{}", recognition_probe.target)).unwrap();
    drop(harness.client.inner.stt_stub().await.unwrap());
    elapse(Duration::from_secs(55)).await;
    assert!(probe.pings.try_recv().is_err());
    elapse(Duration::from_secs(6)).await;
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(3), probe.pings.recv())
            .await
            .unwrap(),
        Some(())
    );
    assert_eq!(harness.client.languages().await.unwrap(), ["en"]);
    assert_eq!(probe.connections.load(Ordering::SeqCst), 1);
    assert!(recognition_probe.pings.try_recv().is_err());
    harness.client.close().await;
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(3), probe.closed.recv())
            .await
            .unwrap(),
        Some(())
    );
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(3), recognition_probe.closed.recv())
            .await
            .unwrap(),
        Some(())
    );
}

#[tokio::test]
async fn disabled_keepalive_sends_no_idle_pings() {
    let (harness, mut probe) = setup(Model::Coda, false).await;
    elapse(Duration::from_secs(120)).await;
    assert!(probe.pings.try_recv().is_err());
    assert_eq!(harness.client.languages().await.unwrap(), ["en"]);
    harness.client.close().await;
}

#[tokio::test]
async fn silent_stall_is_retired_and_the_next_call_reconnects() {
    let (mut harness, mut probe) = setup(Model::MistV3, true).await;
    let discoveries = harness.service.discoveries.load(Ordering::SeqCst);
    probe.stalled.store(true, Ordering::SeqCst);
    Arc::get_mut(&mut harness.client.inner).unwrap().timeout = Some(Duration::from_millis(150));
    assert_eq!(
        harness.client.languages().await.unwrap_err().kind(),
        ErrorKind::Timeout
    );
    Arc::get_mut(&mut harness.client.inner).unwrap().timeout = None;
    elapse(Duration::from_secs(61)).await;
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(3), probe.pings.recv())
            .await
            .unwrap(),
        Some(())
    );
    elapse(Duration::from_secs(18)).await;
    assert!(probe.closed.try_recv().is_err());
    elapse(Duration::from_secs(3)).await;
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(3), probe.closed.recv())
            .await
            .unwrap(),
        Some(())
    );
    assert_eq!(
        harness.service.discoveries.load(Ordering::SeqCst),
        discoveries
    );
    probe.stalled.store(false, Ordering::SeqCst);
    assert_eq!(harness.client.languages().await.unwrap(), ["en"]);
    assert_eq!(
        harness.service.discoveries.load(Ordering::SeqCst),
        discoveries + 1
    );
    assert_eq!(probe.connections.load(Ordering::SeqCst), 2);
    harness.client.close().await;
}
