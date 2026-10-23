//! Connect application-owned audio adapters over a local TCP bridge.
//! The bridge supplies actual device measurements. See the SDK README.
use base64::{engine::general_purpose::STANDARD, Engine};
use rimelabs_sdk::{
    realtime::{
        AudioChunk, ConversationOutput, ConversationPlayback, ConversationRunner,
        ConversationSource, OutputRef, RealtimeConnectOptions, RealtimeEvent,
    },
    Client, Error, PcmFormat,
};
use serde_json::{json, Value};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::TcpStream,
    sync::{mpsc, watch},
};
use tokio_util::sync::CancellationToken;
fn input(message: &str) -> Error {
    Error::input(std::io::Error::other(message))
}
struct Shared {
    send: mpsc::Sender<Value>,
    receipts: Mutex<HashMap<String, watch::Sender<Option<f64>>>>,
    stopped: CancellationToken,
    failed: CancellationToken,
}
impl Shared {
    fn queue(&self, v: Value) -> Result<(), Error> {
        if self.failed.is_cancelled() {
            return Err(input("audio bridge failed"));
        }
        self.send
            .try_send(v)
            .map_err(|_| input("audio bridge output queue is full"))
    }
}
struct Source {
    audio: mpsc::Receiver<Vec<u8>>,
    shared: Arc<Shared>,
}
impl ConversationSource for Source {
    async fn read(&mut self, stop: CancellationToken) -> Result<Option<AudioChunk>, Error> {
        tokio::select! {_=stop.cancelled()=>Err(Error::cancelled()),_=self.shared.failed.cancelled()=>Err(input("audio bridge failed")),data=self.audio.recv()=>Ok(data.map(|data|AudioChunk{data:data.into(),format:PcmFormat::default()}))}
    }
}
struct Output {
    id: String,
    shared: Arc<Shared>,
    receipt: watch::Receiver<Option<f64>>,
}
impl ConversationOutput for Output {
    fn write(&self, chunk: AudioChunk) -> Result<(), Error> {
        self.shared
            .queue(json!({"type":"audio","id":self.id,"data":STANDARD.encode(chunk.data)}))
    }
    fn end(&self) -> Result<(), Error> {
        self.shared.queue(json!({"type":"end","id":self.id}))
    }
    async fn completion(&self, stop: CancellationToken) -> Result<f64, Error> {
        let mut receipt = self.receipt.clone();
        loop {
            if let Some(n) = *receipt.borrow_and_update() {
                return Ok(n);
            }
            tokio::select! {_=stop.cancelled()=>return Err(Error::cancelled()),_=self.shared.failed.cancelled()=>return Err(input("audio bridge failed")),r=receipt.changed()=>if r.is_err(){return Err(input("missing playback receipt"))}}
        }
    }
}
struct Playback(Arc<Shared>);
fn key(r: &OutputRef) -> String {
    format!(
        "{}/{}/{}",
        r.response().response_id(),
        r.item_id(),
        r.output_index()
    )
}
impl ConversationPlayback for Playback {
    type Output = Output;
    fn open(&mut self, r: &OutputRef) -> Result<Arc<Output>, Error> {
        let id = key(r);
        let (tx, receipt) = watch::channel(None);
        self.0.receipts.lock().unwrap().insert(id.clone(), tx);
        self.0.queue(json!({"type":"open","id":id}))?;
        Ok(Arc::new(Output {
            id,
            shared: self.0.clone(),
            receipt,
        }))
    }
    fn interrupt(&mut self, refs: &[OutputRef]) -> Result<(), Error> {
        self.0
            .queue(json!({"type":"interrupt","ids":refs.iter().map(key).collect::<Vec<_>>()}))
    }
    async fn stop(&mut self, stop: CancellationToken) -> Result<(), Error> {
        self.0.queue(json!({"type":"stop"}))?;
        tokio::select! {_=self.0.stopped.cancelled()=>Ok(()),_=stop.cancelled()=>Err(Error::cancelled()),_=self.0.failed.cancelled()=>Err(input("audio bridge failed"))}
    }
}
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let address = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:8766".into());
    let connection = TcpStream::connect(address).await?;
    let (read, mut write) = connection.into_split();
    let (send, mut outgoing) = mpsc::channel::<Value>(128);
    let (audio, capture) = mpsc::channel(128);
    let shared = Arc::new(Shared {
        send,
        receipts: Mutex::new(HashMap::new()),
        stopped: CancellationToken::new(),
        failed: CancellationToken::new(),
    });
    let mut tasks = tokio::task::JoinSet::new();
    let writer = shared.clone();
    tasks.spawn(async move {
        while let Some(v) = outgoing.recv().await {
            let mut bytes = v.to_string().into_bytes();
            bytes.push(b'\n');
            if write.write_all(&bytes).await.is_err() {
                writer.failed.cancel();
                break;
            }
        }
    });
    let reader = shared.clone();
    tasks.spawn(async move {
        let mut read = BufReader::new(read);
        let mut line = Vec::new();
        let mut audio = Some(audio);
        let result: Result<(), Error> = async {
            loop {
                line.clear();
                while !line.ends_with(b"\n") {
                    let available = read.fill_buf().await.map_err(Error::input)?;
                    if available.is_empty() {
                        return Err(input("audio bridge disconnected"));
                    }
                    let n = available
                        .iter()
                        .position(|b| *b == b'\n')
                        .map_or(available.len(), |i| i + 1);
                    if line.len() + n > 1024 * 1024 {
                        return Err(input("audio bridge message exceeds 1 MiB"));
                    }
                    line.extend_from_slice(&available[..n]);
                    read.consume(n);
                }
                let v: Value = serde_json::from_slice(&line).map_err(Error::input)?;
                match v["type"].as_str() {
                    Some("audio") => {
                        let data = STANDARD
                            .decode(v["data"].as_str().ok_or_else(|| input("missing audio"))?)
                            .map_err(Error::input)?;
                        if data.len() > 192000 || data.len() % 2 != 0 {
                            return Err(input("invalid capture chunk"));
                        }
                        audio
                            .as_ref()
                            .ok_or_else(|| input("audio after capture EOF"))?
                            .try_send(data)
                            .map_err(|_| input("capture queue is full"))?;
                    }
                    Some("played") => {
                        let id = v["id"]
                            .as_str()
                            .ok_or_else(|| input("missing receipt id"))?;
                        let n = v["played_ms"]
                            .as_f64()
                            .ok_or_else(|| input("missing measurement"))?;
                        if let Some(tx) = reader.receipts.lock().unwrap().remove(id) {
                            let _ = tx.send(Some(n));
                        }
                    }
                    Some("stopped") => reader.stopped.cancel(),
                    Some("eof") => {
                        audio.take();
                    }
                    _ => return Err(input("unknown bridge message")),
                }
            }
        }
        .await;
        if result.is_err() {
            reader.failed.cancel()
        }
    });
    let client = Client::builder().build()?;
    let session = client
        .realtime()
        .connect(RealtimeConnectOptions::default())
        .await?;
    let source = Source {
        audio: capture,
        shared: shared.clone(),
    };
    let runner = ConversationRunner::new(session, source, Playback(shared)).on_event(|e| {
        if let RealtimeEvent::TextDelta { delta, .. } = &e.payload {
            eprint!("{delta}")
        }
        Ok(())
    });
    let result = runner.run(CancellationToken::new()).await;
    client.close().await;
    tasks.shutdown().await;
    result?;
    Ok(())
}
