//! A reusable HTTP/2 sender whose readiness reflects the connection itself.
//! A buffered tonic Channel only reports queue capacity and can cache a failed
//! reconnect for the next RPC. Keep connection retries before RPC submission.
use crate::{connector::Proxy, error::CoreError};
use futures_util::task::AtomicWaker;
use http_body_util::BodyExt;
use hyper::client::conn::http2::{self, SendRequest};
use hyper_util::rt::{TokioExecutor, TokioIo};
use std::{
    future::{Future, poll_fn},
    pin::Pin,
    sync::{Arc, Weak},
    task::{Context, Poll},
};
use tokio::net::TcpStream;
use tokio_rustls::{
    TlsConnector,
    rustls::{
        self,
        pki_types::{CertificateDer, ServerName, pem::PemObject},
    },
};
use tonic::{
    body::Body,
    codegen::http::{Request, Response, Uri},
};
use tower::Service;

type BoxError = Box<dyn std::error::Error + Send + Sync>;

pub(crate) struct ConnectionConfig {
    target: String,
    origin: Uri,
    tls: Option<TlsConnector>,
    proxy: Option<Proxy>,
}

impl ConnectionConfig {
    pub(crate) fn new(
        target: &str,
        secure: bool,
        extra_ca: Option<&[u8]>,
    ) -> Result<Self, CoreError> {
        let tls = if secure {
            let mut roots = rustls::RootCertStore::empty();
            roots.add_parsable_certificates(rustls_native_certs::load_native_certs().certs);
            let mut add_pem = |pem: &[u8]| -> Result<(), CoreError> {
                for cert in CertificateDer::pem_slice_iter(pem) {
                    roots
                        .add(cert.map_err(|_| {
                            CoreError::new("Unavailable", "Invalid TLS certificate")
                        })?)
                        .map_err(|_| CoreError::new("Unavailable", "Invalid TLS certificate"))?;
                }
                Ok(())
            };
            for name in ["GRPC_DEFAULT_SSL_ROOTS_FILE_PATH", "NODE_EXTRA_CA_CERTS"] {
                if let Some(path) = std::env::var_os(name) {
                    let pem = std::fs::read(path).map_err(|_| {
                        CoreError::new("Unavailable", "Cannot read TLS root certificate file")
                    })?;
                    add_pem(&pem)?;
                }
            }
            if let Some(pem) = extra_ca {
                add_pem(pem)?;
            }
            let provider = rustls::crypto::CryptoProvider::get_default()
                .cloned()
                .unwrap_or_else(|| Arc::new(rustls::crypto::ring::default_provider()));
            let mut config = rustls::ClientConfig::builder_with_provider(provider)
                .with_safe_default_protocol_versions()
                .map_err(|_| CoreError::new("Unavailable", "TLS configuration failed"))?
                .with_root_certificates(roots)
                .with_no_client_auth();
            config.alpn_protocols = vec![b"h2".to_vec()];
            Some(TlsConnector::from(Arc::new(config)))
        } else {
            None
        };
        let scheme = if secure { "https" } else { "http" };
        Ok(Self {
            target: target.into(),
            origin: format!("{scheme}://{target}")
                .parse()
                .map_err(|_| CoreError::input("Invalid endpoint"))?,
            tls,
            proxy: if secure {
                Proxy::from_env(target)
            } else {
                None
            },
        })
    }

    pub(crate) async fn connect(&self) -> Result<Channel, BoxError> {
        let socket = if let Some(proxy) = &self.proxy {
            proxy.connect(&self.target).await?
        } else {
            TcpStream::connect(&self.target).await?
        };
        socket.set_nodelay(true)?;
        let sender = if let Some(tls) = &self.tls {
            let name = ServerName::try_from(self.origin.host().unwrap().to_owned())?;
            let socket = tls.connect(name, socket).await?;
            if socket.get_ref().1.alpn_protocol() != Some(b"h2") {
                return Err(std::io::Error::other("Server did not negotiate HTTP/2").into());
            }
            handshake(socket).await?
        } else {
            handshake(socket).await?
        };
        Ok(Channel {
            sender,
            origin: self.origin.clone(),
        })
    }
}

// Hyper drives socket I/O in a separate task. Wake the handshake after that
// task polls, so it can observe applied SETTINGS without a timer or a probe RPC.
#[derive(Clone)]
struct HandshakeExecutor(Weak<AtomicWaker>);

impl<F> hyper::rt::Executor<F> for HandshakeExecutor
where
    F: Future + Send + 'static,
    F::Output: Send,
{
    fn execute(&self, future: F) {
        let readiness = self.0.clone();
        TokioExecutor::new().execute(async move {
            let mut future = std::pin::pin!(future);
            poll_fn(|cx| {
                let result = future.as_mut().poll(cx);
                if let Some(readiness) = readiness.upgrade() {
                    readiness.wake();
                }
                result
            })
            .await
        });
    }
}

async fn handshake<T>(socket: T) -> Result<SendRequest<Body>, BoxError>
where
    T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let readiness = Arc::new(AtomicWaker::new());
    let (sender, mut connection) =
        http2::Builder::new(HandshakeExecutor(Arc::downgrade(&readiness)))
            .initial_max_send_streams(0)
            .handshake(TokioIo::new(socket))
            .await?;
    // SendRequest::ready only checks whether the dispatcher is closed. Starting
    // with zero capacity makes peer SETTINGS necessary before setup completes.
    poll_fn(|cx| {
        readiness.register(cx.waker());
        if let Poll::Ready(result) = Pin::new(&mut connection).poll(cx) {
            return Poll::Ready(Err(match result {
                Err(error) => Box::new(error) as BoxError,
                Ok(()) => std::io::Error::other("Connection closed during HTTP/2 setup").into(),
            }));
        }
        if connection.current_max_send_streams() > 0 {
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    })
    .await?;
    tokio::spawn(async move {
        let _ = connection.await;
    });
    Ok(sender)
}

#[derive(Clone)]
pub(crate) struct Channel {
    sender: SendRequest<Body>,
    origin: Uri,
}

impl Channel {
    pub(crate) async fn ready(&mut self) -> Result<(), hyper::Error> {
        self.sender.ready().await
    }
}

impl Service<Request<Body>> for Channel {
    type Response = Response<Body>;
    type Error = BoxError;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.sender.poll_ready(cx).map_err(transport_error)
    }

    fn call(&mut self, mut request: Request<Body>) -> Self::Future {
        let mut uri = request.uri().clone().into_parts();
        uri.scheme = self.origin.scheme().cloned();
        uri.authority = self.origin.authority().cloned();
        *request.uri_mut() = Uri::from_parts(uri).expect("valid request URI and origin");
        let response = self.sender.send_request(request);
        Box::pin(async move {
            Ok(response
                .await
                .map_err(transport_error)?
                .map(|body| Body::new(body.map_err(transport_error))))
        })
    }
}

fn transport_error(error: hyper::Error) -> BoxError {
    use std::error::Error;

    let mut source = error.source();
    let mut connection_lost = error.is_closed() || error.is_incomplete_message();
    while let Some(cause) = source {
        // h2 keeps its I/O error outside the standard source chain.
        if cause.is::<std::io::Error>()
            || cause
                .downcast_ref::<h2::Error>()
                .is_some_and(h2::Error::is_io)
        {
            connection_lost = true;
            break;
        }
        source = cause.source();
    }
    if connection_lost {
        // Convert transport failures here, before tonic maps them to Unknown.
        // Server gRPC statuses arrive in responses and do not use this path.
        Box::new(tonic::Status::unavailable("Connection lost"))
    } else {
        Box::new(error)
    }
}
