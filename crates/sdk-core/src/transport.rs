//! Enforce receive limits at the gRPC frame header, before tonic allocates a message.
//!
//! Tonic reports its own receive-limit errors as OUT_OF_RANGE. Checking here lets
//! us return RESOURCE_EXHAUSTED without reclassifying any status sent by a server.
use crate::connection::Channel;
use bytes::Bytes;
use http_body::{Body as HttpBody, Frame};
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll, ready},
};
use tonic::{
    Status,
    body::Body,
    codegen::http::{Request, Response},
};
use tower::Service;

#[derive(Clone)]
pub(crate) struct ReceiveLimit {
    channel: Channel,
    limit: usize,
}

impl ReceiveLimit {
    pub(crate) fn new(channel: Channel, limit: usize) -> Self {
        Self { channel, limit }
    }
}

impl Service<Request<Body>> for ReceiveLimit {
    type Response = Response<Body>;
    type Error = <Channel as Service<Request<Body>>>::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.channel.poll_ready(cx)
    }

    fn call(&mut self, request: Request<Body>) -> Self::Future {
        let response = self.channel.call(request);
        let limit = self.limit;
        Box::pin(async move {
            Ok(response
                .await?
                .map(|body| Body::new(LimitedBody::new(body, limit))))
        })
    }
}

struct LimitedBody {
    inner: Body,
    lengths: MessageLengths,
}

impl LimitedBody {
    fn new(inner: Body, limit: usize) -> Self {
        Self {
            inner,
            lengths: MessageLengths {
                limit,
                header: [0; 5],
                header_len: 0,
                remaining: 0,
                inspect: true,
            },
        }
    }
}

impl HttpBody for LimitedBody {
    type Data = Bytes;
    type Error = Status;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Status>>> {
        let frame = ready!(Pin::new(&mut self.inner).poll_frame(cx));
        if let Some(Ok(frame)) = &frame
            && let Some(data) = frame.data_ref()
            && let Err(error) = self.lengths.check(data)
        {
            self.inner = Body::empty();
            return Poll::Ready(Some(Err(error)));
        }
        Poll::Ready(frame)
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }
}

struct MessageLengths {
    limit: usize,
    header: [u8; 5],
    header_len: usize,
    remaining: usize,
    inspect: bool,
}

impl MessageLengths {
    fn check(&mut self, mut data: &[u8]) -> Result<(), Status> {
        while self.inspect && !data.is_empty() {
            if self.remaining > 0 {
                let skip = self.remaining.min(data.len());
                self.remaining -= skip;
                data = &data[skip..];
                continue;
            }
            let count = (5 - self.header_len).min(data.len());
            self.header[self.header_len..self.header_len + count].copy_from_slice(&data[..count]);
            self.header_len += count;
            data = &data[count..];
            if self.header_len == 5 {
                // SDK clients do not enable compression. Let tonic report an
                // unsupported compression flag or another malformed flag.
                if self.header[0] != 0 {
                    self.inspect = false;
                    break;
                }
                let length = u32::from_be_bytes(self.header[1..].try_into().unwrap()) as usize;
                if length > self.limit {
                    return Err(Status::resource_exhausted(
                        "Received message exceeds size limit",
                    ));
                }
                self.remaining = length;
                self.header_len = 0;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http_body_util::{BodyExt, StreamBody};

    fn body(frames: Vec<Frame<Bytes>>, limit: usize) -> LimitedBody {
        LimitedBody::new(
            Body::new(StreamBody::new(tokio_stream::iter(
                frames.into_iter().map(Ok::<_, Status>),
            ))),
            limit,
        )
    }

    #[tokio::test]
    async fn receive_limit_handles_fragmented_headers_and_multiple_messages() {
        // Empty message, a message exactly at the limit, and another empty one.
        let wire = Bytes::from_static(&[0, 0, 0, 0, 0, 0, 0, 0, 0, 3, 1, 2, 3, 0, 0, 0, 0, 0]);
        for width in 1..=wire.len() {
            let frames = wire
                .chunks(width)
                .map(|data| Frame::data(Bytes::copy_from_slice(data)))
                .collect();
            assert_eq!(body(frames, 3).collect().await.unwrap().to_bytes(), wire);
        }
    }

    #[tokio::test]
    async fn receive_limit_rejects_the_header_without_waiting_for_the_payload() {
        for prefix in [&[][..], &[0, 0, 0, 0, 1, 42][..]] {
            let wire = [prefix, &[0, 0, 0, 0, 4]].concat();
            for width in 1..=wire.len() {
                let frames = wire
                    .chunks(width)
                    .map(|data| Frame::data(Bytes::copy_from_slice(data)))
                    .collect();
                let error = body(frames, 3).collect().await.unwrap_err();
                assert_eq!(error.code(), tonic::Code::ResourceExhausted);
            }
        }
    }

    #[tokio::test]
    async fn receive_limit_preserves_server_status_and_malformed_flags() {
        let mut trailers = tonic::codegen::http::HeaderMap::new();
        trailers.insert("grpc-status", "11".parse().unwrap());
        trailers.insert("grpc-message", "server-out-of-range".parse().unwrap());
        let collected = body(vec![Frame::trailers(trailers.clone())], 3)
            .collect()
            .await
            .unwrap();
        assert_eq!(collected.trailers(), Some(&trailers));
        for flag in [1, 2, 255] {
            let wire = Bytes::from(vec![flag, 0, 0, 0, 4]);
            let collected = body(vec![Frame::data(wire.clone())], 3)
                .collect()
                .await
                .unwrap();
            assert_eq!(collected.to_bytes(), wire);
        }
    }
}
