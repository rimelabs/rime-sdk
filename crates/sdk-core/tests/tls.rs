#![cfg(feature = "test-support")]
use sdk_core::client::{Client, ClientConfig};
use sdk_protocol::*;
use tonic::{Request, Response, Status};
type Audio = tokio_stream::wrappers::ReceiverStream<Result<SynthesisResponseStream, Status>>;
struct Service;
#[tonic::async_trait]
impl text_to_speech_server::TextToSpeech for Service {
    type SynthesizeStream = Audio;
    type SynthesizeStreamingStream = Audio;
    async fn synthesize(&self, _: Request<SynthesisRequest>) -> Result<Response<Audio>, Status> {
        Err(Status::unimplemented("unused"))
    }
    async fn synthesize_streaming(
        &self,
        _: Request<tonic::Streaming<StreamingSynthesisRequest>>,
    ) -> Result<Response<Audio>, Status> {
        Err(Status::unimplemented("unused"))
    }
    async fn normalize_text(
        &self,
        _: Request<NormalizeTextRequest>,
    ) -> Result<Response<NormalizeTextResponse>, Status> {
        Err(Status::unimplemented("unused"))
    }
    async fn get_supported_languages(
        &self,
        request: Request<GetSupportedLanguagesRequest>,
    ) -> Result<Response<GetSupportedLanguagesResponse>, Status> {
        assert_eq!(
            request.metadata().get("authorization").unwrap(),
            "Bearer tls-test"
        );
        Ok(Response::new(GetSupportedLanguagesResponse {
            languages: vec!["en".into()],
        }))
    }
    async fn get_supported_speakers(
        &self,
        _: Request<GetSupportedSpeakersRequest>,
    ) -> Result<Response<GetSupportedSpeakersResponse>, Status> {
        Ok(Response::new(GetSupportedSpeakersResponse {
            speakers: vec![],
        }))
    }
}
#[tokio::test]
async fn tls_verifies_chain_hostname_and_bearer_credentials() {
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let pem = cert.cert.pem();
    let identity = tonic::transport::Identity::from_pem(&pem, cert.signing_key.serialize_pem());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let task = tokio::spawn(
        tonic::transport::Server::builder()
            .tls_config(tonic::transport::ServerTlsConfig::new().identity(identity))
            .unwrap()
            .add_service(text_to_speech_server::TextToSpeechServer::new(Service))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)),
    );
    let config = |host| ClientConfig {
        api_key: "tls-test".into(),
        model: "coda".into(),
        endpoint: Some(format!("{host}:{port}")),
        timeout: Some(2.),
    };
    let trusted = Client::testing_tls(config("localhost"), pem.as_bytes().to_vec()).unwrap();
    assert_eq!(
        trusted.discover(false, None, None, true).await.unwrap(),
        ["en"]
    );
    trusted.close().await;
    let untrusted = Client::new(config("localhost")).unwrap();
    assert!(untrusted.discover(false, None, None, true).await.is_err());
    untrusted.close().await;
    let wrong_name = Client::testing_tls(config("127.0.0.1"), pem.as_bytes().to_vec()).unwrap();
    assert!(wrong_name.discover(false, None, None, true).await.is_err());
    wrong_name.close().await;
    task.abort();
}
