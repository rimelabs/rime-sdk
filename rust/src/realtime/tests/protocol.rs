use crate::realtime::protocol::decode;
use serde_json::Value;

#[test]
fn ipv6_loopback_endpoint_is_accepted() {
    use crate::realtime::{client::settings, RealtimeConnectOptions};
    let endpoint = "ws://[::1]:8765/v1/realtime";
    let uri: tokio_tungstenite::tungstenite::http::Uri = endpoint.parse().unwrap();
    assert_eq!(uri.host(), Some("[::1]"));
    assert!(settings(&RealtimeConnectOptions::default().endpoint(endpoint)).is_ok());
    assert!(settings(
        &RealtimeConnectOptions::default().endpoint("ws://[2001:db8::1]:8765/v1/realtime")
    )
    .is_err());
}

#[test]
fn integer_fields_accept_integral_json_numbers() {
    use crate::realtime::RealtimeEvent;
    for (number, expected) in [
        ("0", 0),
        ("-0.0", 0),
        ("1000.0", 1000),
        ("1e3", 1000),
        ("9007199254740991", 9_007_199_254_740_991),
        ("9007199254740991.0", 9_007_199_254_740_991),
    ] {
        let event = format!(
            r#"{{"type":"prism.input_audio_buffer.speech_hint","audio_start_ms":{number}}}"#
        );
        let wire = decode(&event, "session-1").unwrap().unwrap();
        let Some(RealtimeEvent::SpeechHint { audio_start_ms }) = wire.payload else {
            panic!("missing speech hint for {number}");
        };
        assert_eq!(audio_start_ms, expected, "{number}");
    }
    for number in [
        "-1",
        "-0.5",
        "1.5",
        "9007199254740992",
        "9007199254740992.0",
        "1e20",
        "1e400",
        "true",
        "null",
        "\"1000\"",
    ] {
        let event = format!(
            r#"{{"type":"prism.input_audio_buffer.speech_hint","audio_start_ms":{number}}}"#
        );
        assert!(decode(&event, "session-1").is_err(), "{number}");
    }
}

#[test]
fn connect_without_runtime_returns_input_error() {
    use crate::{Client, ErrorKind, RealtimeConnectOptions};
    use std::{
        future::Future,
        task::{Context, Poll, Waker},
    };
    let client = Client::builder().api_key("test").build().unwrap();
    let realtime = client.realtime();
    let mut connect = Box::pin(realtime.connect(RealtimeConnectOptions::default()));
    let mut context = Context::from_waker(Waker::noop());
    match connect.as_mut().poll(&mut context) {
        Poll::Ready(Err(error)) => assert_eq!(error.kind(), ErrorKind::Input),
        _ => panic!("connect must reject calls outside a Tokio runtime"),
    }
}

#[test]
fn shared_protocol() {
    let cases: Value =
        serde_json::from_str(include_str!("../../../testdata/prism/protocol.json")).unwrap();
    for c in cases.as_array().unwrap() {
        let result = decode(&c["event"].to_string(), "session-1");
        if c["invalid"] == true {
            assert!(result.is_err(), "{}", c["name"])
        } else {
            let event = result.unwrap_or_else(|e| panic!("{}: {e}", c["name"]));
            if let Some(expected) = c.get("expected_truncation") {
                let Some(crate::realtime::RealtimeEvent::ResponseEnded { truncation, .. }) =
                    &event.as_ref().unwrap().payload
                else {
                    panic!("missing response end");
                };
                if expected.is_null() {
                    assert!(truncation.is_none());
                } else {
                    let truncation = truncation.as_ref().unwrap();
                    assert_eq!(
                        truncation.items_left_out,
                        expected["items_left_out"].as_u64().unwrap()
                    );
                    assert_eq!(
                        truncation.first_kept_item_id.as_deref(),
                        expected["first_kept_item_id"].as_str()
                    );
                }
            }
            if let Some(expected) = c.get("expected_usage") {
                let usage = &event.as_ref().unwrap().usage;
                if expected.is_null() {
                    assert!(usage.is_none());
                } else {
                    let usage = usage.as_ref().unwrap();
                    assert_eq!(
                        usage.input_tokens,
                        expected["input_tokens"].as_u64().unwrap()
                    );
                    assert_eq!(
                        usage.output_tokens,
                        expected["output_tokens"].as_u64().unwrap()
                    );
                    assert_eq!(
                        usage.total_tokens,
                        expected["total_tokens"].as_u64().unwrap()
                    );
                    assert_eq!(
                        usage.context_window,
                        expected["context_window"].as_u64().unwrap()
                    );
                }
            }
            if let Some(expected) = c.get("expected_instruction_item_id") {
                assert_eq!(
                    event.as_ref().unwrap().instruction_item_id.as_deref(),
                    expected.as_str()
                );
            }
            if let Some(expected) = c.get("expected_idle_timeout_s") {
                assert_eq!(event.as_ref().unwrap().idle_timeout_s, expected.as_f64());
            }
            if c.get("expected_kept_text").is_some() {
                let Some(crate::realtime::RealtimeEvent::ItemTruncated {
                    item_id,
                    content_index,
                    audio_end_ms,
                    kept_text,
                }) = &event.as_ref().unwrap().payload
                else {
                    panic!("missing truncation: {}", c["name"]);
                };
                assert_eq!(item_id, c["event"]["item_id"].as_str().unwrap());
                assert_eq!(*content_index, 0);
                assert_eq!(*audio_end_ms, 1200);
                assert_eq!(kept_text.as_deref(), c["expected_kept_text"].as_str());
            }
            assert_eq!(
                event.map(|e| e.kind).as_deref().unwrap_or("ignored"),
                c["expected_kind"].as_str().unwrap(),
                "{}",
                c["name"]
            );
        }
    }
}

#[tokio::test]
async fn options_validate_before_connection() {
    use crate::{
        realtime::{PrismFiller, RealtimeConnectOptions, ToolDefinition, TurnDetection},
        Client, ErrorKind,
    };
    let client = Client::builder().api_key("test").build().unwrap();
    let cases = [
        RealtimeConnectOptions::default().endpoint("ws://example.com/v1/realtime"),
        RealtimeConnectOptions::default().endpoint("wss://example.com/wrong"),
        RealtimeConnectOptions::default().endpoint("wss://user:password@example.com/v1/realtime"),
        RealtimeConnectOptions::default().turn_detection(TurnDetection {
            eagerness: Some("invalid".into()),
            end_of_turn_timeout_ms: None,
        }),
        RealtimeConnectOptions::default().turn_detection(TurnDetection {
            eagerness: None,
            end_of_turn_timeout_ms: Some(999),
        }),
        RealtimeConnectOptions::default().tools(vec![ToolDefinition {
            name: "tool".into(),
            description: String::new(),
            parameters: Default::default(),
            prism_filler: Some(PrismFiller::Text(" ".into())),
        }]),
        RealtimeConnectOptions::default().request_id("  "),
        RealtimeConnectOptions::default().request_id("line\nbreak"),
        RealtimeConnectOptions::default().request_id("é"),
    ];
    for options in cases {
        let result = client.realtime().connect(options).await;
        assert_eq!(result.err().unwrap().kind(), ErrorKind::Input)
    }
    client.close().await;
}

#[tokio::test]
async fn duplicate_tool_names_do_not_connect() {
    use crate::{Client, ErrorKind, RealtimeConnectOptions, ToolDefinition};
    use tokio::net::TcpListener;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let client = Client::builder().api_key("test").build().unwrap();
    let tool = |parameters| ToolDefinition {
        name: "lookup".into(),
        description: String::new(),
        parameters,
        prism_filler: None,
    };
    let options = RealtimeConnectOptions::default()
        .endpoint(format!(
            "ws://{}/v1/realtime",
            listener.local_addr().unwrap()
        ))
        .tools(vec![
            tool(serde_json::from_value(serde_json::json!({"type":"object"})).unwrap()),
            tool(serde_json::from_value(serde_json::json!({"type":"string"})).unwrap()),
        ]);
    assert_eq!(
        client
            .realtime()
            .connect(options)
            .await
            .err()
            .unwrap()
            .kind(),
        ErrorKind::Input
    );
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(20), listener.accept())
            .await
            .is_err()
    );
    client.close().await;
}

#[test]
fn response_started_labels_and_inputs() {
    use crate::realtime::{Labels, RealtimeEvent};
    use serde_json::json;
    let started = |mut response: Value| {
        response["id"] = json!("reply-1");
        let raw = json!({"type": "response.created", "response": response});
        match decode(&raw.to_string(), "session-1")
            .unwrap()
            .unwrap()
            .payload
        {
            Some(RealtimeEvent::ResponseStarted {
                metadata,
                input_item_ids,
                ..
            }) => (metadata, input_item_ids),
            _ => panic!("not a started reply"),
        }
    };
    let (labels, inputs) = started(
        json!({"metadata": {"prism_cause": "user_text", "prism_input_item_id": "item-3", "step": "3", "nested": {"a": 1}}}),
    );
    assert_eq!(labels, Labels::from([("step".into(), "3".into())]));
    assert_eq!(inputs, ["item-3"]);
    let inputs = started(json!({"prism_input_item_ids": ["item-1", "item-2"]})).1;
    assert_eq!(inputs, ["item-1", "item-2"]);
    assert_eq!(started(json!({})), (Labels::new(), vec![]));
}
