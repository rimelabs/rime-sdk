use crate::realtime::protocol::decode;
use serde_json::Value;
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
