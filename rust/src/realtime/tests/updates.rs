use super::peer::{event, Peer};
use crate::realtime::{PrismFiller, RealtimeEvent, RealtimeUpdateOptions, ToolDefinition};
use serde_json::{json, Value};

async fn acknowledge(p: &Peer, q: &Value, revision: u64) {
    p.send(json!({"type":"session.updated","prism_request_event_id":q["event_id"],
        "session":{"id":"session-1","voice":"astra","prism_effective_interrupt_response":false,"prism_settings_revision":revision}})).await;
}

#[tokio::test]
async fn partial_update_clear_and_shared_settings() {
    let mut p = Peer::new().await;
    let (c, s) = p.setup().await;
    let original = s.info();
    let clone = s.clone();
    let task = tokio::spawn(async move {
        clone
            .update(
                RealtimeUpdateOptions::default()
                    .instructions("Handle billing.")
                    .tools(vec![ToolDefinition {
                        name: "refund".into(),
                        description: String::new(),
                        parameters: serde_json::from_value(json!({"type":"object"})).unwrap(),
                        prism_filler: Some(PrismFiller::None),
                    }]),
            )
            .await
    });
    let q = p.next("session.update").await;
    assert_eq!(
        q["session"],
        json!({"instructions":"Handle billing.","tools":[{"type":"function","function":{"name":"refund","description":"","parameters":{"type":"object"}},"prism_filler":{"type":"none"}}]})
    );
    assert_eq!(s.info().voice, original.voice);
    acknowledge(&p, &q, 2).await;
    let info = task.await.unwrap().unwrap();
    assert_eq!(info.settings_revision, Some(2));
    assert_eq!(s.info().voice, "astra");
    assert!(!s.info().interrupt_on_speech);
    let clone = s.clone();
    let task = tokio::spawn(async move {
        clone
            .update(
                RealtimeUpdateOptions::default()
                    .instructions("")
                    .tools(vec![])
                    .voice("")
                    .interrupt_on_speech(false),
            )
            .await
    });
    let q = p.next("session.update").await;
    assert_eq!(
        q["session"],
        json!({"instructions":"","tools":[],"voice":"","turn_detection":{"interrupt_response":false}})
    );
    acknowledge(&p, &q, 3).await;
    assert_eq!(task.await.unwrap().unwrap().settings_revision, Some(3));
    assert_eq!(s.clone().info().settings_revision, Some(3));
    let tool = ToolDefinition {
        name: "same".into(),
        description: String::new(),
        parameters: Default::default(),
        prism_filler: None,
    };
    assert!(s
        .update(RealtimeUpdateOptions::default().tools(vec![tool.clone(), tool]))
        .await
        .is_err());
    p.no_request().await;
    let mut events = s.events().unwrap();
    p.send(json!({"type":"response.created", "response":{"id":"early-guess","prism_settings_revision":2}})).await;
    assert!(matches!(
        event(&mut events).await.payload,
        RealtimeEvent::ResponseStarted {
            settings_revision: Some(2),
            ..
        }
    ));
    assert_eq!(s.info().settings_revision, Some(3));
    c.close().await;
}

#[tokio::test]
async fn rejected_concurrent_and_dropped_updates_keep_matching() {
    let mut p = Peer::new().await;
    let (c, s) = p.setup().await;
    let clone = s.clone();
    let task = tokio::spawn(async move {
        clone
            .update(RealtimeUpdateOptions::default().voice("missing"))
            .await
    });
    let q = p.next("session.update").await;
    p.send(json!({"type":"error","error":{"code":"invalid_voice","message":"unknown voice","scope":"event","owner":{"event_id":q["event_id"]}}})).await;
    assert!(task.await.unwrap().is_err());
    assert_eq!(s.info().voice, "test");
    let clone = s.clone();
    let first = tokio::spawn(async move {
        clone
            .update(RealtimeUpdateOptions::default().instructions("First"))
            .await
    });
    let q1 = p.next("session.update").await;
    let clone = s.clone();
    let second = tokio::spawn(async move {
        clone
            .update(RealtimeUpdateOptions::default().tools(vec![]))
            .await
    });
    let q2 = p.next("session.update").await;
    acknowledge(&p, &json!({"event_id":"unrelated"}), 99).await;
    acknowledge(&p, &q1, 2).await;
    assert_eq!(first.await.unwrap().unwrap().settings_revision, Some(2));
    acknowledge(&p, &q2, 3).await;
    assert_eq!(second.await.unwrap().unwrap().settings_revision, Some(3));
    let clone = s.clone();
    let dropped = tokio::spawn(async move {
        clone
            .update(RealtimeUpdateOptions::default().instructions("Billing"))
            .await
    });
    let q = p.next("session.update").await;
    dropped.abort();
    assert!(dropped.await.unwrap_err().is_cancelled());
    acknowledge(&p, &q, 4).await;
    let clone = s.clone();
    let barrier = tokio::spawn(async move { clone.add_message("user", "history").await });
    let q = p.next("conversation.item.create").await;
    p.send(json!({"type":"conversation.item.created","prism_request_event_id":q["event_id"],"item":{"type":"message","id":"history"}})).await;
    barrier.await.unwrap().unwrap();
    assert_eq!(s.info().settings_revision, Some(4));
    c.close().await;
}

#[tokio::test]
async fn update_preserves_tool_round_and_response_revisions() {
    let mut p = Peer::new().await;
    let (c, s) = p.setup().await;
    let mut events = s.events().unwrap();
    let parent = p.turn(&s, "reply-1").await;
    event(&mut events).await;
    event(&mut events).await;
    p.send(json!({"type":"response.function_call_arguments.done","response_id":"reply-1","item_id":"tool-1","call_id":"call-1","name":"lookup","arguments":"{}"})).await;
    let RealtimeEvent::ToolCall { call, .. } = event(&mut events).await.payload else {
        panic!("missing tool")
    };
    let clone = s.clone();
    let update = tokio::spawn(async move {
        clone
            .update(
                RealtimeUpdateOptions::default()
                    .instructions("Billing")
                    .tools(vec![]),
            )
            .await
    });
    let q = p.next("session.update").await;
    acknowledge(&p, &q, 2).await;
    update.await.unwrap().unwrap();
    p.send(json!({"type":"response.done","response":{"id":"reply-1","status":"completed","prism_settings_revision":1}})).await;
    assert!(matches!(
        event(&mut events).await.payload,
        RealtimeEvent::ResponseEnded {
            settings_revision: Some(1),
            ..
        }
    ));
    assert_eq!(s.info().settings_revision, Some(2));
    let clone = s.clone();
    let result = tokio::spawn(async move { clone.submit_tool_result(&call, "transferred").await });
    let q = p.next("conversation.item.create").await;
    p.send(json!({"type":"conversation.item.created","prism_request_event_id":q["event_id"],"item":{"type":"function_call_output","call_id":"call-1"}})).await;
    result.await.unwrap().unwrap();
    let clone = s.clone();
    let next = tokio::spawn(async move { clone.continue_reply(&parent).await });
    let q = p.next("response.create").await;
    p.send(json!({"type":"response.created","prism_request_event_id":q["event_id"],"response":{"id":"reply-2","prism_settings_revision":2,"metadata":q["response"]["metadata"]}})).await;
    next.await.unwrap().unwrap();
    assert!(matches!(
        event(&mut events).await.payload,
        RealtimeEvent::ResponseStarted {
            settings_revision: Some(2),
            ..
        }
    ));
    c.close().await;
}

#[tokio::test]
async fn unchanged_policies_remain_available_and_evicted_changes_fail_explicitly() {
    let mut p = Peer::with_initial_interruption(true).await;
    let (client, session) = p.setup().await;
    for revision in 2..=141 {
        let clone = session.clone();
        let update = tokio::spawn(async move {
            clone
                .update(RealtimeUpdateOptions::default().instructions("Keep the same policy"))
                .await
        });
        let q = p.next("session.update").await;
        p.send(json!({"type":"session.updated","prism_request_event_id":q["event_id"],"session":{"id":"session-1","voice":"test","prism_effective_interrupt_response":true,"prism_settings_revision":revision}})).await;
        update.await.unwrap().unwrap();
    }
    let mut events = session.events().unwrap();
    p.send(json!({"type":"response.created","response":{"id":"same-policy","prism_settings_revision":1}})).await;
    assert!(matches!(
        event(&mut events).await.payload,
        RealtimeEvent::ResponseStarted {
            settings_revision: Some(1),
            ..
        }
    ));
    p.ended("same-policy").await;
    event(&mut events).await;
    for revision in 142..=270 {
        let interrupt = revision % 2 != 0;
        let clone = session.clone();
        let update = tokio::spawn(async move {
            clone
                .update(RealtimeUpdateOptions::default().interrupt_on_speech(interrupt))
                .await
        });
        let q = p.next("session.update").await;
        p.send(json!({"type":"session.updated","prism_request_event_id":q["event_id"],"session":{"id":"session-1","voice":"test","prism_effective_interrupt_response":interrupt,"prism_settings_revision":revision}})).await;
        update.await.unwrap().unwrap();
    }
    p.send(json!({"type":"response.created","response":{"id":"expired-policy","prism_settings_revision":141}})).await;
    let failure = tokio::time::timeout(
        std::time::Duration::from_secs(3),
        futures_util::StreamExt::next(&mut events),
    )
    .await
    .unwrap()
    .unwrap()
    .unwrap_err();
    assert_eq!(failure.kind(), crate::ErrorKind::ResourceLimit);
    client.close().await;
}

#[tokio::test]
async fn delivered_response_policy_outlives_response_retention_until_next_poll() {
    use futures_util::StreamExt;
    let mut p = Peer::with_initial_interruption(true).await;
    let (client, session) = p.setup().await;
    let mut events = session.events().unwrap();
    p.send(json!({"type":"response.created","response":{"id":"delivered-first","prism_settings_revision":1}})).await;
    let RealtimeEvent::ResponseStarted {
        response: first, ..
    } = event(&mut events).await.payload
    else {
        panic!("missing first reply")
    };
    p.ended("delivered-first").await;
    for index in 1..=128 {
        let id = format!("queued-{index}");
        p.send(json!({"type":"response.created","response":{"id":id,"prism_settings_revision":1}}))
            .await;
        if index < 128 {
            p.ended(&id).await;
        }
    }
    // The internal update acknowledgment is a barrier after the full event queue.
    let clone = session.clone();
    let barrier = tokio::spawn(async move { clone.update(RealtimeUpdateOptions::default()).await });
    let q = p.next("session.update").await;
    p.send(json!({"type":"session.updated","prism_request_event_id":q["event_id"],"session":{"id":"session-1","voice":"test","prism_effective_interrupt_response":true,"prism_settings_revision":2}})).await;
    barrier.await.unwrap().unwrap();
    assert_eq!(
        session.cancel(&first).await.unwrap_err().kind(),
        crate::ErrorKind::Input,
        "the worker should have evicted the completed first response"
    );
    assert!(session.response_interrupt_on_speech(&first).unwrap());
    let mut refs = vec![first.clone()];
    for _ in 0..256 {
        if let RealtimeEvent::ResponseStarted { response, .. } =
            events.next().await.unwrap().unwrap().payload
        {
            refs.push(response);
        }
    }
    assert!(session.response_interrupt_on_speech(&first).is_err());
    let last = refs.last().unwrap();
    assert!(session.response_interrupt_on_speech(last).unwrap());
    p.ended(last.response_id()).await;
    assert!(matches!(
        events.next().await.unwrap().unwrap().payload,
        RealtimeEvent::ResponseEnded { .. }
    ));
    for reference in refs {
        assert!(
            session.response_interrupt_on_speech(&reference).is_err(),
            "a drained response must release its captured policy"
        );
    }
    client.close().await;
}
