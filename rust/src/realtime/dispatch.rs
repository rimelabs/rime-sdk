// SPDX-License-Identifier: Apache-2.0
// Private session state transitions; see NOTICE-PRISM.
use super::*;

impl Worker {
    pub(super) fn complete(&mut self, i: usize, e: Wire) -> Result<(), Error> {
        let c = &mut self.pending[i];
        if c.expected != e.kind {
            return Err(failure("unexpected request acknowledgment"));
        }
        let mut answer = Answer::default();
        if let Some(RealtimeEvent::ResponseStarted { response, .. }) = &e.payload {
            c.accepted = Some(response.clone());
            answer.response = Some(response.clone())
        }
        match c.kind {
            Kind::History => {
                answer.item = Some(ItemRef {
                    session_id: self.id.clone(),
                    item_id: e.target,
                })
            }
            Kind::Tool => {
                if e.target != c.target {
                    return Err(failure("tool acknowledgment names another call"));
                }
                self.responses
                    .get_mut(&c.reference.as_ref().expect("reference").response_id)
                    .expect("tool response")
                    .calls
                    .insert(c.target.clone(), c.digest);
            }
            Kind::Clear => self.converter = None,
            Kind::Labels => {
                if let Some(info) = &self.info {
                    info.lock().expect("info mutex").response_metadata =
                        e.info.and_then(|info| info.response_metadata)
                }
            }
            Kind::Update => {
                let mut info = e.info.ok_or_else(|| failure("missing session view"))?;
                info.session_id = self.id.clone();
                if !e.interrupt_known {
                    info.interrupt_on_speech = self.interrupt
                }
                let info = Arc::new(Mutex::new(info));
                self.info = Some(info.clone());
                if let Some(tx) = self.initialized.take() {
                    let _ = tx.send(Ok(info));
                }
            }
            _ => {}
        }
        let abandoned = if c.abandoned() {
            c.accepted.take()
        } else {
            None
        };
        c.finish(Ok(answer));
        if let Some(r) = abandoned {
            self.abandon(r)?
        }
        Ok(())
    }
    pub(super) fn dispatch(&mut self, e: Wire) -> Result<(), Error> {
        let mut matched = vec![];
        for (i, c) in self.pending.iter_mut().enumerate() {
            if c.finished || !c.submitted {
                continue;
            }
            let mut matching = e.request_id.as_deref() == Some(&format!("evt_{}", c.id));
            if c.kind == Kind::Clear && e.kind == "audio.cleared" {
                matching = true
            }
            if c.kind == Kind::Truncate
                && e.kind == "item.truncated"
                && e.target == c.target
                && e.index == 0
            {
                matching = true
            }
            if let Some(RealtimeEvent::ResponseEnded { response, .. }) = &e.payload {
                if c.kind == Kind::Cancel && response.response_id == c.target {
                    matching = true
                }
            }
            if matching && e.kind != "error" {
                if c.kind == Kind::Text && e.kind == "item.created" {
                    c.item_id = e.target.clone();
                    continue;
                }
                if c.expected != e.kind {
                    return Err(failure("unexpected request acknowledgment"));
                }
                if c.kind == Kind::Tool && c.target != e.target {
                    return Err(failure("tool acknowledgment names another call"));
                }
                matched.push(i)
            }
        }
        if let Some(p) = &e.payload {
            use RealtimeEvent::*;
            match p {
                InputReady => {
                    self.epoch += 1;
                    self.ready = true
                }
                SpeechStarted { .. } => {
                    self.epoch += 1;
                    self.ready = false;
                    self.latest.clear()
                }
                ResponseStarted { response, .. } => {
                    self.epoch += 1;
                    let id = &response.response_id;
                    if self.responses.contains_key(id) {
                        return Err(failure("duplicate response.created"));
                    }
                    if self.order.len() >= 128 {
                        let old = self.order.front().expect("response order");
                        if !self.responses[old].done
                            || self.pending.iter().any(|c| {
                                !c.finished
                                    && (c.reference.as_ref().is_some_and(|r| &r.response_id == old)
                                        || c.call
                                            .as_ref()
                                            .is_some_and(|r| &r.response.response_id == old))
                            })
                        {
                            return Err(Error::new(
                                ErrorKind::ResourceLimit,
                                "too many retained active responses",
                            ));
                        }
                        self.responses.remove(old);
                        self.order.pop_front();
                    }
                    self.order.push_back(id.clone());
                    self.responses.insert(
                        id.clone(),
                        Response {
                            reference: response.clone(),
                            done: false,
                            continued: false,
                            abandoned: false,
                            token: None,
                            calls: HashMap::new(),
                            outputs: HashSet::new(),
                        },
                    );
                    self.latest = id.clone();
                    self.ready = false;
                }
                ResponseEnded { response, .. } => {
                    let r = self
                        .responses
                        .get_mut(&response.response_id)
                        .ok_or_else(|| failure("unknown response"))?;
                    if r.done {
                        return Ok(());
                    }
                    r.done = true;
                    r.token = e.token.clone()
                }
                ToolCall { call, .. } => {
                    let r = self
                        .responses
                        .get(&call.response.response_id)
                        .ok_or_else(|| failure("unknown tool response"))?;
                    if r.calls.contains_key(&call.call_id) {
                        return Ok(());
                    }
                    if self
                        .responses
                        .values()
                        .any(|r| r.calls.contains_key(&call.call_id))
                    {
                        return Err(failure("tool call ID reused"));
                    }
                    let r = self
                        .responses
                        .get_mut(&call.response.response_id)
                        .expect("tool response");
                    if r.calls.len() >= 128 {
                        return Err(Error::new(ErrorKind::ResourceLimit, "too many tool calls"));
                    }
                    r.calls.insert(call.call_id.clone(), None);
                }
                MessageStarted { output } => {
                    let r = self
                        .responses
                        .get_mut(&output.response.response_id)
                        .ok_or_else(|| failure("unknown output response"))?;
                    if r.outputs.len() >= 128 {
                        return Err(Error::new(ErrorKind::ResourceLimit, "too many outputs"));
                    }
                    r.outputs.insert(output.clone());
                }
                TextDelta { output, .. }
                | TextDone { output, .. }
                | AudioDelta { output, .. }
                | AudioDone { output } => {
                    if !self.responses.contains_key(&output.response.response_id) {
                        return Err(failure("unknown response"));
                    }
                }
                Fault(f) => {
                    if f.scope == "session" {
                        return Err(protocol::fault_error(f.clone()));
                    }
                    let busy = f.scope == "event"
                        && [
                            "typed_turn_busy",
                            "instructions_busy",
                            "proactive_unavailable",
                        ]
                        .contains(&f.code.as_str());
                    if busy {
                        self.ready = false
                    }
                    for c in &mut self.pending {
                        if c.finished {
                            continue;
                        }
                        if (c.submitted
                            && f.request_id.as_deref() == Some(&format!("evt_{}", c.id)))
                            || (f.code == "typed_turn_superseded"
                                && f.item_id
                                    .as_ref()
                                    .is_some_and(|s| !s.is_empty() && s == &c.item_id))
                        {
                            if busy && matches!(c.kind, Kind::Text | Kind::Reply) && !c.abandoned()
                            {
                                c.id = 0;
                                c.frames.clear();
                                c.submitted = false;
                                c.written = false;
                                c.deadline = c.admission_deadline
                            } else {
                                if c.kind == Kind::Continue
                                    && f.code == "tool_continuation_not_ready"
                                {
                                    if let Some(r) = c
                                        .reference
                                        .as_ref()
                                        .and_then(|r| self.responses.get_mut(&r.response_id))
                                    {
                                        r.continued = false
                                    }
                                }
                                if c.kind == Kind::Update {
                                    return Err(protocol::fault_error(f.clone()));
                                }
                                c.finish(Err(protocol::fault_error(f.clone())))
                            }
                        }
                    }
                }
                _ => {}
            }
            self.publish(p.clone(), e.event_id.clone(), e.request_id.clone())?
        }
        // Only uncorrelated controls can match several waiters; they are serialized.
        if let Some(&i) = matched.first() {
            if self.pending[i].written {
                self.complete(i, e)?
            } else {
                if let Some(RealtimeEvent::ResponseStarted { response, .. }) = &e.payload {
                    self.pending[i].accepted = Some(response.clone())
                }
                self.pending[i].ack = Some(e)
            }
        }
        Ok(())
    }
}
