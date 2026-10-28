// SPDX-License-Identifier: Apache-2.0
// Private session state transitions; see NOTICE-PRISM.
use super::*;
use crate::realtime::transport::Written;

impl Worker {
    pub(super) async fn run(
        &mut self,
        address: &str,
        key: &str,
        request_id: Option<&str>,
        settings: Value,
        stop: CancellationToken,
        wake: Arc<Notify>,
    ) -> Result<(), Error> {
        let connection_deadline = Instant::now() + self.timeouts.connect;
        let mut transport = tokio::select! {
            _ = stop.cancelled() => return Ok(()),
            result = tokio::time::timeout_at(
                connection_deadline,
                Transport::connect(address, key, request_id, self.timeouts.request, stop.clone()),
            ) => result.map_err(|_| Error::new(ErrorKind::Timeout, "realtime handshake timed out"))??,
        };
        let result = self
            .connected(&mut transport, settings, &stop, wake, connection_deadline)
            .await;
        transport.close().await;
        result
    }
    async fn connected(
        &mut self,
        transport: &mut Transport,
        settings: Value,
        stop: &CancellationToken,
        wake: Arc<Notify>,
        connection_deadline: Instant,
    ) -> Result<(), Error> {
        let initialization = tokio::time::sleep_until(connection_deadline);
        tokio::pin!(initialization);
        loop {
            if stop.is_cancelled() {
                return Ok(());
            }
            self.advance(&transport.writes)?;
            let deadline = self
                .pending
                .iter()
                .filter(|c| !c.finished)
                .map(|c| c.deadline)
                .min()
                .unwrap_or_else(|| Instant::now() + std::time::Duration::from_secs(3600));
            tokio::select! {
                _ = stop.cancelled() => return Ok(()),
                _ = &mut initialization, if self.id.is_empty() => {
                    return Err(Error::new(ErrorKind::Timeout, "realtime initialization timed out"));
                }
                _ = wake.notified() => {},
                _ = tokio::time::sleep_until(deadline) => {},
                command = self.commands.recv() => {
                    if let Some(mut command) = command {
                        if self.pending.len() >= 128 {
                            command.finish(Err(Error::new(ErrorKind::ResourceLimit, "too many pending operations")));
                        } else {
                            self.pending.push(command);
                        }
                    }
                }
                result = transport.written.recv() => {
                    if stop.is_cancelled() { return Ok(()); }
                    self.written(result.ok_or_else(|| failure("realtime writer stopped"))?)?;
                }
                result = transport.reads.recv() => {
                    if stop.is_cancelled() { return Ok(()); }
                    let raw = result.ok_or_else(|| failure("realtime reader stopped"))??;
                    if let Some(event) = protocol::decode(&raw, &self.id)? {
                        if event.kind == "session.created" {
                            if self.id.is_empty() {
                                self.id = event.info.as_ref().expect("session view").session_id.clone();
                                let mut command = Command::new(Kind::Update, json!({"session": settings}));
                                command.deadline = Instant::now() + self.timeouts.request;
                                command.admission_deadline = command.deadline;
                                self.pending.push(command);
                            }
                        } else if event.kind == "session.idle_timeout" {
                            let idle = RealtimeIdleTimeout {
                                idle_timeout_s: event.idle_timeout_s.expect("validated idle timeout"),
                            };
                            return Err(Error::new(ErrorKind::Timeout, idle.to_string()).caused_by(idle));
                        } else {
                            self.dispatch(event)?;
                        }
                    }
                }
            }
        }
    }
    fn written(&mut self, result: Written) -> Result<(), Error> {
        result.result?;
        let Some(index) = self.pending.iter().position(|c| c.id == result.id) else {
            return Ok(());
        };
        let command = &mut self.pending[index];
        if result.skipped {
            if matches!(command.kind, Kind::Text | Kind::Reply) && command.epoch == self.epoch {
                self.ready = true;
            }
            if command.kind == Kind::Continue {
                if let Some(response) = command
                    .reference
                    .as_ref()
                    .and_then(|reference| self.responses.get_mut(&reference.response_id))
                {
                    response.continued = false;
                }
            }
            command.finish(Err(Error::new(
                ErrorKind::Cancelled,
                "operation cancelled before submission",
            )));
        } else {
            command.written = true;
            if command.kind == Kind::Audio {
                self.converter = command.converter.take();
            }
            if command.expected.is_empty() {
                command.finish(Ok(Answer::default()));
            } else if let Some(event) = command.ack.take() {
                self.complete(index, event)?;
            }
        }
        Ok(())
    }
    pub(super) fn publish(
        &self,
        payload: RealtimeEvent,
        event_id: String,
        request_id: Option<String>,
    ) -> Result<(), Error> {
        self.events
            .try_send(SessionEvent {
                session_id: self.id.clone(),
                event_id,
                request_id,
                payload,
            })
            .map_err(|error| {
                if let RealtimeEvent::ResponseStarted { response, .. } = error.into_inner().payload
                {
                    self.response_policies
                        .lock()
                        .expect("response policy mutex")
                        .remove(&response);
                }
                Error::new(
                    ErrorKind::ResourceLimit,
                    "realtime event consumer is too slow",
                )
            })
    }
    pub(super) fn response(&self, r: &ResponseRef) -> Result<&Response, Error> {
        if r.session_id != self.id {
            return Err(Error::new(
                ErrorKind::Input,
                "reference belongs to another session",
            ));
        }
        self.responses
            .get(&r.response_id)
            .ok_or_else(|| Error::new(ErrorKind::Input, "unknown or expired response reference"))
    }
    pub(super) fn abandon(&mut self, r: ResponseRef) -> Result<(), Error> {
        let Some(state) = self.responses.get_mut(&r.response_id) else {
            return Ok(());
        };
        if state.abandoned {
            return Ok(());
        }
        state.abandoned = true;
        let done = state.done;
        self.publish(
            RealtimeEvent::ResponseAbandoned {
                response: r.clone(),
            },
            String::new(),
            None,
        )?;
        if !done {
            if self.pending.len() >= 128 {
                return Err(Error::new(
                    ErrorKind::ResourceLimit,
                    "too many cancellation requests",
                ));
            }
            let mut c = Command::new(Kind::Cancel, json!({"response_id":r.response_id}));
            c.reference = Some(r);
            c.deadline = Instant::now() + self.timeouts.ready;
            c.admission_deadline = c.deadline;
            self.pending.push(c)
        }
        Ok(())
    }
    fn advance(&mut self, writes: &mpsc::Sender<Write>) -> Result<(), Error> {
        // Keep accepted results until the future has received them or its drop guard fires.
        self.pending.retain(|c| {
            !c.finished || (c.accepted.is_some() && c.status.load(Ordering::Acquire) != 1)
        });
        let mut i = 0;
        while i < self.pending.len() {
            if self.pending[i].abandoned() {
                if let Some(r) = self.pending[i].accepted.take() {
                    self.abandon(r)?
                }
                if !self.pending[i].submitted {
                    self.pending[i].finish(Err(Error::new(
                        ErrorKind::Cancelled,
                        "operation cancelled before submission",
                    )))
                }
            }
            if self.pending[i].finished {
                i += 1;
                continue;
            }
            if Instant::now() >= self.pending[i].deadline {
                if self.pending[i].submitted {
                    return Err(Error::new(
                        ErrorKind::Timeout,
                        "realtime request timed out; outcome unknown",
                    ));
                }
                self.pending[i].finish(Err(Error::new(
                    ErrorKind::Timeout,
                    "realtime admission timed out",
                )));
                i += 1;
                continue;
            }
            // One active write at a time, while reads and acknowledgments keep progressing.
            if self.pending[i].submitted
                || self
                    .pending
                    .iter()
                    .any(|c| c.submitted && !c.written && !c.finished)
            {
                i += 1;
                continue;
            }
            let mut c = self.pending.remove(i);
            let prepared = self.prepare(&mut c);
            match prepared {
                Err(e) => c.finish(Err(e)),
                Ok(true) => {}
                Ok(false) if !c.finished => {
                    if writes
                        .try_send(Write {
                            id: c.id,
                            frames: c.frames.clone(),
                            status: c.status.clone(),
                            cancelled: c.cancelled.clone(),
                            audio: c.kind == Kind::Audio,
                        })
                        .is_err()
                    {
                        self.pending.insert(i, c);
                        i += 1;
                        continue;
                    }
                    // Retain tracking from writer handoff. There is no await
                    // before this command is reinserted, so incoming events and
                    // cancellation cannot observe a partially registered request.
                    // Only a writer-confirmed skip can undo this reservation.
                    c.submitted = true;
                    c.epoch = self.epoch;
                    c.deadline = Instant::now() + self.timeouts.request;
                    if matches!(c.kind, Kind::Text | Kind::Reply) {
                        self.ready = false
                    }
                    if c.kind == Kind::Continue {
                        self.responses
                            .get_mut(&c.reference.as_ref().expect("parent").response_id)
                            .expect("parent state")
                            .continued = true
                    }
                }
                _ => {}
            }
            self.pending.insert(i, c);
            i += 1;
        }
        Ok(())
    }
    fn prepare(&mut self, c: &mut Command) -> Result<bool, Error> {
        if self.pending.iter().any(|p| {
            !p.finished
                && p.submitted
                && (p.kind == c.kind
                    || (matches!(p.kind, Kind::Audio | Kind::Clear)
                        && matches!(c.kind, Kind::Audio | Kind::Clear)))
                && matches!(
                    c.kind,
                    Kind::Tool | Kind::Clear | Kind::Truncate | Kind::Audio | Kind::Cancel
                )
        }) {
            return Ok(true);
        }
        let wire = match c.kind {
            Kind::Text | Kind::Reply => {
                if let Some(call) = &c.call {
                    let r = self.response(&call.response)?;
                    if !matches!(r.calls.get(&call.call_id), Some(Some(_))) {
                        return Err(Error::new(
                            ErrorKind::Input,
                            "record the tool result before requesting its report",
                        ));
                    }
                }
                if !self.ready {
                    return Ok(true);
                }
                c.expected = "response.started";
                "response.create"
            }
            Kind::Continue | Kind::Cancel | Kind::Tool | Kind::Truncate | Kind::Drain => {
                let r = self.response(c.reference.as_ref().expect("reference"))?;
                match c.kind {
                    Kind::Continue => {
                        if self.latest != r.reference.response_id || r.continued {
                            return Err(Error::new(
                                ErrorKind::Input,
                                "tool round is superseded or already continued",
                            ));
                        }
                        if !r.done {
                            return Ok(true);
                        }
                        if r.calls.is_empty() {
                            return Err(Error::new(ErrorKind::Input, "response has no tool calls"));
                        }
                        if r.calls.values().any(Option::is_none) {
                            return Ok(true);
                        }
                        c.expected = "response.started";
                        "response.create"
                    }
                    Kind::Cancel => {
                        if r.done {
                            c.finish(Ok(Answer::default()));
                            return Ok(false);
                        }
                        c.expected = "response.ended";
                        c.target = r.reference.response_id.clone();
                        "response.cancel"
                    }
                    Kind::Tool => {
                        let call = c.call.as_ref().expect("call");
                        let d = r
                            .calls
                            .get(&call.call_id)
                            .ok_or_else(|| Error::new(ErrorKind::Input, "unknown tool call"))?;
                        if let Some(d) = d {
                            if Some(*d) != c.digest {
                                return Err(Error::new(
                                    ErrorKind::Input,
                                    "a recorded tool result cannot change",
                                ));
                            }
                            c.finish(Ok(Answer::default()));
                            return Ok(false);
                        }
                        c.expected = "tool.result.created";
                        c.target = call.call_id.clone();
                        "conversation.item.create"
                    }
                    Kind::Truncate => {
                        let output = c.output.as_ref().expect("output");
                        if !r.outputs.contains(output) {
                            return Err(Error::new(ErrorKind::Input, "unknown output reference"));
                        }
                        c.expected = "item.truncated";
                        c.target = output.item_id.clone();
                        "conversation.item.truncate"
                    }
                    Kind::Drain => {
                        if !r.done {
                            return Ok(true);
                        }
                        let Some(token) = &r.token else {
                            c.finish(Ok(Answer::default()));
                            return Ok(false);
                        };
                        c.body["drain_token"] = json!(token);
                        "prism.playback.drained"
                    }
                    _ => unreachable!(),
                }
            }
            Kind::HistoryGet => {
                c.expected = "history.listed";
                "prism.conversation.get"
            }
            Kind::HistoryDelete => {
                c.expected = "item.deleted";
                "conversation.item.delete"
            }
            Kind::HistoryResult => {
                c.expected = "tool.result.created";
                "conversation.item.create"
            }
            Kind::History => {
                c.expected = "item.created";
                "conversation.item.create"
            }
            Kind::Update | Kind::Labels => {
                c.expected = "session.updated";
                "session.update"
            }
            Kind::Clear => {
                c.expected = "audio.cleared";
                "input_audio_buffer.clear"
            }
            Kind::Audio => "input_audio_buffer.append",
        };
        if c.id != 0 {
            return Ok(false);
        }
        self.sequence += 1;
        c.id = self.sequence;
        if c.kind == Kind::Audio {
            let a = c.audio.as_ref().expect("audio");
            let mut converter = match &self.converter {
                Some((f, v)) if *f == a.format => v.clone(),
                _ => InputAudio::new(a.format),
            };
            let mut data = vec![];
            for bytes in a.data.chunks(SOURCE_BYTES) {
                data.extend(converter.feed(bytes))
            }
            for chunk in data.chunks(1280) {
                c.frames.push(json!({"type":wire,"event_id":format!("evt_{}",c.id),"audio":STANDARD.encode(chunk)}).to_string())
            }
            c.converter = Some((a.format, converter));
        } else {
            c.body["type"] = json!(wire);
            c.body["event_id"] = json!(format!("evt_{}", c.id));
            c.frames.push(c.body.to_string())
        }
        Ok(false)
    }
}

#[cfg(test)]
#[path = "tests/submission.rs"]
mod submission_tests;
