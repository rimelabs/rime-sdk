package rime

import (
	"context"
	"errors"
	"reflect"
	"sync/atomic"
	"testing"
	"time"
)

func rtReviewRequest(p *realtimePeer, id, response string, index uint64) {
	p.send(rtObject{"type": "prism.review.requested", "review_id": id, "response_id": response, "item_id": "output", "index": index, "text": "A spoken chunk."})
}

func TestRealtimeReviewBeforeResponseAndWithoutConsumer(t *testing.T) {
	p := newRealtimePeer(t)
	var calls atomic.Int32
	_, session := p.setupWith(t, RealtimeConnectOptions{Review: &Review{TimeoutMS: 1000, Callback: func(ctx context.Context, request ReviewRequest) (ReviewDecision, error) {
		calls.Add(1)
		if request.ResponseID != "provisional" || request.ItemID != "output" || request.Text != "A spoken chunk." {
			t.Errorf("request: %#v", request)
		}
		return ReviewAllow{}, nil
	}}})
	rtReviewRequest(p, "review-1", "provisional", 0)
	result := p.next("prism.review.result")
	if result["review_id"] != "review-1" || result["action"] != "allow" {
		t.Fatal(result)
	}
	rtReviewRequest(p, "review-1", "provisional", 0)
	// The update receipt is a barrier through the reader and writer after the duplicate.
	done := make(chan error, 1)
	go func() { _, err := session.Update(context.Background(), RealtimeUpdateOptions{}); done <- err }()
	request := p.next("session.update")
	rtAcknowledgeUpdate(p, request, 2)
	rtWait(t, done)
	if calls.Load() != 1 {
		t.Fatal("duplicate callback", calls.Load())
	}
	// No acknowledgment is sent for the result. A later operation remains usable.
	if err := session.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestRealtimeReviewPointerDecisionsReachServer(t *testing.T) {
	for _, tc := range []struct {
		name     string
		decision ReviewDecision
		action   string
	}{
		{"allow", &ReviewAllow{}, "allow"},
		{"stop", &ReviewStop{}, "stop"},
		{"replace", &ReviewReplace{Text: "Approved replacement."}, "replace"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := newRealtimePeer(t)
			_, session := p.setupWith(t, RealtimeConnectOptions{Review: &Review{Callback: func(context.Context, ReviewRequest) (ReviewDecision, error) {
				return tc.decision, nil
			}}})
			rtReviewRequest(p, "pointer", "provisional", 0)
			result := p.next("prism.review.result")
			if result["action"] != tc.action || tc.action == "replace" && result["text"] != "Approved replacement." {
				t.Fatal(result)
			}
			// A valid pointer result must not fail the session after writing Stop.
			p.turn(session, "after-pointer")
		})
	}
}

func TestRealtimeReviewPointerTimeoutPolicies(t *testing.T) {
	for _, tc := range []struct {
		policy ReviewTimeout
		action string
	}{
		{&ReviewTimeoutStop{}, "stop"},
		{&ReviewTimeoutSpeak{}, "speak"},
		{&ReviewTimeoutSay{Text: "Please wait."}, "say"},
	} {
		settings, err := rtReviewSettings(Review{OnTimeout: tc.policy, Callback: func(context.Context, ReviewRequest) (ReviewDecision, error) { return ReviewAllow{}, nil }})
		if err != nil || settings["on_timeout"] != tc.action || tc.action == "say" && settings["timeout_line"] != "Please wait." {
			t.Fatal(settings, err)
		}
	}
	for _, policy := range []ReviewTimeout{(*ReviewTimeoutStop)(nil), (*ReviewTimeoutSpeak)(nil), (*ReviewTimeoutSay)(nil)} {
		_, err := rtReviewSettings(Review{OnTimeout: policy, Callback: func(context.Context, ReviewRequest) (ReviewDecision, error) { return ReviewAllow{}, nil }})
		if !errors.Is(err, ErrInput) {
			t.Fatal("nil timeout pointer accepted", err)
		}
	}
	for _, decision := range []ReviewDecision{(*ReviewAllow)(nil), (*ReviewStop)(nil), (*ReviewReplace)(nil)} {
		if _, _, err := rtReviewVerdict(decision); !errors.Is(err, ErrInput) {
			t.Fatal("nil decision pointer accepted", err)
		}
	}
}

func TestRealtimeReviewStopPreservesEarlierAndCancelsLater(t *testing.T) {
	p := newRealtimePeer(t)
	started := make(chan uint64, 3)
	release := make(chan struct{})
	releaseEarlier := make(chan struct{})
	cancelled := make(chan struct{}, 1)
	_, session := p.setupWith(t, RealtimeConnectOptions{Review: &Review{TimeoutMS: 3000, Callback: func(ctx context.Context, request ReviewRequest) (ReviewDecision, error) {
		started <- request.Index
		switch request.Index {
		case 0:
			select {
			case <-releaseEarlier:
				return ReviewAllow{}, nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		case 1:
			select {
			case <-release:
				return ReviewReplace{Text: "Approved replacement."}, nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		default:
			<-ctx.Done()
			cancelled <- struct{}{}
			return ReviewAllow{}, nil
		}
	}}})
	for i := uint64(0); i < 3; i++ {
		rtReviewRequest(p, []string{"early", "replace", "later"}[i], "provisional", i)
	}
	for i := 0; i < 3; i++ {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("callbacks were not concurrent")
		}
	}
	close(release)
	results := map[string]rtObject{}
	result := p.next("prism.review.result")
	if result["review_id"] != "replace" {
		t.Fatal("earlier callback did not wait", result)
	}
	results["replace"] = result
	close(releaseEarlier)
	result = p.next("prism.review.result")
	results[result["review_id"].(string)] = result
	if results["early"]["action"] != "allow" || results["replace"]["action"] != "replace" || results["replace"]["text"] != "Approved replacement." {
		t.Fatal(results)
	}
	select {
	case <-cancelled:
	case <-time.After(time.Second):
		t.Fatal("later callback was not cancelled")
	}
	if err := session.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestRealtimeReviewExpiryDiscardsLateDecision(t *testing.T) {
	p := newRealtimePeer(t)
	expired := make(chan struct{})
	_, session := p.setupWith(t, RealtimeConnectOptions{Review: &Review{TimeoutMS: 100, OnTimeout: ReviewTimeoutSpeak{}, Callback: func(ctx context.Context, request ReviewRequest) (ReviewDecision, error) {
		<-ctx.Done()
		close(expired)
		return ReviewAllow{}, nil
	}}})
	rtReviewRequest(p, "expires", "provisional", 0)
	select {
	case <-expired:
	case <-time.After(time.Second):
		t.Fatal("review did not expire")
	}
	done := make(chan error, 1)
	go func() { _, err := session.Update(context.Background(), RealtimeUpdateOptions{}); done <- err }()
	request := p.next("session.update")
	rtAcknowledgeUpdate(p, request, 2)
	rtWait(t, done)
	if err := session.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case request := <-p.requests:
		t.Fatalf("late result sent: %v", request)
	default:
	}
}

func TestRealtimeReviewCloseCancelsAndJoins(t *testing.T) {
	p := newRealtimePeer(t)
	started, returned := make(chan struct{}), make(chan struct{})
	_, session := p.setupWith(t, RealtimeConnectOptions{Review: &Review{Callback: func(ctx context.Context, request ReviewRequest) (ReviewDecision, error) {
		close(started)
		<-ctx.Done()
		close(returned)
		return nil, ctx.Err()
	}}})
	rtReviewRequest(p, "close", "provisional", 0)
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("callback not started")
	}
	if err := session.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-returned:
	default:
		t.Fatal("close did not join callback")
	}
}

func TestRealtimeReviewCallbackFailureStopsThenFails(t *testing.T) {
	for _, name := range []string{"error", "invalid", "panic"} {
		t.Run(name, func(t *testing.T) {
			p := newRealtimePeer(t)
			cause := errors.New("policy failed")
			_, session := p.setupWith(t, RealtimeConnectOptions{Review: &Review{Callback: func(ctx context.Context, request ReviewRequest) (ReviewDecision, error) {
				switch name {
				case "error":
					return nil, cause
				case "panic":
					panic("policy panic")
				default:
					return ReviewReplace{Text: " "}, nil
				}
			}}})
			rtReviewRequest(p, "failure", "provisional", 0)
			result := p.next("prism.review.result")
			if result["action"] != "stop" {
				t.Fatal(result)
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			_, err := session.Recv(ctx)
			if !errors.Is(err, ErrStream) || name == "error" && !errors.Is(err, cause) {
				t.Fatal(err)
			}
		})
	}
}

func TestRealtimeReviewConcurrencyBound(t *testing.T) {
	p := newRealtimePeer(t)
	started := make(chan struct{}, rtReviewConcurrency)
	var active atomic.Int32
	_, session := p.setupWith(t, RealtimeConnectOptions{Review: &Review{TimeoutMS: 5000, Callback: func(ctx context.Context, request ReviewRequest) (ReviewDecision, error) {
		active.Add(1)
		defer active.Add(-1)
		started <- struct{}{}
		<-ctx.Done()
		return nil, ctx.Err()
	}}})
	for i := 0; i < rtReviewConcurrency; i++ {
		rtReviewRequest(p, string(rune('A'+i)), "provisional", uint64(i))
	}
	for i := 0; i < rtReviewConcurrency; i++ {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("callback not started")
		}
	}
	rtReviewRequest(p, "overflow", "provisional", rtReviewConcurrency)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := session.Recv(ctx)
	if !errors.Is(err, ErrResourceLimit) {
		t.Fatal(err)
	}
	if active.Load() != 0 {
		t.Fatal("callback work survived failure", active.Load())
	}
}

func TestRealtimeReviewTimeoutPoliciesAndReceivedSettings(t *testing.T) {
	for _, policy := range []ReviewTimeout{ReviewTimeoutStop{}, ReviewTimeoutSpeak{}, ReviewTimeoutSay{Text: "Please wait."}} {
		settings, err := rtReviewSettings(Review{OnTimeout: policy, Callback: func(context.Context, ReviewRequest) (ReviewDecision, error) { return ReviewAllow{}, nil }})
		if err != nil {
			t.Fatal(err)
		}
		decoder := &rtDecoder{}
		settings["timeout_ms"] = float64(1000)
		view := decoder.view(rtObject{"id": "session-1", "prism_review": settings})
		if decoder.err != nil || view.Review == nil || view.Review.TimeoutMS != 1000 {
			t.Fatal(view, decoder.err)
		}
		if !reflect.DeepEqual(view.Review.OnTimeout, policy) {
			t.Fatalf("decoded timeout policy = %#v, want %#v", view.Review.OnTimeout, policy)
		}
	}
}

func TestRealtimeReviewRejectsInvalidConfigurationAndEvents(t *testing.T) {
	callback := func(context.Context, ReviewRequest) (ReviewDecision, error) { return ReviewStop{}, nil }
	for _, review := range []Review{
		{},
		{Callback: callback, TimeoutMS: 99},
		{Callback: callback, TimeoutMS: 5001},
		{Callback: callback, OnTimeout: ReviewTimeoutSay{Text: " "}},
	} {
		if _, _, _, err := rtSettings(RealtimeConnectOptions{Review: &review}); !errors.Is(err, ErrInput) {
			t.Fatal("accepted invalid configuration", err)
		}
	}
	for _, raw := range []string{
		`{"type":"session.updated","session":{"id":"s","prism_review":null}}`,
		`{"type":"session.updated","session":{"id":"s","prism_review":{"timeout_ms":1000,"on_timeout":"say","timeout_line":" "}}}`,
		`{"type":"prism.review.requested","review_id":"r","response_id":"p","item_id":"i","index":-1,"text":"words"}`,
		`{"type":"prism.review.requested","review_id":"r","response_id":"p","item_id":"i","index":0,"text":null}`,
	} {
		if _, err := decodeRealtime([]byte(raw), "s"); !errors.Is(err, ErrStream) {
			t.Fatal("accepted malformed review", raw, err)
		}
	}
}

func TestRealtimeReviewCancellationDoesNotWaitForResponseDone(t *testing.T) {
	p := newRealtimePeer(t)
	started, cancelled := make(chan struct{}), make(chan struct{})
	_, session := p.setupWith(t, RealtimeConnectOptions{Review: &Review{Callback: func(ctx context.Context, request ReviewRequest) (ReviewDecision, error) {
		close(started)
		<-ctx.Done()
		close(cancelled)
		return nil, ctx.Err()
	}}})
	response := p.turn(session, "reply")
	rtReviewRequest(p, "cancel", "reply", 0)
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("callback not started")
	}
	done := make(chan error, 1)
	go func() { done <- session.Cancel(context.Background(), response) }()
	p.next("response.cancel")
	select {
	case <-cancelled:
	case <-time.After(time.Second):
		t.Fatal("review still running after cancellation write")
	}
	p.ended("reply")
	rtWait(t, done)
}
