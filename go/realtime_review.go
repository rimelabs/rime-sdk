package rime

import (
	"context"
	"fmt"
	"time"
)

const rtReviewConcurrency = 32
const rtReviewRetention = 128

func rtReviewSettings(review Review) (rtObject, error) {
	if review.Callback == nil {
		return nil, failure(ErrInput, "review requires a callback")
	}
	timeout := review.TimeoutMS
	if timeout == 0 {
		timeout = 1000
	}
	if timeout < 100 || timeout > 5000 {
		return nil, failure(ErrInput, "review timeout must be from 100 through 5000 ms")
	}
	result := rtObject{"timeout_ms": timeout}
	switch policy := review.OnTimeout.(type) {
	case nil, ReviewTimeoutStop:
		result["on_timeout"] = "stop"
	case ReviewTimeoutSpeak:
		result["on_timeout"] = "speak"
	case ReviewTimeoutSay:
		if rtText(policy.Text, 500) != nil {
			return nil, failure(ErrInput, "review timeout text must be valid nonblank UTF-8")
		}
		result["on_timeout"], result["timeout_line"] = "say", policy.Text
	default:
		return nil, failure(ErrInput, "invalid review timeout policy")
	}
	return result, nil
}

func rtReviewVerdict(decision ReviewDecision) (rtObject, bool, error) {
	switch value := decision.(type) {
	case ReviewAllow:
		return rtObject{"action": "allow"}, false, nil
	case ReviewStop:
		return rtObject{"action": "stop"}, true, nil
	case ReviewReplace:
		if rtInputText(value.Text) != nil {
			return nil, false, failure(ErrInput, "review replacement must be valid nonblank UTF-8")
		}
		return rtObject{"action": "replace", "text": value.Text}, true, nil
	default:
		return nil, false, failure(ErrInput, "invalid review decision")
	}
}

type rtReviewState struct {
	request                    ReviewRequest
	ctx                        context.Context
	cancel                     context.CancelFunc
	deadline                   time.Time
	settled, returned, cutsOff bool
}

type rtReviewResult struct {
	state    *rtReviewState
	decision ReviewDecision
	err      error
}

func (w *rtWorker) beginReview(request ReviewRequest) error {
	if w.s.review == nil {
		return failure(ErrStream, "review request arrived without a configured callback")
	}
	if _, duplicate := w.reviews[request.ReviewID]; duplicate {
		return nil
	}
	for _, state := range w.reviews {
		if state.cutsOff && state.request.ResponseID == request.ResponseID && request.Index > state.request.Index {
			return nil
		}
	}
	if w.reviewRunning >= rtReviewConcurrency {
		return failure(ErrResourceLimit, "too many concurrent review callbacks")
	}
	if len(w.reviews) >= rtReviewRetention {
		removed := false
		for i, id := range w.reviewOrder {
			state := w.reviews[id]
			if state.settled && state.returned && time.Now().After(state.deadline) {
				delete(w.reviews, id)
				w.reviewOrder = append(w.reviewOrder[:i], w.reviewOrder[i+1:]...)
				removed = true
				break
			}
		}
		if !removed {
			return failure(ErrResourceLimit, "too many retained reviews")
		}
	}
	deadline := time.Now().Add(time.Duration(w.s.review.TimeoutMS) * time.Millisecond)
	ctx, cancel := context.WithDeadline(w.s.op.ctx, deadline)
	state := &rtReviewState{request: request, ctx: ctx, cancel: cancel, deadline: deadline}
	if w.reviews == nil {
		w.reviews = make(map[string]*rtReviewState)
	}
	w.reviews[request.ReviewID] = state
	w.reviewOrder = append(w.reviewOrder, request.ReviewID)
	w.reviewRunning++
	w.s.reviewTasks.Add(1)
	go func() {
		defer w.s.reviewTasks.Done()
		result := rtReviewResult{state: state}
		func() {
			defer func() {
				if recovered := recover(); recovered != nil {
					result.err = fmt.Errorf("review callback panicked: %v", recovered)
				}
			}()
			result.decision, result.err = w.s.review.Callback(ctx, request)
		}()
		select {
		case w.s.reviewResults <- result:
		case <-w.s.op.ctx.Done():
		}
	}()
	return nil
}

func (w *rtWorker) reviewResult(result rtReviewResult) error {
	w.reviewRunning--
	state := result.state
	state.returned = true
	if state.settled || state.ctx.Err() != nil {
		state.settled = true
		state.cancel()
		return nil
	}
	body, cutsOff, err := rtReviewVerdict(result.decision)
	if result.err != nil {
		err = result.err
	}
	var callbackFailure error
	if err != nil {
		body, cutsOff = rtObject{"action": "stop"}, true
		callbackFailure = &Error{Kind: ErrStream, Message: "review callback failed", Cause: err}
	}
	if cutsOff {
		state.cutsOff = true
		for _, other := range w.reviews {
			if other.request.ResponseID == state.request.ResponseID && other.request.Index > state.request.Index {
				other.settled = true
				other.cancel()
			}
		}
	}
	if len(w.pending) >= 128 {
		return failure(ErrResourceLimit, "too many pending review results")
	}
	body["review_id"] = state.request.ReviewID
	w.pending = append(w.pending, &rtCommand{ctx: state.ctx, kind: "review", body: body, deadline: state.deadline, review: state, reviewFailure: callbackFailure})
	return nil
}

func (w *rtWorker) expireReviews() {
	for _, state := range w.reviews {
		if !state.settled && state.ctx.Err() != nil {
			state.settled = true
			state.cancel()
		}
	}
}

func (w *rtWorker) cancelReviews(responseID string) {
	for _, state := range w.reviews {
		if responseID == "" || state.request.ResponseID == responseID {
			state.settled = true
			state.cancel()
		}
	}
}
