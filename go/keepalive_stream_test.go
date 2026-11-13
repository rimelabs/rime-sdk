package rime

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	pb "github.com/rimelabs/rime-api/go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

func TestHostedTTSKeepalivePausedSourceLifetime(t *testing.T) {
	t.Parallel()
	for _, withDeadline := range []bool{false, true} {
		name := "no_deadline"
		configuration := Config{EnableTTSKeepalive: true}
		if withDeadline {
			name = "overall_deadline"
			configuration = Config{Model: "mistv3", Timeout: keepaliveTestInterval + 3*time.Second, EnableTTSKeepalive: true}
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			released := make(chan struct{})
			waitingForSource := make(chan struct{})
			service := &testService{synthesis: func(rpc grpc.BidiStreamingServer[pb.StreamingSynthesisRequest, pb.SynthesisResponseStream]) error {
				defer close(released)
				if _, err := rpc.Recv(); err != nil {
					return err
				}
				if err := rpc.SendHeader(metadata.Pairs("x-rime-audio-content-type", "audio/pcm")); err != nil {
					return err
				}
				if err := rpc.Send(audioResponse([]byte{1, 0})); err != nil {
					return err
				}
				<-rpc.Context().Done()
				return rpc.Context().Err()
			}}
			client, connections, _ := setupKeepaliveClient(t, configuration, service)
			client.limits.progress = 200 * time.Millisecond
			first := true
			stream, err := client.TTS.Stream(context.Background(), func(ctx context.Context) (string, error) {
				if first {
					first = false
					return "Hello.", nil
				}
				close(waitingForSource)
				<-ctx.Done()
				return "", ctx.Err()
			}, SynthesisOptions{})
			if err != nil {
				t.Fatal(err)
			}
			defer stream.Close()
			awaitSignal(t, waitingForSource)
			if _, err := stream.Recv(); err != nil {
				t.Fatal(err)
			}
			connection := <-connections
			pingReceived := false
			observation := time.NewTimer(keepaliveTestObservation)
			defer observation.Stop()
		observe:
			for {
				select {
				case <-connection.pings:
					pingReceived = true
				case <-stream.op.ctx.Done():
					if !withDeadline || !errors.Is(context.Cause(stream.op.ctx), context.DeadlineExceeded) {
						t.Fatalf("paused stream ended unexpectedly: %v", context.Cause(stream.op.ctx))
					}
					break observe
				case <-observation.C:
					if withDeadline {
						select {
						case <-stream.op.ctx.Done():
						case <-time.After(3 * time.Second):
							t.Fatal("overall deadline did not cancel paused synthesis")
						}
					}
					break observe
				}
			}
			if !pingReceived {
				t.Fatal("paused RPC sent no keepalive ping")
			}
			if !withDeadline {
				select {
				case <-released:
					t.Fatal("peer released an RPC that the caller left open")
				default:
				}
				if err := stream.Close(); err != nil {
					t.Fatal(err)
				}
			} else if _, err := stream.Recv(); !errors.Is(err, ErrTimeout) {
				t.Fatalf("overall deadline: %v", err)
			}
			awaitSignal(t, released)
			awaitSignal(t, stream.workersDone)
			if _, err := client.Languages.List(context.Background(), DiscoveryOptions{}); err != nil {
				t.Fatalf("client unusable after RPC cleanup: %v", err)
			}
		})
	}
}

func TestHostedTTSKeepaliveAbandonedStreamCancellation(t *testing.T) {
	t.Parallel()
	for _, model := range []string{"coda", "mistv3"} {
		for _, state := range []string{"waiting_for_text", "unread_audio"} {
			for _, action := range []string{"stream_close", "context_cancel", "client_close", "transport_close", "deadline"} {
				t.Run(model+"/"+state+"/"+action, func(t *testing.T) {
					t.Parallel()
					started, released := make(chan struct{}), make(chan struct{})
					service := &testService{synthesis: func(rpc grpc.BidiStreamingServer[pb.StreamingSynthesisRequest, pb.SynthesisResponseStream]) error {
						defer close(released)
						if err := rpc.SendHeader(metadata.Pairs("x-rime-audio-content-type", "audio/pcm")); err != nil {
							return err
						}
						close(started)
						if state == "unread_audio" {
							chunk := audioResponse(make([]byte, 65536))
							for {
								if err := rpc.Send(chunk); err != nil {
									return err
								}
							}
						}
						<-rpc.Context().Done()
						return rpc.Context().Err()
					}}
					client, connections, _ := setupKeepaliveClient(t, Config{Model: model, EnableTTSKeepalive: true}, service)
					ctx, cancel := context.WithCancel(context.Background())
					defer cancel()
					options := SynthesisOptions{}
					if action == "deadline" {
						limit := 2 * time.Second
						options.Timeout = &limit
					}
					source := func(ctx context.Context) (string, error) {
						<-ctx.Done()
						return "", ctx.Err()
					}
					if state == "unread_audio" {
						source = func(context.Context) (string, error) { return "Hello.", io.EOF }
					}
					stream, err := client.TTS.Stream(ctx, source, options)
					if err != nil {
						t.Fatal(err)
					}
					defer stream.Close()
					awaitSignal(t, started)
					connection := <-connections
					if state == "unread_audio" {
						deadline := time.After(time.Second)
						for {
							stream.queue.mu.Lock()
							blocked := stream.queue.bytes != 0 && stream.queue.writers != 0
							stream.queue.mu.Unlock()
							if blocked {
								break
							}
							select {
							case <-deadline:
								t.Fatal("audio did not reach consumer backpressure")
							case <-time.After(time.Millisecond):
							}
						}
					}
					want := ErrCancelled
					switch action {
					case "stream_close":
						stream.Close()
					case "context_cancel":
						cancel()
					case "client_close":
						client.Close()
					case "transport_close":
						connection.Close()
						want = ErrUnavailable
					case "deadline":
						want = ErrTimeout
					}
					awaitSignal(t, released)
					var terminal error
					if state == "unread_audio" && action == "transport_close" {
						// Reading buffered audio lets the SDK receive the terminal
						// transport error after the server has been cancelled.
						_, terminal = drain(stream)
					} else {
						awaitSignal(t, stream.workersDone)
						_, terminal = stream.Recv()
					}
					if !errors.Is(terminal, want) {
						t.Fatalf("terminal error: got %v, want %v", terminal, want)
					}
					awaitSignal(t, stream.workersDone)
				})
			}
		}
	}
}
