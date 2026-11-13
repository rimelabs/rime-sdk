package rime

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/rimelabs/rime-api/go"
	"golang.org/x/net/http2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/resolver/manual"
	"google.golang.org/grpc/test/bufconn"
)

type keepaliveTestConnection struct {
	net.Conn
	stalled      atomic.Bool
	pings        chan time.Time
	observer     *io.PipeWriter
	observerDone chan struct{}
	closeOnce    sync.Once
	writeMutex   sync.Mutex
}

func observeKeepaliveConnection(connection net.Conn) *keepaliveTestConnection {
	reader, writer := io.Pipe()
	observed := &keepaliveTestConnection{
		Conn: connection, pings: make(chan time.Time, 8), observer: writer,
		observerDone: make(chan struct{}),
	}
	go func() {
		defer close(observed.observerDone)
		defer reader.Close()
		preface := make([]byte, len(http2.ClientPreface))
		if _, err := io.ReadFull(reader, preface); err != nil {
			return
		}
		frames := http2.NewFramer(nil, reader)
		for {
			frame, err := frames.ReadFrame()
			if err != nil {
				return
			}
			if ping, ok := frame.(*http2.PingFrame); ok && !ping.IsAck() {
				select {
				case observed.pings <- time.Now():
				default:
				}
			}
		}
	}()
	return observed
}

func (connection *keepaliveTestConnection) Write(data []byte) (int, error) {
	connection.writeMutex.Lock()
	defer connection.writeMutex.Unlock()
	if _, err := connection.observer.Write(data); err != nil {
		return 0, err
	}
	if connection.stalled.Load() {
		return len(data), nil
	}
	return connection.Conn.Write(data)
}

func (connection *keepaliveTestConnection) Read(data []byte) (int, error) {
	for {
		count, err := connection.Conn.Read(data)
		if err != nil || !connection.stalled.Load() {
			return count, err
		}
	}
}

func (connection *keepaliveTestConnection) Close() error {
	var err error
	connection.closeOnce.Do(func() {
		connection.observer.Close()
		err = connection.Conn.Close()
	})
	return err
}

func setupKeepaliveClient(t *testing.T, config Config, service *testService) (*Client, <-chan *keepaliveTestConnection, *atomic.Int32) {
	t.Helper()
	config.APIKey = "test-key"
	client, err := NewClient(config)
	if err != nil {
		t.Fatal(err)
	}
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer(grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
		MinTime: time.Second, PermitWithoutStream: true,
	}))
	pb.RegisterTextToSpeechServer(server, service)
	go func() { _ = server.Serve(listener) }()
	connections := make(chan *keepaliveTestConnection, 8)
	dials := new(atomic.Int32)
	addresses := manual.NewBuilderWithScheme("dns")
	addresses.InitialState(resolver.State{Addresses: []resolver.Address{{Addr: "local"}}})
	client.dialOptions = []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithResolvers(addresses),
		// Fixed windows prevent bandwidth-estimation pings from obscuring keepalive.
		grpc.WithInitialWindowSize(1 << 20),
		grpc.WithInitialConnWindowSize(1 << 20),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			connection, err := listener.DialContext(ctx)
			if err != nil {
				return nil, err
			}
			observed := observeKeepaliveConnection(connection)
			dials.Add(1)
			connections <- observed
			return observed, nil
		}),
	}
	client.sttDialOptions = client.dialOptions
	t.Cleanup(func() { client.Close(); server.Stop(); listener.Close() })
	if dials.Load() != 0 || client.conn != nil || client.sttConn != nil {
		t.Fatal("construction started a connection")
	}
	return client, connections, dials
}

func TestHostedTTSKeepaliveScope(t *testing.T) {
	t.Parallel()
	type outcome struct {
		name string
		err  error
	}
	cases := []struct {
		name    string
		config  Config
		service rpcService
		want    bool
	}{
		{name: "default_coda"},
		{name: "default_mist", config: Config{Model: "mistv3"}},
		{name: "enabled_coda", config: Config{EnableTTSKeepalive: true}, want: true},
		{name: "enabled_mist", config: Config{Model: "mistv3", EnableTTSKeepalive: true}, want: true},
		{name: "explicit_coda", config: Config{Endpoint: "coda.api.rime.ai:443", EnableTTSKeepalive: true}, want: true},
		{name: "explicit_mist", config: Config{Endpoint: "mist.api.rime.ai", EnableTTSKeepalive: true}, want: true},
		{name: "normalized_target", config: Config{Endpoint: "CODA.API.RIME.AI:00443", EnableTTSKeepalive: true}, want: true},
		{name: "override_model_target", config: Config{Model: "mistv3", Endpoint: "coda.api.rime.ai", EnableTTSKeepalive: true}, want: true},
		{name: "custom_host", config: Config{Endpoint: "private.example:443", EnableTTSKeepalive: true}},
		{name: "hostname_lookalike", config: Config{Endpoint: "coda.api.rime.ai.example", EnableTTSKeepalive: true}},
		{name: "custom_port", config: Config{Endpoint: "mist.api.rime.ai:8443", EnableTTSKeepalive: true}},
		{name: "explicit_coda_without_opt_in", config: Config{Endpoint: "coda.api.rime.ai:443"}},
		{name: "explicit_mist_without_opt_in", config: Config{Endpoint: "mist.api.rime.ai:443"}},
		{name: "stt_on_hosted_target", config: Config{EnableTTSKeepalive: true}, service: transcriptionService},
	}
	results := make(chan outcome, len(cases))
	for _, test := range cases {
		client, connections, dials := setupKeepaliveClient(t, test.config, &testService{})
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		if test.service == transcriptionService {
			client.sttTarget = client.target
			if _, _, err := client.prepareService(ctx, transcriptionService); err != nil {
				cancel()
				t.Fatal(err)
			}
		} else if _, err := client.Languages.List(ctx, DiscoveryOptions{}); err != nil {
			cancel()
			t.Fatal(err)
		}
		cancel()
		connection := <-connections
		idleStarted := time.Now()
		go func() {
			result := outcome{name: test.name}
			defer func() { results <- result }()
			select {
			case ping := <-connection.pings:
				if !test.want {
					result.err = errors.New("ineligible connection sent a keepalive ping")
					return
				}
				if elapsed := ping.Sub(idleStarted); elapsed < 55*time.Second || elapsed > 75*time.Second {
					result.err = fmt.Errorf("keepalive interval: got %v, want approximately 60s", elapsed)
					return
				}
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				if _, err := client.Languages.List(ctx, DiscoveryOptions{}); err != nil {
					result.err = err
					return
				}
				if dials.Load() != 1 {
					result.err = errors.New("healthy keepalive replaced the connection")
				}
			case <-time.After(75 * time.Second):
				if test.want {
					result.err = errors.New("hosted TTS connection sent no idle keepalive ping")
				}
			}
		}()
	}
	for range cases {
		result := <-results
		t.Run(result.name, func(t *testing.T) {
			if result.err != nil {
				t.Fatal(result.err)
			}
		})
	}
}

func TestHostedTTSKeepaliveRecoversSilentStall(t *testing.T) {
	t.Parallel()
	service := &testService{}
	client, connections, dials := setupKeepaliveClient(t, Config{EnableTTSKeepalive: true}, service)
	if _, err := client.Languages.List(context.Background(), DiscoveryOptions{}); err != nil {
		t.Fatal(err)
	}
	connection := <-connections
	connection.stalled.Store(true)
	stalledAt := time.Now()
	for attempt := 0; attempt < 2; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
		_, err := client.Languages.List(ctx, DiscoveryOptions{})
		cancel()
		var detail *Error
		if !errors.Is(err, ErrTimeout) || !errors.As(err, &detail) || detail.RequestID != "" {
			t.Fatalf("stalled call: expected timeout without request ID, got %v", err)
		}
	}
	if client.conn.GetState() != connectivity.Ready || dials.Load() != 1 {
		t.Fatal("request deadlines replaced the stalled connection")
	}
	var pingedAt time.Time
	select {
	case pingedAt = <-connection.pings:
		if elapsed := pingedAt.Sub(stalledAt); elapsed < 55*time.Second || elapsed > 75*time.Second {
			t.Fatalf("keepalive interval: %v", elapsed)
		}
	case <-time.After(75 * time.Second):
		t.Fatal("stalled connection sent no keepalive ping")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if !client.conn.WaitForStateChange(ctx, connectivity.Ready) {
		t.Fatal("keepalive did not retire the stalled transport")
	}
	if elapsed := time.Since(pingedAt); elapsed < 18*time.Second {
		t.Fatalf("transport closed before the 20s response budget: %v", elapsed)
	}
	if service.discoveries.Load() != 1 {
		t.Fatal("timed-out discovery was replayed")
	}
	ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := client.Languages.List(ctx, DiscoveryOptions{}); err != nil {
		t.Fatalf("next call did not recover: %v", err)
	}
	if dials.Load() != 2 || service.discoveries.Load() != 2 {
		t.Fatalf("recovery counts: dials=%d discoveries=%d", dials.Load(), service.discoveries.Load())
	}
	select {
	case <-connection.observerDone:
	case <-time.After(5 * time.Second):
		t.Fatal("retired connection left its observer running")
	}
}

func TestHostedTTSKeepaliveCloseWhileIdle(t *testing.T) {
	client, connections, _ := setupKeepaliveClient(t, Config{EnableTTSKeepalive: true}, &testService{})
	if _, err := client.Languages.List(context.Background(), DiscoveryOptions{}); err != nil {
		t.Fatal(err)
	}
	connection := <-connections
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-connection.observerDone:
	case <-time.After(5 * time.Second):
		t.Fatal("client close left its connection observer running")
	}
}
