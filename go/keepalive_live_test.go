package rime

import (
	"context"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/stats"
)

type keepaliveLiveStatistics struct {
	mutex       sync.Mutex
	connections int
	requestIDs  []string
}

func (statistics *keepaliveLiveStatistics) TagConn(ctx context.Context, _ *stats.ConnTagInfo) context.Context {
	return ctx
}

func (statistics *keepaliveLiveStatistics) HandleConn(_ context.Context, event stats.ConnStats) {
	if _, ok := event.(*stats.ConnBegin); ok {
		statistics.mutex.Lock()
		statistics.connections++
		statistics.mutex.Unlock()
	}
}

func (statistics *keepaliveLiveStatistics) TagRPC(ctx context.Context, _ *stats.RPCTagInfo) context.Context {
	return ctx
}

func (statistics *keepaliveLiveStatistics) HandleRPC(_ context.Context, event stats.RPCStats) {
	if headers, ok := event.(*stats.InHeader); ok {
		if values := headers.Header.Get("x-request-id"); len(values) != 0 {
			statistics.mutex.Lock()
			statistics.requestIDs = append(statistics.requestIDs, values[0])
			statistics.mutex.Unlock()
		}
	}
}

// TestHostedTTSKeepaliveLive makes read-only discovery calls over verified TLS.
// RIME_TEST_LIVE_KEEPALIVE=1 opts in. RIME_TEST_KEEPALIVE_CODA_ADDRESS and
// RIME_TEST_KEEPALIVE_MIST_ADDRESS can select a regional destination as host:port
// while preserving the public hostname for TLS verification and gRPC authority.
func TestHostedTTSKeepaliveLive(t *testing.T) {
	if os.Getenv("RIME_TEST_LIVE_KEEPALIVE") != "1" {
		t.Skip("set RIME_TEST_LIVE_KEEPALIVE=1 for live idle qualification")
	}
	if os.Getenv("RIME_API_KEY") == "" {
		t.Fatal("RIME_API_KEY must be set for live discovery")
	}
	for _, model := range []struct {
		name     string
		override string
	}{
		{name: "coda", override: "RIME_TEST_KEEPALIVE_CODA_ADDRESS"},
		{name: "mistv3", override: "RIME_TEST_KEEPALIVE_MIST_ADDRESS"},
	} {
		configuration := Config{Model: model.name, EnableTTSKeepalive: true}
		client, err := NewClient(configuration)
		if err != nil {
			t.Fatal(err)
		}
		hostname, port, err := net.SplitHostPort(client.target)
		client.Close()
		if err != nil {
			t.Fatal(err)
		}
		var destinations []string
		if override := os.Getenv(model.override); override != "" {
			if _, _, err := net.SplitHostPort(override); err != nil {
				t.Fatalf("%s must be host:port", model.override)
			}
			destinations = []string{override}
		} else {
			addresses, err := net.DefaultResolver.LookupIPAddr(context.Background(), hostname)
			if err != nil {
				t.Fatal(err)
			}
			seen := make(map[string]bool)
			for _, address := range addresses {
				if address.IP.To4() != nil && !seen[address.IP.String()] {
					seen[address.IP.String()] = true
					destinations = append(destinations, net.JoinHostPort(address.IP.String(), port))
				}
			}
			if len(destinations) == 0 {
				t.Fatal("hosted target has no IPv4 address")
			}
		}
		for _, destination := range destinations {
			t.Run(model.name+"/"+destination, func(t *testing.T) {
				t.Parallel()
				client, err := NewClient(configuration)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { client.Close() })
				statistics := new(keepaliveLiveStatistics)
				client.dialOptions = []grpc.DialOption{
					grpc.WithStatsHandler(statistics),
					grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
						return (&net.Dialer{}).DialContext(ctx, "tcp", destination)
					}),
				}
				discover := func() {
					ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
					defer cancel()
					if _, err := client.Languages.List(ctx, DiscoveryOptions{}); err != nil {
						t.Fatal(err)
					}
				}
				discover()
				t.Logf("idle qualification started at %s", time.Now().UTC().Format(time.RFC3339))
				<-time.After(425 * time.Second)
				discover()
				statistics.mutex.Lock()
				defer statistics.mutex.Unlock()
				if statistics.connections != 1 {
					t.Fatalf("idle connection was replaced: transports=%d", statistics.connections)
				}
				if len(statistics.requestIDs) != 2 {
					t.Fatalf("expected two request IDs, got %d", len(statistics.requestIDs))
				}
				t.Logf("same TLS transport after 425s idle; UTC=%s request_ids=%v", time.Now().UTC().Format(time.RFC3339), statistics.requestIDs)
			})
		}
	}
}
