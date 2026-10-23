package rime

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

func TestRealtimeProxyEnvironment(t *testing.T) {
	// Go caches proxy settings. Each case needs a fresh process.
	for _, bypass := range []bool{false, true} {
		t.Run(map[bool]string{false: "HTTPS_PROXY", true: "NO_PROXY"}[bypass], func(t *testing.T) {
			requests := make(chan string, 1)
			proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests <- r.Method + " " + r.Host
				http.Error(w, "audit proxy stopped the connection", http.StatusForbidden)
			}))
			defer proxy.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestRealtimeProxyChild$")
			for _, value := range os.Environ() {
				name := strings.ToUpper(strings.SplitN(value, "=", 2)[0])
				if name != "HTTPS_PROXY" && name != "HTTP_PROXY" && name != "NO_PROXY" && name != "ALL_PROXY" && name != "REQUEST_METHOD" {
					cmd.Env = append(cmd.Env, value)
				}
			}
			cmd.Env = append(cmd.Env, "RIME_PROXY_TEST_CHILD=1", "HTTPS_PROXY="+proxy.URL)
			if bypass {
				cmd.Env = append(cmd.Env, "NO_PROXY=sdk-audit.invalid")
			}
			if output, err := cmd.CombinedOutput(); err != nil {
				t.Fatalf("child failed: %v\n%s", err, output)
			}
			select {
			case request := <-requests:
				if bypass || request != "CONNECT sdk-audit.invalid:443" {
					t.Fatalf("unexpected proxy request: %s", request)
				}
				t.Log(request)
			default:
				if !bypass {
					t.Fatal("HTTPS_PROXY did not receive the connection")
				}
				t.Log("NO_PROXY bypassed the proxy")
			}
		})
	}
}

func TestRealtimeProxyChild(t *testing.T) {
	if os.Getenv("RIME_PROXY_TEST_CHILD") == "" {
		t.Skip("child process only")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	c, err := rtDial(ctx, "wss://sdk-audit.invalid/v1/realtime", "audit-test-key", "")
	if c != nil {
		_ = c.CloseNow()
	}
	if err == nil {
		t.Fatal("expected the audit endpoint to reject the connection")
	}
}
