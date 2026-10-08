// Package rime streams Coda and Mist v3 speech. Applications own playback.
package rime

import (
	"context"
	"errors"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	pb "github.com/rimelabs/rime-api/go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
)

// Config selects credentials and a deployment. Zero values select Coda and no overall timeout.
type Config struct {
	APIKey   string        // Defaults to RIME_API_KEY. The SDK does not read .env files.
	Model    string        // "coda" or "mistv3".
	Endpoint string        // TLS hostname with optional port. No scheme or path.
	Timeout  time.Duration // Zero disables the overall timeout; negative values are invalid.
}

// SynthesisOptions configures one stream. A nil Timeout inherits the client timeout.
type SynthesisOptions struct {
	Voice       string
	Language    string
	AudioFormat AudioFormat
	Timeout     *time.Duration
}

type DiscoveryOptions struct{ Timeout *time.Duration }
type VoiceListOptions struct {
	Language string
	Timeout  *time.Duration
}

// Client can be shared by concurrent goroutines. Close cancels all its operations.
type Client struct {
	TTS                *TTSService
	Voices             *VoiceService
	Languages          *LanguageService
	mu                 sync.Mutex
	closed             bool
	conn               *grpc.ClientConn
	key, target, voice string
	timeout            time.Duration
	operations         map[*operation]struct{}
	dialOptions        []grpc.DialOption // Private seam for tests with local services.
	limits             streamLimits
}

type TTSService struct{ client *Client }
type VoiceService struct{ client *Client }
type LanguageService struct{ client *Client }

type operation struct {
	ctx          context.Context
	cancel       context.CancelCauseFunc
	stopDeadline context.CancelFunc
	client       *Client
	once         sync.Once
}

func (o *operation) close() {
	o.once.Do(func() {
		o.stopDeadline()
		o.cancel(context.Canceled)
		o.client.mu.Lock()
		delete(o.client.operations, o)
		o.client.mu.Unlock()
	})
}

var hostnameLabel = regexp.MustCompile(`^[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?$`)

// NewClient validates configuration. Network work starts with the first operation.
func NewClient(config Config) (*Client, error) {
	key := config.APIKey
	if key == "" {
		key = os.Getenv("RIME_API_KEY")
	}
	if key == "" {
		return nil, failure(ErrAuthentication, "set APIKey or RIME_API_KEY")
	}
	for _, char := range key {
		if char < 0x21 || char > 0x7e {
			return nil, failure(ErrAuthentication, "API key must contain printable ASCII without whitespace")
		}
	}
	model := config.Model
	if model == "" {
		model = "coda"
	}
	target, voice := "coda.api.rime.ai:443", "clementine"
	if model == "mistv3" {
		target, voice = "mist.api.rime.ai:443", "astra"
	} else if model != "coda" {
		return nil, failure(ErrInput, "model must be coda or mistv3")
	}
	if config.Timeout < 0 {
		return nil, failure(ErrInput, "timeout must not be negative")
	}
	if config.Endpoint != "" {
		invalid := failure(ErrInput, "endpoint must be a hostname with an optional port, without a scheme or path")
		parts := strings.Split(config.Endpoint, ":")
		if len(parts) > 2 || len(parts[0]) > 253 {
			return nil, invalid
		}
		for _, label := range strings.Split(parts[0], ".") {
			if !hostnameLabel.MatchString(label) {
				return nil, invalid
			}
		}
		port := 443
		if len(parts) == 2 {
			if len(parts[1]) == 0 || len(parts[1]) > 5 {
				return nil, invalid
			}
			for _, char := range parts[1] {
				if char < '0' || char > '9' {
					return nil, invalid
				}
			}
			var err error
			port, err = strconv.Atoi(parts[1])
			if err != nil || port < 1 || port > 65535 {
				return nil, invalid
			}
		}
		target = fmt.Sprintf("%s:%d", strings.ToLower(parts[0]), port)
	}
	c := &Client{key: key, target: target, voice: voice, timeout: config.Timeout, operations: make(map[*operation]struct{}), limits: defaultStreamLimits}
	c.TTS = &TTSService{c}
	c.Voices = &VoiceService{c}
	c.Languages = &LanguageService{c}
	return c, nil
}

func (c *Client) operation(ctx context.Context, override *time.Duration, cap time.Duration) (*operation, error) {
	if ctx == nil {
		return nil, failure(ErrInput, "context must not be nil")
	}
	budget := c.timeout
	if override != nil {
		budget = *override
	}
	if budget < 0 {
		return nil, failure(ErrInput, "timeout must not be negative")
	}
	if cap > 0 && (budget == 0 || budget > cap) {
		budget = cap
	}
	deadlineCtx, stop := context.WithCancel(ctx)
	if budget > 0 {
		stop()
		deadlineCtx, stop = context.WithTimeout(ctx, budget)
	}
	operationCtx, cancel := context.WithCancelCause(deadlineCtx)
	o := &operation{ctx: operationCtx, cancel: cancel, stopDeadline: stop, client: c}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		stop()
		cancel(context.Canceled)
		return nil, failure(ErrInput, "client is closed")
	}
	c.operations[o] = struct{}{}
	return o, nil
}

func (c *Client) prepare(ctx context.Context) (pb.TextToSpeechClient, context.Context, error) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, ctx, failure(ErrCancelled, "client is closed")
	}
	if c.conn == nil {
		options := []grpc.DialOption{grpc.WithTransportCredentials(credentials.NewTLS(nil)), grpc.WithDisableRetry(), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(4194304), grpc.MaxCallSendMsgSize(131072), grpc.MaxRetryRPCBufferSize(0))}
		options = append(options, c.dialOptions...)
		var err error
		c.conn, err = grpc.NewClient(c.target, options...)
		if err != nil {
			c.mu.Unlock()
			return nil, ctx, operationError(err, "")
		}
	}
	conn, key := c.conn, c.key
	c.mu.Unlock()
	connectCtx, cancel := context.WithTimeout(ctx, c.limits.connection)
	defer cancel()
	conn.Connect()
	for {
		state := conn.GetState()
		if state == connectivity.Ready {
			break
		}
		if !conn.WaitForStateChange(connectCtx, state) {
			return nil, ctx, operationError(context.Cause(connectCtx), "")
		}
		if state == connectivity.Shutdown {
			return nil, ctx, failure(ErrCancelled, "connection is closed")
		}
	}
	return pb.NewTextToSpeechClient(conn), metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+key), nil
}

// Close is safe to call more than once. A blocked user TextSource must honor its context.
func (c *Client) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	for operation := range c.operations {
		operation.cancel(failure(ErrCancelled, "client closed"))
	}
	conn := c.conn
	c.key = ""
	c.mu.Unlock()
	if conn != nil {
		return conn.Close()
	}
	return nil
}

func requestID(headers, trailers metadata.MD) string {
	for _, values := range []metadata.MD{headers, trailers} {
		if ids := values.Get("x-request-id"); len(ids) > 0 {
			return ids[0]
		}
	}
	return ""
}

type discoveryCall func(context.Context, pb.TextToSpeechClient, ...grpc.CallOption) ([]string, error)

func (c *Client) discover(ctx context.Context, timeout *time.Duration, call discoveryCall) ([]string, error) {
	o, err := c.operation(ctx, timeout, 10*time.Second)
	if err != nil {
		return nil, err
	}
	defer o.close()
	stub, rpcCtx, err := c.prepare(o.ctx)
	if err != nil {
		return nil, err
	}
	lastID := ""
	for attempt := 0; ; attempt++ {
		var headers, trailers metadata.MD
		result, err := call(rpcCtx, stub, grpc.Header(&headers), grpc.Trailer(&trailers))
		id := requestID(headers, trailers)
		if id != "" {
			lastID = id
		}
		if o.ctx.Err() != nil {
			return nil, operationError(context.Cause(o.ctx), lastID)
		}
		if err == nil {
			return result, nil
		}
		mapped := operationError(err, id)
		if !errors.Is(mapped, ErrUnavailable) || attempt == 2 {
			return nil, mapped
		}
		timer := time.NewTimer(time.Duration(50*(1<<attempt)) * time.Millisecond)
		select {
		case <-o.ctx.Done():
			timer.Stop()
			return nil, operationError(context.Cause(o.ctx), lastID)
		case <-timer.C:
		}
	}
}

func (v *VoiceService) List(ctx context.Context, options VoiceListOptions) ([]string, error) {
	var language *string
	if options.Language != "" {
		if strings.TrimSpace(options.Language) == "" {
			return nil, failure(ErrInput, "language must not be blank")
		}
		language = &options.Language
	}
	request := &pb.GetSupportedSpeakersRequest{Language: language}
	return v.client.discover(ctx, options.Timeout, func(ctx context.Context, stub pb.TextToSpeechClient, callOptions ...grpc.CallOption) ([]string, error) {
		response, err := stub.GetSupportedSpeakers(ctx, request, callOptions...)
		return response.GetSpeakers(), err
	})
}
func (l *LanguageService) List(ctx context.Context, options DiscoveryOptions) ([]string, error) {
	return l.client.discover(ctx, options.Timeout, func(ctx context.Context, stub pb.TextToSpeechClient, callOptions ...grpc.CallOption) ([]string, error) {
		response, err := stub.GetSupportedLanguages(ctx, &pb.GetSupportedLanguagesRequest{}, callOptions...)
		return response.GetLanguages(), err
	})
}
