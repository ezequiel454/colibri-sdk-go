// Modified in the ezequiel454 fork for forge M3; see FORK.md

package restserver

import (
	"bytes"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/colibriproject-dev/colibri-sdk-go/pkg/base/config"
	"github.com/colibriproject-dev/colibri-sdk-go/pkg/base/logging"
	"github.com/colibriproject-dev/colibri-sdk-go/pkg/base/monitoring"
	"github.com/colibriproject-dev/colibri-sdk-go/pkg/base/observer"
	"github.com/colibriproject-dev/colibri-sdk-go/pkg/web/restclient"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	collectortrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	"google.golang.org/protobuf/proto"
)

// The tracing-enabled path against a real OTLP/HTTP collector owned by the test: native SDK
// exporter, real HTTP server and client, nothing mocked in the SDK.

const (
	trSentinelHeader = "SENTINEL_OTLP_HEADER_SECRET"
	trSentinelQuery  = "SENTINEL_QUERY"
	trSentinelToken  = "SENTINEL_BEARER_TOKEN"
	trSentinelBody   = "SENTINEL_REQUEST_BODY"
	trSentinelErr    = "SENTINEL_RAW_ERROR_TEXT"
	trSentinelPeer   = "SENTINEL_PEER_ERROR_BODY"
	trSentinelCorr   = "SENTINEL_CORRELATION"
)

type lockedBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *lockedBuffer) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *lockedBuffer) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

// otlpCollector decodes the OTLP/HTTP trace exports it receives and keeps the raw bytes, so
// the privacy assertions look at exactly what left the process.
type otlpCollector struct {
	*httptest.Server
	mu      sync.Mutex
	raw     [][]byte
	headers []http.Header
	paths   []string
	spans   []collectedSpan
}

type collectedSpan struct {
	Name         string
	TraceID      string
	SpanID       string
	ParentSpanID string
	Kind         int32
	StatusCode   int32
	StatusMsg    string
	Attrs        map[string]string
	Events       []string
}

func newOTLPCollector(t *testing.T) *otlpCollector {
	t.Helper()
	c := &otlpCollector{}
	c.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		c.mu.Lock()
		c.raw = append(c.raw, body)
		c.headers = append(c.headers, r.Header.Clone())
		c.paths = append(c.paths, r.URL.Path)
		c.mu.Unlock()
		if r.URL.Path == "/v1/traces" {
			var req collectortrace.ExportTraceServiceRequest
			if proto.Unmarshal(body, &req) == nil {
				c.mu.Lock()
				for _, rs := range req.ResourceSpans {
					for _, ss := range rs.ScopeSpans {
						for _, s := range ss.Spans {
							cs := collectedSpan{
								Name: s.Name, TraceID: hex.EncodeToString(s.TraceId), SpanID: hex.EncodeToString(s.SpanId),
								ParentSpanID: hex.EncodeToString(s.ParentSpanId), Kind: int32(s.Kind), Attrs: map[string]string{},
							}
							if s.Status != nil {
								cs.StatusCode, cs.StatusMsg = int32(s.Status.Code), s.Status.Message
							}
							for _, a := range s.Attributes {
								cs.Attrs[a.Key] = attrString(a.Value)
							}
							for _, e := range s.Events {
								cs.Events = append(cs.Events, e.Name)
								for _, a := range e.Attributes {
									cs.Attrs["event."+e.Name+"."+a.Key] = attrString(a.Value)
								}
							}
							c.spans = append(c.spans, cs)
						}
					}
				}
				c.mu.Unlock()
			}
		}
		w.Header().Set("Content-Type", "application/x-protobuf")
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(c.Close)
	return c
}

func attrString(v *commonpb.AnyValue) string {
	if s, ok := v.Value.(*commonpb.AnyValue_StringValue); ok {
		return s.StringValue
	}
	return v.String()
}

func (c *otlpCollector) snapshot() (spans []collectedSpan, raw []byte, hdrs []http.Header, paths []string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	spans = append(spans, c.spans...)
	raw = bytes.Join(c.raw, nil)
	hdrs = append(hdrs, c.headers...)
	paths = append(paths, c.paths...)
	return
}

// tracingFixture wires logging, observer and monitoring for one test and puts the globals back.
type tracingFixture struct {
	logs *lockedBuffer
	col  *otlpCollector
}

func newTracingFixture(t *testing.T, enabled bool) *tracingFixture {
	t.Helper()
	isolateServerGlobals(t)

	env, ep, mep, hdrs, name := config.ENVIRONMENT, config.OTEL_EXPORTER_OTLP_ENDPOINT, config.OTEL_EXPORTER_OTLP_METRICS_ENDPOINT, config.OTEL_EXPORTER_OTLP_HEADERS, config.APP_NAME
	f := &tracingFixture{logs: &lockedBuffer{}, col: newOTLPCollector(t)}
	config.ENVIRONMENT = config.ENVIRONMENT_PRODUCTION
	config.APP_NAME = "tracing-test"
	config.OTEL_EXPORTER_OTLP_ENDPOINT, config.OTEL_EXPORTER_OTLP_METRICS_ENDPOINT = "", ""
	config.OTEL_EXPORTER_OTLP_HEADERS = ""
	if enabled {
		config.OTEL_EXPORTER_OTLP_ENDPOINT = f.col.URL
		config.OTEL_EXPORTER_OTLP_HEADERS = "x-collector-key=" + trSentinelHeader
	}
	logging.InitializeWith(f.logs)
	observer.InitializeWith(func(os.Signal) {})
	monitoring.Initialize()
	t.Cleanup(func() {
		config.ENVIRONMENT, config.OTEL_EXPORTER_OTLP_ENDPOINT, config.OTEL_EXPORTER_OTLP_METRICS_ENDPOINT = env, ep, mep
		config.OTEL_EXPORTER_OTLP_HEADERS, config.APP_NAME = hdrs, name
		monitoring.Initialize()
		logging.InitializeWith(os.Stdout)
	})
	return f
}

type downstream struct {
	*httptest.Server
	mu      sync.Mutex
	headers []http.Header
	uris    []string
}

func newDownstream(t *testing.T) *downstream {
	d := &downstream{}
	d.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		d.mu.Lock()
		d.headers = append(d.headers, r.Header.Clone())
		d.uris = append(d.uris, r.RequestURI)
		d.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadGateway)
		_, _ = w.Write([]byte(`{"message":"` + trSentinelPeer + `"}`))
	}))
	t.Cleanup(d.Close)
	return d
}

func runTracedRequest(t *testing.T, f *tracingFixture, down *downstream) (status int, corr string) {
	t.Helper()
	client := restclient.NewRestClient(&restclient.RestClientConfig{Name: "down", BaseURL: down.URL, Timeout: 3, Retries: 1})
	AddRoutes([]Route{{URI: "/v1/echo", Method: http.MethodPost, Function: func(ctx WebContext) {
		tx := monitoring.GetTransactionInContext(ctx.Context())
		monitoring.NoticeError(tx, errors.New(trSentinelErr+" host=db.internal"))
		resp := restclient.Request[map[string]any, map[string]any]{
			Ctx: ctx.Context(), Client: client, HttpMethod: http.MethodGet, Path: "/peer/" + trSentinelQuery + "?q=" + trSentinelQuery,
		}.Call()
		ctx.JsonResponse(http.StatusOK, map[string]int{"peer": resp.StatusCode()})
	}}})
	ls := startServer(t)
	defer ls.shutdown(t)

	req, err := http.NewRequest(http.MethodPost, "http://127.0.0.1:"+strconv.Itoa(ls.port)+"/v1/echo?token="+trSentinelQuery, strings.NewReader(trSentinelBody))
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer "+trSentinelToken)
	req.Header.Set("Cookie", "session="+trSentinelToken)
	req.Header.Set("X-Correlation-Id", trSentinelCorr+" not valid")
	req.Header.Set("traceparent", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
	resp, err := (&http.Client{Timeout: 5 * time.Second}).Do(req)
	require.NoError(t, err)
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	return resp.StatusCode, resp.Header.Get("X-Correlation-Id")
}

func TestTracingEnabledExportsAndNeverLeaks(t *testing.T) {
	f := newTracingFixture(t, true)
	require.True(t, monitoring.UseOTELMonitoring())
	down := newDownstream(t)
	// the service installs its normalizer before the first listener, as forge's core.Run does
	SetCorrelationPolicy(func(raw string) string {
		if raw != "" && !strings.ContainsAny(raw, " \t") && len(raw) <= 64 {
			return raw
		}
		return "norm-0123456789abcdef"
	}, "X-Correlation-Id")

	status, corr := runTracedRequest(t, f, down)
	assert.Equal(t, http.StatusOK, status)
	assert.Equal(t, "norm-0123456789abcdef", corr, "the invalid correlation id is replaced by a normalized one")
	assert.NotContains(t, corr, "SENTINEL")

	// the shutdown (run by startServer's caller) flushed the batch: nothing waits for the batch timer
	spans, raw, hdrs, paths := f.col.snapshot()
	require.NotEmpty(t, spans, "spans reached the collector by the end of the shutdown")
	assert.Contains(t, paths, "/v1/traces")
	require.NotEmpty(t, hdrs)
	assert.Equal(t, trSentinelHeader, hdrs[0].Get("X-Collector-Key"), "configured OTLP headers are sent to the collector only")

	var server, client *collectedSpan
	for i := range spans {
		switch {
		case spans[i].Kind == 2 && strings.HasPrefix(spans[i].Name, "POST"):
			server = &spans[i]
		case spans[i].Name == "HTTP Client":
			client = &spans[i]
		}
	}
	require.NotNil(t, server, "server span: %+v", spans)
	require.NotNil(t, client, "client span: %+v", spans)
	assert.Equal(t, "4bf92f3577b34da6a3ce929d0e0e4736", server.TraceID, "the incoming traceparent is continued")
	assert.Equal(t, "00f067aa0ba902b7", server.ParentSpanID)
	assert.Equal(t, server.TraceID, client.TraceID)
	assert.Equal(t, corr, server.Attrs["correlationId"], "the normalized correlation id is the span attribute")
	assert.Contains(t, server.Events, "exception")
	assert.Contains(t, server.Attrs["event.exception.exception.type"], "errorString")
	assert.Equal(t, int32(2), server.StatusCode, "error status")
	assert.Empty(t, server.StatusMsg, "no error text in the status")

	// propagation: the downstream service received this trace and the normalized correlation id
	down.mu.Lock()
	require.NotEmpty(t, down.headers)
	for _, h := range down.headers {
		assert.Contains(t, h.Get("Traceparent"), "4bf92f3577b34da6a3ce929d0e0e4736")
		assert.Equal(t, corr, h.Get("X-Correlation-Id"))
	}
	down.mu.Unlock()

	// privacy: collector payload (all signals) and logs
	logs := f.logs.String()
	for _, s := range []string{trSentinelQuery, trSentinelToken, trSentinelBody, trSentinelErr, trSentinelPeer, trSentinelCorr, "db.internal", "SENTINEL_CORR"} {
		assert.NotContains(t, string(raw), s, "telemetry payload")
		assert.NotContains(t, logs, s, "log output")
	}
	assert.NotContains(t, logs, trSentinelHeader)
	assert.NotContains(t, string(raw), trSentinelHeader)
	assert.NotContains(t, logs, "127.0.0.1", "no URL or address in the logs")
}

func TestTracingDisabledByConfigExportsNothing(t *testing.T) {
	f := newTracingFixture(t, false)
	require.False(t, monitoring.UseOTELMonitoring())
	down := newDownstream(t)

	status, corr := runTracedRequest(t, f, down)
	assert.Equal(t, http.StatusOK, status)
	assert.NotContains(t, corr, "SENTINEL")

	spans, raw, _, paths := f.col.snapshot()
	assert.Empty(t, spans)
	assert.Empty(t, raw)
	assert.Empty(t, paths, "the collector received no request at all")
	down.mu.Lock()
	for _, h := range down.headers {
		assert.Empty(t, h.Get("Traceparent"), "no trace context when tracing is off")
	}
	down.mu.Unlock()
	assert.NotContains(t, f.logs.String(), "SENTINEL")
}
