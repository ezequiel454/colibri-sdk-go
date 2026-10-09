// Modified in the ezequiel454 fork for forge M3; see FORK.md

package restserver

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/colibriproject-dev/colibri-sdk-go/pkg/base/config"
	"github.com/colibriproject-dev/colibri-sdk-go/pkg/base/logging"
	"github.com/colibriproject-dev/colibri-sdk-go/pkg/base/observer"
	"github.com/colibriproject-dev/colibri-sdk-go/pkg/base/security"
	"github.com/gofiber/fiber/v3"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/valyala/fasthttp"
)

const (
	sentinelPanic = "SENTINEL_PANIC_VALUE"
	sentinelCorr  = "SENTINEL_RAW_CORRELATION"
	sentinelAuth  = "SENTINEL_AUTH_DETAIL"

	sentinelWriterPanic = "SENTINEL_WRITER_PANIC_LOG"
	problemType         = "application/problem+json"
)

// writerCall is what the test writer was handed.
type writerCall struct {
	status int
	err    error
}

type writerRecorder struct {
	mu    sync.Mutex
	calls []writerCall
}

func (r *writerRecorder) writer() ErrorWriter {
	return func(ctx WebContext, status int, err error) {
		r.mu.Lock()
		r.calls = append(r.calls, writerCall{status, err})
		r.mu.Unlock()

		// like the final forge writer: a fixed problem body, never the error text
		ctx.JsonResponse(status, map[string]any{"type": "about:blank", "status": status})
		ctx.AddHeader("Content-Type", problemType)
	}
}

func (r *writerRecorder) last(t *testing.T) writerCall {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	require.NotEmpty(t, r.calls, "the writer was not called")

	return r.calls[len(r.calls)-1]
}

// testNormalizer is a stand-in for the forge policy: empty becomes a fixed id, anything that is
// not a short token becomes "h-" plus the length.
func testNormalizer(raw string) string {
	switch {
	case raw == "":
		return "generated-id"
	case len(raw) > 32 || strings.Contains(raw, "SENTINEL"):
		return fmt.Sprintf("h-%d", len(raw))
	}

	return raw
}

type failingAuth struct{}

func (failingAuth) Apply(ctx WebContext) (*security.AuthenticationContext, error) {
	if strings.HasPrefix(ctx.Path(), "/api/") {
		return nil, errors.New(sentinelAuth)
	}

	return security.NewAuthenticationContext("t", "u"), nil
}

type conflictMiddleware struct{}

func (conflictMiddleware) Apply(ctx WebContext) *MiddlewareError {
	if ctx.Path() == "/public/use-conflict" {
		return NewMiddlewareError(http.StatusConflict, errors.New(sentinelAuth))
	}

	return nil
}

func hookRoutes(seenCorr *sync.Map) []Route {
	return []Route{
		{URI: "ok", Method: http.MethodGet, Prefix: PublicApi, Function: func(ctx WebContext) {
			seenCorr.Store("ctx", ctx.Context().Value(logging.CorrelationIDParam))
			ctx.JsonResponse(http.StatusOK, map[string]string{"ok": "1"})
		}},
		{URI: "panic", Method: http.MethodGet, Prefix: PublicApi, Function: func(WebContext) { panic(sentinelPanic) }},
		{URI: "teapot", Method: http.MethodGet, Prefix: PublicApi, Function: func(ctx WebContext) {
			ctx.ErrorResponse(http.StatusTeapot, errors.New(sentinelAuth))
		}},
		{URI: "before", Method: http.MethodGet, Prefix: PublicApi,
			BeforeEnter: func(WebContext) *MiddlewareError {
				return NewMiddlewareError(http.StatusConflict, errors.New(sentinelAuth))
			},
			Function: func(ctx WebContext) { ctx.EmptyResponse(http.StatusOK) }},
		{URI: "users", Method: http.MethodGet, Prefix: AuthenticatedApi, Function: func(ctx WebContext) {
			ctx.EmptyResponse(http.StatusOK)
		}},
	}
}

// TestErrorWriterReceivesEveryErrorPath sends a request down each path of the spec to a real
// server and checks status, writer, problem content type, one correlation header and no echo.
func TestErrorWriterReceivesEveryErrorPath(t *testing.T) {
	isolateServerGlobals(t)
	config.WAIT_GROUP_TIMEOUT_SECONDS = 5
	observer.Initialize()

	rec := &writerRecorder{}
	SetErrorWriter(rec.writer())
	SetCorrelationPolicy(testNormalizer, "X-Correlation-Id")
	seen := &sync.Map{}
	AddRoutes(hookRoutes(seen))
	CustomAuthMiddleware(failingAuth{})
	Use(conflictMiddleware{})

	ls := startServer(t)
	defer ls.shutdown(t)

	type tc struct {
		name   string
		do     func() (*http.Response, string)
		status int
		check  func(t *testing.T, err error)
	}
	get := func(method, path string) func() (*http.Response, string) {
		return func() (*http.Response, string) {
			return ls.do(t, method, path, map[string]string{"X-Correlation-Id": sentinelCorr})
		}
	}
	rawReq := func(p string) func() (*http.Response, string) {
		return func() (*http.Response, string) { return ls.raw(t, p) }
	}

	cases := []tc{
		{"custom auth 401", get("GET", "/api/users"), 401, func(t *testing.T, err error) { assert.EqualError(t, err, sentinelAuth) }},
		{"Use error", get("GET", "/public/use-conflict"), 409, nil},
		{"BeforeEnter error", get("GET", "/public/before"), 409, nil},
		{"ErrorResponse", get("GET", "/public/teapot"), 418, nil},
		{"panic", get("GET", "/public/panic"), 500, func(t *testing.T, err error) {
			var pe *PanicError
			require.True(t, errors.As(err, &pe))
			assert.Equal(t, "string", pe.ValueType())
			assert.NotContains(t, err.Error(), sentinelPanic)
		}},
		{"router 404", get("GET", "/public/nope"), 404, nil},
		{"router 405", get("POST", "/public/ok"), 405, nil},
		{"protocol 400", rawReq("GET /public/ok HTTP/1.1\r\nHost: x\r\nBad Header Line\r\n\r\n"), 400, func(t *testing.T, err error) {
			assert.EqualError(t, err, "Bad Request", "the writer must not receive the raw request text")
		}},
		{"protocol 413", rawReq("POST /public/ok HTTP/1.1\r\nHost: x\r\nContent-Length: 99999999\r\n\r\n"), 413, nil},
		{"protocol 431", rawReq("GET /public/ok HTTP/1.1\r\nHost: x\r\nX-Big: " + strings.Repeat("a", 20000) + "\r\n\r\n"), 431, nil},
		{"unknown method 501", rawReq("FOO /public/ok HTTP/1.1\r\nHost: x\r\n\r\n"), 501, nil},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			before := len(rec.calls)
			res, body := c.do()
			assert.Equal(t, c.status, res.StatusCode)
			assert.Equal(t, problemType, strings.Join(res.Header.Values("Content-Type"), ","), "a single problem content type")
			assert.Greater(t, len(rec.calls), before, "the response must come from the writer")
			assert.Equal(t, c.status, rec.last(t).status)
			assert.Len(t, res.Header.Values("X-Correlation-Id"), 1)
			assert.NotContains(t, res.Header.Get("X-Correlation-Id"), "SENTINEL")
			for _, s := range []string{sentinelPanic, sentinelCorr, sentinelAuth, "Bad Header"} {
				assert.NotContains(t, body, s)
			}
			if c.check != nil {
				c.check(t, rec.last(t).err)
			}
		})
	}

	t.Run("correlation is normalized in the context and returned", func(t *testing.T) {
		res, _ := ls.do(t, "GET", "/public/ok", map[string]string{"X-Correlation-Id": sentinelCorr})
		assert.Equal(t, 200, res.StatusCode)
		want := testNormalizer(sentinelCorr)
		assert.Equal(t, want, res.Header.Get("X-Correlation-Id"))
		v, _ := seen.Load("ctx")
		assert.Equal(t, want, v, "the id in the request context (the logs) must be the normalized one")

		res, _ = ls.do(t, "GET", "/public/ok", nil)
		assert.Equal(t, "generated-id", res.Header.Get("X-Correlation-Id"))
	})

	t.Run("health docs preflight metrics keep working", func(t *testing.T) {
		for _, p := range []string{"/health", "/metrics"} {
			res, _ := ls.do(t, "GET", p, nil)
			assert.Equal(t, 200, res.StatusCode, p)
			assert.Equal(t, "generated-id", res.Header.Get("X-Correlation-Id"), p)
		}
		res, _ := ls.do(t, "OPTIONS", "/public/ok", map[string]string{"Origin": "http://a.example", "Access-Control-Request-Method": "GET"})
		assert.Equal(t, 204, res.StatusCode)
		assert.Equal(t, "generated-id", res.Header.Get("X-Correlation-Id"))
	})
}

// TestProtocolRejectionDoesNotRunApplicationCode pins that a request rejected by the protocol
// never reaches the authentication, the Use middlewares or the gate, which fiber's error path
// would otherwise run for it.
func TestProtocolRejectionDoesNotRunApplicationCode(t *testing.T) {
	isolateServerGlobals(t)
	config.WAIT_GROUP_TIMEOUT_SECONDS = 5
	observer.Initialize()

	rec := &writerRecorder{}
	SetErrorWriter(rec.writer())
	var applied sync.Map
	CustomAuthMiddleware(authFunc(func(WebContext) (*security.AuthenticationContext, error) {
		applied.Store("auth", true)
		return security.NewAuthenticationContext("t", "u"), nil
	}))
	Use(useFunc(func(WebContext) *MiddlewareError { applied.Store("use", true); return nil }))

	ls := startServer(t)
	defer ls.shutdown(t)

	res, _ := ls.raw(t, "GET /public/ok HTTP/1.1\r\nHost: x\r\nBad Header Line\r\n\r\n")
	assert.Equal(t, 400, res.StatusCode)
	_, authRan := applied.Load("auth")
	_, useRan := applied.Load("use")
	assert.False(t, authRan, "authentication must not run for a protocol rejection")
	assert.False(t, useRan, "Use middlewares must not run for a protocol rejection")

	// and a normal request does run them, so the test would notice the marker leaking
	ls.do(t, "GET", "/health", nil)
	_, authRan = applied.Load("auth")
	assert.True(t, authRan)
}

type authFunc func(WebContext) (*security.AuthenticationContext, error)

func (f authFunc) Apply(c WebContext) (*security.AuthenticationContext, error) { return f(c) }

type useFunc func(WebContext) *MiddlewareError

func (f useFunc) Apply(c WebContext) *MiddlewareError { return f(c) }

// TestProtocolTimeoutIsAnsweredByTheWriter covers the 408 mapping, which a loopback client cannot
// provoke without a read timeout.
func TestProtocolTimeoutIsAnsweredByTheWriter(t *testing.T) {
	isolateServerGlobals(t)
	rec := &writerRecorder{}
	SetErrorWriter(rec.writer())

	f := &fiberWebServer{}
	f.initialize()

	rctx := &fasthttp.RequestCtx{}
	rctx.Request.Header.SetMethod("GET")
	rctx.Request.SetRequestURI("/x")
	f.srv.Server().ErrorHandler(rctx, &net.OpError{Op: "read", Err: timeoutError{}})

	assert.Equal(t, 408, rctx.Response.StatusCode())
	assert.Equal(t, 408, rec.last(t).status)
	assert.Equal(t, problemType, string(rctx.Response.Header.ContentType()))
}

type timeoutError struct{}

func (timeoutError) Error() string   { return "i/o timeout" }
func (timeoutError) Timeout() bool   { return true }
func (timeoutError) Temporary() bool { return true }

// countingGate wraps the real gate of an instance to count the admissions.
type countingGate struct {
	inner admitter
	mu    sync.Mutex
	paths []string
}

func (g *countingGate) admit() bool {
	g.mu.Lock()
	g.paths = append(g.paths, "admitted")
	g.mu.Unlock()

	return g.inner.admit()
}

func (g *countingGate) count() int {
	g.mu.Lock()
	defer g.mu.Unlock()

	return len(g.paths)
}

// TestGateCountsEveryAdmittedRouteAndReleasesOnPanic drives the middleware chain without a socket:
// application, SDK, preflight and error chains are admitted and every admission is released,
// the panicking one included, so the drain finds no work left.
func TestGateCountsEveryAdmittedRouteAndReleasesOnPanic(t *testing.T) {
	isolateServerGlobals(t)
	logging.Initialize()
	config.WAIT_GROUP_TIMEOUT_SECONDS = 1
	observer.Initialize()

	rec := &writerRecorder{}
	SetErrorWriter(rec.writer())
	AddRoutes(hookRoutes(&sync.Map{}))
	ensureSDKRoutes()
	Use(conflictMiddleware{})

	o := newRestObserver(&fiberWebServer{srv: &fiber.App{}})
	gate := &countingGate{inner: o}
	f := &fiberWebServer{gate: gate}
	f.initialize()
	f.injectMiddlewares()
	f.injectCustomMiddlewares()
	f.injectRoutes()

	type req struct {
		method, path string
		headers      map[string]string
		status       int
	}
	preflight := map[string]string{"Origin": "http://a.example", "Access-Control-Request-Method": "GET"}
	reqs := []req{
		{"GET", "/public/ok", nil, 200},
		{"GET", "/health", nil, 200},
		{"GET", "/metrics", nil, 200},
		{"OPTIONS", "/public/ok", preflight, 204},
		{"GET", "/public/teapot", nil, 418},
		{"GET", "/public/before", nil, 409},
		{"GET", "/public/use-conflict", nil, 409},
		{"GET", "/public/panic", nil, 500},
	}
	for _, r := range reqs {
		hr := httptest.NewRequest(r.method, r.path, nil)
		for k, v := range r.headers {
			hr.Header.Set(k, v)
		}
		before := gate.count()
		res, err := f.srv.Test(hr)
		require.NoError(t, err, r.path)
		assert.Equal(t, r.status, res.StatusCode, r.path)
		assert.Equal(t, before+1, gate.count(), "%s %s must be admitted by the gate", r.method, r.path)
	}

	assert.False(t, observer.WaitRunningTimeout(), "every admission, the panicking one included, must be released")

	// after Stop the same chain refuses with ErrDraining, status 500, and closes the connection
	o.Stop()
	res, err := f.srv.Test(httptest.NewRequest("GET", "/public/ok", nil))
	require.NoError(t, err)
	assert.Equal(t, 500, res.StatusCode)
	assert.True(t, res.Close, "the refused request must close its connection")
	assert.True(t, errors.Is(rec.last(t).err, ErrDraining))
	assert.False(t, observer.WaitRunningTimeout())
}

// TestWithoutHooksTheBodiesAreThoseOfV025 pins the opt-in contract: no writer, no policy.
func TestWithoutHooksTheBodiesAreThoseOfV025(t *testing.T) {
	isolateServerGlobals(t)
	config.WAIT_GROUP_TIMEOUT_SECONDS = 5
	observer.Initialize()

	seen := &sync.Map{}
	AddRoutes(hookRoutes(seen))
	CustomAuthMiddleware(failingAuth{})
	Use(conflictMiddleware{})

	ls := startServer(t)
	defer ls.shutdown(t)

	expect := func(method, path string, status int, contentType, body string) {
		t.Helper()
		res, got := ls.do(t, method, path, nil)
		assert.Equal(t, status, res.StatusCode, path)
		assert.True(t, strings.HasPrefix(res.Header.Get("Content-Type"), contentType), "%s: %s", path, res.Header.Get("Content-Type"))
		assert.Equal(t, body, got, path)
	}

	expect("GET", "/api/users", 401, "application/json", "{}")
	expect("GET", "/public/use-conflict", 409, "application/json", `{"error":"`+sentinelAuth+`"}`)
	expect("GET", "/public/before", 409, "application/json", `{"error":"`+sentinelAuth+`"}`)
	expect("GET", "/public/teapot", 418, "application/json", `{"error":"`+sentinelAuth+`"}`)
	expect("GET", "/public/panic", 500, "application/json", `{"error":"internal server error occurred"}`)
	expect("GET", "/public/nope", 404, "text/plain", "Not Found")
	expect("POST", "/public/ok", 405, "text/plain", "Method Not Allowed")

	res, _ := ls.do(t, "GET", "/public/ok", map[string]string{"X-Correlation-Id": "abc"})
	assert.Empty(t, res.Header.Get("X-Correlation-Id"), "without a policy the id is not returned, as in v0.2.5")
	v, _ := seen.Load("ctx")
	assert.Equal(t, "abc", v, "without a policy the raw id goes to the context, as in v0.2.5")

	ls.do(t, "GET", "/public/ok", nil)
	v, _ = seen.Load("ctx")
	generated, _ := v.(string)
	_, uuidErr := uuid.Parse(generated)
	assert.NoError(t, uuidErr, "without a header a UUID is generated, as in v0.2.5")

	res, body := ls.raw(t, "FOO /public/ok HTTP/1.1\r\nHost: x\r\n\r\n")
	assert.Equal(t, 501, res.StatusCode)
	assert.Equal(t, "Not Implemented", body)
}

func TestPanicErrorExposesOnlyTheType(t *testing.T) {
	p := newPanicError(errors.New(sentinelPanic))
	assert.Equal(t, "*errors.errorString", p.ValueType())
	assert.NotContains(t, p.Error(), "SENTINEL")
	assert.NotContains(t, fmt.Sprintf("%v %+v %#v", p, p, p), sentinelPanic)
}

// TestAWriterThatPanicsGetsTheGenericProblem runs a real server whose opted-in writer panics, before
// and after it started writing the response. The client must get the generic 500 problem+json, one
// content type, the correlation header kept and nothing the writer or the request carried.
func TestAWriterThatPanicsGetsTheGenericProblem(t *testing.T) {
	const sentinelWriter = "SENTINEL_WRITER_PANIC"
	const wantBody = `{"type":"about:blank","title":"Internal Server Error","status":500}`

	for _, mode := range []string{"before any write", "after partial writes"} {
		t.Run(mode, func(t *testing.T) {
			isolateServerGlobals(t)
			config.WAIT_GROUP_TIMEOUT_SECONDS = 5
			observer.Initialize()

			SetCorrelationPolicy(testNormalizer, "X-Correlation-Id")
			SetErrorWriter(func(ctx WebContext, status int, err error) {
				if mode == "after partial writes" {
					ctx.JsonResponse(status, map[string]string{"leak": sentinelWriter})
					ctx.AddHeader("Content-Type", "text/plain")
					ctx.AddHeader("Content-Encoding", "identity")
					ctx.AddHeader("X-Partial", sentinelWriter)
				}
				panic(sentinelWriter)
			})
			AddRoutes(hookRoutes(&sync.Map{}))

			ls := startServer(t)
			defer ls.shutdown(t)

			for _, path := range []string{"/missing", "/public/panic"} {
				res, body := ls.do(t, http.MethodGet, path, map[string]string{"X-Correlation-Id": sentinelCorr})
				assert.Equal(t, 500, res.StatusCode, path)
				assert.Equal(t, []string{problemType}, res.Header.Values("Content-Type"), path)
				assert.JSONEq(t, wantBody, body, path)
				assert.Equal(t, []string{testNormalizer(sentinelCorr)}, res.Header.Values("X-Correlation-Id"), path)
				assert.Empty(t, res.Header.Get("Content-Encoding"), path)
				for _, sentinel := range []string{sentinelWriter, sentinelPanic, sentinelCorr} {
					assert.NotContains(t, body, sentinel, path)
				}
			}
		})
	}

	t.Run("without a writer a failing path keeps the v0.2.5 body", func(t *testing.T) {
		isolateServerGlobals(t)
		config.WAIT_GROUP_TIMEOUT_SECONDS = 5
		observer.Initialize()
		AddRoutes(hookRoutes(&sync.Map{}))

		ls := startServer(t)
		defer ls.shutdown(t)

		res, body := ls.do(t, http.MethodGet, "/public/panic", nil)
		assert.Equal(t, 500, res.StatusCode)
		assert.Equal(t, `{"error":"internal server error occurred"}`, body)
	})
}

// TestLogsCarryNoRawValue runs a server with the writer and the policy in a child process and scans
// everything it wrote to stdout/stderr.
func TestLogsCarryNoRawValue(t *testing.T) {
	out := runChild(t, "TestLogScanChild")

	assert.Contains(t, out, "CHILD_DONE")
	assert.Contains(t, out, "writer panic status 500", "the failing writer was exercised")
	for _, s := range []string{sentinelPanic, sentinelCorr, sentinelAuth, sentinelWriterPanic} {
		assert.NotContains(t, out, s, "a raw value reached the logs")
	}
	handlerLogs := linesContaining(out, "handler log")
	require.NotEmpty(t, handlerLogs)
	assert.Contains(t, handlerLogs[0], `"correlationId":"`+testNormalizer(sentinelCorr)+`"`, "logs carry the normalized id")
	assert.NotEmpty(t, linesContaining(out, "panic type string"), "the writer saw the panic type")
}

func TestLogScanChild(t *testing.T) {
	if os.Getenv(childEnv) == "" {
		return
	}

	isolateServerGlobals(t)
	config.WAIT_GROUP_TIMEOUT_SECONDS = 5
	observer.Initialize()

	SetErrorWriter(func(ctx WebContext, status int, err error) {
		if ctx.Path() == "/public/writer-panic" {
			ctx.JsonResponse(status, map[string]string{"leak": sentinelWriterPanic})
			panic(sentinelWriterPanic)
		}
		var pe *PanicError
		if errors.As(err, &pe) {
			logging.Error(ctx.Context()).Msgf("panic type %s", pe.ValueType())
		}
		ctx.JsonResponse(status, map[string]int{"status": status})
	})
	SetCorrelationPolicy(testNormalizer, "X-Correlation-Id")
	AddRoutes(hookRoutes(&sync.Map{}))
	AddRoutes([]Route{{URI: "log", Method: http.MethodGet, Prefix: PublicApi, Function: func(ctx WebContext) {
		logging.Info(ctx.Context()).Msg("handler log")
		ctx.EmptyResponse(200)
	}}})
	CustomAuthMiddleware(failingAuth{})

	ls := startServer(t)
	h := map[string]string{"X-Correlation-Id": sentinelCorr}
	ls.do(t, "GET", "/public/log", h)
	ls.do(t, "GET", "/public/panic", h)
	ls.do(t, "GET", "/api/users", h)
	res, _ := ls.do(t, "GET", "/public/writer-panic", h)
	fmt.Println("writer panic status", res.StatusCode)
	ls.shutdown(t)
	time.Sleep(10 * time.Millisecond)
	fmt.Println("CHILD_DONE")
}

func TestResetClearsTheRegistration(t *testing.T) {
	isolateServerGlobals(t)
	AddRoutes([]Route{{URI: "/v1/x", Method: http.MethodGet, Function: func(WebContext) {}}})
	Use(nopMiddleware{})
	CustomAuthMiddleware(nopAuth{})
	SetErrorWriter(func(WebContext, int, error) {})
	SetCorrelationPolicy(func(string) string { return "" }, "X-Id")

	Reset()
	assert.Empty(t, srvRoutes)
	assert.Empty(t, customMiddlewares)
	assert.Nil(t, customAuth)
	assert.Nil(t, GetErrorWriter())
	n, h := correlationSettings()
	assert.Nil(t, n)
	assert.Empty(t, h)

	// a server composed after Reset serves the routes registered after it
	observer.InitializeWith(func(os.Signal) {})
	var hit atomic.Int32
	AddRoutes([]Route{{URI: "/v1/y", Method: http.MethodGet, Function: func(ctx WebContext) { hit.Add(1); ctx.EmptyResponse(204) }}})
	ls := startServer(t)
	resp, err := http.Get("http://127.0.0.1:" + strconv.Itoa(ls.port) + "/v1/y")
	require.NoError(t, err)
	_ = resp.Body.Close()
	assert.Equal(t, 204, resp.StatusCode)
	assert.Equal(t, int32(1), hit.Load())
	assert.Panics(t, Reset, "Reset must refuse to run under a live server")
	ls.shutdown(t)
}

type nopMiddleware struct{}

func (nopMiddleware) Apply(WebContext) *MiddlewareError { return nil }

type nopAuth struct{}

func (nopAuth) Apply(WebContext) (*security.AuthenticationContext, error) { return nil, nil }
