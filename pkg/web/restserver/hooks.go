// Modified in the ezequiel454 fork for forge M3; see FORK.md

package restserver

import (
	"errors"
	"fmt"
	"net/http"
	"sync"

	"github.com/colibriproject-dev/colibri-sdk-go/pkg/base/logging"
	"github.com/gofiber/fiber/v3"
	"github.com/google/uuid"
)

// ErrorWriter writes every error response the server produces: the error of the custom
// authentication, of Use and BeforeEnter, WebContext.ErrorResponse, a recovered panic (a
// *PanicError), the errors of the router (404, 405), the protocol rejections of the HTTP server
// (400, 408, 413, 431 and the 501 of an unknown method) and a request refused while the server
// is stopping (ErrDraining, status 500).
//
// The writer owns the whole response: status, headers and body. err is for logging and
// classification only and is never meant to be shown to the client as is. For the router and
// protocol rejections it is a *fiber.Error carrying the standard text of the status, never a
// piece of the request.
type ErrorWriter func(ctx WebContext, status int, err error)

// ErrDraining is passed to the ErrorWriter, with status 500, for a request that arrives on an
// open connection after the server stopped accepting work.
var ErrDraining = errors.New("server is shutting down")

var (
	hooksMu                   sync.RWMutex
	errorWriter               ErrorWriter
	correlationPolicy         func(raw string) string
	correlationResponseHeader string

	instMu sync.Mutex
	// current is the instance created by the running ListenAndServe, nil between two of them.
	current *restObserver
	// pendingReady is the Ready channel of the next instance, so a caller that asks for it
	// before ListenAndServe has started waits on the channel that instance is going to close.
	pendingReady = make(chan struct{})
)

// protocolRejectionKey marks, in the fasthttp request, a protocol rejection that fiber is
// answering through the middleware chain.
type protocolRejectionKeyType struct{}

var protocolRejectionKey = protocolRejectionKeyType{}

// SetErrorWriter registers the writer of every error response of the server; see ErrorWriter.
// nil restores the default bodies of v0.2.5. It must be called before ListenAndServe.
func SetErrorWriter(w ErrorWriter) {
	hooksMu.Lock()
	defer hooksMu.Unlock()

	errorWriter = w
}

// GetErrorWriter returns the registered ErrorWriter, nil when there is none.
func GetErrorWriter() ErrorWriter {
	hooksMu.RLock()
	defer hooksMu.RUnlock()

	return errorWriter
}

// SetCorrelationPolicy sets how the inbound correlation id is normalized. normalize receives
// the raw X-Correlation-ID header ("" when absent) and returns the id to use; it runs in the
// first middleware after the OpenTelemetry ones, so the id put in the request context (the
// logs) and in the tracing attribute is never the raw value. responseHeader, when not empty,
// is the header the id is returned in, on every response, protocol rejections included. A nil
// normalize keeps the v0.2.5 behavior for the id. It must be called before ListenAndServe.
func SetCorrelationPolicy(normalize func(raw string) string, responseHeader string) {
	hooksMu.Lock()
	defer hooksMu.Unlock()

	correlationPolicy, correlationResponseHeader = normalize, responseHeader
}

func correlationSettings() (func(string) string, string) {
	hooksMu.RLock()
	defer hooksMu.RUnlock()

	return correlationPolicy, correlationResponseHeader
}

// resolveCorrelationID applies the policy to the raw header. Without a policy it is the v0.2.5
// behavior: the raw value, or a new UUID when absent.
func resolveCorrelationID(raw string) string {
	policy, _ := correlationSettings()
	id := raw
	if policy != nil {
		id = policy(raw)
	}

	if id == "" {
		id = uuid.New().String()
	}

	return id
}

// Ready returns a channel closed when the listener of the current server instance is bound. It
// belongs to the instance of the running ListenAndServe: a previous server never leaves its
// closed channel behind. Between two servers it is the channel the next one will close.
func Ready() <-chan struct{} {
	instMu.Lock()
	defer instMu.Unlock()

	if current != nil {
		return current.ready
	}

	return pendingReady
}

func registerInstance(o *restObserver) {
	instMu.Lock()
	defer instMu.Unlock()

	o.ready = pendingReady
	pendingReady = make(chan struct{})
	current = o
}

func releaseInstance(o *restObserver) {
	instMu.Lock()
	defer instMu.Unlock()

	if current == o {
		current = nil
	}
}

// PanicError is what the ErrorWriter receives for a recovered panic. It exposes the dynamic
// type of the panic value and nothing else: the value can carry secrets, so the SDK neither
// keeps it nor logs it when a writer is set.
type PanicError struct{ valueType string }

// Error is fixed text; it never contains the panic value.
func (p *PanicError) Error() string { return "panic recovered" }

// ValueType is the %T of the panic value.
func (p *PanicError) ValueType() string { return p.valueType }

func newPanicError(r any) *PanicError { return &PanicError{valueType: fmt.Sprintf("%T", r)} }

// writeError answers an error through the ErrorWriter, or with the v0.2.5 body when there is none.
func writeError(ctx WebContext, status int, err error) {
	if w := GetErrorWriter(); w != nil {
		runWriter(w, ctx, status, err)
		return
	}

	ctx.JsonResponse(status, Error{err.Error()})
}

// genericProblemBody is the fixed RFC 7807 answer the server gives when the ErrorWriter itself
// fails. It carries nothing from the request, the error or the panic.
const genericProblemBody = `{"type":"about:blank","title":"Internal Server Error","status":500}`

// runWriter shields the server from a writer that panics: the client gets a fixed generic 500
// problem+json and the panic value, which could carry anything, is dropped and never logged.
func runWriter(w ErrorWriter, ctx WebContext, status int, err error) {
	fctx, _ := ctx.(*fiberWebContext)
	correlation := ""
	if fctx != nil {
		if _, header := correlationSettings(); header != "" {
			correlation = string(append([]byte(nil), fctx.ctx.Response().Header.Peek(header)...))
		}
	}

	defer func() {
		if recover() != nil {
			writeGenericProblem(ctx, correlation)
		}
	}()

	w(ctx, status, err)
}

// writeGenericProblem replaces whatever the failed writer may have written (status, body,
// content type) by the generic problem, keeping the correlation header.
func writeGenericProblem(ctx WebContext, correlation string) {
	defer func() { _ = recover() }()

	fctx, ok := ctx.(*fiberWebContext)
	if !ok {
		ctx.JsonResponse(http.StatusInternalServerError, map[string]any{
			"type": "about:blank", "title": "Internal Server Error", "status": http.StatusInternalServerError,
		})
		ctx.AddHeader("Content-Type", "application/problem+json")

		return
	}

	res := fctx.ctx.Response()
	res.ResetBody()
	res.Header.Del("Content-Length")
	res.Header.Del("Content-Encoding")
	res.SetStatusCode(http.StatusInternalServerError)
	res.Header.SetContentType("application/problem+json")
	res.SetBodyString(genericProblemBody)

	if _, header := correlationSettings(); header != "" && correlation != "" {
		res.Header.Set(header, correlation)
	}
}

// isProtocolRejection reports whether fiber is answering a protocol rejection (400, 408, 413,
// 431) by walking the middleware chain. That request was never admitted: the application
// middlewares must not run for it and it is not counted as running work.
func isProtocolRejection(c fiber.Ctx) bool {
	return c.RequestCtx().UserValue(protocolRejectionKey) != nil
}

// fiberErrorHandler answers the errors that reach fiber: router 404/405, protocol rejections
// and the unknown method. Without a writer it is fiber's own handler.
func fiberErrorHandler(c fiber.Ctx, err error) error {
	w := GetErrorWriter()
	if w == nil {
		return fiber.DefaultErrorHandler(c, err)
	}

	code := fiber.StatusInternalServerError
	var fe *fiber.Error
	if errors.As(err, &fe) {
		// the message of a protocol error can carry a piece of the request: the writer gets the
		// standard text of the status instead
		code = fe.Code
		err = fiber.NewError(code, http.StatusText(code))
	}

	ensureCorrelation(c)
	runWriter(w, newFiberWebContext(c), code, err)

	return nil
}

// ensureCorrelation normalizes the correlation id for the responses no middleware handled: the
// protocol rejections and the router errors that fiber answers before the chain.
func ensureCorrelation(c fiber.Ctx) {
	policy, header := correlationSettings()
	if policy == nil && header == "" {
		return
	}

	if header != "" && c.GetRespHeader(header) != "" {
		return
	}

	id := resolveCorrelationID(c.Get("X-Correlation-ID"))
	if header != "" {
		c.Set(header, id)
	}

	if c.Context().Value(logging.CorrelationIDParam) == nil {
		c.SetContext(logging.InjectCorrelationIDInContext(c.Context(), id))
	}
}

// Reset returns the registration state of the package (routes, middlewares, custom
// authentication, error writer and correlation policy) to that of a fresh process. The package
// keeps one registration per process, so a second composition in the same process (a test
// binary run with -count=N, or several scenarios) would otherwise serve the routes, with the
// handlers, registered by the first one. It is for such tests; a service never calls it. It
// panics when a server is running: it does not stop anything.
func Reset() {
	instMu.Lock()
	if current != nil {
		instMu.Unlock()
		panic("restserver: Reset while a server is running")
	}
	instMu.Unlock()

	srvRoutes, customMiddlewares, customAuth = nil, nil, nil
	SetErrorWriter(nil)
	SetCorrelationPolicy(nil, "")
}
