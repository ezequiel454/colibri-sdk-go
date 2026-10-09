// Modified in the ezequiel454 fork for forge M3; see FORK.md

package restserver

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/colibriproject-dev/colibri-sdk-go/pkg/base/config"
	"github.com/colibriproject-dev/colibri-sdk-go/pkg/base/logging"
	"github.com/colibriproject-dev/colibri-sdk-go/pkg/base/monitoring"
	"github.com/colibriproject-dev/colibri-sdk-go/pkg/base/observer"
	swagger "github.com/gofiber/contrib/v3/swaggo"
	"github.com/gofiber/fiber/v3"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/valyala/fasthttp"
	"github.com/valyala/fasthttp/fasthttpadaptor"
)

// admitter is the admission gate of a server instance.
type admitter interface {
	admit() bool
}

type fiberWebServer struct {
	srv  *fiber.App
	gate admitter

	// mu guards the listener and the closed flag: a listener is never bound after
	// closeListener, whichever side wins the race.
	mu     sync.Mutex
	ln     net.Listener
	closed bool
}

func createFiberServer(gate admitter) Server {
	return &fiberWebServer{gate: gate}
}

func (f *fiberWebServer) initialize() {
	f.srv = fiber.New(fiber.Config{
		ServerHeader: "colibri-sdk-go",
		AppName:      config.APP_NAME,
		ErrorHandler: fiberErrorHandler,
	})

	// protocol rejections and unknown methods are answered by fasthttp and fiber before any
	// route, so the ErrorWriter is wired in at both entrances; without a writer both delegate
	// to the original behavior
	app := f.srv
	known := map[string]bool{}
	for _, m := range app.Config().RequestMethods {
		known[m] = true
	}
	if len(known) == 0 {
		for _, m := range fiber.DefaultMethods {
			known[m] = true
		}
	}

	next := app.Server().Handler
	app.Server().Handler = func(rctx *fasthttp.RequestCtx) {
		if known[string(rctx.Method())] || GetErrorWriter() == nil {
			next(rctx)
			return
		}

		// fiber answers a method outside RequestMethods with a hard-coded 501 text before any
		// handler
		c := app.AcquireCtx(rctx)
		defer app.ReleaseCtx(c)
		_ = fiberErrorHandler(c, fiber.ErrNotImplemented)
	}

	nextProtocolError := app.Server().ErrorHandler
	app.Server().ErrorHandler = func(rctx *fasthttp.RequestCtx, err error) {
		if GetErrorWriter() != nil {
			// fiber walks the Use middlewares before it calls the ErrorHandler; the mark keeps
			// the application ones (authentication, Use) out of a request that was rejected
			// by the protocol and never admitted
			rctx.SetUserValue(protocolRejectionKey, true)
		}
		nextProtocolError(rctx, err)
	}
}

// shutdown waits for the connections still open. It starts in the first phase of the graceful
// shutdown, so its bound covers the drain of the second phase plus the time the connections
// need to finish writing.
func (f *fiberWebServer) shutdown() error {
	err := f.srv.ShutdownWithTimeout(observer.DrainBudget(1, 1) + 10*time.Second)
	if errors.Is(err, net.ErrClosed) {
		// closeListener has already closed the listener
		return nil
	}

	return err
}

func (f *fiberWebServer) closeListener() {
	f.mu.Lock()
	f.closed = true
	ln := f.ln
	f.mu.Unlock()

	if ln != nil {
		_ = ln.Close()
	}
}

func (f *fiberWebServer) injectMiddlewares() {
	if monitoring.UseOTELMonitoring() {
		f.srv.Use(newOpenTelemetryFiberMiddleware())
		f.srv.Use(httpMetricsFiberMiddleware())
	}
	f.srv.Use(correlationIdMiddleware())
	// the gate counts every request that reaches the application: the routes of the service,
	// the ones of the SDK (health, documentation, metrics), the CORS preflight and the error
	// chains. It comes before the CORS middleware for the preflight to be counted.
	f.srv.Use(drainMiddleware(f.gate))
	f.srv.Use(accessControlFiberMiddleware())
	f.srv.Use(panicRecoverMiddleware())
	if customAuth != nil {
		f.srv.Use(customAuthenticationContextFiberMiddleware())
	} else {
		f.srv.Use(authenticationContextFiberMiddleware())
	}
}

func (f *fiberWebServer) injectCustomMiddlewares() {
	for _, middleware := range customMiddlewares {
		f.registerCustomMiddleware(middleware)
	}
}

func (f *fiberWebServer) convertUriToFiberUri(uri string, replacer *strings.Replacer) string {
	paths := strings.Split(uri, "/")

	for idx, path := range paths {
		if f.pathIsPathParam(path) {
			paths[idx] = fmt.Sprintf(":%s", replacer.Replace(path))
		}
	}

	return strings.Join(paths, "/")
}

func (f *fiberWebServer) pathIsPathParam(path string) bool {
	return strings.Contains(path, "{")
}

func (f *fiberWebServer) injectRoutes() {
	f.addMetricsRoute()
	f.addSwaggerUI()

	replacer := strings.NewReplacer(
		"{", "",
		"}", "",
	)

	for _, route := range srvRoutes {
		routeUri := string(route.Prefix) + f.convertUriToFiberUri(route.URI, replacer)
		fn := route.Function
		beforeEnter := route.BeforeEnter

		f.srv.Add([]string{route.Method}, routeUri, func(fctx fiber.Ctx) error {
			fctx.Set(parameterizedURLHeaderKey, routeUri)
			webContext := newFiberWebContext(fctx)
			if beforeEnter != nil {
				if err := beforeEnter(webContext); err != nil {
					writeError(webContext, err.StatusCode, err.Err)
					return nil
				}
			}

			fn(webContext)
			return nil
		}).Name(routeUri)

		logging.
			Info(context.Background()).
			Msgf("Registered route [%7s] %s", route.Method, string(route.Prefix)+route.URI)
	}
}

// listen is the bind. It is a variable only so a test can hold the bind in progress and prove
// that closeListener cannot return around it.
var listen = net.Listen

// listenAndServe owns the listener, instead of leaving it to fiber's Listen, so closeListener
// can close it at any moment. Checking the closed flag, binding and publishing the listener are
// one critical section under f.mu, the same lock closeListener takes: a bind in progress makes
// closeListener wait for it (a local bind, bounded by the operating system, never by the drain
// of the admitted requests) and then close the socket it published, and a closeListener that
// won the lock first means nothing is bound at all. So no socket opens after closeListener
// returned.
func (f *fiberWebServer) listenAndServe(onListen func()) error {
	f.mu.Lock()
	if f.closed {
		f.mu.Unlock()
		return nil
	}

	ln, err := listen("tcp", fmt.Sprintf(":%d", config.PORT))
	if err != nil {
		f.mu.Unlock()
		return fmt.Errorf("failed to listen: %w", err)
	}
	f.ln = ln
	f.mu.Unlock()

	if onListen != nil {
		onListen()
	}

	return f.srv.Listener(ln, fiber.ListenConfig{DisableStartupMessage: true})
}

func (f *fiberWebServer) addMetricsRoute() {
	const route = "/metrics"

	p := fasthttpadaptor.NewFastHTTPHandler(promhttp.Handler())
	f.srv.Get(route, func(c fiber.Ctx) error {
		p(c.RequestCtx())
		return nil
	})
}

func (f *fiberWebServer) addSwaggerUI() {
	if config.IsDevelopmentEnvironment() {
		f.srv.Get("/swagger/*", swagger.New(swagger.Config{URL: "/api-docs"}))
	}
}

func (f *fiberWebServer) registerCustomMiddleware(m CustomMiddleware) {
	fn := func(ctx fiber.Ctx) error {
		if isProtocolRejection(ctx) {
			return ctx.Next()
		}

		webCtx := &fiberWebContext{ctx: ctx}
		if err := m.Apply(webCtx); err != nil {
			writeError(webCtx, err.StatusCode, err.Err)
			return nil
		}

		return ctx.Next()
	}

	f.srv.Use(fn)
}
