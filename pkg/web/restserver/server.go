// Modified in the ezequiel454 fork for forge M3; see FORK.md

package restserver

import (
	"context"
	"errors"
	"net/http"

	"github.com/colibriproject-dev/colibri-sdk-go/pkg/base/config"
	"github.com/colibriproject-dev/colibri-sdk-go/pkg/base/logging"
	"github.com/colibriproject-dev/colibri-sdk-go/pkg/base/observer"
)

var (
	srvRoutes         []Route
	customMiddlewares []CustomMiddleware
	customAuth        CustomAuthenticationMiddleware

	errUserUnauthenticated = errors.New("user not authenticated")
)

// Server defines the contract for the HTTP server implementation.
type Server interface {
	// initialize prepares the server for running.
	initialize()
	// shutdown gracefully closes the server, waiting for the connections still open.
	shutdown() error
	// closeListener closes the listener without waiting for anything. Once it has been called
	// the server never binds a new one.
	closeListener()
	// injectMiddlewares adds standard middlewares to the server.
	injectMiddlewares()
	// injectCustomMiddlewares adds user-defined middlewares to the server.
	injectCustomMiddlewares()
	// injectRoutes registers all routes in the server.
	injectRoutes()
	// listenAndServe binds the listener, calls onListen once it is bound and serves until the
	// listener is closed.
	listenAndServe(onListen func()) error
}

// AddRoutes adds a list of routes to the web rest server.
func AddRoutes(routes []Route) {
	srvRoutes = append(srvRoutes, routes...)
}

// CustomAuthMiddleware adds a custom authentication middleware to the web server.
func CustomAuthMiddleware(fn CustomAuthenticationMiddleware) {
	customAuth = fn
}

// Use adds a custom middleware to the web server.
func Use(m CustomMiddleware) {
	customMiddlewares = append(customMiddlewares, m)
}

// ensureSDKRoutes registers the health check and the documentation routes once, however many
// servers the process starts.
func ensureSDKRoutes() {
	has := func(uri string) bool {
		for _, r := range srvRoutes {
			if r.URI == uri && r.Method == http.MethodGet && r.Prefix == "" {
				return true
			}
		}

		return false
	}

	if !has("/health") {
		addHealthCheckRoute()
	}

	if !has("/api-docs") {
		addDocumentationRoute()
	}
}

// ListenAndServe initializes, configures, and starts the web rest server.
//
// Each call is a server instance of its own (Ready, the admission gate, the lifecycle state).
// It returns when the listener is closed and, if the server was stopped by the graceful
// shutdown, only after the whole shutdown has finished, so the caller can let the process exit.
// A bind failure ends in logging.Fatal, as always, and never waits for the shutdown. When the
// graceful shutdown has already started the listener is not opened and it returns.
func ListenAndServe() {
	ctx := context.Background()
	ensureSDKRoutes()

	o := newRestObserver(nil)
	o.server = createFiberServer(o)
	registerInstance(o)
	defer releaseInstance(o)

	o.server.initialize()
	o.server.injectMiddlewares()
	o.server.injectCustomMiddlewares()
	o.server.injectRoutes()

	done, attached := observer.TryAttach(o)
	if !attached {
		logging.Warn(ctx).Msg("graceful shutdown already started, http server not started")
		return
	}

	logging.Info(ctx).Msgf("Service '%s' running in %d port", "WEB-REST", config.PORT)
	if err := o.serve(); err != nil {
		logging.
			Fatal(ctx).
			Err(err).
			Msg("Error on trying to initialize rest server")
	}

	if o.isStopping() {
		<-o.shutdownDone
		<-done
	}
}
