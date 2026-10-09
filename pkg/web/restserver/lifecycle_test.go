// Modified in the ezequiel454 fork for forge M3; see FORK.md

package restserver

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/colibriproject-dev/colibri-sdk-go/pkg/base/config"
	"github.com/colibriproject-dev/colibri-sdk-go/pkg/base/logging"
	"github.com/colibriproject-dev/colibri-sdk-go/pkg/base/observer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const loopbackTimeout = 10 * time.Second

// isolateServerGlobals gives a test empty package state (routes, middlewares, authentication,
// hooks, current instance) and puts the previous one back at the end, so the tests do not depend
// on the order they run in or on what the older tests left registered.
func isolateServerGlobals(t *testing.T) {
	t.Helper()

	logging.Initialize()

	routes, middlewares, auth, port, drain := srvRoutes, customMiddlewares, customAuth, config.PORT, config.WAIT_GROUP_TIMEOUT_SECONDS
	srvRoutes, customMiddlewares, customAuth = nil, nil, nil
	SetErrorWriter(nil)
	SetCorrelationPolicy(nil, "")
	instMu.Lock()
	current = nil
	instMu.Unlock()

	t.Cleanup(func() {
		srvRoutes, customMiddlewares, customAuth, config.PORT, config.WAIT_GROUP_TIMEOUT_SECONDS = routes, middlewares, auth, port, drain
		SetErrorWriter(nil)
		SetCorrelationPolicy(nil, "")
		instMu.Lock()
		current = nil
		instMu.Unlock()
	})
}

// freePort returns a port nothing listens on right now.
func freePort(t *testing.T) int {
	t.Helper()

	l, err := net.Listen("tcp", ":0")
	require.NoError(t, err)
	defer l.Close()

	return l.Addr().(*net.TCPAddr).Port
}

// liveServer is a ListenAndServe running on a loopback port.
type liveServer struct {
	port     int
	ready    <-chan struct{}
	returned chan struct{}
	panicked chan any
}

// startServer runs ListenAndServe on a free port and waits for its Ready. The Ready channel is
// asked for before the server starts, which is how a caller that does not know whether the
// server is up yet uses it.
func startServer(t *testing.T) *liveServer {
	t.Helper()

	ls := &liveServer{
		port:     freePort(t),
		ready:    Ready(),
		returned: make(chan struct{}),
		panicked: make(chan any, 1),
	}
	config.PORT = ls.port

	go func() {
		defer close(ls.returned)
		defer func() {
			if r := recover(); r != nil {
				ls.panicked <- r
			}
		}()

		ListenAndServe()
	}()

	select {
	case <-ls.ready:
	case <-time.After(loopbackTimeout):
		t.Fatal("the server did not become ready")
	}

	return ls
}

// shutdown runs the graceful shutdown and waits for ListenAndServe to return.
func (ls *liveServer) shutdown(t *testing.T) {
	t.Helper()

	observer.Notify()
	ls.waitReturned(t)
}

func (ls *liveServer) waitReturned(t *testing.T) {
	t.Helper()

	select {
	case <-ls.returned:
	case <-time.After(loopbackTimeout):
		t.Fatal("ListenAndServe did not return")
	}
}

func (ls *liveServer) url(path string) string {
	return fmt.Sprintf("http://127.0.0.1:%d%s", ls.port, path)
}

// loopbackClient never reuses a connection: every request is one connection, so a request never
// rides on a connection another test left open.
var loopbackClient = &http.Client{
	Timeout:   loopbackTimeout,
	Transport: &http.Transport{DisableKeepAlives: true},
}

func (ls *liveServer) do(t *testing.T, method, path string, headers map[string]string) (*http.Response, string) {
	t.Helper()

	req, err := http.NewRequest(method, ls.url(path), nil)
	require.NoError(t, err)
	for k, v := range headers {
		req.Header.Set(k, v)
	}

	res, err := loopbackClient.Do(req)
	require.NoError(t, err)
	defer res.Body.Close()
	body, err := io.ReadAll(res.Body)
	require.NoError(t, err)

	return res, string(body)
}

// raw sends bytes the http client would never produce and reads the response.
func (ls *liveServer) raw(t *testing.T, payload string) (*http.Response, string) {
	t.Helper()

	conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", ls.port), loopbackTimeout)
	require.NoError(t, err)
	defer conn.Close()
	require.NoError(t, conn.SetDeadline(time.Now().Add(loopbackTimeout)))

	_, err = conn.Write([]byte(payload))
	require.NoError(t, err)

	res, err := http.ReadResponse(bufio.NewReader(conn), nil)
	require.NoError(t, err)
	defer res.Body.Close()
	body, _ := io.ReadAll(res.Body)

	return res, string(body)
}

func portIsRefused(port int) bool {
	conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), time.Second)
	if err != nil {
		return true
	}
	_ = conn.Close()

	return false
}

func portIsFree(t *testing.T, port int) bool {
	t.Helper()

	l, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return false
	}
	_ = l.Close()

	return true
}

// eventLog collects events in the order they happened.
type eventLog struct {
	mu     sync.Mutex
	events []string
}

func (l *eventLog) add(e string) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.events = append(l.events, e)
}

func (l *eventLog) snapshot() []string {
	l.mu.Lock()
	defer l.mu.Unlock()

	return append([]string(nil), l.events...)
}

func (l *eventLog) index(e string) int {
	for i, got := range l.snapshot() {
		if got == e {
			return i
		}
	}

	return -1
}

// fakeResource stands in for the database: an observer attached before the server, so it is
// closed in the closing phase. Work that runs after it closed is counted.
type fakeResource struct {
	log    *eventLog
	closed atomic.Bool
	// afterClose counts the application work that found the resource already closed
	afterClose atomic.Int32
}

func (r *fakeResource) Close() {
	r.closed.Store(true)
	r.log.add("resource-close")
}

// use is what an application handler does with the resource.
func (r *fakeResource) use() {
	if r.closed.Load() {
		r.afterClose.Add(1)
	}
}

// dialProbe is an observer attached after the server. The first phase stops the observers in
// attach order, so its Stop runs right after the one of the server returned: a connection it
// opens then must be refused by the operating system.
type dialProbe struct {
	log  *eventLog
	port int
	// resourceClosedAtStop records whether the resource was already closed at that moment
	resource *fakeResource
}

func (p *dialProbe) Stop() {
	if portIsRefused(p.port) {
		p.log.add("dial-refused-after-stop")
	} else {
		p.log.add("dial-accepted-after-stop")
	}
	if p.resource.closed.Load() {
		p.log.add("resource-closed-before-stop-returned")
	}
}

func (p *dialProbe) Close() {}

// TestSequentialListenAndServeCyclesAreIndependent is the restart case: two servers in one process,
// the second after the shutdown of the first. Nothing of the first (the Ready channel, the closed
// gate, the observer) may be seen by the second.
func TestSequentialListenAndServeCyclesAreIndependent(t *testing.T) {
	isolateServerGlobals(t)
	config.WAIT_GROUP_TIMEOUT_SECONDS = 5

	resources := []*fakeResource{{log: &eventLog{}}, {log: &eventLog{}}}
	var instances []*restObserver
	var readies []<-chan struct{}
	var dones []<-chan struct{}

	for cycle := range 2 {
		observer.Initialize()
		observer.Attach(resources[cycle])
		dones = append(dones, observer.Done())

		ls := startServer(t)
		readies = append(readies, ls.ready)
		instMu.Lock()
		instances = append(instances, current)
		instMu.Unlock()

		res, body := ls.do(t, http.MethodGet, "/health", nil)
		assert.Equal(t, http.StatusOK, res.StatusCode, "cycle %d must admit requests: the gate reopens", cycle)
		assert.Contains(t, body, "OK")

		ls.shutdown(t)
		assert.True(t, isClosedChan(dones[cycle]), "Done of cycle %d", cycle)
		assert.True(t, portIsRefused(ls.port))

		// between the two servers Ready must be the channel of the next one, never the closed
		// one of the server that just ended
		assert.False(t, isClosedChan(Ready()), "Ready after cycle %d must not be stale", cycle)
	}

	assert.NotEqual(t, readies[0], readies[1], "each server has its own Ready channel")
	assert.True(t, isClosedChan(readies[0]))
	assert.True(t, isClosedChan(readies[1]))
	assert.NotSame(t, instances[0], instances[1], "the observer of the first server must not be reused")
	assert.NotEqual(t, dones[0], dones[1])
	assert.True(t, resources[0].closed.Load())
	assert.True(t, resources[1].closed.Load())
	assert.Equal(t, int32(0), resources[0].afterClose.Load())

	healthRoutes := 0
	for _, r := range srvRoutes {
		if r.URI == "/health" {
			healthRoutes++
		}
	}
	assert.Equal(t, 1, healthRoutes, "the SDK routes must not pile up with each server")
}

func isClosedChan(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

// TestShutdownBeforeListenNeverBinds covers the signal that arrives before the server attached or
// listened: no listener may open after the shutdown began, and ListenAndServe must return.
func TestShutdownBeforeListenNeverBinds(t *testing.T) {
	isolateServerGlobals(t)

	t.Run("signal handled before ListenAndServe", func(t *testing.T) {
		done := make(chan struct{})
		observer.InitializeWith(func(os.Signal) {
			observer.Notify()
			close(done)
		})
		assert.NoError(t, syscall.Kill(syscall.Getpid(), syscall.SIGHUP))
		select {
		case <-done:
		case <-time.After(loopbackTimeout):
			t.Fatal("the signal was not handled")
		}

		port := freePort(t)
		config.PORT = port
		returned := make(chan struct{})
		go func() {
			defer close(returned)
			ListenAndServe()
		}()

		select {
		case <-returned:
		case <-time.After(loopbackTimeout):
			t.Fatal("ListenAndServe waited for a shutdown that had already happened")
		}
		assert.True(t, portIsFree(t, port), "no listener may open after the shutdown began")
		assert.False(t, isClosedChan(Ready()), "Ready must not close for a server that never listened")
	})

	t.Run("shutdown in progress when ListenAndServe starts", func(t *testing.T) {
		observer.Initialize()
		hold := &blockingCloser{closing: make(chan struct{}), hold: make(chan struct{})}
		observer.Attach(hold)
		notified := make(chan struct{})
		go func() {
			defer close(notified)
			observer.Notify()
		}()
		<-hold.closing

		port := freePort(t)
		config.PORT = port
		returned := make(chan struct{})
		go func() {
			defer close(returned)
			ListenAndServe()
		}()
		select {
		case <-returned:
		case <-time.After(loopbackTimeout):
			t.Fatal("ListenAndServe did not return while the shutdown was in progress")
		}
		assert.True(t, portIsFree(t, port))

		close(hold.hold)
		<-notified
	})

	t.Run("Stop before serve", func(t *testing.T) {
		port := freePort(t)
		config.PORT = port
		o := newRestObserver(&fiberWebServer{})
		o.server.(*fiberWebServer).initialize()
		o.Stop()

		assert.NoError(t, o.serve())
		assert.True(t, portIsFree(t, port), "a stopped instance must not bind")
		assert.False(t, isClosedChan(o.ready))
	})

	t.Run("closeListener before the bind", func(t *testing.T) {
		port := freePort(t)
		config.PORT = port
		f := &fiberWebServer{}
		f.initialize()
		f.closeListener()

		assert.NoError(t, f.listenAndServe(func() { t.Error("onListen must not run") }))
		assert.True(t, portIsFree(t, port))
	})
}

// TestNoSocketOpensAfterCloseListenerReturned interleaves a bind in progress with closeListener
// using the real bind and real sockets. The bind is held by the test through the listen seam:
// closeListener must not return around it, and once it returned a connection is refused.
func TestNoSocketOpensAfterCloseListenerReturned(t *testing.T) {
	isolateServerGlobals(t)
	port := freePort(t)
	config.PORT = port

	f := &fiberWebServer{}
	f.initialize()

	entered, release := make(chan struct{}), make(chan struct{})
	original := listen
	t.Cleanup(func() { listen = original })
	listen = func(network, address string) (net.Listener, error) {
		close(entered)
		<-release

		return net.Listen(network, address)
	}

	onListen := make(chan struct{})
	serveDone := make(chan error, 1)
	go func() { serveDone <- f.listenAndServe(func() { close(onListen) }) }()
	<-entered

	stopped := make(chan struct{})
	go func() {
		f.closeListener()
		close(stopped)
	}()

	// the bind is in progress and not finished: closeListener must be waiting for it
	select {
	case <-stopped:
		t.Fatal("closeListener returned while a bind was in progress: the socket can open after it")
	case <-time.After(300 * time.Millisecond):
	}

	close(release)
	select {
	case <-stopped:
	case <-time.After(loopbackTimeout):
		t.Fatal("closeListener did not return after the bind finished")
	}

	assert.True(t, portIsRefused(port), "no connection may be accepted after closeListener returned")
	select {
	case err := <-serveDone:
		assert.NoError(t, err)
	case <-time.After(loopbackTimeout):
		t.Fatal("listenAndServe did not return after its listener was closed")
	}
	assert.True(t, portIsFree(t, port))
}

// TestBindErrorDoesNotBlockCloseListener pins that a failing bind releases the lock.
func TestBindErrorDoesNotBlockCloseListener(t *testing.T) {
	isolateServerGlobals(t)
	original := listen
	t.Cleanup(func() { listen = original })
	listen = func(string, string) (net.Listener, error) { return nil, errors.New("bind refused") }

	f := &fiberWebServer{}
	f.initialize()
	assert.ErrorContains(t, f.listenAndServe(nil), "bind refused")

	done := make(chan struct{})
	go func() {
		f.closeListener()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(loopbackTimeout):
		t.Fatal("closeListener blocked after a failed bind")
	}
}

type blockingCloser struct {
	closing chan struct{}
	hold    chan struct{}
	once    sync.Once
}

func (b *blockingCloser) Close() {
	b.once.Do(func() { close(b.closing) })
	<-b.hold
}

// TestListenAndServeOnOccupiedPort pins the bind failure: logging.Fatal as before (a panic), Ready
// never closes and nothing waits for the shutdown, which will never come.
func TestListenAndServeOnOccupiedPort(t *testing.T) {
	isolateServerGlobals(t)
	observer.Initialize()

	occupied, err := net.Listen("tcp", ":0")
	require.NoError(t, err)
	defer occupied.Close()
	config.PORT = occupied.Addr().(*net.TCPAddr).Port

	ls := &liveServer{ready: Ready(), returned: make(chan struct{}), panicked: make(chan any, 1)}
	go func() {
		defer close(ls.returned)
		defer func() {
			if r := recover(); r != nil {
				ls.panicked <- r
			}
		}()
		ListenAndServe()
	}()

	ls.waitReturned(t)
	select {
	case r := <-ls.panicked:
		assert.Equal(t, "Error on trying to initialize rest server", r)
	default:
		t.Fatal("a bind failure must end in logging.Fatal")
	}
	assert.False(t, isClosedChan(ls.ready), "Ready must not close when the bind failed")

	// the instance stayed attached to the service: its shutdown must not hang on a listener that
	// never existed
	notified := make(chan struct{})
	go func() {
		defer close(notified)
		observer.Notify()
	}()
	select {
	case <-notified:
	case <-time.After(loopbackTimeout):
		t.Fatal("the shutdown hung on the instance whose bind failed")
	}
}

// TestStopIsSynchronousBeforeResourceClose is the core ordering: when the Stop of the http server
// returns, a new connection is refused by the operating system, and that is true before any
// resource is closed. The probe, attached after the server, runs right after the Stop of the
// server in the same phase.
func TestStopIsSynchronousBeforeResourceClose(t *testing.T) {
	isolateServerGlobals(t)
	config.WAIT_GROUP_TIMEOUT_SECONDS = 5

	log := &eventLog{}
	res := &fakeResource{log: log}
	observer.Initialize()
	observer.Attach(res)

	// a held request keeps the drain phase busy, so a connection accepted after Stop could not be
	// explained by the server having finished already
	entered, release := make(chan struct{}), make(chan struct{})
	AddRoutes([]Route{{URI: "hold", Method: http.MethodGet, Prefix: PublicApi, Function: func(ctx WebContext) {
		close(entered)
		<-release
		ctx.JsonResponse(http.StatusOK, map[string]string{"ok": "1"})
	}}})

	ls := startServer(t)
	observer.Attach(&dialProbe{log: log, port: ls.port, resource: res})

	status := make(chan int, 1)
	go func() {
		r, _ := ls.do(t, http.MethodGet, "/public/hold", nil)
		status <- r.StatusCode
	}()
	<-entered

	notified := make(chan struct{})
	go func() {
		defer close(notified)
		observer.Notify()
	}()

	deadline := time.Now().Add(loopbackTimeout)
	for log.index("dial-refused-after-stop") < 0 && log.index("dial-accepted-after-stop") < 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	assert.GreaterOrEqual(t, log.index("dial-refused-after-stop"), 0,
		"a connection right after the Stop of the server must be refused: %v", log.snapshot())
	assert.Equal(t, -1, log.index("dial-accepted-after-stop"))
	assert.Equal(t, -1, log.index("resource-closed-before-stop-returned"))
	assert.False(t, res.closed.Load(), "the resource must stay open while the held request is in flight")

	close(release)
	assert.Equal(t, http.StatusOK, <-status)
	<-notified
	ls.waitReturned(t)
	assert.Less(t, log.index("dial-refused-after-stop"), log.index("resource-close"))
}

// TestNativeShutdownDrainsApplicationAndSDKRoutesBeforeClosingResources runs the real server over
// loopback with requests in flight on an application route and on the SDK routes (health and
// metrics, held by a Use middleware). They must finish before the resource closes, and no
// application handler may run after it.
func TestNativeShutdownDrainsApplicationAndSDKRoutesBeforeClosingResources(t *testing.T) {
	isolateServerGlobals(t)
	config.WAIT_GROUP_TIMEOUT_SECONDS = 10

	log := &eventLog{}
	res := &fakeResource{log: log}
	observer.Initialize()
	observer.Attach(res)

	release := make(chan struct{})
	entered := make(chan string, 4)
	AddRoutes([]Route{{URI: "slow", Method: http.MethodGet, Prefix: PublicApi, Function: func(ctx WebContext) {
		res.use()
		entered <- "app"
		<-release
		res.use()
		log.add("done:app")
		ctx.JsonResponse(http.StatusOK, map[string]string{"route": "app"})
	}}})
	Use(holdMiddleware{paths: []string{"/health", "/metrics"}, entered: entered, release: release, log: log})

	ls := startServer(t)

	type result struct {
		path   string
		status int
	}
	results := make(chan result, 3)
	for _, p := range []string{"/public/slow", "/health", "/metrics"} {
		go func() {
			r, _ := ls.do(t, http.MethodGet, p, nil)
			results <- result{p, r.StatusCode}
		}()
	}
	for range 3 {
		select {
		case <-entered:
		case <-time.After(loopbackTimeout):
			t.Fatal("the in-flight requests did not reach their handlers")
		}
	}

	notified := make(chan struct{})
	go func() {
		defer close(notified)
		observer.Notify()
	}()

	// the listener is closed while the three requests are still held
	deadline := time.Now().Add(loopbackTimeout)
	for !portIsRefused(ls.port) && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	assert.True(t, portIsRefused(ls.port), "the listener must close in the first phase")
	assert.False(t, res.closed.Load(), "no resource may close while admitted requests are in flight")
	select {
	case <-notified:
		t.Fatal("the shutdown finished with admitted requests in flight")
	case <-time.After(200 * time.Millisecond):
	}

	close(release)
	got := map[string]int{}
	for range 3 {
		select {
		case r := <-results:
			got[r.path] = r.status
		case <-time.After(loopbackTimeout):
			t.Fatal("an in-flight request did not finish")
		}
	}
	assert.Equal(t, map[string]int{"/public/slow": 200, "/health": 200, "/metrics": 200}, got,
		"requests in flight when the signal arrived must be answered, not cut")

	<-notified
	ls.waitReturned(t)

	closeAt := log.index("resource-close")
	require.GreaterOrEqual(t, closeAt, 0)
	for _, e := range []string{"done:app", "done:/health", "done:/metrics"} {
		at := log.index(e)
		assert.GreaterOrEqual(t, at, 0, "missing %s in %v", e, log.snapshot())
		assert.Less(t, at, closeAt, "%s must finish before the resource closes: %v", e, log.snapshot())
	}
	assert.Equal(t, int32(0), res.afterClose.Load(), "no application work may run after the resource closed")
}

// holdMiddleware keeps the requests to some paths admitted (a Use middleware runs inside the
// gate) until the test releases them.
type holdMiddleware struct {
	paths   []string
	entered chan<- string
	release <-chan struct{}
	log     *eventLog
}

func (h holdMiddleware) Apply(ctx WebContext) *MiddlewareError {
	for _, p := range h.paths {
		if ctx.Path() == p {
			h.entered <- p
			<-h.release
			h.log.add("done:" + p)
		}
	}

	return nil
}

// --- child process: the drain timeout log and the log scan need the real stdout of a process ---

const childEnv = "RESTSERVER_LIFECYCLE_CHILD"

// runChild runs one of the Test*Child tests in a separate process and returns its output. The
// logger writes to the stdout of the process, which a test cannot capture in-process.
func runChild(t *testing.T, name string) string {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^"+name+"$", "-test.count=1", "-test.v")
	cmd.Env = append(os.Environ(), childEnv+"=1")
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "child output:\n%s", out)

	return string(out)
}

// TestDrainTimeoutIsLoggedAndShutdownStillReturns runs a server whose request outlives
// WAIT_GROUP_TIMEOUT_SECONDS.
func TestDrainTimeoutIsLoggedAndShutdownStillReturns(t *testing.T) {
	out := runChild(t, "TestDrainTimeoutChild")

	assert.Contains(t, out, "waiting timed out, forcing the shutdown of the running work")
	assert.Contains(t, out, "CHILD_RETURNED")
	assert.NotContains(t, out, "CHILD_NO_TIMEOUT")
}

// TestDrainTimeoutChild is the body of TestDrainTimeoutIsLoggedAndShutdownStillReturns; it does
// nothing unless the parent started it.
func TestDrainTimeoutChild(t *testing.T) {
	if os.Getenv(childEnv) == "" {
		return
	}

	isolateServerGlobals(t)
	config.WAIT_GROUP_TIMEOUT_SECONDS = 1
	observer.Initialize()

	entered := make(chan struct{})
	AddRoutes([]Route{{URI: "slow", Method: http.MethodGet, Prefix: PublicApi, Function: func(ctx WebContext) {
		close(entered)
		time.Sleep(2500 * time.Millisecond)
		ctx.JsonResponse(http.StatusOK, map[string]string{"ok": "1"})
	}}})

	ls := startServer(t)
	go func() { ls.do(t, http.MethodGet, "/public/slow", nil) }()
	<-entered

	start := time.Now()
	ls.shutdown(t)
	if time.Since(start) < 900*time.Millisecond {
		fmt.Println("CHILD_NO_TIMEOUT")
	}
	fmt.Println("CHILD_RETURNED")
}

func linesContaining(out, needle string) []string {
	var lines []string
	for _, l := range strings.Split(out, "\n") {
		if strings.Contains(l, needle) {
			lines = append(lines, l)
		}
	}

	return lines
}
