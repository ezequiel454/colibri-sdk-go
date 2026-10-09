// Modified in the ezequiel454 fork for forge M3; see FORK.md

package restserver

import (
	"context"
	"sync"
	"time"

	"github.com/colibriproject-dev/colibri-sdk-go/pkg/base/logging"
	"github.com/colibriproject-dev/colibri-sdk-go/pkg/base/observer"
)

// restObserver is the state of one ListenAndServe: the server, the admission gate and the
// channels of its lifecycle. Each call creates its own, so nothing a previous server left
// (a closed channel, a stopping flag) is ever seen by the next one.
type restObserver struct {
	server Server

	// mu guards the admission gate and the lifecycle flags: admitting a request and stopping
	// the server are mutually exclusive, so once Stop has returned nothing new is counted as
	// running work.
	mu        sync.Mutex
	stopping  bool
	listening bool

	ready     chan struct{}
	readyOnce sync.Once
	// listenDone is closed when the listener of this instance is closed and its serve call has
	// returned (or never started).
	listenDone chan struct{}
	// shutdownDone is closed when the shutdown of the http server, started by Stop, finished.
	shutdownDone chan struct{}
	stopOnce     sync.Once
}

func newRestObserver(server Server) *restObserver {
	return &restObserver{
		server:       server,
		ready:        make(chan struct{}),
		listenDone:   make(chan struct{}),
		shutdownDone: make(chan struct{}),
	}
}

func (o *restObserver) markReady() {
	o.readyOnce.Do(func() { close(o.ready) })
}

// admit counts one request as running work unless the server is stopping.
func (o *restObserver) admit() bool {
	o.mu.Lock()
	defer o.mu.Unlock()

	if o.stopping {
		return false
	}

	observer.AddRunning()

	return true
}

func (o *restObserver) isStopping() bool {
	o.mu.Lock()
	defer o.mu.Unlock()

	return o.stopping
}

// serve binds the listener and serves until it is closed. It never starts once Stop was called.
func (o *restObserver) serve() error {
	o.mu.Lock()
	if o.stopping {
		o.mu.Unlock()
		close(o.listenDone)

		return nil
	}
	o.listening = true
	o.mu.Unlock()

	defer close(o.listenDone)

	return o.server.listenAndServe(o.markReady)
}

// Stop runs in the first phase of the graceful shutdown. It closes the admission gate, so a
// request that arrives afterwards is refused, closes the listener and returns only when the
// serving of this instance has ended, which means a new connection is refused by the operating
// system. It does not wait for the requests already admitted: they keep running, are drained
// in the second phase and finish before any resource is closed. The wait is bounded by the
// drain budget, so a listener that does not return cannot hold the shutdown.
func (o *restObserver) Stop() {
	o.mu.Lock()
	o.stopping = true
	listening := o.listening
	o.mu.Unlock()

	o.stopOnce.Do(func() {
		ctx := context.Background()

		o.server.closeListener()
		logging.Info(ctx).Msg("http server stopped accepting")

		// the http shutdown waits for the open connections, so it runs on its own goroutine;
		// Close waits for it
		go func() {
			defer close(o.shutdownDone)

			if err := o.server.shutdown(); err != nil {
				logging.Error(ctx).Err(err).Msg("error when closing http server")
			}
		}()

		if !listening {
			return
		}

		timer := time.NewTimer(observer.DrainBudget(1, 1))
		defer timer.Stop()

		select {
		case <-o.listenDone:
		case <-timer.C:
			logging.Warn(ctx).Msg("waiting timed out for the http listener to close")
		}
	})
}

// Close runs in the closing phase of the graceful shutdown, after the work started by the
// requests already accepted has been drained and the resources were closed. It waits for the
// shutdown of the http server, which lets the connections still open finish writing, bounded by
// the timeout of that shutdown.
func (o *restObserver) Close() {
	ctx := context.Background()

	logging.Info(ctx).Msg("closing http server")
	o.Stop()
	<-o.shutdownDone
	logging.Info(ctx).Msg("http server closed")
}
