// Modified in the ezequiel454 fork for forge M3; see FORK.md

package observer

import (
	"context"
	"os"
	"os/signal"
	"slices"
	"sync"
	"syscall"

	"github.com/colibriproject-dev/colibri-sdk-go/pkg/base/logging"
)

const (
	attachBeforeInitialize string = "observer.Initialize must be called before attaching %T"
	drainTimedOut          string = "waiting timed out, forcing the shutdown of the running work"
	initializeRefused      string = "observer.Initialize refused: a graceful shutdown is in progress"
)

var (
	servicesMu sync.Mutex
	// services is the current service. Every Initialize replaces it with a new one, so the
	// channels and the shutdown state never outlive the service they belong to.
	services *service
	// signals is the signal registration of the current service.
	signals *signalRegistration
)

// signalRegistration owns one signal.Notify registration and the goroutine reading it.
type signalRegistration struct {
	ch   chan os.Signal
	quit chan struct{}
}

func (r *signalRegistration) stop() {
	signal.Stop(r.ch)
	close(r.quit)
}

// Initialize starts the observation of system signals to trigger graceful shutdown.
func Initialize() {
	InitializeWith(nil)
}

// InitializeWith is Initialize with the signal handling owned by the caller: handler receives
// every termination signal, from the same signal.Notify registration, so there is no window in
// which a signal falls to the default action. The handler decides when to call Notify. A nil
// handler keeps the default behavior: log and Notify. Signals are delivered one at a time, on
// the registration goroutine, so a handler that blocks delays the next signal.
//
// Every call creates a new service with its own observers, Done channel and shutdown state, and
// stops the signal registration of the previous one. A call made while the current service is
// in the middle of a graceful shutdown is refused and leaves the current service untouched.
func InitializeWith(handler func(os.Signal)) {
	servicesMu.Lock()
	defer servicesMu.Unlock()

	if services != nil && services.inProgress() {
		logging.Warn(context.Background()).Msg(initializeRefused)
		return
	}

	s := &service{observers: make([]Observer, 0), done: make(chan struct{})}
	if handler == nil {
		handler = func(sig os.Signal) {
			logging.Warn(context.Background()).Msgf("notify shutdown: %+v", sig)
			s.notify()
		}
	}

	reg := &signalRegistration{ch: make(chan os.Signal, 1), quit: make(chan struct{})}

	// the new registration is in place before the previous one is stopped, so the process
	// always has a signal.Notify active and a signal in between is never lost to the default
	// action
	signal.Notify(reg.ch, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP, syscall.SIGKILL, os.Interrupt)
	if signals != nil {
		signals.stop()
	}
	signals = reg
	services = s

	go func() {
		for {
			select {
			case sig := <-reg.ch:
				handler(sig)
			case <-reg.quit:
				return
			}
		}
	}()
}

// Done returns a channel closed when the graceful shutdown of the current service has finished
// all its phases. It is bound to the service current at the time of the call: a later Initialize
// does not close it, and it is never a channel of a previous service. Without a service it
// returns a nil channel, which blocks forever.
func Done() <-chan struct{} {
	servicesMu.Lock()
	defer servicesMu.Unlock()

	if services == nil {
		return nil
	}

	return services.done
}

// Notifying reports whether the graceful shutdown of the current service has started.
func Notifying() bool {
	servicesMu.Lock()
	s := services
	servicesMu.Unlock()

	return s != nil && s.isNotifying()
}

// Attach adds an observer to the notification list for graceful shutdown.
func Attach(o Observer) {
	servicesMu.Lock()
	defer servicesMu.Unlock()

	if services == nil {
		logging.Fatal(context.Background()).Msgf(attachBeforeInitialize, o)
		return
	}

	services.attach(o)
}

// TryAttach is Attach for a component that must not start when the graceful shutdown has
// already begun: the observer is attached only if the current service has not started
// notifying, atomically with the snapshot the shutdown takes, so an attached observer is
// always notified. It also returns the Done channel of the service it was attached to, which
// stays valid even if a later Initialize replaces the service.
func TryAttach(o Observer) (<-chan struct{}, bool) {
	servicesMu.Lock()
	defer servicesMu.Unlock()

	if services == nil {
		logging.Fatal(context.Background()).Msgf(attachBeforeInitialize, o)
		return nil, false
	}

	if !services.attachUnlessNotifying(o) {
		return services.done, false
	}

	return services.done, true
}

// Notify runs the whole graceful shutdown, the same way a termination signal does: every
// attached observer is stopped, then the running work is drained, then everything is closed.
// It is idempotent per service: the phases run once, and every other caller, concurrent or
// later, waits for the same Done.
func Notify() {
	servicesMu.Lock()
	s := services
	servicesMu.Unlock()

	if s != nil {
		s.notify()
	}
}

type service struct {
	mu        sync.Mutex
	observers []Observer
	notifying bool
	done      chan struct{}
}

func (s *service) attach(observer Observer) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.observers = append(s.observers, observer)
}

func (s *service) attachUnlessNotifying(observer Observer) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.notifying {
		return false
	}

	s.observers = append(s.observers, observer)

	return true
}

func (s *service) isNotifying() bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.notifying
}

// inProgress reports whether the shutdown has started and not yet finished.
func (s *service) inProgress() bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.notifying {
		return false
	}

	select {
	case <-s.done:
		return false
	default:
		return true
	}
}

// notify runs the graceful shutdown as a pipeline of phases, once per service. The first caller
// runs it; any other caller waits for done. A caller must not be an observer of the shutdown
// itself, which would wait for its own completion.
//
// Every module goes through the same phase before any of them moves to the next one, which is
// what keeps a module from closing a resource another one still needs to drain through.
//
// It runs on a copy of the observers because one of them may attach another while it is
// being notified.
func (s *service) notify() {
	observers, first := s.begin()
	if !first {
		<-s.done

		return
	}

	// done is closed even if an observer panics, so nobody waits forever on a shutdown that
	// is not going to finish
	defer close(s.done)

	// phase 1: block new work from entering. Nothing waits here, so the components still
	// draining keep every resource they need available.
	for _, o := range observers {
		if stopper, ok := o.(Stopper); ok {
			stopper.Stop()
		}
	}

	// phase 2: drain the work in flight, once for the whole application. A module waiting on
	// its own would spend another timeout on work this wait already covers.
	if WaitRunningTimeout() {
		logging.Warn(context.Background()).Msg(drainTimedOut)
	}

	// phase 3: release the resources, now that nothing is left using them
	for _, o := range observers {
		o.Close()
	}
}

// begin marks the shutdown as started and takes the snapshot in the same critical section, so
// an observer is either in the snapshot or refused by attachUnlessNotifying. It reports
// whether this caller is the one running the phases.
func (s *service) begin() ([]Observer, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.notifying {
		return nil, false
	}

	s.notifying = true

	return slices.Clone(s.observers), true
}
