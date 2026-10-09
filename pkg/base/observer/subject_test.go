// Modified in the ezequiel454 fork for forge M3; see FORK.md

package observer

import (
	"fmt"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type observerTest struct {
	closed bool
}

func (o *observerTest) Close() {
	o.closed = true
	fmt.Println("close observer")
}

// shutdownSequence collects the shutdown events in the order they happened.
type shutdownSequence struct {
	mu     sync.Mutex
	events []string
}

func (s *shutdownSequence) record(event string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.events = append(s.events, event)
}

func (s *shutdownSequence) recorded() []string {
	s.mu.Lock()
	defer s.mu.Unlock()

	return append([]string(nil), s.events...)
}

// phasedObserverTest implements both contracts, so it records which phase it went through and
// when, the way a module that stops in the first phase and closes in the last one does.
type phasedObserverTest struct {
	name string
	seq  *shutdownSequence
	// stopped is released after the last observer has been stopped, which is what releases the
	// work the drain phase waits for
	stopped *sync.WaitGroup
}

func (o phasedObserverTest) Stop() {
	o.seq.record("stop:" + o.name)
	o.stopped.Done()
}

func (o phasedObserverTest) Close() {
	o.seq.record("close:" + o.name)
}

// closeOnlyObserverTest implements Observer alone, the way a component with nothing to block
// does. The pipeline must still close it.
type closeOnlyObserverTest struct {
	seq  *shutdownSequence
	name string
}

func (o closeOnlyObserverTest) Close() {
	o.seq.record("close:" + o.name)
}

// closingCounterObserverTest only counts, so the time the shutdown takes is the time the
// pipeline itself spent waiting.
type closingCounterObserverTest struct {
	closed *atomic.Int32
}

func (o closingCounterObserverTest) Close() {
	o.closed.Add(1)
}

func TestSubjectNotify(t *testing.T) {
	o := &observerTest{closed: false}
	Initialize()
	resetRunning(t)
	Attach(o)

	assert.False(t, o.closed)
	Notify()
	assert.True(t, o.closed)
}

func TestSubjectNotifyRunsTheShutdownPipeline(t *testing.T) {
	t.Run("Should stop every observer, then drain, then close every observer", func(t *testing.T) {
		const observers = 3

		withDrainTimeout(t, 5)
		Initialize()
		resetRunning(t)

		seq := &shutdownSequence{}
		var stopped sync.WaitGroup
		stopped.Add(observers)

		// work in flight, released only once every observer has been stopped: an observer
		// closed before the release means the drain phase was skipped or merged into the
		// closing one
		AddRunning()
		go func() {
			stopped.Wait()
			seq.record("drain")
			DoneRunning()
		}()

		for i := range observers {
			Attach(phasedObserverTest{name: fmt.Sprintf("module%d", i), seq: seq, stopped: &stopped})
		}

		Notify()

		assert.Equal(t,
			[]string{
				"stop:module0", "stop:module1", "stop:module2",
				"drain",
				"close:module0", "close:module1", "close:module2",
			},
			seq.recorded(),
			"the shutdown must stop everything, then drain, then close everything",
		)
	})

	t.Run("Should close an observer that does not implement Stopper", func(t *testing.T) {
		Initialize()
		resetRunning(t)

		seq := &shutdownSequence{}
		Attach(closeOnlyObserverTest{seq: seq, name: "closeOnly"})

		Notify()

		assert.Equal(t, []string{"close:closeOnly"}, seq.recorded(),
			"an observer implementing only Close must still be closed by the pipeline")
	})

	t.Run("Should wait for the running work only once", func(t *testing.T) {
		const observers = 4

		withDrainTimeout(t, 1)
		Initialize()
		resetRunning(t)

		// work nobody ever finishes, so the shutdown reaches its bound instead of returning
		// early and every extra wait costs another whole timeout
		AddRunning()
		t.Cleanup(DoneRunning)

		var closed atomic.Int32
		for range observers {
			Attach(closingCounterObserverTest{closed: &closed})
		}

		start := time.Now()
		Notify()
		elapsed := time.Since(start)

		assert.Equal(t, int32(observers), closed.Load(), "every observer should have been closed")
		assert.GreaterOrEqual(t, elapsed, 900*time.Millisecond, "the shutdown did not wait for the drain")
		assert.Less(t, elapsed, 2*time.Second,
			"the shutdown waited for the running work more than once")
	})
}

// lifecycleObserverTest counts each phase it goes through and can hold the closing phase, so a
// test can observe a shutdown while it is in progress.
type lifecycleObserverTest struct {
	stops  atomic.Int32
	closes atomic.Int32
	// hold, when not nil, blocks Close until it is closed
	hold chan struct{}
	// closing is closed when Close starts
	closing chan struct{}
	once    sync.Once
}

func (o *lifecycleObserverTest) Stop() { o.stops.Add(1) }

func (o *lifecycleObserverTest) Close() {
	o.closes.Add(1)
	if o.closing != nil {
		o.once.Do(func() { close(o.closing) })
	}

	if o.hold != nil {
		<-o.hold
	}
}

func isClosedChan(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

func waitChan(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()

	select {
	case <-ch:
	case <-time.After(10 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
	}
}

// TestInitializeTwiceBuildsIndependentServices is the restart case: a second Initialize must not
// see the Done channel, the observers or the signal registration of the first one.
func TestInitializeTwiceBuildsIndependentServices(t *testing.T) {
	resetRunning(t)

	Initialize()
	firstReg := signals
	firstDone := Done()
	first := &lifecycleObserverTest{}
	Attach(first)

	assert.False(t, Notifying())
	Notify()
	assert.True(t, Notifying())
	assert.True(t, isClosedChan(firstDone), "the first Done must close when its shutdown ends")
	assert.Equal(t, int32(1), first.closes.Load())

	Initialize()
	secondDone := Done()
	assert.NotEqual(t, firstDone, secondDone, "the second service must have a Done channel of its own")
	assert.False(t, isClosedChan(secondDone), "the second Done must not come already closed")
	assert.False(t, Notifying(), "the second service must not inherit the shutdown state of the first")
	assert.True(t, isClosedChan(firstReg.quit), "the signal registration of the first service must be stopped")

	second := &lifecycleObserverTest{}
	Attach(second)
	Notify()

	assert.True(t, isClosedChan(secondDone))
	assert.Equal(t, int32(1), second.stops.Load())
	assert.Equal(t, int32(1), second.closes.Load())
	assert.Equal(t, int32(1), first.stops.Load(), "the observers of the first service must not be notified again")
	assert.Equal(t, int32(1), first.closes.Load(), "the observers of the first service must not be notified again")
}

// TestInitializeDoesNotLeakSignalGoroutines pins that replacing the service stops the previous
// registration goroutine instead of leaving one behind per Initialize.
func TestInitializeDoesNotLeakSignalGoroutines(t *testing.T) {
	Initialize()
	time.Sleep(50 * time.Millisecond)
	before := runtime.NumGoroutine()

	for range 50 {
		Initialize()
	}

	deadline := time.Now().Add(5 * time.Second)
	for runtime.NumGoroutine() > before+2 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}

	assert.LessOrEqual(t, runtime.NumGoroutine(), before+2, "each Initialize must retire the goroutine of the previous one")
}

// TestReplacedRegistrationReceivesNoSignal sends a real signal after a second Initialize: only the
// registration of the current service may be listening.
func TestReplacedRegistrationReceivesNoSignal(t *testing.T) {
	Initialize()
	oldReg := signals

	var handled atomic.Int32
	got := make(chan struct{}, 4)
	InitializeWith(func(os.Signal) {
		handled.Add(1)
		got <- struct{}{}
	})

	assert.NoError(t, syscall.Kill(syscall.Getpid(), syscall.SIGHUP))
	waitChan(t, got, "the signal to reach the handler of the current service")

	select {
	case <-oldReg.ch:
		t.Fatal("the stopped registration still received a signal")
	default:
	}
	assert.Equal(t, int32(1), handled.Load())
}

// TestNotifyIsIdempotentAndConcurrentCallersWait runs the shutdown from a signal, an explicit
// Notify and several concurrent callers: the phases run once and nobody returns before they end.
func TestNotifyIsIdempotentAndConcurrentCallersWait(t *testing.T) {
	resetRunning(t)

	Initialize()
	o := &lifecycleObserverTest{hold: make(chan struct{}), closing: make(chan struct{})}
	Attach(o)
	done := Done()

	// the first signal starts the shutdown, which then sits in the closing phase
	assert.NoError(t, syscall.Kill(syscall.Getpid(), syscall.SIGHUP))
	waitChan(t, o.closing, "the shutdown to reach the closing phase")

	// a second signal and explicit callers meanwhile
	assert.NoError(t, syscall.Kill(syscall.Getpid(), syscall.SIGHUP))

	const callers = 5
	returned := make(chan struct{}, callers)
	for range callers {
		go func() {
			Notify()
			returned <- struct{}{}
		}()
	}

	select {
	case <-returned:
		t.Fatal("a repeated Notify returned before the shutdown it joined had finished")
	case <-time.After(200 * time.Millisecond):
	}

	close(o.hold)
	waitChan(t, done, "Done")
	for range callers {
		select {
		case <-returned:
		case <-time.After(10 * time.Second):
			t.Fatal("a repeated Notify did not return after the shutdown finished")
		}
	}

	assert.Equal(t, int32(1), o.stops.Load(), "the stopping phase must run once")
	assert.Equal(t, int32(1), o.closes.Load(), "the closing phase must run once")
}

// TestNotifyReleasesWaitersWhenAnObserverPanics pins that Done still closes, so nobody waits for
// a shutdown that is never going to finish.
func TestNotifyReleasesWaitersWhenAnObserverPanics(t *testing.T) {
	resetRunning(t)

	Initialize()
	Attach(panickingObserverTest{})
	done := Done()

	assert.Panics(t, Notify)
	assert.True(t, isClosedChan(done))

	assert.NotPanics(t, Notify, "a later Notify joins the finished shutdown instead of running it again")
}

type panickingObserverTest struct{}

func (panickingObserverTest) Close() { panic("observer failed") }

// TestInitializeDuringShutdownIsRefused pins that a service in the middle of its shutdown cannot
// be replaced: the current state, Done channel and signal registration stay as they were.
func TestInitializeDuringShutdownIsRefused(t *testing.T) {
	resetRunning(t)

	Initialize()
	o := &lifecycleObserverTest{hold: make(chan struct{}), closing: make(chan struct{})}
	Attach(o)
	done := Done()
	reg := signals

	notified := make(chan struct{})
	go func() {
		defer close(notified)
		Notify()
	}()
	waitChan(t, o.closing, "the shutdown to reach the closing phase")

	Initialize()
	InitializeWith(func(os.Signal) {})

	assert.Equal(t, done, Done(), "the service in shutdown must stay the current one")
	assert.True(t, Notifying())
	assert.Same(t, reg, signals, "the signal registration of the service in shutdown must stay")
	assert.False(t, isClosedChan(reg.quit))

	close(o.hold)
	waitChan(t, notified, "Notify")
	assert.True(t, isClosedChan(done))

	Initialize()
	assert.NotEqual(t, done, Done(), "after the shutdown ended a new service is accepted")
}

// TestTryAttachRefusesOnceTheShutdownStarted pins the atomicity the http server relies on to not
// open a listener after the shutdown began: an observer is attached, and so notified, or refused.
func TestTryAttachRefusesOnceTheShutdownStarted(t *testing.T) {
	resetRunning(t)

	Initialize()
	done := Done()

	before := &lifecycleObserverTest{}
	got, ok := TryAttach(before)
	assert.True(t, ok)
	assert.Equal(t, done, got)

	Notify()

	after := &lifecycleObserverTest{}
	got, ok = TryAttach(after)
	assert.False(t, ok, "a shutdown that already started cannot take a new observer")
	assert.Equal(t, done, got)
	assert.Equal(t, int32(1), before.closes.Load())
	assert.Equal(t, int32(0), after.closes.Load())
}
