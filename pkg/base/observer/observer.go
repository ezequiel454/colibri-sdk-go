// Modified in the ezequiel454 fork for forge M3; see FORK.md

package observer

// Observer is the default contract for components that need to perform a graceful shutdown.
type Observer interface {
	// Close performs the cleanup or shutdown logic for the component.
	Close()
}

// Stopper blocks new work from entering the application. It is called in the first phase of
// the graceful shutdown. It must not wait for the work in flight to finish: draining belongs to
// phase 2. It may wait, boundedly (for instance by DrainBudget), for the component's own
// acceptance of new work to end, such as a listener being closed, so that once Stop has returned
// nothing new can enter that component.
//
// It is optional and additive: an observer may implement both interfaces, and is attached
// once. An observer that only implements Observer is closed in the closing phase as always.
type Stopper interface {
	// Stop blocks new work from entering the component, without waiting for the work already
	// in flight. It may wait, with a bound, until the component no longer accepts new work.
	Stop()
}
