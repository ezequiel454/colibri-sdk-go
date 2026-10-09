// Modified in the ezequiel454 fork for forge M3; see FORK.md

package restserver

import (
	"testing"

	"github.com/colibriproject-dev/colibri-sdk-go/pkg/base/config"
	"github.com/colibriproject-dev/colibri-sdk-go/pkg/base/logging"
	"github.com/colibriproject-dev/colibri-sdk-go/pkg/base/observer"
	"github.com/gofiber/fiber/v3"

	"github.com/stretchr/testify/assert"
)

func isClosed(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

func TestCloseServer(t *testing.T) {
	logging.Initialize()

	o := newRestObserver(&fiberWebServer{srv: &fiber.App{}})

	o.Close()
	assert.True(t, isClosed(o.shutdownDone))
	assert.True(t, o.isStopping())
}

func TestCloseServerWithTimeout(t *testing.T) {
	logging.Initialize()
	config.WAIT_GROUP_TIMEOUT_SECONDS = 1
	observer.GetWaitGroup().Add(1)
	defer observer.GetWaitGroup().Done()

	o := newRestObserver(&fiberWebServer{srv: &fiber.App{}})

	o.Close()
	assert.True(t, isClosed(o.shutdownDone))
}

// A Stop that finds nothing listening must not wait for a listener that will never start.
func TestStopWithoutListenerDoesNotWait(t *testing.T) {
	logging.Initialize()

	o := newRestObserver(&fiberWebServer{srv: &fiber.App{}})
	o.Stop()
	o.Stop()

	assert.True(t, o.isStopping())
	assert.False(t, o.admit(), "the gate must be closed once Stop has run")
	assert.NoError(t, o.serve(), "a stopped instance must not start serving")
	assert.True(t, isClosed(o.listenDone))
}
