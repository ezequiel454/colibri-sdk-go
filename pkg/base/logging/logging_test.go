// Modified in the ezequiel454 fork for forge M3; see FORK.md

package logging

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"os"
	"testing"

	"github.com/colibriproject-dev/colibri-sdk-go/pkg/base/config"
	"github.com/stretchr/testify/assert"
)

func TestLogging(t *testing.T) {
	var buf bytes.Buffer
	logOutput = &buf

	config.ENVIRONMENT = config.ENVIRONMENT_TEST
	ctx := context.Background()
	ctx = InjectCorrelationIDInContext(ctx, "test-correlation-id")

	cleanupFunc := func() {
		os.Unsetenv("LOG_LEVEL")
		logger = nil
	}

	t.Run("Should initialize logger with default level when LOG_LEVEL is not set", func(t *testing.T) {
		defer cleanupFunc()
		os.Unsetenv("LOG_LEVEL")

		Initialize()

		assert.NotNil(t, logger)
	})

	t.Run("Should initialize logger with specified level when LOG_LEVEL is set", func(t *testing.T) {
		defer cleanupFunc()
		os.Setenv("LOG_LEVEL", "debug")

		Initialize()

		assert.NotNil(t, logger)
		assert.Equal(t, "debug", logLevel)
	})

	t.Run("Should initialize logger with specified level when LOG_LEVEL is set in upper case", func(t *testing.T) {
		defer cleanupFunc()
		os.Setenv(config.ENV_LOG_LEVEL, "DEBUG")

		Initialize()

		assert.NotNil(t, logger)
		assert.Equal(t, "debug", logLevel)
	})

	t.Run("Should initialize logger with debug level and show debug messages", func(t *testing.T) {
		defer cleanupFunc()
		buf.Reset()
		os.Setenv(config.ENV_LOG_LEVEL, "debug")
		Initialize()

		Debug(ctx).Msg("debug message test")

		output := buf.String()
		assert.Contains(t, output, "debug message test")
		assert.Contains(t, output, "level=DEBUG")
	})

	t.Run("Should create logging instance with correlation ID when context has correlation ID", func(t *testing.T) {
		defer cleanupFunc()
		log := Info(ctx)

		assert.Equal(t, "test-correlation-id", log.correlationID)
	})

	t.Run("Should add parameters to logging instance when AddParam is called", func(t *testing.T) {
		defer cleanupFunc()
		log := Info(ctx).AddParam("testKey", "testValue")

		assert.Equal(t, "testValue", log.params["testKey"])
	})

	t.Run("Should add error to logging instance when Err is called", func(t *testing.T) {
		defer cleanupFunc()
		testErr := errors.New("test error")

		log := Error(ctx).Err(testErr)

		assert.Equal(t, testErr, log.err)
	})

	t.Run("Should format message correctly when Msg is called", func(t *testing.T) {
		defer cleanupFunc()
		Initialize()

		Info(ctx).AddParam("test", "value").Msg("Test message 123")

		output := buf.String()
		assert.Contains(t, output, "Test message 123")
		assert.Contains(t, output, "test=value")
	})

	t.Run("Should format message correctly when Msgf is called", func(t *testing.T) {
		defer cleanupFunc()
		Initialize()

		Info(ctx).AddParam("test", "value").Msgf("Test %s %d", "message", 456)

		output := buf.String()
		assert.Contains(t, output, "Test message 456")
		assert.Contains(t, output, "test=value")
	})

	t.Run("Should panic when Fatal is called with Msg", func(t *testing.T) {
		defer cleanupFunc()
		assert.Panics(t, func() {
			Fatal(ctx).Msg("fatal message")
		})
	})

	t.Run("Should return correct log level when parseLevel is called", func(t *testing.T) {
		defer cleanupFunc()
		testCases := []struct {
			input    string
			expected slog.Level
		}{
			{"error", slog.LevelError},
			{"warn", slog.LevelWarn},
			{"warning", slog.LevelWarn},
			{"debug", slog.LevelDebug},
			{"info", slog.LevelInfo},
			{"invalid", slog.LevelInfo},
		}

		for _, tc := range testCases {
			t.Run("Should return "+tc.input+" level", func(t *testing.T) {
				result := parseLevel(tc.input)

				assert.EqualValues(t, tc.expected, result)
			})
		}
	})

	t.Run("Should clean function name correctly when cleanFunctionName is called", func(t *testing.T) {
		defer cleanupFunc()
		testCases := []struct {
			input    string
			expected string
		}{
			{"github.com/colibriproject-dev/colibri-sdk-go/pkg/base/logging.TestLogging", "logging.TestLogging"},
			{"(*Logging).AddParam", "Logging.AddParam"},
			{"simpleFunction", "simpleFunction"},
		}

		for _, tc := range testCases {
			t.Run("Should clean "+tc.input, func(t *testing.T) {
				result := cleanFunctionName(tc.input)

				assert.EqualValues(t, tc.expected, result)
			})
		}
	})
}

type stateErr struct{ code string }

func (e stateErr) Error() string    { return "SENTINEL_driver_text host=db.internal password=SENTINEL" }
func (e stateErr) SQLState() string { return e.code }

func TestErrTypeLogsNeverTheMessage(t *testing.T) {
	var buf bytes.Buffer
	defer func() { logger = nil; logOutput = os.Stdout }()
	config.ENVIRONMENT = config.ENVIRONMENT_PRODUCTION
	InitializeWith(&buf)

	Error(context.Background()).ErrType(stateErr{"28P01"}).Msg("boom")
	Error(context.Background()).ErrType(errors.New("SENTINEL_plain")).Msg("plain")
	Error(context.Background()).ErrType(stateErr{"bad;state"}).Msg("badstate")
	Error(context.Background()).ErrType(nil).Msg("nil")

	out := buf.String()
	assert.NotContains(t, out, "SENTINEL")
	assert.NotContains(t, out, "password")
	assert.Contains(t, out, `"errorType":"logging.stateErr"`)
	assert.Contains(t, out, `"sqlState":"28P01"`)
	assert.NotContains(t, out, "bad;state")
	assert.Contains(t, out, `"errorType":"*errors.errorString"`)
}

func TestInitializeWithReplacesTheLogger(t *testing.T) {
	var a, b bytes.Buffer
	defer func() { logger = nil; logOutput = os.Stdout }()
	config.ENVIRONMENT = config.ENVIRONMENT_PRODUCTION
	InitializeWith(&a)
	Info(context.Background()).Msg("first")
	InitializeWith(&b)
	Info(context.Background()).Msg("second")
	assert.Contains(t, a.String(), "first")
	assert.NotContains(t, a.String(), "second")
	assert.Contains(t, b.String(), "second")
}
