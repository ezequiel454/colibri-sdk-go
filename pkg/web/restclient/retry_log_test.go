// Modified in the ezequiel454 fork for forge M3; see FORK.md

package restclient

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/colibriproject-dev/colibri-sdk-go/pkg/base/config"
	"github.com/colibriproject-dev/colibri-sdk-go/pkg/base/logging"
	"github.com/colibriproject-dev/colibri-sdk-go/pkg/base/monitoring"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The retry warning carries the attempt, the method and the status, and nothing the peer, the
// URL or the transport put in: neither the path, the query or the user info of the URL, nor the
// error body of the peer, nor the text of a transport error (which quotes the URL).
func TestRetryWarningNeverLogsURLsOrBodies(t *testing.T) {
	var buf bytes.Buffer
	env := config.ENVIRONMENT
	config.ENVIRONMENT = config.ENVIRONMENT_PRODUCTION
	logging.InitializeWith(&buf)
	monitoring.Initialize()
	t.Cleanup(func() { config.ENVIRONMENT = env; logging.InitializeWith(os.Stdout) })

	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
		_, _ = w.Write([]byte(`{"message":"SENTINEL_PEER_BODY"}`))
	}))
	defer peer.Close()

	c := NewRestClient(&RestClientConfig{Name: "retry-log", BaseURL: peer.URL, Timeout: 2})
	c.retries = 1 // NewRestClient does not copy RestClientConfig.Retries (v0.2.5); the field is set directly

	resp := Request[map[string]any, map[string]string]{
		Ctx: context.Background(), Client: c, HttpMethod: http.MethodGet, Path: "/SENTINEL_PATH?token=SENTINEL_QUERY",
	}.Call()
	require.Equal(t, http.StatusBadGateway, resp.StatusCode())

	// a transport failure: the error text quotes the URL
	dead := NewRestClient(&RestClientConfig{Name: "retry-log-dead", BaseURL: "http://user:SENTINEL_PASS@127.0.0.1:1", Timeout: 1})
	dead.retries = 1
	Request[map[string]any, map[string]string]{Ctx: context.Background(), Client: dead, HttpMethod: http.MethodGet, Path: "/SENTINEL_PATH?token=SENTINEL_QUERY"}.Call()

	out := buf.String()
	assert.Equal(t, 4, strings.Count(out, "call failed, retrying"), out)
	assert.NotContains(t, out, "SENTINEL")
	assert.NotContains(t, out, "127.0.0.1")
	assert.NotContains(t, out, "http://")
	assert.Contains(t, out, `"errorType"`)
}
