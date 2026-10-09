// Modified in the ezequiel454 fork for forge M3; see FORK.md

package colibri_otel

import (
	"context"
	"strings"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"

	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// sanitizingExporter is the last step before a span leaves the process. The instrumentation
// libraries (fiber, otelhttp, otelsql) put the request URL, its query and path, free error
// text and header values on spans; none of that is under the control of the service, so it is
// removed here, for every span and whatever produced it:
//
//   - attributes that carry a URL, a path, a query or a statement text are dropped (the route
//     template http.route, the method, the status and the server address stay);
//   - attributes that carry free error text (db.sql.error, error.message, ...; error.type stays);
//   - attributes whose key names a credential (authorization, cookie, token, password, secret,
//     api key, dsn, credential, header) are dropped;
//   - the status description and the message and stack of exception events are dropped (the
//     status code and exception.type stay).
type sanitizingExporter struct {
	next sdktrace.SpanExporter
}

var droppedAttributeKeys = map[string]struct{}{
	"url.full": {}, "url.path": {}, "url.query": {}, "http.url": {}, "http.target": {}, "http.route.raw": {},
	"db.statement": {}, "db.query.text": {}, "db.connection_string": {}, "db.user": {},
	"exception.message": {}, "exception.stacktrace": {},
}

var credentialKeyParts = []string{"authorization", "cookie", "token", "password", "passwd", "secret", "apikey", "api_key", "dsn", "header", "credential"}

func sensitiveKey(k string) bool {
	if _, ok := droppedAttributeKeys[k]; ok {
		return true
	}
	low := strings.ToLower(k)
	// free error text (otelsql puts the driver error, which quotes the offending value, on
	// db.sql.error): only the low cardinality error.type of the semantic conventions stays
	if k != "error.type" && (strings.HasSuffix(low, ".error") || strings.Contains(low, "error.message") || strings.HasSuffix(low, ".error_message")) {
		return true
	}
	for _, p := range credentialKeyParts {
		if strings.Contains(low, p) {
			return true
		}
	}
	return false
}

func cleanAttributes(in []attribute.KeyValue) []attribute.KeyValue {
	out := make([]attribute.KeyValue, 0, len(in))
	for _, kv := range in {
		if !sensitiveKey(string(kv.Key)) {
			out = append(out, kv)
		}
	}
	return out
}

func (e sanitizingExporter) ExportSpans(ctx context.Context, spans []sdktrace.ReadOnlySpan) error {
	stubs := tracetest.SpanStubsFromReadOnlySpans(spans)
	for i := range stubs {
		stubs[i].Attributes = cleanAttributes(stubs[i].Attributes)
		stubs[i].Status.Description = ""
		for j := range stubs[i].Events {
			stubs[i].Events[j].Attributes = cleanAttributes(stubs[i].Events[j].Attributes)
		}
		for j := range stubs[i].Links {
			stubs[i].Links[j].Attributes = cleanAttributes(stubs[i].Links[j].Attributes)
		}
	}
	return e.next.ExportSpans(ctx, stubs.Snapshots())
}

func (e sanitizingExporter) Shutdown(ctx context.Context) error { return e.next.Shutdown(ctx) }

// sanitizingMetricView drops, from every instrument, the attributes the span exporter drops.
func sanitizingMetricView() sdkmetric.View {
	return sdkmetric.NewView(
		sdkmetric.Instrument{Name: "*"},
		sdkmetric.Stream{AttributeFilter: func(kv attribute.KeyValue) bool { return !sensitiveKey(string(kv.Key)) }},
	)
}
