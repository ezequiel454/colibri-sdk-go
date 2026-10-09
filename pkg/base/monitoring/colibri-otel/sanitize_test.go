// Modified in the ezequiel454 fork for forge M3; see FORK.md

package colibri_otel

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/exemplar"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

type captureExporter struct{ spans []sdktrace.ReadOnlySpan }

func (c *captureExporter) ExportSpans(_ context.Context, s []sdktrace.ReadOnlySpan) error {
	c.spans = append(c.spans, s...)
	return nil
}
func (c *captureExporter) Shutdown(context.Context) error { return nil }

func TestSanitizingExporterDropsURLsCredentialsAndErrorText(t *testing.T) {
	capture := &captureExporter{}
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sdktrace.NewSimpleSpanProcessor(sanitizingExporter{next: capture})))
	_, span := tp.Tracer("t").Start(context.Background(), "POST /v1/x")
	span.SetAttributes(
		attribute.String("url.full", "http://h/p?token=SENTINEL"),
		attribute.String("url.query", "token=SENTINEL"),
		attribute.String("url.path", "/SENTINEL"),
		attribute.String("http.request.header.authorization", "Bearer SENTINEL"),
		attribute.String("db.query.text", "SELECT SENTINEL"),
		attribute.String("db.sql.error", "pq: invalid input syntax for type uuid: \"SENTINEL\""),
		attribute.String("error.message", "SENTINEL"),
		attribute.String("error.type", "502"),
		attribute.String("http.route", "/v1/x"),
		attribute.String("http.request.method", "POST"),
	)
	span.AddEvent("exception", trace.WithAttributes(
		attribute.String("exception.type", "*errors.errorString"),
		attribute.String("exception.message", "SENTINEL"),
		attribute.String("exception.stacktrace", "SENTINEL"),
	))
	span.SetStatus(codes.Error, "SENTINEL description")
	span.End()
	require.NoError(t, tp.Shutdown(context.Background()))

	require.Len(t, capture.spans, 1)
	stub := tracetest.SpanStubFromReadOnlySpan(capture.spans[0])
	got := map[string]string{}
	for _, kv := range stub.Attributes {
		got[string(kv.Key)] = kv.Value.Emit()
	}
	assert.Equal(t, map[string]string{"http.route": "/v1/x", "http.request.method": "POST", "error.type": "502"}, got)
	assert.Equal(t, codes.Error, stub.Status.Code)
	assert.Empty(t, stub.Status.Description)
	require.Len(t, stub.Events, 1)
	require.Len(t, stub.Events[0].Attributes, 1)
	assert.Equal(t, "exception.type", string(stub.Events[0].Attributes[0].Key))
	assert.Equal(t, "POST /v1/x", stub.Name)
	assert.True(t, stub.SpanContext.TraceID().IsValid(), "identity of the span survives")
}

func TestSanitizingMetricViewDropsErrorTextAttributes(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader), sdkmetric.WithView(sanitizingMetricView()), sdkmetric.WithExemplarFilter(exemplar.AlwaysOffFilter))
	c, err := mp.Meter("t").Int64Counter("db.sql.latency")
	require.NoError(t, err)
	c.Add(context.Background(), 1, metric.WithAttributes(
		attribute.String("db.sql.error", "pq: invalid input syntax: \"SENTINEL\""),
		attribute.String("db.sql.status", "ERROR"),
		attribute.String("url.full", "http://h/?token=SENTINEL"),
	))
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))
	require.Len(t, rm.ScopeMetrics, 1)
	sum := rm.ScopeMetrics[0].Metrics[0].Data.(metricdata.Sum[int64])
	require.Len(t, sum.DataPoints, 1)
	got := map[string]string{}
	for _, kv := range sum.DataPoints[0].Attributes.ToSlice() {
		got[string(kv.Key)] = kv.Value.Emit()
	}
	assert.Equal(t, map[string]string{"db.sql.status": "ERROR"}, got)
}
