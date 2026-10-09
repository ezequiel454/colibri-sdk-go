# Fork notice

This repository is a fork of [`colibriproject-dev/colibri-sdk-go`](https://github.com/colibriproject-dev/colibri-sdk-go),
maintained at `github.com/ezequiel454/colibri-sdk-go` (branch `codex/forge-backend-hardening-m3`) for the
forge backend hardening (M3). It is the owner's own maintenance fork (the GitHub repository is public): no contribution, issue or other contact with
the upstream project was made or is implied.

- **Base:** tag `v0.2.5`, commit `0c700b0091f3a0fbd63de26837b3bb8fec4a9dd4`.
- **License:** Apache-2.0, unchanged (`LICENSE` is the upstream file; upstream ships no `NOTICE`).
  As required by Apache-2.0 §4(b), every source file changed or added in this fork carries the line
  `Modified in the ezequiel454 fork for forge M3; see FORK.md` at the top.
- **Dependencies:** none added or changed (`go.mod` and `go.sum` are the upstream ones). `tracing_test.go` imports `go.opentelemetry.io/proto/otlp` and `google.golang.org/protobuf` directly; both are already in the module graph (marked `// indirect`, which `go mod tidy` would drop; left alone on purpose).

## Why

The pinned SDK writes error bodies, logs and shutdown behavior that no service code can reach: error
responses are fixed JSON, a panic value and the raw correlation id are logged, the signal handler closes
the database before the HTTP server, requests in flight are not counted and the process exits when the
listener closes. The changes below are opt-in and, when the new hooks are not used, keep the v0.2.5
response bodies.

## Changes

| File | Change |
|---|---|
| `pkg/web/restserver/hooks.go` (new) | `SetErrorWriter`/`GetErrorWriter`/`ErrorWriter`, `PanicError` (dynamic type only), `ErrDraining`, `SetCorrelationPolicy`, `Ready`, per-instance bookkeeping |
| `pkg/web/restserver/server.go` | one server instance per `ListenAndServe`; no listener after the shutdown began; returns after the whole graceful shutdown; SDK routes registered once |
| `pkg/web/restserver/observer.go` | `restObserver` is per instance and a `Stopper`: synchronous stop of the listener (bounded by `DrainBudget`), admission gate, `Close` awaits the HTTP shutdown |
| `pkg/web/restserver/fiber_server.go` | fiber `ErrorHandler`, unknown-method 501 and protocol rejections routed to the writer, own listener, gate and writer on `Use`/`BeforeEnter` |
| `pkg/web/restserver/middlewares.go` | writer on authentication errors and panics, correlation policy before context/logs/tracing attribute, drain gate middleware |
| `pkg/web/restserver/fiber_web_context.go` | `ErrorResponse` goes through the writer |
| `pkg/base/observer/observer.go` | documentation only: `Stopper.Stop` must not wait for the work in flight, but may wait, boundedly, for the component's own acceptance of new work to end |
| `pkg/base/observer/subject.go` | a `service` per `Initialize`, `InitializeWith(handler)`, `Done`, `Notifying`, `TryAttach`, idempotent `Notify`, `Initialize` refused during a shutdown |
| `pkg/base/logging/logging.go` | FORGE-042: `InitializeWith(w io.Writer)` replaces the logger and injects its destination (the package bound it to `os.Stdout` once per process); `ErrType(err)` logs only the dynamic type of an error and its 5 character SQLSTATE, never the message. `Err` is unchanged (full text) and documented as unsafe for errors that carry outside data |
| `pkg/web/restclient/request.go` | FORGE-042: the retry warning logs attempt, method, status and `errorType`; it no longer logs the URL (path, query, user info), the error text (the transport quotes the URL) or the error body of the peer |
| `pkg/database/sqlDB/{sql_transaction,sql_db,migrations,observer,closer}.go` | FORGE-042: transaction (begin, callback, commit), boot connection (`logging.Fatal` on open/ping/migration), migration, close errors are logged with `ErrType`, not with the driver or caller error text (lib/pq quotes users, hosts and DSN fragments) |
| `pkg/base/monitoring/colibri-otel/open_telemetry.go` | FORGE-042: `NoticeError` exports the exception type only (v0.2.5 exported `err.Error()` as exception message and status description); exporter/provider failures are logged by type; the OTEL error handler (which printed export errors, endpoint URL included, to stderr) is replaced by a rate limited JSON log by type |
| `pkg/base/monitoring/colibri-otel/sanitize.go` (new) | FORGE-042: every span passes a `sanitizingExporter` before the OTLP exporter: drops `url.full`/`url.path`/`url.query`/`http.url`/`http.target`/statement text, credential-named attributes, status descriptions and exception message/stacktrace. Applies to spans of any instrumentation (fiber, otelhttp, otelsql) |
| `pkg/web/restserver/tracing_test.go`, `pkg/web/restclient/retry_log_test.go`, `pkg/database/sqlDB/log_privacy_test.go`, `pkg/base/monitoring/colibri-otel/sanitize_test.go` | FORGE-042: tracing enabled against an OTLP/HTTP collector owned by the test (spans exported, incoming `traceparent` continued and propagated to the downstream, normalized correlation id as attribute, flush on shutdown, off by config exports and propagates nothing, no sentinel in payload or logs), retry log, transaction logs with a failing driver, sanitizer |
| `pkg/web/restserver/hooks.go` | FORGE-042: `Reset()` returns routes, middlewares, authentication, error writer and correlation policy to a fresh-process state (panics under a live server), so a test binary can compose the server more than once (`-count=N`): the route table of the first composition otherwise keeps serving its own handlers. For tests, not for services |
| `pkg/web/restserver/{observer,hooks,lifecycle}_test.go`, `pkg/base/observer/subject_test.go` | tests for the above |

## Known limits

- One current server per process (package-level state, as upstream).
- The rest server's `Stop` waits, bounded by `DrainBudget`, for its own listener to close, never for the admitted
  requests to drain; the `Stopper` contract in `observer.go` says so.
- A bind in progress delays `Stop` only for the duration of the (local) bind: bind, ownership of the listener and
  `closeListener` share one lock.
- The tracing attribute of the correlation id is set after normalization; the tracing-enabled path is exercised by `tracing_test.go` (FORGE-042).
- `NewRestClient` never copies `RestClientConfig.Retries`/`RetrySleepInSeconds` to the client (v0.2.5 defect, kept: fixing it would start retrying and sleeping for existing callers), so the retry warning is unreachable through the public API; its content was hardened anyway and `retry_log_test.go` sets the field directly. `retry_log_test.go` shares the Docker `TestMain` of the package; it was run locally with `client_test.go` set aside.
- `logging.Err(err)` and the `%v` of an error in `Msgf` still log the full text. The SDK paths above use `ErrType`; code of a service must too (forge's template has a gate for its own code).
- `MIGRATION_SOURCE_URL` (a file path) is still logged at INFO by the migration runner.
- The span sanitizer is a deny-list by key: an attribute that a custom instrumentation names outside it still leaves the process. Application code must not put outside data in span attributes.
