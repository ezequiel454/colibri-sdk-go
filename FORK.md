# Fork notice

This repository is a fork of [`colibriproject-dev/colibri-sdk-go`](https://github.com/colibriproject-dev/colibri-sdk-go),
maintained at `github.com/ezequiel454/colibri-sdk-go` (branch `codex/forge-backend-hardening-m3`) for the
forge backend hardening (M3). It is the owner's own maintenance fork (the GitHub repository is public): no contribution, issue or other contact with
the upstream project was made or is implied.

- **Base:** tag `v0.2.5`, commit `0c700b0091f3a0fbd63de26837b3bb8fec4a9dd4`.
- **License:** Apache-2.0, unchanged (`LICENSE` is the upstream file; upstream ships no `NOTICE`).
  As required by Apache-2.0 §4(b), every source file changed or added in this fork carries the line
  `Modified in the ezequiel454 fork for forge M3; see FORK.md` at the top.
- **Dependencies:** none added or changed (`go.mod` and `go.sum` are the upstream ones).

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
| `pkg/web/restserver/{observer,hooks,lifecycle}_test.go`, `pkg/base/observer/subject_test.go` | tests for the above |

## Known limits

- One current server per process (package-level state, as upstream).
- The rest server's `Stop` waits, bounded by `DrainBudget`, for its own listener to close, never for the admitted
  requests to drain; the `Stopper` contract in `observer.go` says so.
- A bind in progress delays `Stop` only for the duration of the (local) bind: bind, ownership of the listener and
  `closeListener` share one lock.
- The tracing attribute of the correlation id is set after normalization, but the tracing-enabled path is not exercised by tests.
