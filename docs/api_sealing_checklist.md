# ducknng API sealing checklist

This checklist is narrower than `docs/design_review_checklist.md`. It tracks what still blocks calling the current public API sealed or stable enough that user-facing names, contracts, and examples should stop changing casually.

## Current position

`ducknng` already has a real public surface for:

- service lifecycle control
- method registry administration
- raw request/reply helpers
- generic NNG socket patterns
- raw aio handles
- the first raw unary RPC aio wrappers
- query-session control helpers
- TLS config handles
- a low-level HTTP/HTTPS client helper

That is enough for serious use and interop work, but it is not yet enough to call the full API sealed.

## Must-resolve before sealing

### 1. Session ownership and identity

The largest blocker is the session-family ownership model.

Before the API is considered sealed, `query_open`, `fetch`, `close`, and `cancel` need a concrete owner identity model that works across multiple clients. A bare `session_id` lookup is not acceptable as the final security and lifecycle contract.

This means the project must choose and implement one coherent identity model:

- transport-derived identity
- RPC-level authentication and capability binding
- per-pipe identity

Until that lands, the session family should remain documented as experimental.

### 2. HTTP server and URL-routed higher-level helpers

The HTTP transport direction is only partially complete.

Before sealing, the project should implement:

- `ducknng_start_http_server(...)`
- operation-oriented routing of existing request/RPC/session helpers over `http://` and `https://`
- any HTTP aio helpers that are intended to be part of the stable async story

The key constraint remains unchanged: HTTP must stay a carrier for the same manifest methods, session lifecycle, and Arrow-versus-JSON payload rules.

### 3. Generic client socket TLS dialing

Listener-side TLS and one-shot request helpers are already wired, but the broader socket-handle dial surface still needs a coherent TLS story before `tls+tcp://` can be considered equally complete across the supported protocol family.

### 4. Final async surface scope

The project now has:

- raw transport aio helpers
- the first raw unary RPC aio wrappers

Before sealing, it should decide whether the stable async contract is:

- raw-frame-first only
- or raw plus additional structured async wrappers

What matters is not maximizing surface area, but freezing a clear model: aio launch timeout bounds the pending operation, `ducknng_aio_collect(..., wait_ms)` is later polling/collection, and async helpers do not silently become a second background-job protocol.

### 5. Final type and fetch ergonomics stance

The documented long-term Arrow type target is broader than the currently emitted unary row subset. Before sealing, the project should either:

- finish the next planned type wave, especially for the explicitly named gaps, or
- freeze and document the narrower supported set as the sealed initial contract

The same applies to fetched Arrow payload ergonomics: decide whether the stable public story is control metadata plus raw Arrow payloads only, or whether a SQL-side row decoder for fetch payloads is part of the sealed API.

## Strongly recommended before sealing

### 6. Representative protocol examples and tests

The repo now demonstrates req-style RPC, `pair`, `push` / `pull`, `pub` / `sub`, raw aio, unary RPC aio, and the current session control flow. Before sealing, the remaining question is not whether the protocol family is entirely undocumented, but whether the current examples and tests are representative enough for the less-common raw socket patterns that remain open/close-oriented rather than tutorial-driven.

At minimum, the project should keep the existing runnable examples green and decide whether additional tutorial coverage for `bus` or any later event-oriented patterns belongs in the sealed public story.

### 7. Explicit scope for `ws://` and `wss://`

`ws://` and `wss://` should remain out of scope for the sealed API unless the project deliberately enables and documents them. Right now the build disables the NNG WS/WSS transports and the HTTP transport contract explicitly defers WebSocket work.

That is a valid project boundary, but it should stay explicit.

## Already addressed in the current pass

These items were worth resolving before the API hardens further and should stay fixed:

- runtime teardown now cleans up services, aio handles, client sockets, TLS configs, the runtime mutex, and the global runtime registry entry instead of leaving destruction partial
- the vendored NNG Windows MinGW fallback is now documented through an explicit local patch file and patch ledger under `patches/nng/`
- README examples now include representative raw protocol slices beyond req/pair, including `push` / `pull`, `pub` / `sub`, and `surveyor` / `respondent`

## Not sealing blockers by themselves

These are still important, but they do not need to be finished before the API can be considered sealed if the above items are settled:

- splitting `src/ducknng_sql_api.c`
- a future codec framework
- pipe event notification work
- a future DuckDB-native Arrow re-plumb, if one ever becomes viable without unstable or deprecated APIs
