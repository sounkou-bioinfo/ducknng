# ducknng API sealing checklist

This checklist is narrower than `docs/design_review_checklist.md`. It tracks what still blocks calling the current public API sealed or stable enough that public names, contracts, and examples should stop changing casually.

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
- an HTTP/HTTPS server helper plus URL-routed synchronous request/RPC/session helpers
- NNG WebSocket transport schemes through `ws://` and `wss://`
- a low-level HTTP/HTTPS client helper
- built-in content-type driven body codec helpers for raw/text/JSON/CSV/TSV/Parquet/Arrow IPC/frame bodies

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

### 2. Final HTTP async scope

The synchronous HTTP transport direction is now in place:

- `ducknng_start_http_server(...)` is implemented
- the existing synchronous request/RPC/session helpers route over `http://` and `https://`

Before sealing, the remaining HTTP question is whether any HTTP aio helpers are intended to be part of the stable async story. The key constraint remains unchanged: HTTP must stay a carrier for the same manifest methods, session lifecycle, and Arrow-versus-JSON payload rules.

### 3. Final generic socket transport coverage story

The generic socket dial surface now accepts an explicit `tls_config_id`, which makes `tls+tcp://` and `wss://` usable through the existing handle model. Before sealing, the remaining question is not whether TLS dialing exists, but whether the repo's examples and docs cover the intended supported transport matrix clearly enough across the broader protocol family.

### 4. Final async surface scope

The project now has:

- raw transport aio helpers
- the first raw unary RPC aio wrappers

Before sealing, it should decide whether the stable async contract is:

- raw-frame-first only
- or raw plus additional structured async wrappers

What matters is not maximizing surface area, but freezing a clear model: aio launch timeout bounds the pending operation, `ducknng_aio_collect(..., wait_ms)` is later polling/collection, and async helpers do not silently become a second background-job protocol.

### 5. Final type and fetch ergonomics stance

The unary Arrow row path now covers the next planned type wave for practical DuckDB tabular values: temporal types, decimals, lists, and structs in addition to the original scalar subset. Before sealing, the remaining type question is whether that documented set is the initial public contract or whether any additional Arrow features, such as map values, dictionary-preserving roundtrips, extension types, or broader deep-nesting guarantees, must land first.

Fetched Arrow payload ergonomics are still a separate sealing question: decide whether the stable public story is control metadata plus raw Arrow payloads only, or whether a SQL-side row decoder for fetch payloads is part of the sealed API.

## Strongly recommended before sealing

### 6. Representative protocol examples and tests

The repo now demonstrates req-style RPC, `pair`, `push` / `pull`, `pub` / `sub`, raw aio, unary RPC aio, and the current session control flow. Before sealing, the remaining question is not whether the protocol family is entirely undocumented, but whether the current examples and tests are representative enough for the less-common raw socket patterns that remain open/close-oriented rather than tutorial-driven.

At minimum, the project should keep the existing runnable examples green and decide whether additional tutorial coverage for `bus` or any later event-oriented patterns belongs in the sealed public story.

### 7. Explicit scope for `ws://` and `wss://`

`ws://` and `wss://` are now explicit NNG transport schemes in scope for the public API. What still needs to stay explicit is the boundary: they belong to the NNG transport family, not to the HTTP carrier layer. The HTTP transport contract may still defer browser-style or HTTP-carrier WebSocket work even while NNG WebSocket transports are supported.

## Already addressed in the current pass

These items were worth resolving before the API hardens further and should stay fixed:

- runtime teardown now cleans up services, aio handles, client sockets, TLS configs, the runtime mutex, and the global runtime registry entry instead of leaving destruction partial
- the vendored NNG Windows MinGW fallback is now documented through an explicit local patch file and patch ledger under `patches/nng/`
- README examples now include representative raw protocol slices beyond req/pair, including `push` / `pull`, `pub` / `sub`, and `surveyor` / `respondent`
- the README and `docs/lifetime.md` now make the low-level manual-lifecycle contract explicit instead of implying a nanonext-style GC/finalizer model that DuckDB SQL does not actually provide for user-visible handles
- the unary Arrow row path now includes `DATE`, `TIME`, `TIMESTAMP`, `DECIMAL`, `LIST`, and `STRUCT` coverage with SQL-visible regression tests

## Not sealing blockers by themselves

These are still important, but they do not need to be finished before the API can be considered sealed if the above items are settled:

- splitting `src/ducknng_sql_api.c`
- user-defined codec registration hooks beyond the current built-in body codec providers
- pipe event notification work
- a future DuckDB-native Arrow re-plumb, if one ever becomes viable without unstable or deprecated APIs
