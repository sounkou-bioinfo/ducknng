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
- built-in content-type driven body codec helpers for raw/text/JSON/Arrow IPC/frame bodies, with CSV/TSV/Parquet recognized and using the generic `body BLOB` fallback pending a memory-backed reader path

That is enough for serious use and interop work, but it is not yet enough to call the full API sealed.

## Must-resolve before sealing

### 1. Session ownership and execution-lane policy review

The previous bare-`session_id` ownership blocker is addressed by the current query-family bearer token model: `query_open` returns `session_token`, and `fetch`, `close`, and `cancel` must present it with the session id. Transport-derived mTLS identity has also landed for TLS-authenticated services, sessions opened with a verified peer identity are bound to that identity in addition to the token, and services can now enforce exact verified-peer allowlists, IP/CIDR allowlists, and service-level SQL authorizer callbacks. Before sealing, the remaining ownership question is whether this bearer-token plus optional mTLS owner-identity model is the stable public owner contract or whether an envelope-level RPC authentication layer is still required.

The current service execution model is a single serialized DuckDB execution lane backed by the runtime init connection. That is no longer treated as an automatic sealing blocker: deployments can scale or isolate work by using multiple DuckDB contexts, multiple `ducknng` services or instances, or an upstream router. The remaining decision is how strongly the stable API must expose and document that lane model, and which filesystem, extension-loading, external-access, and attachment capabilities are allowed when deployments expose SQL over RPC. Per-session or per-request DuckDB connections remain a possible hard-isolation mode, not the only acceptable sealed posture.

### 2. Resource quotas for multi-client services

The current implementation enforces listener receive-size limits, descriptor request/reply-size limits, service-level session idle timeout, a service-level `max_open_sessions` cap, an NNG `max_active_pipes` cap, a carrier-neutral `max_inflight_requests` cap, and an mTLS-owner `max_sessions_per_peer_identity` cap set with `ducknng_set_service_limits(...)`; `query_open` replies now expose the server-owned effective `idle_timeout_ms`, and the manifest exposes server-owned session policy including `max_open_sessions` and `max_sessions_per_peer_identity`. It does not yet implement all owner/pipe-level quotas useful for a sealed multi-tenant RPC service. Before sealing multi-client deployment semantics, decide whether the stable surface needs explicit limits for concurrent in-flight requests per owner/pipe, principal-based open-session caps, cumulative reply bytes per owner, and session-open rate. If per-session idle-timeout hints are later accepted from clients, they should be bounded by server-side defaults and maxima rather than being an unbounded client choice.

### 3. HTTP async and web-server framework scope

The HTTP transport direction is now in place:

- `ducknng_start_http_server(...)` is implemented
- the existing synchronous request/RPC/session helpers route over `http://` and `https://`
- `ducknng_ncurl_aio(...)` and `ducknng_ncurl_aio_collect(...)` provide the raw asynchronous HTTP/HTTPS client counterpart to `ducknng_ncurl(...)`

The remaining HTTP question is the broader server framework: the current server is a non-blocking NNG HTTP server that mounts one framed RPC handler; a later web-toolkit layer may add explicit route handlers, static responses, or SQL-backed handlers, but it must not create path-specific copies of existing RPC methods. The key constraint remains unchanged: HTTP must stay a carrier for the same manifest methods, session lifecycle, and Arrow-versus-JSON payload rules unless a separate web-framework surface is designed and documented.

### 4. Final generic socket transport coverage story

The generic socket dial surface now accepts an explicit `tls_config_id`, which makes `tls+tcp://` and `wss://` usable through the existing handle model. Before sealing, the remaining question is not whether TLS dialing exists, but whether the repo's examples and docs cover the intended supported transport matrix clearly enough across the broader protocol family.

### 5. Final async surface scope

The project now has:

- raw transport aio helpers
- raw HTTP/HTTPS ncurl aio helpers
- the first raw unary RPC aio wrappers

Before sealing, it should decide whether the stable async contract is:

- raw-frame-first only
- or raw plus additional structured async wrappers

What matters is not maximizing surface area, but freezing a clear model: aio launch timeout bounds the pending operation, `ducknng_aio_collect(..., wait_ms)` is later polling/collection, and async helpers do not silently become a second background-job protocol.

### 6. Final type and fetch ergonomics stance

The unary Arrow row path now covers the next planned type wave for practical DuckDB tabular values: temporal types, decimals, lists, and structs in addition to the original scalar subset. Before sealing, the remaining type question is whether that documented set is the initial public contract or whether any additional Arrow features, such as map values, dictionary-preserving roundtrips, extension types, or broader deep-nesting guarantees, must land first.

Fetched Arrow payload ergonomics are still a separate sealing question: decide whether the stable public story is control metadata plus raw Arrow payloads only, or whether a SQL-side row decoder for fetch payloads is part of the sealed API.

## Strongly recommended before sealing

### 7. Representative protocol examples and tests

The repo now demonstrates req-style RPC, `pair`, `push` / `pull`, `pub` / `sub`, raw aio, unary RPC aio, and the current session control flow. Before sealing, the remaining question is not whether the protocol family is entirely undocumented, but whether the current examples and tests are representative enough for the less-common raw socket patterns that remain open/close-oriented rather than tutorial-driven.

At minimum, the project should keep the existing runnable examples green and decide whether additional tutorial coverage for `bus` or any later event-oriented patterns belongs in the sealed public story.

### 8. Explicit scope for `ws://` and `wss://`

`ws://` and `wss://` are now explicit NNG transport schemes in scope for the public API. What still needs to stay explicit is the boundary: they belong to the NNG transport family, not to the HTTP carrier layer. The HTTP transport contract may still defer browser-style or HTTP-carrier WebSocket work even while NNG WebSocket transports are supported.

## Already addressed in the current pass

These items were worth resolving before the API hardens further and should stay fixed:

- runtime teardown now cleans up services, aio handles, client sockets, TLS configs, the runtime mutex, and the global runtime registry entry instead of leaving destruction partial
- the vendored NNG Windows MinGW fallback is now documented through an explicit local patch file and patch ledger under `patches/nng/`
- README examples now include representative raw protocol slices beyond req/pair, including `push` / `pull`, `pub` / `sub`, and `surveyor` / `respondent`
- the README and `docs/lifetime.md` now make the low-level manual-lifecycle contract explicit instead of implying a nanonext-style GC/finalizer model that DuckDB SQL does not actually provide for user-visible handles
- the unary Arrow row path now includes `DATE`, `TIME`, `TIMESTAMP`, `DECIMAL`, `LIST`, and `STRUCT` coverage with SQL-visible regression tests
- query sessions now bind ownership to an explicit `session_token` bearer capability instead of treating `session_id` as a capability
- TLS-authenticated services now attach verified mTLS peer identity to requests and bind sessions to that identity when present
- `ducknng_register_exec_method(true)` can register the opt-in `exec` descriptor with verified peer identity required, while the zero-argument form remains backwards compatible
- `ducknng_set_method_auth(name, requires_auth)` can protect registry-backed methods such as `manifest` using the same descriptor-level auth path
- unregistration now refuses to remove sessionful methods or families while any service has open sessions
- `ducknng_ncurl_aio(...)` now provides the nanonext-style async HTTP client slice and collects through `ducknng_ncurl_aio_collect(...)`
- `ducknng_read_monitor(...)`, `ducknng_monitor_status(...)`, and `ducknng_list_pipes(...)` provide NNG pipe event and active-pipe telemetry
- `src/ducknng_sql_api.c` has been split into focused SQL registration modules and is now only the top-level registration orchestrator
- lifecycle tests cover stopping services with live sessions and rejecting same-service stop while a service-owned SQL authorizer request is active
- unsupported URL schemes and supplied TLS configuration on non-TLS schemes have regression coverage across the major client/helper families

## Not sealing blockers by themselves

These are still important, but they do not need to be finished before the API can be considered sealed if the above items are settled:

- user-defined codec registration hooks beyond the current built-in body codec providers
- scalarfs-style in-memory filesystem/provider research for CSV/TSV/Parquet body parsing, because the generic `body BLOB` fallback is an acceptable stable behavior until a clean provider exists
- a future DuckDB-native Arrow re-plumb, if one ever becomes viable without unstable or deprecated APIs
