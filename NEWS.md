# News

## ducknng 0.1.0

### Latest additions

- Implemented `ducknng_start_http_server(...)` as the first real HTTP/HTTPS server slice over the existing registry-backed framed RPC surface, including adapter-level `405` / `415` / `400` handling and `200 OK` frame replies for protocol-level success and failure.
- Taught the synchronous request, RPC, and session helper family to route by URL scheme so `ducknng_request(...)`, `ducknng_request_raw(...)`, `ducknng_get_rpc_manifest(...)`, `ducknng_get_rpc_manifest_raw(...)`, `ducknng_run_rpc(...)`, `ducknng_run_rpc_raw(...)`, `ducknng_query_rpc(...)`, `ducknng_open_query(...)`, `ducknng_fetch_query(...)`, `ducknng_close_query(...)`, and `ducknng_cancel_query(...)` now work over `http://` and `https://` without minting a second RPC surface.
- Enabled the vendored NNG `ws://` and `wss://` transports and documented them as part of the NNG transport family rather than as part of the HTTP carrier layer.
- Extended `ducknng_dial_socket(...)` to take `tls_config_id` and applied the same reusable TLS-handle model to generic socket dialing, including `wss://`.
- Changed synchronous `ducknng_request_socket(...)` to use the already-connected req socket handle directly instead of silently re-dialing from the stored URL, so socket-handle dialing and TLS settings actually carry through to the request path.
- Added sqllogictest coverage for the new HTTP server/routed-helper surface and for `ws://` / `wss://` transport use through both service and socket-handle paths.
- Hardened client socket lifetime management with runtime-owned retain/release tracking so close/destroy waits for in-flight users, pending socket-bound aio operations keep their socket alive while they are actually pending, and terminal aio handles no longer block later socket close just because the caller has not dropped the collected aio row yet.
- Hardened server-side query session lifetime tracking so `fetch` now runs against an acquired session reference instead of an unlocked borrowed pointer, close/cancel/prune/stop detach sessions before destroy and wait for in-flight users to drain, service introspection reads a published session-count snapshot instead of re-locking service state from inside service-owned SQL, and `query_open` now refuses to publish a fresh session while shutdown is already in progress.
- Hardened service-owned SQL execution and HTTP shutdown around `init_con`: service-side `exec` / `query_open` now serialize through a runtime-owned init-connection gate, HTTP shutdown waits for handler finalization before freeing handler-owned state, and self-stop from a service's own request path is rejected instead of deadlocking or tearing the service out from under the active request.
- Expanded Arrow IPC row mapping for unary `ducknng_query_rpc(...)` / `exec(..., want_result = true)` paths beyond the initial scalar subset to include `DATE`, `TIME`, timezone-free `TIMESTAMP` units, DuckDB `DECIMAL` via Arrow `decimal128`, Arrow lists as DuckDB lists, and Arrow structs as DuckDB structs, with sqllogictest coverage for scalar temporal/decimal and nested list/struct roundtrips.
- Added the first content-type driven body codec provider layer: `ducknng_list_codecs()`, `ducknng_parse_body(body, content_type)`, and `ducknng_ncurl_table(...)` now expose built-in raw/text/JSON/Arrow IPC/frame parsing while keeping `ducknng_ncurl(...)` raw and keeping the HTTP server RPC endpoint frame-only. JSON parsing now stays in memory through DuckDB JSON functions; CSV/TSV/Parquet media types are recognized but use the generic `body BLOB` fallback until a scalarfs-style memory filesystem/provider path lands. The HTTP frame endpoint now also accepts the frame media type case-insensitively with parameters.
- Bound query sessions to generated bearer capabilities: `query_open` / `ducknng_open_query(...)` now return `session_token`, and `fetch`, `close`, and `cancel` reject calls that present a matching `session_id` without the matching token.
- Extended `ducknng_register_exec_method(...)` with an optional `requires_auth` boolean so deployments can register `exec` with descriptor-level verified peer identity enforcement instead of relying only on listener-wide mTLS policy.
- Added transport-derived mTLS caller identity for `tls+tcp://`, `wss://`, and `https://` service requests. TLS authentication mode `2` now requires a verified peer certificate identity before dispatch; sessions opened over verified mTLS are bound to that identity in addition to the bearer `session_token`, and `ducknng_list_servers()` exposes `tls_enabled`, `tls_auth_mode`, and `peer_identity_required`.
- Added sqllogictest coverage for mTLS manifest roundtrips over NNG TLS, WSS, and the HTTPS frame carrier, plus query-session identity binding over NNG TLS, wrong-identity `fetch` / `close` / `cancel` failures, required-mTLS no-certificate rejection, and optional-auth token-only versus identity-bound session behavior.
- Added the first raw unary RPC aio wrappers: `ducknng_get_rpc_manifest_raw_aio(...)` and `ducknng_run_rpc_raw_aio(...)`.
- Added a documented local NNG patch under `patches/nng/` so the vendored Windows clock fallback for DuckDB CI's Rtools42 MinGW environment is explicit rather than an undocumented edit inside `third_party/nng/`.
- Fixed the next Windows MinGW portability blocker after the vendored NNG gate: self-signed TLS material generation now uses portable temp-directory helpers instead of calling `mkdtemp()` directly from `ducknng_sql_api.c`.
- Replaced the old no-op Windows stubs in `ducknng_util.c` with real Win32 implementations for time, sleep, threads, mutexes, and condition variables so runtime-owned services and aio state no longer depend on POSIX-only helper behavior on Windows builds.
- Fixed several concrete teardown and lifetime hazards: `ducknng_runtime_destroy(...)` now disconnects the init connection, removes the runtime from the global registry before teardown, cleans up services before client sockets, and fully frees its owned structures; service stop/teardown now releases per-context aio state safely and frees restartable allocations; SQL registration no longer shares one static cross-database context pointer; method error replies no longer read DuckDB result error text after destroying the result object; manifest JSON now uses DuckDB allocators consistently; and async send teardown now drains pending `nng_msg` ownership instead of leaving messages attached to freed aio objects.
- Added `docs/api_sealing_checklist.md` to track what still blocks calling the current public API sealed, including session ownership and execution-isolation questions.
- Expanded the README with runnable `push` / `pull`, `pub` / `sub`, and `surveyor` / `respondent` raw messaging examples and matched them with sqllogictest coverage instead of leaving the broader protocol family only implied by the function list.
- Retitled the project in the README and package metadata as a DuckDB binding to the NNG Scalability Protocols library and an Arrow IPC-based RPC framework, instead of the older too-narrow REQ/REP-only framing.
- Added a short getting-started section near the top of the README and wrapped the generated function catalog in a foldable block so the README no longer opens with a long wall of generated catalog content.
- Made the README getting-started SQL actually execute during rendering instead of showing only static unevaluated snippets.
- Stopped the rendered README from leaking machine-local absolute extension paths; the examples now use relative extension paths instead.
- Added `docs/lifetime.md` and a matching README section to document the current low-level manual-lifecycle contract: DuckDB gives destroy callbacks for extension-internal function/bind/init state, but not a nanonext-style GC/finalizer model for long-lived SQL handles, so servers, sockets, aio handles, TLS configs, and query sessions still need explicit cleanup.
- Cleaned README wording so the lifetime section states the contract directly without clunky meta-labels, clarified that the HTTPS hello example is a `ducknng_ncurl()` call against a local `nanonext` HTTPS server, made that README HTTPS setup more robust by polling the local server before the SQL example runs instead of relying on a fixed one-second sleep, and reformatted the visible README code so long lines do not force unnecessary horizontal scrolling.
- Removed the stale `docs/design_review.md` snapshot instead of leaving it in the repo as if it were current documentation, and updated `docs/design_review_checklist.md` to stand on its own.
- Clarified the README and protocol docs so the layering is explicit: the generic socket layer is the transport substrate, higher-level RPC helpers wrap manifest-declared request/reply methods, session helpers wrap the fixed `query_open` / `fetch` / `close` / `cancel` lifecycle, and aio launch `timeout_ms` is distinct from later `ducknng_aio_collect(..., wait_ms)` polling.
- Reframed the README and transport docs so `ws://` and `wss://` are documented as enabled NNG transports, while browser-style or HTTP-carrier WebSocket work remains explicitly deferred.
- Switched the README HTTP client illustration to a visible local `nanonext` HTTPS server so the example shows the real carrier and TLS story rather than hiding it behind generic setup prose.
- Implemented `ducknng_ncurl(...)` as the first low-level HTTP/HTTPS client slice, returning in-band `ok`, `status`, `error`, `headers_json`, `body`, and `body_text` columns.
- Added `make http_smoke` and a local Python-stdlib smoke harness to validate real HTTP GET and POST roundtrips without depending on the public internet.
- Added `docs/http.md` to pin the first HTTP transport contract: planned SQL signatures for `ducknng_start_http_server(...)` and `ducknng_ncurl(...)`, frame-over-HTTP carriage, and the invariant that session methods and Arrow record batches keep the same protocol semantics under HTTP.
- Added a transport-family URL parser above the NNG shim so `http://` and `https://` route through the HTTP carrier adapter for synchronous helpers, while generic NNG socket/listener paths reject those schemes instead of treating them as malformed NNG endpoints.
- Added a runtime-owned aio registry and SQL-visible raw aio helpers for both request/reply and generic socket operations: `ducknng_request_raw_aio()`, `ducknng_request_socket_raw_aio()`, `ducknng_send_socket_raw_aio()`, `ducknng_recv_socket_raw_aio()`, `ducknng_aio_ready()`, `ducknng_aio_status()`, `ducknng_aio_collect()`, `ducknng_aio_cancel()`, and `ducknng_aio_drop()`.
- `ducknng_aio_collect()` and `ducknng_aio_status()` are now exposed as SQL macros over internal scalar helpers so dynamic arguments can work without relying on lateral-capable stable-C-API table-function parameters.
- Expanded the generic socket surface to the broader nanonext-style NNG protocol family: `bus`, `pair`, `poly`, `push`, `pull`, `pub`, `sub`, `req`, `rep`, `surveyor`, and `respondent`.
- Added generic raw socket verbs `ducknng_listen_socket()`, `ducknng_send_socket_raw()`, `ducknng_recv_socket_raw()`, `ducknng_subscribe_socket()`, and `ducknng_unsubscribe_socket()`.
- Added SQL-visible session wrappers `ducknng_open_query()`, `ducknng_fetch_query()`, `ducknng_close_query()`, and `ducknng_cancel_query()` over the existing `query_open` / `fetch` / `close` / `cancel` RPC family.
- The docs contract for the session query family now fixes the intended lifecycle: `query_open` returns JSON control metadata and a session id, `fetch` is the only row-bearing method, `close` is the normal cleanup path, and `cancel` is best-effort.

### Earlier 0.1.0 groundwork

- Added the client-side SQL helper family now exposed as `ducknng_get_rpc_manifest(...)`, `ducknng_run_rpc(...)`, `ducknng_query_rpc(...)`, plus raw-frame variants for callers that need explicit frame handling.
- Added in-band result-table request helpers `ducknng_request(...)` and `ducknng_request_socket(...)` so client-side transport and protocol failures can be handled as rows, alongside raw scalar variants when a bare reply frame is specifically needed.
- Added SQL-native req-style client socket helpers now exposed as `ducknng_open_socket(protocol)`, `ducknng_dial_socket(socket_id, url, timeout_ms, tls_config_id)`, `ducknng_request_socket(...)`, `ducknng_close_socket(socket_id)`, and `ducknng_list_sockets()`.
- Added `ducknng_query_rpc(url, sql, tls_config_id)` as the unary row-reply client table function, exposing Arrow IPC row replies as DuckDB tables for the supported unary row subset.
- `exec` request payloads are now Arrow IPC tables containing `sql` and `want_result` fields.
- `manifest` replies are now JSON exported from the runtime method registry.
- `exec` metadata replies are now Arrow IPC generated with vendored nanoarrow C.
- Unary `exec(..., want_result = true)` initially returned Arrow IPC row payloads for the first scalar subset: BOOLEAN, signed/unsigned integers, FLOAT/DOUBLE, VARCHAR, and BLOB.
- Removed unstable and deprecated DuckDB Arrow entrypoints from the implementation and kept the row-result Arrow path on explicit nanoarrow-based schema and batch mapping.
- Removed the dead deprecated Arrow-wrapper compatibility layer so the tree no longer carries unused `duckdb_query_arrow*` scaffolding.
- Added a registry-backed built-in method surface with `manifest` and `exec` descriptors.
- `manifest` now remains the only always-on built-in RPC method; `exec` must be registered explicitly with `ducknng_register_exec_method()`.
- Added SQL-visible method registry administration with `ducknng_register_exec_method()`, `ducknng_unregister_method(name)`, `ducknng_unregister_family(family)`, and `ducknng_list_methods()`.
- Added `docs/security.md` and `docs/registry.md` as binding design and implementation contracts.
- Added a project-local Pi skill at `.pi/skills/ducknng-rpc-framework/` for protocol, registry, session, and security work in this repo.
- Added `test/rpc_smoke.R` plus `make rpc_smoke` to validate manifest discovery and Arrow-metadata `exec` replies over real NNG REQ/REP.
- Added a versioned RPC envelope with a method name, flags, error field, and payload length instead of the older ad hoc opcode frame.
- Added initial `function_catalog/functions.yaml` metadata plus generated markdown and TSV catalogs.
- Added initial SQLLogicTest coverage in `test/sql/ducknng_server_start.test`.
- Added extension metadata file `description.yml`.
- Added excluded platform metadata for wasm and Windows targets while native Linux development is underway.
- Added `README.Rmd` and `make rdm` for generated documentation.
- Vendored `nng` and `nanoarrow` as third-party dependencies.
- Added `ducknng_list_servers()` table-function introspection over the per-database runtime service registry.
- `ducknng_start_server(...)` now creates the requested number of REP contexts on one REP socket instead of hard-coding a single worker.
- Added phase-1 SQL control functions that evolved into the current names:
  - `ducknng_start_server(...)`
  - `ducknng_stop_server(name)`
- Added a real NNG REP listener lifecycle with one context, one AIO, and one worker thread.
- Added a phase-1 pure C runtime keyed by DuckDB database handle.
- Renamed the template extension to `ducknng`.

## Planned next steps

- Extend the async RPC wrapper family beyond the first raw unary slice only where that can be done honestly on top of the existing aio substrate, and decide whether any HTTP aio helpers belong in the sealed public story.
- Continue Arrow type coverage beyond the current practical core where needed, and add a clean SQL-side decoder path for fetched Arrow payloads if richer session ergonomics are desired.
- Decide whether the current bearer-token plus optional mTLS owner-identity model is the sealed session identity contract, and continue toward isolated per-session/per-request DuckDB execution if multi-client state isolation is required.
- Keep tightening lifetime and concurrency behavior around runtime-owned sockets, sessions, and aio handles now that the transport matrix is broader.
