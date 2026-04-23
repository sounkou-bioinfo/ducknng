# News

## ducknng 0.1.0

### Latest additions

- Added the first raw unary RPC aio wrappers: `ducknng_get_rpc_manifest_raw_aio(...)` and `ducknng_run_rpc_raw_aio(...)`.
- Added a documented local NNG patch under `patches/nng/` so the vendored Windows clock fallback for DuckDB CI's Rtools42 MinGW environment is explicit rather than an undocumented edit inside `third_party/nng/`.
- Clarified the README and protocol docs so the layering is explicit: the generic socket layer is the transport substrate, higher-level RPC helpers wrap manifest-declared request/reply methods, session helpers wrap the fixed `query_open` / `fetch` / `close` / `cancel` lifecycle, and aio launch `timeout_ms` is distinct from later `ducknng_aio_collect(..., wait_ms)` polling.
- Switched the README HTTP client illustration to a visible local `nanonext` HTTPS server so the example shows the real carrier and TLS story rather than hiding it behind generic setup prose.
- Implemented `ducknng_ncurl(...)` as the first low-level HTTP/HTTPS client slice, returning in-band `ok`, `status`, `error`, `headers_json`, `body`, and `body_text` columns.
- Added `make http_smoke` and a local Python-stdlib smoke harness to validate real HTTP GET and POST roundtrips without depending on the public internet.
- Added `docs/http.md` to pin the first HTTP transport contract: planned SQL signatures for `ducknng_start_http_server(...)` and `ducknng_ncurl(...)`, frame-over-HTTP carriage, and the invariant that session methods and Arrow record batches keep the same protocol semantics under HTTP.
- Added a transport-family URL parser above the NNG shim so current NNG paths fail fast on `http://` and `https://` as reserved future transport adapters instead of treating them as malformed NNG endpoints.
- Added a runtime-owned aio registry and SQL-visible raw aio helpers for both request/reply and generic socket operations: `ducknng_request_raw_aio()`, `ducknng_request_socket_raw_aio()`, `ducknng_send_socket_raw_aio()`, `ducknng_recv_socket_raw_aio()`, `ducknng_aio_ready()`, `ducknng_aio_status()`, `ducknng_aio_collect()`, `ducknng_aio_cancel()`, and `ducknng_aio_drop()`.
- `ducknng_aio_collect()` and `ducknng_aio_status()` are now exposed as SQL macros over internal scalar helpers so dynamic arguments can work without relying on lateral-capable stable-C-API table-function parameters.
- Expanded the generic socket surface to the broader nanonext-style NNG protocol family: `bus`, `pair`, `poly`, `push`, `pull`, `pub`, `sub`, `req`, `rep`, `surveyor`, and `respondent`.
- Added generic raw socket verbs `ducknng_listen_socket()`, `ducknng_send_socket_raw()`, `ducknng_recv_socket_raw()`, `ducknng_subscribe_socket()`, and `ducknng_unsubscribe_socket()`.
- Added SQL-visible session wrappers `ducknng_open_query()`, `ducknng_fetch_query()`, `ducknng_close_query()`, and `ducknng_cancel_query()` over the existing `query_open` / `fetch` / `close` / `cancel` RPC family.
- The docs contract for the session query family now fixes the intended lifecycle: `query_open` returns JSON control metadata and a session id, `fetch` is the only row-bearing method, `close` is the normal cleanup path, and `cancel` is best-effort until the implementation can bind sessions to a concrete owner identity.

### Earlier 0.1.0 groundwork

- Added client-side SQL helpers `ducknng_remote_manifest(url)` and `ducknng_remote_exec(url, sql)` so DuckDB can request manifest and metadata-only exec operations from another ducknng-compatible service.
- Added non-throwing result-table companions `ducknng_remote_manifest_result(url)` and `ducknng_remote_exec_result(url, sql)` so client-side transport and protocol failures can be handled in-band as rows.
- Added SQL-native req-style client handle helpers `ducknng_socket(protocol)`, `ducknng_dial(socket_id, url, timeout_ms)`, `ducknng_request_socket(socket_id, payload, timeout_ms)`, `ducknng_request(url, payload, timeout_ms)`, `ducknng_close(socket_id)`, and `ducknng_sockets()`.
- Added non-throwing raw request companions `ducknng_request_result(url, payload, timeout_ms)` and `ducknng_request_socket_result(socket_id, payload, timeout_ms)`.
- Added `ducknng_remote(url, sql)` as the current unary row-reply client table function, exposing Arrow IPC row replies as DuckDB tables for the currently supported unary row subset.
- `exec` request payloads are now Arrow IPC tables containing `sql` and `want_result` fields.
- `manifest` replies are now JSON exported from the runtime method registry.
- `exec` metadata replies are now Arrow IPC generated with vendored nanoarrow C.
- Unary `exec(..., want_result = true)` now returns Arrow IPC row payloads for the current scalar subset: BOOLEAN, signed/unsigned integers, FLOAT/DOUBLE, VARCHAR, and BLOB.
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
- Added `ducknng_servers()` table-function introspection over the per-database runtime service registry.
- `ducknng_server_start(...)` now creates the requested number of REP contexts on one REP socket instead of hard-coding a single worker.
- Added phase-1 SQL control functions:
  - `ducknng_server_start(...)`
  - `ducknng_server_stop(name)`
- Added a real NNG REP listener lifecycle with one context, one AIO, and one worker thread.
- Added a phase-1 pure C runtime keyed by DuckDB database handle.
- Renamed the template extension to `ducknng`.

## Planned next steps

- Implement `ducknng_start_http_server(...)` and route existing request/RPC/session helpers over `http://` and `https://` without creating a second RPC surface.
- Extend the async RPC wrapper family beyond the first raw unary slice where that can be done honestly on top of the existing aio substrate.
- Add a coherent TLS-config story for generic client socket dialing so `tls+tcp://` is as complete there as it is for listeners and one-shot request helpers.
- Continue broader Arrow type coverage and add a clean SQL-side decoder path for fetched Arrow payloads if richer session ergonomics are desired.
- Bind session ownership to a concrete multi-client identity model so the session family can stop being documented as experimental.
