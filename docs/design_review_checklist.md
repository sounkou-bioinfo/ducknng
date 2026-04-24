# ducknng design review checklist

This checklist tracks the implementation status of the main architecture, transport, Arrow, session, and async hardening items for `ducknng`. It separates work that is already landed, work completed in this running line of development, and items blocked by deeper architectural replacement work.

## Completed before this pass

- [x] Make `exec` opt-in instead of always-on.
  - `manifest` remains always registered.
  - `exec` must be enabled explicitly with `ducknng_register_exec_method()`.
- [x] Add SQL-visible method registry administration.
  - `ducknng_register_exec_method()`
  - `ducknng_unregister_method(name)`
  - `ducknng_unregister_family(family)`
  - `ducknng_list_methods()`
- [x] Make service teardown ownership explicit.
  - `ducknng_stop_server(name)` already removes the service from the runtime table and destroys it.
- [x] Keep server runtime introspection real.
  - `ducknng_list_servers()` reflects the live runtime registry rather than a fake catalog.
- [x] Add reusable TLS config handles.
  - `ducknng_tls_config_from_files()`
  - `ducknng_tls_config_from_pem()`
  - `ducknng_self_signed_tls_config()`
  - `ducknng_drop_tls_config()`
  - `ducknng_list_tls_configs()`

## Completed in this pass

- [x] Collapse `ducknng_start_server` to one public signature.
  - New public signature: `ducknng_start_server(name, listen, contexts, recv_max_bytes, session_idle_ms, tls_config_id)`.
  - Plaintext now uses `tls_config_id = 0`.
  - Inline file-argument TLS startup was removed from the public SQL surface.
- [x] Collapse `ducknng_request_raw` to one public signature.
  - New public signature: `ducknng_request_raw(url, payload, timeout_ms, tls_config_id)`.
  - Plaintext now uses `tls_config_id = 0::UBIGINT`.
- [x] Give structured one-shot RPC helpers the same TLS reach as their raw counterparts.
  - `ducknng_request(url, payload, timeout_ms, tls_config_id)`
  - `ducknng_get_rpc_manifest(url, tls_config_id)`
  - `ducknng_run_rpc(url, sql, tls_config_id)`
  - `ducknng_query_rpc(url, sql, tls_config_id)`
  - raw companions now also take `tls_config_id` consistently.
- [x] Unify allocator discipline for resolved listen URLs.
  - `ducknng_listener_resolve_url()` now allocates with `duckdb_malloc()`.
  - service teardown now frees resolved URLs with `duckdb_free()`.
- [x] Make `ducknng_open_socket()` honest by implementing the broader NNG protocol family.
  - `bus`, `pair`, `pair0`, `pair1`, `poly`, `push`, `pull`, `pub`, `sub`, `req`, `rep`, `surveyor`, and `respondent` now open through the public socket surface.
- [x] Add generic raw socket verbs on top of that broader protocol family.
  - `ducknng_listen_socket()`
  - `ducknng_send_socket_raw()`
  - `ducknng_recv_socket_raw()`
  - `ducknng_subscribe_socket()`
  - `ducknng_unsubscribe_socket()`
- [x] Add SQL-visible aio handles for both request/reply and generic socket operations.
  - `ducknng_request_raw_aio()`
  - `ducknng_request_socket_raw_aio()`
  - `ducknng_send_socket_raw_aio()`
  - `ducknng_recv_socket_raw_aio()`
  - `ducknng_aio_ready()`
  - `ducknng_aio_status()`
  - `ducknng_aio_collect()`
  - `ducknng_aio_cancel()`
  - `ducknng_aio_drop()`
- [x] Add real SQLLogicTest coverage for the widened socket and aio surface.
  - `test/sql/ducknng_socket_protocols.test`
- [x] Make the docs/examples/catalog match the widened socket and aio surface.
  - `README.Rmd`
  - generated `README.md` / `README.html`
  - `function_catalog/functions.yaml`
  - generated `function_catalog/functions.md` / `functions.tsv`
- [x] Keep the current implementation off unstable and deprecated DuckDB Arrow entrypoints.
  - Unstable and deprecated DuckDB Arrow entrypoints were removed from implementation code.
  - The row-bearing RPC export path remains on explicit nanoarrow schema and batch mapping.
- [x] Add a transport-family boundary for future HTTP / `ncurl` work.
  - URL-family parsing now lives in `src/ducknng_transport.c`.
  - NNG-specific socket/listener/TLS behavior remains isolated in `src/ducknng_nng_compat.c`.
  - `http://` and `https://` now fail fast as reserved future transport-adapter schemes instead of leaking into NNG operations.

## Partial / clarified but not fully solved

- [~] Keep structured-vs-raw helper duplication under explicit review.
  - Current state: signatures were unified and transport reach no longer diverges.
  - Remaining work: decide whether to delete the raw or structured twins entirely.
- [~] Prepare HTTP / HTTPS transport adapters without inventing a second RPC surface.
  - Current state: `docs/transports.md` and `docs/http.md` now fix the intended boundary, `ducknng_start_http_server(...)` is implemented, `ducknng_ncurl(...)` remains the low-level HTTP/HTTPS client slice, and the synchronous request/RPC/session helpers now route by URL scheme.
  - Remaining work: decide whether any HTTP aio helpers belong in the public async surface and keep the docs/examples clear about the HTTP-carrier boundary.

## Blocked by larger architectural replacement work

- [~] Replace the hand-rolled Arrow encode/decode path with DuckDB-native Arrow C API plumbing.
  - Current state: the row-bearing RPC export path stays on manual nanoarrow schema and batch mapping.
  - Remaining work:
    - decide whether a future non-deprecated DuckDB Arrow seam exists that would justify another re-plumb
    - until then, keep the manual mappings careful, documented, and fully tested instead of mixing in unstable or deprecated DuckDB entrypoints
- [~] Harden the session query family now that SQL helpers exist.
  - Current state:
    - server-side `query_open` / `fetch` / `close` / `cancel` methods are registered
    - SQL-visible wrappers now exist as `ducknng_open_query()`, `ducknng_fetch_query()`, `ducknng_close_query()`, and `ducknng_cancel_query()`
  - Remaining work:
    - bind session ownership to a real client identity model
    - add a SQL-side Arrow batch decoder or higher-level row-decoding helper for session fetch payloads
    - decide whether `ducknng_query_rpc()` should later be rebuilt as a convenience wrapper over the session family
- [~] Add the codec framework for body and Arrow extension serde.
  - Current state:
    - `ducknng_list_codecs()` lists the built-in content-type driven providers
    - `ducknng_parse_body(body, content_type)` parses raw bodies through built-in providers
    - `ducknng_ncurl_table(...)` performs HTTP/HTTPS and parses successful response bodies by `Content-Type`
    - JSON uses DuckDB JSON functions in memory; Arrow IPC still uses nanoarrow plus the stable manual mapping layer
    - CSV, TSV, and Parquet are recognized but use the generic `body BLOB` fallback until a scalarfs-style memory filesystem/provider path is available
  - Remaining work:
    - decide whether user-defined hooks such as `ducknng_register_codec` and `ducknng_unregister_codec` belong in the sealed API
    - keep user-defined Arrow extension serde blocked until ownership, security, and the future DuckDB Arrow seam are clear enough not to freeze an unsafe callback ABI
- [ ] Surface pipe events / readiness notifications.
  - Blocker: depends on the async/aio/runtime event model and broader session cleanup strategy.
- [ ] Split `src/ducknng_sql_api.c` into smaller modules.
  - Blocker: still worth doing, but much safer after the Arrow/session/aio shape stabilizes so files are not split and then immediately re-merged during deeper rewrites.

## Validation for this checklist pass

- [x] `make release`
- [x] `make test_release`

## Current blockers to report upstream

1. **Current DuckDB-facing Arrow work stays on manual nanoarrow mappings.** The implementation no longer compiles unstable or deprecated DuckDB Arrow entrypoints, so any future Arrow re-plumb must wait for a non-deprecated seam or be abandoned in favor of maintaining the explicit mappings.
2. **Session-family work is only partially complete.** This pass already has real service-owned query session scaffolding, registry-visible `query_open` / `fetch` / `close` / `cancel` methods, and SQL-visible wrappers, but a bare `session_id` protocol without concrete owner rules is still not acceptable as the final multi-client design.
3. **HTTP / HTTPS transport adapters are landed synchronously but the async scope is still open.** The transport-family boundary is explicit in docs and code, `ducknng_start_http_server(...)` is implemented, and the synchronous request/RPC/session helpers route over HTTP and HTTPS, but any HTTP aio helpers still need an explicit scope decision.
4. **Codec work should not be built on undocumented mapping behavior.** If the project continues using the current manual nanoarrow route, codec decisions should sit on top of explicit tested mappings rather than implicit assumptions about a future Arrow helper path.
5. **Generic client socket TLS dialing is implemented, but the supported transport matrix still needs durable documentation.** Listener-side TLS, one-shot req/rep TLS, and socket-handle dialing now share the same TLS-config handle model, including `wss://`, but the final sealed examples/docs set should stay explicit about what is supported where.
