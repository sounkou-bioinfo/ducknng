# News

## ducknng 0.1.0

- Renamed the template extension to `ducknng`.
- Added a phase-1 pure C runtime keyed by DuckDB database handle.
- Added a real NNG REP listener lifecycle with one context, one AIO, and one worker thread.
- Added phase-1 SQL control functions:
  - `ducknng_server_start(...)`
  - `ducknng_server_stop(name)`
- Added phase-1 service lifecycle scaffolding while remote SQL protocol work is still in progress.
- Added `ducknng_servers()` table-function introspection over the per-database runtime service registry.
- `ducknng_server_start(...)` now creates the requested number of REP contexts on one REP socket instead of hard-coding a single worker.
- Vendored `nng` and `nanoarrow` as third-party dependencies.
- Added `README.Rmd` and `make rdm` for generated documentation.
- Added extension metadata file `description.yml`.
- Added excluded platform metadata for wasm and Windows targets while native Linux development is underway.
- Added initial SQLLogicTest coverage in `test/sql/ducknng_server_start.test`.
- Added initial `function_catalog/functions.yaml` metadata plus generated markdown and TSV catalogs.
- Added a versioned RPC envelope with a method name, flags, error field, and payload length instead of the older ad hoc opcode frame.
- Added a registry-backed built-in method surface with `manifest` and `exec` descriptors.
- `manifest` now remains the only always-on built-in RPC method; `exec` must be registered explicitly with `ducknng_register_exec_method()`.
- Added SQL-visible method registry administration with `ducknng_register_exec_method()`, `ducknng_unregister_method(name)`, `ducknng_unregister_family(family)`, and `ducknng_list_methods()`.
- Added `docs/security.md` and `docs/registry.md` as binding design and implementation contracts.
- Added a project-local Pi skill at `.pi/skills/ducknng-rpc-framework/` for protocol, registry, session, and security work in this repo.
- Added `test/rpc_smoke.R` plus `make rpc_smoke` to validate manifest discovery and Arrow-metadata `exec` replies over real NNG REQ/REP.
- `exec` request payloads are now Arrow IPC tables containing `sql` and `want_result` fields.
- `manifest` replies are now JSON exported from the runtime method registry.
- `exec` metadata replies are now Arrow IPC generated with vendored nanoarrow C.
- Unary `exec(..., want_result = true)` now returns Arrow IPC row payloads for the current scalar subset: BOOLEAN, signed/unsigned integers, FLOAT/DOUBLE, VARCHAR, and BLOB.
- Added client-side SQL helpers `ducknng_remote_manifest(url)` and `ducknng_remote_exec(url, sql)` so DuckDB can request manifest and metadata-only exec operations from another ducknng-compatible service.
- Added non-throwing result-table companions `ducknng_remote_manifest_result(url)` and `ducknng_remote_exec_result(url, sql)` so client-side transport and protocol failures can be handled in-band as rows.
- Added SQL-native req-style client handle helpers `ducknng_socket(protocol)`, `ducknng_dial(socket_id, url, timeout_ms)`, `ducknng_request_socket(socket_id, payload, timeout_ms)`, `ducknng_request(url, payload, timeout_ms)`, `ducknng_close(socket_id)`, and `ducknng_sockets()`.
- Added non-throwing raw request companions `ducknng_request_result(url, payload, timeout_ms)` and `ducknng_request_socket_result(socket_id, payload, timeout_ms)`.
- Added `ducknng_remote(url, sql)` as the current unary row-reply client table function, exposing Arrow IPC row replies as DuckDB tables for the currently supported unary row subset.
- Wider row-result type coverage and session-based query streaming are still pending.
- The docs contract for the session query family now fixes the intended lifecycle: `query_open` returns JSON control metadata and a session id, `fetch` is the only row-bearing method, `close` is the normal cleanup path, and `cancel` is best-effort until the implementation can bind sessions to a concrete owner identity.

## Planned next steps

- Add `ducknng_servers()` table-function introspection.
- Add phase-2 `EXEC` request handling.
- Add vendored `mbedTLS` and turn on TLS transport support.
- Add nanonext-based README examples and interop tests.
- Add Arrow IPC ingress/egress and streaming query sessions using the documented `query_open` / `fetch` / `close` / `cancel` contract and owner-checked session lifecycle.
