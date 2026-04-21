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
- Added extension metadata files `description.yml` and `description.yaml`.
- Added excluded platform metadata for wasm and Windows targets while native Linux development is underway.
- Added initial SQLLogicTest coverage in `test/sql/ducknng_server_start.test`.
- Added initial `function_catalog/functions.yaml` metadata plus generated markdown and TSV catalogs.
- Added a versioned RPC envelope with a method name, flags, error field, and payload length instead of the older ad hoc opcode frame.
- Added a registry-backed built-in method surface with `manifest` and `exec` descriptors.
- Added `docs/security.md` and `docs/registry.md` as binding design and implementation contracts.
- Added a project-local Pi skill at `.pi/skills/ducknng-rpc-framework/` for protocol, registry, session, and security work in this repo.
- Added `test/rpc_smoke.R` plus `make rpc_smoke` to validate manifest discovery and Arrow-metadata `exec` replies over real NNG REQ/REP.
- `exec` request payloads are now Arrow IPC tables containing `sql` and `want_result` fields.
- `manifest` replies are now JSON exported from the runtime method registry.
- `exec` metadata replies are now Arrow IPC generated with vendored nanoarrow C.
- Unary `exec(..., want_result = true)` now returns Arrow IPC row payloads for the current scalar subset: BOOLEAN, signed/unsigned integers, FLOAT/DOUBLE, VARCHAR, and BLOB.
- Added client-side SQL helpers `ducknng_remote_manifest(url)` and `ducknng_remote_exec(url, sql)` so DuckDB can request manifest and metadata-only exec operations from another ducknng-compatible service.
- Wider row-result type coverage, a remote table-function client path, and session-based query streaming are still pending.
- The docs contract for the session query family now fixes the intended lifecycle: `query_open` returns JSON control metadata and a session id, `fetch` is the only row-bearing method, `close` is the normal cleanup path, and `cancel` is best-effort until the implementation can bind sessions to a concrete owner identity.

## Planned next steps

- Add `ducknng_servers()` table-function introspection.
- Add phase-2 `EXEC` request handling.
- Add vendored `mbedTLS` and turn on TLS transport support.
- Add nanonext-based README examples and interop tests.
- Add Arrow IPC ingress/egress and streaming query sessions using the documented `query_open` / `fetch` / `close` / `cancel` contract and owner-checked session lifecycle.
