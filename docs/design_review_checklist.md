# ducknng design review checklist

This checklist tracks the implementation status of the recommendations in `docs/design_review.md`. It separates work that is already landed, work completed in the current pass, and items blocked by deeper architectural replacement work.

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
- [x] Make the docs/examples/catalog match the TLS-handle-only startup/request model.
  - `README.Rmd`
  - generated `README.md` / `README.html`
  - `function_catalog/functions.yaml`
  - generated `function_catalog/functions.md` / `functions.tsv`
  - SQLLogicTests and `test/rpc_smoke.R`
- [x] Land the first real unstable-ABI Arrow conversion rewrite.
  - The unstable ABI choice is now justified by implementation, not only by metadata.
  - Row-bearing RPC export no longer relies solely on the older hand-built row encoder on this path.
  - The current landed rewrite uses DuckDB unstable Arrow conversion APIs directly in the extension:
    - `duckdb_result_get_arrow_options()`
    - `duckdb_to_arrow_schema()`
    - `duckdb_data_chunk_to_arrow()`
  - This is the current real bridge from DuckDB result chunks to Arrow IPC batches for row-returning RPC replies.

## Partial / clarified but not fully solved

- [~] Make `ducknng_open_socket()` honest.
  - Current state: docs and catalog now say clearly that only `req` is implemented.
  - Remaining work: either implement the broader NNG protocol set or rename/split the client socket API around actual protocol support.
- [~] Keep structured-vs-raw helper duplication under explicit review.
  - Current state: signatures were unified and transport reach no longer diverges.
  - Remaining work: decide whether to delete the raw or structured twins entirely.

## Blocked by larger architectural replacement work

- [~] Replace the hand-rolled Arrow encode/decode path with DuckDB-native Arrow C API plumbing.
  - Current state: the row-bearing RPC export path now uses DuckDB unstable Arrow conversion APIs for schema and batch conversion.
  - Landed implementation:
    - `duckdb_result_get_arrow_options()`
    - `duckdb_to_arrow_schema()`
    - `duckdb_data_chunk_to_arrow()`
  - Remaining work:
    - replace the remaining manual Arrow ingress/decode path with DuckDB-native Arrow scan APIs
    - remove the remaining mirrored/manual conversion logic that still exists outside the landed export path
- [ ] Implement the session query family.
  - `query_open`
  - `fetch`
  - `close`
  - `cancel`
  - Blocker: the session ownership and lifecycle model is still not concretized enough for safe multi-client semantics, and it should be built on the Arrow re-plumb rather than the current manual path.
- [ ] Add SQL-visible async/aio request handles.
  - `ducknng_request_aio`
  - `ducknng_aio_ready`
  - `ducknng_aio_collect`
  - `ducknng_aio_cancel`
  - `ducknng_aio_drop`
  - Blocker: needs a runtime-owned aio registry, completion signaling, and lifecycle rules that do not yet exist.
- [ ] Add the codec framework for user-defined Arrow extension serde.
  - `ducknng_register_codec`
  - `ducknng_unregister_codec`
  - `ducknng_list_codecs`
  - Blocker: codec hooks should land on top of the future DuckDB-native Arrow path, not the current hand-rolled encoder/decoder.
- [ ] Surface pipe events / readiness notifications.
  - Blocker: depends on the async/aio/runtime event model and broader session cleanup strategy.
- [ ] Split `src/ducknng_sql_api.c` into smaller modules.
  - Blocker: still worth doing, but much safer after the Arrow/session/aio shape stabilizes so files are not split and then immediately re-merged during deeper rewrites.
- [ ] Expand the public protocol surface beyond current REQ client + REP server support.
  - Blocker: requires deliberate API design for `pub/sub`, `push/pull`, `pair`, `bus`, and `surveyor/respondent`, not just opening more NNG sockets.

## Validation for this checklist pass

- [x] `make release`
- [x] `make test_release`
- [x] `make rdm`

## Current blockers to report upstream

1. **Arrow re-plumbing is now partially landed, not merely aspirational.** The important change in this pass is that the unstable ABI stance is now justified by implementation: the row-bearing RPC export path uses DuckDB unstable Arrow conversion APIs directly through `duckdb_ext_api`, specifically `duckdb_result_get_arrow_options()`, `duckdb_to_arrow_schema()`, and `duckdb_data_chunk_to_arrow()`. The earlier attempt against deprecated `duckdb_query_arrow_schema()` / `duckdb_query_arrow_array()` wrappers was not the right seam here and led to a crash, so that path was discarded. What remains blocked is the rest of the rewrite: Arrow ingress/scan-side plumbing and deletion of the remaining manual conversion code outside the landed export path.
2. **Session-family work is only partially unblocked.** This pass added real service-owned query session scaffolding and registry-visible `query_open` / `fetch` / `close` / `cancel` methods, but a bare `session_id` protocol without concrete owner rules is still not acceptable as the final multi-client design.
3. **Async/aio work is blocked on runtime lifecycle design.** SQL-visible futures need a registry, cleanup policy, and completion signaling layer first.
4. **Codec work should not be built on the remaining manual Arrow paths.** It belongs after the rest of the Arrow replacement, not before it.
5. **Full multi-protocol NNG exposure is a product-level API redesign.** It is larger than a cleanup patch and should be scoped deliberately. During this pass, a first client-side broadening attempt showed a lower-level blocker too: the current vendored/build-linked NNG artifact for this extension does not export additional protocol entry points such as `nng_sub0_open` into the final extension load path, so simply widening the SQL surface is not enough.
