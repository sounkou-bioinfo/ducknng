# ducknng design review

This document is a point-in-time review of the `ducknng` extension against the goals declared in `AGENTS.md`, `docs/protocol.md`, and `docs/registry.md`, using the reference projects vendored under `.sync/` as comparators. It identifies bloat, inconsistency, and simplification opportunities, and proposes concrete next steps for the items the current design leaves implicit — in particular what "async" should mean at the SQL surface, what NNG's actual data and readiness primitives are, and how a SQL-UDF-driven custom serializer framework would plug into the existing Arrow IPC payload path.

The reference model for the transport-facing surface is `nanonext` (`.sync/nanonext`), which is a direct NNG binding with a dual functional/OO interface, explicit aio handles, condition variables, pipe notifications, and TLS configuration handles. The reference model for the RPC direction is `mangoro` (`.sync/mangoro`), which layers a thin envelope and Arrow IPC payload over `nanonext` sockets. `ducknng` aspires to be both at once, inside a DuckDB extension. This review is framed against that aspiration rather than against a minimal extension that only happens to speak NNG.

## Source-tree shape

The current C tree is 5,536 lines. The distribution is unbalanced:

| File | LOC | Role |
| --- | ---: | --- |
| `src/ducknng_sql_api.c` | 2613 | every SQL-visible function, all `bind`/`init`/`scan` glue, TLS scalars, socket lifecycle, Arrow-schema-to-logical-type mapping |
| `src/ducknng_ipc_out.c` | 482 | Arrow IPC encoding |
| `src/ducknng_registry.c` | 365 | method descriptor registry + manifest JSON |
| `src/ducknng_service.c` | 365 | REP listener, aio worker loop, service lifecycle |
| `src/ducknng_runtime.c` | 307 | runtime container, service/socket/TLS tables |
| `src/ducknng_ipc_in.c` | 251 | Arrow IPC decoding |
| `src/ducknng_nng_compat.c` | 233 | NNG compatibility layer (socket/listener/TLS) |
| `src/ducknng_methods.c` | 202 | built-in method descriptors and handlers (`manifest`, `exec`) |
| `src/ducknng_util.c` | 118 | misc helpers |
| `src/ducknng_wire.c` | 80 | envelope frame encode/decode |
| `src/ducknng_extension.c` | 31 | entry point |
| `src/ducknng_duckdb_streaming_compat.c` | 21 | version shim |
| `src/ducknng_session.c` | 1 | empty — `#include "ducknng_session.h"` only |

`ducknng_sql_api.c` is 47% of the codebase. Several modules promised by the public docs are absent or stub-only. `ducknng_session.c` is a single include line; the session-query family documented in `docs/protocol.md` is a specification without implementation.

## Alignment with stated goals

| Contract item | State | Evidence |
| --- | --- | --- |
| Method descriptor + registry + bulk register/unregister | Present | `src/include/ducknng_registry.h`, `src/ducknng_registry.c` |
| Manifest derived from registry | Present | `ducknng_method_registry_manifest_json`, called from `manifest` handler |
| Wire envelope (version, type, flags, name, error, payload, LE ints, UTF-8 strings) | Present | `src/ducknng_wire.c`, `src/include/ducknng_wire.h` |
| Arrow IPC payload via nanoarrow on `exec` | Present | `ducknng_method_exec_handler` (`methods.c:52`) |
| URL-scheme autodetection for transports | Partial | `nng_compat.c` does it; SQL surface still exposes TLS-specific overloads |
| Session family `query_open` / `fetch` / `close` / `cancel` | Not implemented | `methods.c:192` only registers `manifest` and `exec`; `session.c` empty |
| nanonext-style client ergonomics | Shallow | socket + dial + close exist; no aio, no pipe events, no cv bridge |
| TLS material from files and from in-memory PEM | Present | `ducknng_tls_config_from_files`, `ducknng_tls_config_from_pem`, `ducknng_self_signed_tls_config` |
| Multi-client dispatcher over NNG ctx + aio | Present | `ducknng_rep_worker_main` in `service.c:150` |
| Structured error reply separate from handler code | Present | `ducknng_method_reply_set_error` + reply flag validation in `service.c:59-67` |

The registry, manifest, envelope, Arrow IPC path, and dispatcher skeleton all match the docs. The two main gaps are the session family being paper-only and the SQL surface not expressing the nanonext ergonomic model the docs say it should.

## Bloat and duplication

### Raw/structured twins (highest-impact redundancy)

Every RPC-adjacent operation is registered twice, once as a scalar returning the raw reply frame as BLOB and once as a table function returning a structured row:

| Raw (scalar `BLOB`) | Structured (table) |
| --- | --- |
| `ducknng_get_rpc_manifest_raw(url)` | `ducknng_get_rpc_manifest(url)` |
| `ducknng_run_rpc_raw(url, sql)` | `ducknng_run_rpc(url, sql)` |
| `ducknng_request_raw(url, payload, timeout)` and `ducknng_request_raw(url, payload, timeout, tls_config_id)` | `ducknng_request(url, payload, timeout)` |
| `ducknng_request_socket_raw(socket_id, payload, timeout)` | `ducknng_request_socket(socket_id, payload, timeout)` |

`ducknng_decode_frame(BLOB)` already exists as a separate table function (`sql_api.c:2548`). That means every structured twin is equivalent to `ducknng_decode_frame(..._raw(...))` plus a method-specific projection. This duplication adds, per twin, one `*_bind_data` struct, one `*_bind` function, one `*_scan` function, one `*_destroy_bind_data` destructor, one `register_*_table_named` helper, one registration call, and one parallel error path that is guaranteed to drift. Concretely this accounts for roughly 600–800 lines in `ducknng_sql_api.c`.

Recommendation: keep only the raw scalars plus `ducknng_decode_frame`, or keep only the structured tables. Do not keep both. The raw-plus-decode shape is strictly more composable: users can request once and decode many times, can route the raw frame through other storage or logging paths, and can build higher-level table functions in SQL itself.

### Overload-driven TLS surface

`ducknng_start_server` is registered twice with different arities:

- `ducknng_start_server(name, listen, contexts, recv_max_bytes, session_idle_ms, tls_cert_key_file, tls_ca_file, tls_auth_mode)` (8 args)
- `ducknng_start_server(name, listen, contexts, recv_max_bytes, session_idle_ms, tls_config_id)` (6 args)

`ducknng_request_raw` is registered twice as well — a 3-arg plaintext form and a 4-arg form whose only new parameter is `tls_config_id`.

`AGENTS.md:5` explicitly identifies this pattern as the anti-pattern to avoid: "transport is chosen by the URL scheme, not by a proliferation of transport-specific helper functions." TLS is transport configuration. The right signature is a single operation that takes a `tls_config_id` (0 meaning plaintext), and the PEM/file inputs stay behind the dedicated `ducknng_tls_config_from_{files,pem,self_signed}` constructors.

Recommendation:

1. Delete the 8-arg `ducknng_start_server`. Force callers through `ducknng_tls_config_from_files` or `ducknng_tls_config_from_pem` first.
2. Delete the 3-arg `ducknng_request_raw`. A `tls_config_id` of `0` is sufficient to express plaintext.
3. Give `ducknng_request` (table form) the same `tls_config_id` argument its raw counterpart exposes. The table and raw shapes should never diverge on what transports they can reach.

### Inline-duplicated glue

Ten destructor stubs (`destroy_servers_bind_data`, `destroy_sockets_bind_data`, `destroy_tls_configs_bind_data`, `destroy_query_rpc_bind_data`, `destroy_manifest_result_bind_data`, `destroy_exec_result_bind_data`, `destroy_request_bind_data`, `destroy_frame_decode_bind_data`, plus their `_init_data` counterparts — `sql_api.c:303-405`) are structurally identical.

Ten `register_named_*_table` helpers (`sql_api.c:2366-2566`) differ only in table name, parameter type list, and bind/init/scan function pointers.

Recommendation: one generic `register_table_function(con, name, const duckdb_type *param_types, size_t n_params, bind_fn, init_fn, scan_fn)` and a typed `bind_data_destroy` macro. This collapses two ~200-line stretches into table-driven registration. The server, socket, and TLS list tables additionally share the "row vector materialized at bind time, emitted N per scan" idiom — factor that into a single helper and the three listing implementations drop to the column extractors only.

### Single-file concentration

`ducknng_sql_api.c` mixes four concerns: server control scalars, client socket lifecycle, TLS config scalars, and RPC helpers. The natural split is the one the SQL surface already presents:

- `ducknng_sql_servers.c` — `start_server`, `stop_server`, `list_servers`.
- `ducknng_sql_sockets.c` — `open_socket`, `dial_socket`, `close_socket`, `list_sockets`, `send`, `recv`.
- `ducknng_sql_tls.c` — `tls_config_from_files`, `tls_config_from_pem`, `self_signed_tls_config`, `drop_tls_config`, `list_tls_configs`.
- `ducknng_sql_rpc.c` — `get_rpc_manifest*`, `run_rpc*`, `request*`, `query_rpc`.
- `ducknng_sql_frame.c` — `decode_frame` and any future frame-inspection helpers.

Each of those files would be in the 200–500 LOC range. The Arrow-schema-to-logical-type routine currently at `sql_api.c:1219` belongs in a new `ducknng_arrow_types.c` shared with the session-family fetch handler.

## Inconsistencies

- Allocator discipline leaks. `service.c:228, 333` use `free()` for `svc->resolved_listen_url`, while the rest of the service struct uses `duckdb_free`. Pick one. NNG-allocated strings returned by `nng_listener_get_addr` must be freed through whatever allocator NNG uses; wrap the call in the nng compatibility layer so the choice is made once.
- `ducknng_open_socket` accepts the parameter name `protocol` but rejects anything other than `"req"` (`sql_api.c:910`). The surface pretends to be generic and is not. Either implement the other protocols the signature advertises or narrow the accepted set and rename the scalar so the lie is not baked into the public catalog.
- Dispatcher accepts `MANIFEST` and `CALL` only; `RESULT`, `ERROR`, and `EVENT` types are defined on the wire but rejected on input (`service.c:23-31`). That is correct for a REQ/REP-only server today, but `docs/protocol.md` reserves `EVENT` for PUB/SUB. Reconcile: either gate `EVENT` behind the (not-yet-built) PUB socket path, or remove it from the current enum so the surface is not misread as capable of event dispatch.
- `ducknng_stop_server` sets `running = 0` and tears down the listener and contexts, but the runtime's services table is not pruned on stop. `ducknng_list_servers` will keep displaying a stopped service until process exit. Decide whether `stop` means "remove" or "halt but keep registered" and make that explicit.
- Reply-flag validation in `service.c:59` rejects unsupported flag bits, but the corresponding `accepted_request_flags` mask on `manifest` is `0` while `manifest` accepts no flags; the `exec` mask is `0` (`methods.c:146`) while clients today never set request flags. That is fine as a default but means the protocol doc's flag-driven control path is not being exercised anywhere. Add at least one flag on a method that genuinely uses it so the validator is not dead code.
- Heap vs. static descriptor ownership. `ducknng_method_exec` and `ducknng_method_manifest` are file-scope statics registered by pointer. The registry stores `const ducknng_method_descriptor **` (`registry.h:85`), which works for statics but cannot hold handler-owned descriptors built at runtime. If dynamic method registration becomes part of the public contract, the registry needs a clear ownership model (copy in, or reference-count, or refuse runtime descriptors). The docs should pick one.

## What is "async" at the SQL surface?

The honest answer today is "nothing visible." The server side does use NNG contexts, aio handles, worker threads, and condition variables (`service.c:80-199`), so concurrent REP handling scales with `ncontexts`. But the client-facing SQL API is entirely synchronous:

- `ducknng_request`, `ducknng_request_socket`, `ducknng_run_rpc`, `ducknng_get_rpc_manifest`, `ducknng_query_rpc` all call `ducknng_req_transact` synchronously (`sql_api.c:800`). The DuckDB execution thread blocks for up to `timeout_ms` per row.
- There is no SQL-visible aio handle. You cannot kick off N remote calls and join them. You cannot poll for readiness. You cannot cancel an in-flight request without closing the socket.
- `ducknng_session.c` is empty, so the session-query family — which is the natural home for long-running asynchronous workloads (large result sets, incremental fetch, cancellation) — is unbuilt.
- There is no equivalent of nanonext's `send_aio` / `recv_aio` / `request` (aio form) / `collect_aio` / `unresolved` / `pipe_notify` / `cv`.

If async is a project goal, the minimum SQL surface to mirror nanonext inside DuckDB is:

1. `ducknng_request_aio(url, payload, timeout_ms, tls_config_id) → UBIGINT aio_id` — scalar. Returns immediately, backed internally by `nng_aio_alloc` + a completion callback that stashes the reply in a runtime-owned aio table.
2. `ducknng_aio_ready(aio_id) → BOOLEAN` — scalar. Non-blocking readiness check. Used in correlated subqueries or predicates.
3. `ducknng_aio_collect(aio_ids, wait_ms) → TABLE(aio_id UBIGINT, ok BOOLEAN, error VARCHAR, payload BLOB)` — table function. Blocks up to `wait_ms` for the first-ready aio in the set and returns structured rows. Repeated calls drain the set; pass `wait_ms = 0` for a pure poll.
4. `ducknng_aio_cancel(aio_id) → BOOLEAN` — scalar. Calls `nng_aio_cancel` on the pending request.
5. `ducknng_aio_drop(aio_id) → BOOLEAN` — scalar. Releases a resolved aio slot. Needed because SQL has no destructors.

Implementation notes for the aio table inside the runtime:

- Slot layout: `{aio_id, aio_handle, state, reply_msg, error, owner_context}`.
- State transitions: `pending → ready → consumed`. Only `consumed` can be freed.
- Completion callback signals a runtime-wide condition variable (already available via `ducknng_cond_*` in `thread.h`). `ducknng_aio_collect` blocks on that cv, checks the requested id set each wake, and returns when any are ready or the deadline passes.
- The aio table is locked by the same runtime mutex that guards services and client sockets. Readers of `list_sockets`-style tables already operate under that mutex, so adding aio slots to the runtime is not a new locking story.

The session family (below) composes naturally on top of this. `query_open` returns a session id and, optionally, an aio id so the first batch can be prefetched while the client keeps working.

## Session-query family: the biggest missing piece

`docs/protocol.md:§21` declares `query_open`, `fetch`, `close`, `cancel` as a four-method contract. `docs/registry.md:§11` specifies descriptor-level session behaviors. `src/ducknng_session.c` is empty and no session descriptors are registered. The gap between documentation and implementation is the largest single coherence issue in the codebase.

A concrete implementation plan:

1. Populate `src/ducknng_session.c` with lifecycle helpers: `ducknng_session_create(runtime, conn, stmt)`, `ducknng_session_fetch_batch(session, max_rows, max_bytes, out_ipc, out_eos)`, `ducknng_session_close(session)`, `ducknng_session_cancel(session)`.
2. Back the session store with a runtime-owned table keyed by `session_id`, mirroring the existing service/client-socket/tls-config tables. Use `duckdb_prepared_statement` + `duckdb_pending_result` for incremental execution; the stream-to-IPC path already exists in `ducknng_ipc_out.c`.
3. Register four descriptors in `ducknng_methods.c`: `query_open` (opens session, Arrow in, JSON out), `fetch` (requires session, JSON in, Arrow or JSON out), `close` (closes session, JSON both sides), `cancel` (cancels session, JSON both sides). Use the existing `session_behavior` enum so the dispatcher can resolve the session pointer before handler entry, per `docs/registry.md:§17`.
4. Extend the dispatcher so that when `requires_session`, `opens_session`, `closes_session`, or `cancels_session` is set, it performs the session lookup (or allocation) and populates `req_ctx.session` before calling the handler. Today the context's `session` field is declared (`registry.h:40`) and always NULL.
5. Idle cleanup: the `session_idle_ms` field on `ducknng_service` is already accepted at service creation time but not enforced. Run a per-service reaper thread (or hook into the existing REP worker wake cycle) that closes sessions past their idle deadline.
6. Add a SQL surface: `ducknng_query_open(url, sql)`, `ducknng_fetch(url, session_id, max_rows, max_bytes)`, `ducknng_close_query(url, session_id)`, `ducknng_cancel_query(url, session_id)`. Prefer these as table functions returning either `(session_id, state, schema_json)` or the next Arrow batch.

Once the session family exists, the current `query_rpc` table function (which does a one-shot request expecting an Arrow IPC reply) becomes a thin convenience wrapper around `query_open` + `fetch` + `close`, and the unary `exec` stays as the fast path for small metadata-only calls.

## NNG's actual data and readiness primitives

For the "readiness primitive" question, a short inventory of what NNG exposes and which parts ducknng surfaces today:

| NNG primitive | Purpose | Surfaced by ducknng? | Surfaced by nanonext? |
| --- | --- | --- | --- |
| `nng_socket` | protocol-typed endpoint | Yes (internal + `ducknng_open_socket`) | Yes (`socket()`) |
| `nng_dialer` / `nng_listener` | outbound/inbound connection objects | Partially (listener inside service; dialer via `dial_socket`) | Yes (`dial()` / `listen()` return dialer/listener) |
| `nng_ctx` | multi-request state carrier on a single socket | Internal in service | Yes (`context()`) |
| `nng_aio` | asynchronous operation handle with timeout and completion callback | No SQL surface | Yes (`send_aio`, `recv_aio`, `request`) |
| `nng_msg` | zero-copy message with header/body and refcount | Internal | Yes (partial, through serialization) |
| `nng_pipe` and `NNG_PIPE_EV_*` | peer-level connect/disconnect notifications | Not exposed | Yes (`pipe_notify()`) |
| `nng_stat_*` | socket-level statistics | Not exposed | Yes (`stats()`) |
| `nng_tls_config` | TLS material and auth mode | Yes (configs table + three constructors) | Yes (`tls_config()`) |

The only real readiness primitive NNG itself provides is `nng_aio` — a future with a timeout and a completion callback. Everything else (polling for data, waiting on peer connect, fanning out to N requests) is expressed in terms of aio handles plus NNG's internal pipe events. `nanonext`'s `cv()` / `wait()` / `until()` / `pipe_notify()` are all constructions layered on top of `nng_aio` + condition variables; they are not separate NNG objects.

ducknng has already built the condition-variable substrate internally (`ducknng_cond_*` in `src/include/ducknng_thread.h`) for the REP worker loop. Surfacing an aio handle and a pipe-notify table on top of that substrate is additive work, not a new subsystem.

The pipe-notify gap matters more than it first appears. Over `tcp://` a session cannot discover that its client disconnected except by timing out on `fetch`. A `ducknng_pipe_events` table function, or a push to a DuckDB `APPENDER` registered with the runtime, lets the session layer reap dead sessions promptly and lets clients reason about peer liveness without synthesizing heartbeats.

## Custom serializers and deserializers via SQL UDFs

The cleanest way to layer user-provided serde on top of the current wire is to ride Arrow extension types and keep the codec logic in DuckDB SQL rather than in the C envelope. This avoids adding a parallel "ducknng custom type" mechanism and keeps the payload interoperable with any other Arrow-speaking consumer.

### Design

1. **Wire format unchanged.** Arrow IPC already supports per-field metadata including the well-known `ARROW:extension:name` and `ARROW:extension:metadata` keys. An Arrow consumer that does not know the extension reads the storage type and ignores the extension; a consumer that knows it decodes further. ducknng does not need a new envelope type byte or flag.
2. **Codec catalog.** A runtime-owned registry with columns `(name VARCHAR PRIMARY KEY, storage_type VARCHAR, encode_fn VARCHAR, decode_fn VARCHAR, arrow_extension_name VARCHAR)`. Exposed through:
   - `ducknng_register_codec(name, storage_type, encode_fn, decode_fn, arrow_extension_name)` — scalar, returns BOOLEAN.
   - `ducknng_unregister_codec(name)` — scalar.
   - `ducknng_list_codecs()` — table function.
3. **Binding to DuckDB functions.** `encode_fn` and `decode_fn` are names of existing DuckDB scalar functions (built-in, UDF, or registered from another extension). The registrar resolves them through `duckdb_bind_function` or the equivalent catalog lookup and caches the resolved function pointer by name. If the function is dropped, the next encode/decode call fails with a structured error and the codec entry is removed.
4. **Encode path hook.** In `ducknng_result_to_ipc_stream` (`ipc_out.c`), before handing a chunk to nanoarrow, inspect the column catalog entry or a companion projection specification. For each column tagged with a codec, project the column through `encode_fn(col)` to produce the storage-typed column, and attach `ARROW:extension:name = arrow_extension_name` to the resulting ArrowSchema field.
5. **Decode path hook.** In `ducknng_query_rpc_assign_column` (`sql_api.c:1280`) and in the forthcoming `fetch` handler, inspect each incoming ArrowSchema field for the extension-name metadata. If present and matching a registered codec, bind the reader to the storage type (the usual path) and then project the resulting DuckDB vector through `decode_fn` on its way into the output chunk.
6. **Scoping.** The codec catalog is per-runtime, not per-connection. That keeps the encode side deterministic across REP workers. A per-connection override is possible later using the session context but is not required in v1.

### Example

```sql
-- Register a codec that stores as BLOB but presents as STRUCT
SELECT ducknng_register_codec(
    'latlon',
    'BLOB',
    'encode_latlon',      -- existing scalar: STRUCT(lat DOUBLE, lon DOUBLE) -> BLOB
    'decode_latlon',      -- existing scalar: BLOB -> STRUCT(lat DOUBLE, lon DOUBLE)
    'ducknng.ext.latlon'
);

-- Server-side query whose output is encoded for the wire
SELECT point::latlon AS point FROM events;

-- Client-side fetch transparently decodes back to STRUCT
SELECT * FROM ducknng_query_rpc('tcp://host:7777', 'SELECT point::latlon FROM events');
```

The `::latlon` suffix is one possible surface; an alternative is an explicit `ducknng_codec_encode('latlon', col)` scalar that users wrap around columns in the server query. Either way, the codec table is the single source of truth and no new bytes enter the envelope.

### What this unlocks

- User-defined compression: register `encode_fn = zstd_compress(col)`, `decode_fn = zstd_decompress(col)` for large text columns.
- Domain types: geometry, money, tensors stored in their storage type and reified at the SQL boundary.
- Schema migration: a codec can present a v2 struct while the storage type remains a v1 layout, so wire compatibility survives server upgrades.

## Simplification checklist

Ordered by payoff:

1. **Delete the raw-or-structured twin.** Keep one axis of SQL RPC helpers, with `ducknng_decode_frame` as the decoder. Removes ~600–800 LOC and eliminates a structural drift surface.
2. **Collapse `ducknng_start_server` and `ducknng_request_raw` overloads.** One signature each, with `tls_config_id = 0` for plaintext. Aligns with the URL-scheme autodetection rule in `AGENTS.md`.
3. **Split `ducknng_sql_api.c`** along servers/sockets/tls/rpc/frame lines and extract the generic `register_table_function` and `bind_data_destroy` helpers. Target: no file over 600 LOC.
4. **Implement the session family or remove it from `docs/protocol.md`.** The current state is the worst of both. The implementation sketch above is the smaller of the two moves.
5. **Decide the async story, in or out.** If in, add `ducknng_request_aio`, `ducknng_aio_collect`, `ducknng_aio_ready`, `ducknng_aio_cancel`, `ducknng_aio_drop`, backed by a runtime-owned aio table. If out, remove the word from design discussion and be explicit that NNG is used synchronously at the SQL layer and concurrently only across independent clients.
6. **Prune `ducknng_open_socket` to honest behavior.** Either accept all protocols nanonext does or narrow the accepted set and rename until you do.
7. **Unify allocator discipline.** One of `duckdb_malloc`/`duckdb_free` or the system allocator, centrally. Wrap any NNG-owned strings behind the compat layer so the choice is not re-made per caller.
8. **Fix service teardown ownership.** Decide whether `ducknng_stop_server` removes the entry from the runtime table or leaves it. Reflect the choice in `ducknng_list_servers`.
9. **Land the codec framework.** Three new scalars, two hook points in the existing IPC encode/decode, no envelope change. This is the cheapest way to give users a real type-extension story without growing the wire.
10. **Surface pipe events.** A `ducknng_pipe_events(service_name, wait_ms)` table function backed by NNG's pipe event callbacks turns peer connect/disconnect into SQL-visible rows, which is the readiness primitive sessions will need for prompt cleanup.

## The Arrow path is hand-rolled and ignores DuckDB's C Arrow interface

This is a larger finding than the duplication issues above and deserves its own section. The current implementation of Arrow IPC encode and decode does not use any of the DuckDB C extension Arrow entry points, and it does not borrow any of the ideas prototyped in `.sync/duckdb-nanoarrow`. The consequence is a slower, narrower, and more error-prone Arrow path than the one DuckDB already ships to extension authors.

### What DuckDB's C extension API already offers

The extension-visible C API in `duckdb_capi/duckdb_extension.h` exposes a full Arrow interoperability surface via the standard Arrow C Data Interface:

- `duckdb_query_arrow(connection, query, out_result)` — run a query and obtain a streaming Arrow result handle.
- `duckdb_query_arrow_schema(result, out_schema)` — pull the ArrowSchema for a running query result.
- `duckdb_query_arrow_array(result, out_array)` — pull the next ArrowArray (one batch) from a running query result. Returns `DuckDBSuccess` and a NULL array at end of stream.
- `duckdb_result_arrow_array(result, chunk, out_array)` — convert a single `duckdb_data_chunk` to an ArrowArray without copying.
- `duckdb_execute_prepared_arrow(stmt, out_result)` — same, on a prepared statement.
- `duckdb_arrow_scan(connection, table_name, arrow_stream)` — register an external `ArrowArrayStream` as a DuckDB table.
- `duckdb_arrow_array_scan(connection, table_name, schema, array, out_stream)` — same, given schema + single array.

Every one of these is declared in the extension header and reachable through the `duckdb_ext_api` function table that ducknng already binds (`duckdb_extension.h:943-956`).

### What ducknng does instead

`src/ducknng_ipc_out.c` re-implements the encode side from scratch:

- `ducknng_build_result_schema` walks `duckdb_column_logical_type` per column and translates to nanoarrow type IDs inside a hand-coded switch (`ipc_out.c:33-76`). This misses TIMESTAMP, DATE, TIME, INTERVAL, DECIMAL, UUID, HUGEINT, LIST, STRUCT, MAP, UNION, and ENUM entirely. The function simply returns an error on any type outside the 13 scalar cases it knows about.
- `ducknng_append_vector_value` reads each row of each vector individually (`ipc_out.c:107-158`) using per-element appenders such as `ArrowArrayAppendInt`, `ArrowArrayAppendString`, and `ArrowArrayAppendBytes`. That is O(nrows × ncols) virtual dispatch and per-row validity checks where DuckDB's native path produces an ArrowArray from a whole chunk in a vectorized conversion.
- `ducknng_append_chunk_to_arrow` then calls `ArrowArrayFinishElement` once per row (`ipc_out.c:197`). For a 100k-row result this is 100k function calls the native path skips.

`src/ducknng_sql_api.c` re-implements the decode side equally manually:

- `ducknng_arrow_schema_to_logical_type` (`sql_api.c:1219`) is the symmetric inverse of the encode-side switch and inherits the same type-coverage gap.
- `ducknng_query_rpc_bind` parses the IPC stream header with nanoarrow, reads exactly one batch, and stores the `ArrowSchema` and `ArrowArray` on the bind data. It does not support multi-batch replies (`sql_api.c:1408`).
- `ducknng_query_rpc_assign_column` (`sql_api.c:1280`) manually copies each column's values into DuckDB vectors using `duckdb_vector_assign_string_element`-style calls. Nulls are translated by hand. 128 lines.

So the extension has two parallel type systems, one in `ipc_out.c` for encode and one in `sql_api.c` for decode, and both cover the same small subset of DuckDB types. Any new type support requires editing both.

### What `.sync/duckdb-nanoarrow` does that applies here

`.sync/duckdb-nanoarrow/src/writer/to_arrow_ipc.cpp` is the reference for what the encode side should look like. Summarised:

1. `to_arrow_ipc(subquery)` is registered as a table function. Bind time stores the logical types. Init time creates an `ArrowAppender` and a `ColumnDataCollectionSerializer`. Scan time feeds each incoming `DataChunk` into the appender and, once the chunk threshold is reached, finalises the batch and emits `(ipc BLOB, header BOOLEAN)` rows.
2. The appender is DuckDB's native `ArrowAppender`, which already knows how to emit every DuckDB logical type into the Arrow C Data Interface. No per-type switch is needed in the extension.
3. The IPC writer in `arrow_stream_writer.cpp` handles schema emission, dictionary batches, and batch framing. The extension does not hand-roll IPC framing.
4. Reading is symmetric. `src/ipc/stream_reader/ipc_buffer_stream_reader.cpp` turns a BLOB into an `ArrowArrayStream`, and `src/scanner/scan_arrow_ipc.cpp` registers that stream as a DuckDB table function so the decoded batches flow straight into the query pipeline.

Crucially, every step operates at the batch level, never per row, and reuses DuckDB's own Arrow conversion routines. The extension contributes framing and scanning; it does not contribute type translation.

Note that duckdb-nanoarrow uses the DuckDB C++ API, not the C extension API. The C++ `ArrowAppender` / `ArrowConverter::ToArrowArray` / `ArrowConverter::ToArrowSchema` are the primitives there. ducknng is committed to the pure C extension API (`AGENTS.md:11`), so the equivalent primitives at its disposal are the C-API Arrow entry points enumerated above, specifically `duckdb_result_arrow_array` and `duckdb_query_arrow_schema`. Those are the types-system-aware, vectorized replacements for the hand-coded path currently in `ipc_out.c` and `sql_api.c`.

### Proposed Arrow re-plumbing

Encode side, replacing `ducknng_build_result_schema` + `ducknng_append_chunk_to_arrow`:

1. Run the query via `duckdb_query_arrow(svc->rt->init_con, sql, &arrow_result)` (or `duckdb_execute_prepared_arrow` once prepared statements are in scope).
2. Obtain the schema via `duckdb_query_arrow_schema(arrow_result, &schema)`. This is a full, DuckDB-native ArrowSchema — every type, not 13.
3. Loop: `duckdb_query_arrow_array(arrow_result, &array)`. A NULL array signals end of stream.
4. Feed each `(schema, array)` pair into nanoarrow's IPC writer to produce the outgoing IPC stream bytes.
5. Release the arrow handle with `duckdb_destroy_arrow`.

The only piece of encoding code ducknng has to own is "IPC framing of a sequence of batches." Everything type-related is delegated to DuckDB.

Decode side, replacing `ducknng_query_rpc_bind` + `ducknng_arrow_schema_to_logical_type` + `ducknng_query_rpc_assign_column`:

1. Parse the incoming IPC blob into an `ArrowArrayStream` with nanoarrow's IPC reader.
2. Register that stream as a transient DuckDB table via `duckdb_arrow_scan(svc->rt->init_con, temp_name, stream)`.
3. Execute `SELECT * FROM <temp_name>` from the bind/scan path and emit rows as usual.

At that point `ducknng_query_rpc` is no longer a custom single-batch decoder — it is a thin adapter that lands Arrow IPC bytes as a DuckDB table using DuckDB's own scanner. Multi-batch support comes for free. Support for LIST, STRUCT, TIMESTAMP, DECIMAL, ENUM, and every other DuckDB type comes for free. The decode-side error path collapses from 128 lines of per-column assignment into one call.

### Additional surfaces this unlocks

Once the encode side goes through `duckdb_query_arrow` and the decode side goes through `duckdb_arrow_scan`, several features become trivial or free:

- **Streaming replies for the session family.** `fetch` can pull one `duckdb_query_arrow_array` at a time, IPC-frame it, and return. The per-session server state is just the `duckdb_arrow` handle. No manual cursor bookkeeping, no per-chunk reinvention.
- **`COPY (<query>) TO 'tcp://host:port/method' (FORMAT ducknng)`.** Register a DuckDB `COPY` function whose writer is the same batch-loop above. Users get a SQL-native way to push query results across RPC.
- **`CREATE TABLE <name> AS FROM ducknng_query_rpc('tcp://...', '<sql>')`.** Already composable today but cheap and correct once decode goes through `duckdb_arrow_scan`, because the scanner integrates with the optimizer and respects projection pushdown.
- **External Arrow producers.** `duckdb_arrow_scan` accepts any `ArrowArrayStream`. The codec framework proposed earlier becomes a special case: a codec is just an `ArrowArrayStream` adapter that wraps the raw stream and projects extension-typed columns through `encode_fn` / `decode_fn`.
- **Zero-copy where possible.** `duckdb_result_arrow_array` converts a `duckdb_data_chunk` to an `ArrowArray` in place when the layout matches. The current per-row appender always copies.

### Why this is the biggest leverage item

The hand-rolled Arrow path is the primary limiter on what ducknng can honestly claim in `docs/types.md`. Today the advertised type subset has to match the 13 cases the switch statements cover, which rules out the entire DuckDB catalog of composite and temporal types. Swapping to the native Arrow entry points lifts that limit in a single change. It also removes one of the two symmetric maintenance burdens (encode-side switch vs. decode-side switch) and aligns ducknng's Arrow model with the Arrow model that every other DuckDB extension and every other Arrow-speaking client already uses.

This item should be ranked above the session family in the simplification checklist, because the session-family `fetch` handler will have to encode Arrow batches — and writing that handler against the current hand-rolled encode path would be bolting new work onto a foundation that needs to be replaced anyway.

### Updated simplification priority

Given this finding, the ordered list above should be re-read with a new item one:

0. **Replace `ducknng_ipc_out.c` and the decode half of `ducknng_sql_api.c` with `duckdb_query_arrow` + `duckdb_result_arrow_array` on the encode side and `duckdb_arrow_scan` on the decode side.** Preserve the nanoarrow IPC framing code (that is the part ducknng genuinely owns). Delete the per-type switches and per-row appenders. Everything downstream — session `fetch`, codec framework, type-table expansion in `docs/types.md` — depends on this.

## Type mapping: the current matrix is wrong, narrow, and duplicated

The current type translation lives in two places that must stay in sync by hand and neither matches what `docs/types.md` claims. With no backward compatibility obligations, the right move is to throw both switches out and redefine the contract in terms of DuckDB's own native Arrow converter. This section critiques the current mapping per-type and proposes concrete alternatives for every case.

### Where the mapping lives today

- Encode (DuckDB → Arrow): `ducknng_set_arrow_schema_type` + `ducknng_append_vector_value` in `src/ducknng_ipc_out.c:33-158`. 13 cases on the schema side, 13 on the value side, anything else errors.
- Decode (Arrow → DuckDB): `ducknng_arrow_schema_to_logical_type` + `ducknng_query_rpc_assign_column` in `src/ducknng_sql_api.c:1219-1406`. Same 13 cases, mirrored.

The request/metadata sub-schemas (`ducknng_build_exec_request_schema`, `ducknng_build_metadata_schema`, `ipc_out.c:9-31`) are a third, smaller hard-coded schema pair that does not even participate in the general type system.

### The claim-vs-reality gap

`docs/types.md:7` declares the "initial support target" as "Boolean values, signed integers, unsigned integers, floating-point values, UTF-8 strings, binary values, dates, microsecond-resolution times, microsecond-resolution timestamps, structs, and lists." The implementation supports only the first six families. `DATE`, `TIME`, `TIMESTAMP`, `STRUCT`, and `LIST` are in the doc and not in the code.

`docs/types.md:13` acknowledges this for the unary path by narrowing the active subset, but then `docs/types.md:11` still promises "canonical output" policy across temporal types the code cannot produce. The doc makes a contract the runtime cannot honour.

The zero-backward-compatibility window is the right moment to fix the doc and the code in the same pass by redefining the contract as a delegation instead of a switch.

### Per-DuckDB-type critique and proposed mapping

For each DuckDB logical type, the current status, the problem with the current choice, and the proposed canonical Arrow mapping are listed below. The proposed mappings match what DuckDB's own C-API `duckdb_result_arrow_array` emits, so adopting them means the encode side gets them for free once the Arrow path is re-plumbed (see the "Arrow path is hand-rolled" section above).

#### Primitives that are already handled but could be better

| DuckDB type | Current Arrow mapping | Problem | Proposed mapping |
| --- | --- | --- | --- |
| `BOOLEAN` | `bool` via `ArrowArrayAppendInt` | Appender treats each value as an integer and re-bit-packs; strictly slower than the vectorised converter and loses the ability to share the validity buffer. | `bool` (bit-packed), produced via `duckdb_result_arrow_array`, validity buffer copied directly. |
| `TINYINT`..`BIGINT` | `int8`..`int64` via `ArrowArrayAppendInt(int64_t)` | Every value is widened to `int64_t` at the appender boundary and narrowed on the other side. Wasted work on large result sets. | `int8`..`int64` produced from the matching DuckDB vector via zero-copy buffer sharing. |
| `UTINYINT`..`UBIGINT` | `uint8`..`uint64` via `ArrowArrayAppendUInt(uint64_t)` | Same width-widening story; additionally, `UBIGINT` is truncated when passed through `ArrowArrayAppendUInt` for values above `INT64_MAX` on some nanoarrow versions. | `uint8`..`uint64` zero-copy. |
| `FLOAT` | `float` via `ArrowArrayAppendDouble` | Every float is promoted to double and demoted back. Loss of bit-exactness on denormals and a pointless cost. | `float32` zero-copy. |
| `DOUBLE` | `float64` via `ArrowArrayAppendDouble` | Works but still per-row dispatch. | `float64` zero-copy. |
| `VARCHAR` | `utf8` (32-bit offsets) via `ArrowArrayAppendString` | Arrow 32-bit offsets cap a batch at 2 GiB total string bytes. DuckDB internally represents strings as views (inline + pointer), so emitting `utf8` forces a concat-copy pass. | Default to `utf8_view` on DuckDB ≥ 1.1 (matches DuckDB's internal layout, zero-copy) and fall back to `utf8` only when the client manifest declares it cannot read views. `large_utf8` is not useful here — `utf8_view` covers the large case without the allocator penalty. |
| `BLOB` | `binary` (32-bit offsets) via `ArrowArrayAppendBytes` | Same 2 GiB batch ceiling and same view-vs-offset story. | `binary_view` by default; `binary` only when views are not negotiated. |

#### Types declared in the doc but not implemented

| DuckDB type | `docs/types.md` claim | Implementation status | Proposed mapping |
| --- | --- | --- | --- |
| `DATE` | maps to Arrow `date32` | Rejected at encode and decode | `date32[days]`. DuckDB stores days since epoch internally, so this is a direct buffer copy. |
| `TIME` | maps to Arrow `time64[us]` | Rejected | `time64[us]`. DuckDB stores microseconds since midnight; direct copy. |
| `TIMESTAMP` | maps to Arrow `timestamp[us]` | Rejected | `timestamp[us]` (no tz). Direct copy. |
| `STRUCT` | "in scope because immediately useful" | Rejected | Arrow `struct` with recursively mapped children. `duckdb_result_arrow_array` handles this by construction; the switch-based path cannot because it cannot enumerate children. |
| `LIST` | "in scope" | Rejected | Arrow `list<T>` (32-bit offsets) for lists up to 2 GiB per batch; `list_view<T>` for DuckDB internal layouts where elements are not contiguous. Prefer `list_view` when the optimiser emits non-contiguous lists to avoid a copy. |

#### Types that are neither in the doc nor in the code

These are the DuckDB types that fall out of the switch today and are also silent in `docs/types.md`. Each one is a gap a real client will hit fast.

| DuckDB type | Why it matters | Proposed mapping |
| --- | --- | --- |
| `TIMESTAMP_S`, `TIMESTAMP_MS`, `TIMESTAMP_NS` | SQL input often carries seconds/ms/ns timestamps. | `timestamp[s]`, `timestamp[ms]`, `timestamp[ns]` with matching unit. Do **not** normalise to `timestamp[us]`; lossy conversion is worse than keeping the unit. |
| `TIMESTAMP WITH TIME ZONE` | Distinct from naive timestamp. | `timestamp[us, UTC]` with the tz field of ArrowSchema set to `"UTC"`. DuckDB stores as UTC internally, so this is honest and the client is free to apply a session tz. |
| `TIME WITH TIME ZONE` | No direct Arrow equivalent. | Struct `{ time: time64[us], offset_seconds: int32 }` with `ARROW:extension:name = "duckdb.time_tz"`. Clients that understand the extension reconstruct the value; others see the struct. |
| `INTERVAL` | DuckDB stores `(months, days, micros)`. | `interval_month_day_nano` (Arrow's 128-bit interval). Exact fit; no information loss. |
| `DECIMAL(p, s)` with p ≤ 18 | Stored as `int64` internally. | `decimal128(p, s)`. Not `int64`, because the precision/scale metadata matters to clients. |
| `DECIMAL(p, s)` with 18 < p ≤ 38 | Stored as `int128`. | `decimal128(p, s)`. Direct buffer copy. |
| `HUGEINT` (`int128`) | Distinct from DECIMAL(38,0). | `decimal128(38, 0)` by default with `ARROW:extension:name = "duckdb.hugeint"`. Alternative: `fixed_size_binary(16)` + extension tag — equally faithful but less idiomatic. |
| `UHUGEINT` (`uint128`) | DuckDB 1.x type. | `fixed_size_binary(16)` + `ARROW:extension:name = "duckdb.uhugeint"`. `decimal128` cannot express the full unsigned range. |
| `UUID` | Common. | `fixed_size_binary(16)` + `ARROW:extension:name = "arrow.uuid"` (the standard canonical Arrow extension). |
| `ENUM` | DuckDB's compact categorical. | `dictionary<int8|int16|int32, utf8>` with the dictionary index width chosen to match DuckDB's underlying physical type. The category table emits once per stream; batches carry only indices. |
| `MAP` | DuckDB MAP is an ordered key-value list. | Arrow `map<K, V>` with the keys-sorted flag copied from DuckDB's metadata. |
| `LIST` fixed-length variants / `ARRAY(T, N)` | DuckDB ARRAY(T, N) is distinct from LIST(T). | `fixed_size_list<T, N>`. |
| `UNION` | Rare but present. | Arrow `dense_union<...>`. If exotic, defer behind an extension tag; do not silently lower to `VARCHAR`. |
| `BIT` / `VARINT` | DuckDB 1.x. | `binary_view` + `ARROW:extension:name = "duckdb.bit"` / `"duckdb.varint"`. No standard Arrow equivalent. |
| `JSON` | DuckDB stores as `VARCHAR` with a tag. | Same storage as `VARCHAR` (`utf8` or `utf8_view`) plus `ARROW:extension:name = "arrow.json"`. The canonical Arrow extension covers this exactly. |
| Named SQL `ROW` / anonymous struct | Identical to STRUCT on the wire. | Arrow `struct` with the inferred field names. |
| Nested lists / structs of any depth | Currently cannot be represented. | Recursively. The delegation-to-DuckDB approach handles this trivially because `duckdb_result_arrow_array` emits nested ArrowArrays by construction. |

### Problems that are not per-type but architectural

Five problems are systemic to the current matrix and will not be fixed by expanding the switch:

1. **Encode and decode are two copies of the same map.** Any new type has to be added twice. The switches already disagree in error wording (`ipc_out.c:153` vs `sql_api.c:1275`) and will continue to drift. The delegation approach removes the duplication: encode is `duckdb_result_arrow_array` and decode is `duckdb_arrow_scan`.
2. **Per-row virtual dispatch.** Every cell passes through an `ArrowArrayAppend*` call on encode and an `ArrowArrayViewGet*Unsafe` call on decode. A 1M-row, 10-column result is 20M function calls. Vectorised buffer sharing cuts that to O(ncols × nbatches).
3. **No schema-level metadata.** Nothing in the current code attaches `ARROW:extension:name`, timezone strings, decimal precision, or dictionary indices. Every type with richer-than-primitive semantics is therefore either unrepresentable or silently lossy. The proposal above makes extension metadata first-class because extension codecs and JSON/UUID/HUGEINT all ride the same mechanism.
4. **No nullability declaration on the schema side.** `ducknng_set_arrow_schema_type` never sets `schema->flags |= ARROW_FLAG_NULLABLE`. The wire therefore claims every column is non-nullable, then writes null validity bitmaps anyway. Clients that respect the schema will treat nulls as corruption. Fix: all data columns are nullable by default; non-null fields exist only on hand-declared request schemas (`sql`, `want_result`).
5. **Canonical-output policy is prescriptive in the wrong direction.** `docs/types.md:11` says "prefer ordinary UTF-8 over large UTF-8" and "prefer microsecond timestamps." Both rules force lossy or allocation-heavy rewrites of whatever DuckDB actually produced. The replacement policy is the opposite: emit exactly what DuckDB's Arrow converter produces for each logical type; let clients that cannot consume `utf8_view` or `timestamp[s]` negotiate through the manifest.

### Proposed policy for `docs/types.md` (replacement)

Since there is no backward compatibility to keep, `docs/types.md` should be rewritten around three rules rather than a prescriptive subset:

1. **Delegation rule.** The canonical DuckDB → Arrow mapping is whatever `duckdb_result_arrow_array` emits, as of the DuckDB C extension API version ducknng links against. `docs/types.md` enumerates the mapping table (for clarity and versioning) but the implementation never re-encodes.
2. **Extension-metadata rule.** Types that have no canonical Arrow equivalent (HUGEINT, UHUGEINT, UUID, JSON, BIT, VARINT, TIME WITH TIME ZONE, domain-specific user types) ride on `ARROW:extension:name` metadata with documented names under the `duckdb.*` and `ducknng.*` namespaces, plus the standard `arrow.json` and `arrow.uuid` where applicable. A client that understands the extension reconstructs the value; a client that does not sees the storage type and stays correct.
3. **Preservation rule.** The server preserves the DuckDB-emitted Arrow representation without normalisation. No silent unit conversion, no silent width narrowing, no forced `utf8` vs `utf8_view` rewrite. Clients that need a specific canonical subset send an Arrow IPC request carrying their accepted-type catalog in the envelope, and the server projects to that subset explicitly. Normalisation becomes an opt-in request, not a server-side default.

The manifest changes accordingly. Each method's `response_schema_json` stops enumerating a small supported list and instead declares "any DuckDB type, serialised as per the DuckDB → Arrow mapping in `docs/types.md`, with extension tags listed in `extensions`." The small list surfaces in the manifest only when a method explicitly constrains it (for example the `exec` metadata reply, whose schema is fixed by the handler and not by a SQL query).

### Method-schema critique (exec and metadata)

The two hand-declared schemas in `ipc_out.c:9-31` also need attention:

- `ducknng_build_exec_request_schema` declares `sql: utf8` and `want_result: bool`, both non-nullable. The `want_result` field is a footgun: it forces callers to decide in advance whether the statement will produce rows, and the handler rejects results when `want_result = false` (`methods.c:98`). Propose: delete `want_result`. The handler already has enough information from `duckdb_result_return_type` to decide whether to emit an Arrow stream or a metadata record. If the client truly wants the server to skip Arrow encoding for DDL, expose it as a request flag in the envelope (one bit, not a column) rather than a payload field.
- `ducknng_build_metadata_schema` declares `rows_changed: uint64`, `statement_type: int32`, `result_type: int32`. The two `int32` fields are really enums. Propose: `dictionary<int8, utf8>` in Arrow, with the dictionary carrying the DuckDB statement-type and result-type names. Clients then see `"SELECT"` or `"INSERT"` without a side lookup table, and the numeric stability of DuckDB's internal enum values stops being a wire-level concern.

### Updated simplification priority

With the type mapping added, the ordering stands:

0. Re-plumb the Arrow path through `duckdb_query_arrow` / `duckdb_result_arrow_array` / `duckdb_arrow_scan`. This alone obsoletes the two switches and unlocks every type in the per-type table above.
0a. In the same pass, rewrite `docs/types.md` around delegation / extension-metadata / preservation. Remove the narrow prescriptive subset.
0b. Drop `want_result` from the `exec` request schema; replace with an envelope flag if needed.
0c. Emit `statement_type` / `result_type` in the `exec` metadata reply as dictionary-encoded enums rather than bare `int32`.
1. (existing) Delete raw/structured twins.
2. (existing) Collapse TLS overloads.
3. (existing) Split `ducknng_sql_api.c`.
4. (existing) Implement or remove the session family.
... and so on.

## Transport coverage: every NNG protocol is a first-class citizen

The current project claims to be an NNG extension but only opens two of NNG's eight protocols: REQ on the client side (`nng_compat.c:128`) and REP on the server side (`nng_compat.c:127`). `ducknng_open_socket` explicitly rejects any protocol string other than `"req"` (`sql_api.c:910`), and the service layer always calls `ducknng_rep_socket_open` with no parameterisation (`service.c:266`). `docs/protocol.md:§18` mentions PUB/SUB as future work but says nothing about PUSH/PULL, PAIR, BUS, SURVEY/RESPONDENT. The `ducknng_transport_pattern` enum in `registry.h:8-11` enumerates only two values.

NNG's own protocol set is fixed and small: `pair` (1:1 symmetric), `req`/`rep` (RPC with state-machine retries), `pub`/`sub` (topic fan-out), `push`/`pull` (load-balanced pipeline), `surveyor`/`respondent` (one-to-many RPC with deadline), and `bus` (many-to-many broadcast). nanonext exposes every one (`.sync/nanonext/R/socket.R` lists eleven socket types including the `poly` mode). ducknng should too, because transport pattern is a SQL-observable property, not a library implementation detail.

### What needs to change

1. **Expand `ducknng_transport_pattern`** to the NNG set. The descriptor field becomes the set of patterns a method is valid on (a bit mask), not a single value. `manifest` and `server_info` are valid on REQ/REP and SURVEY/RESPONDENT. Event-emitting methods are valid on PUB/SUB. A work-queue method like `ingest` is valid on PUSH/PULL. Methods with no reply channel (PUB, PUSH) have `response_payload_format = NONE` by construction and the dispatcher enforces that at registration time.

2. **Extend `nng_compat.c`** with an open function per protocol: `ducknng_pair_socket_open`, `ducknng_pub_socket_open`, `ducknng_sub_socket_open`, `ducknng_push_socket_open`, `ducknng_pull_socket_open`, `ducknng_surveyor_socket_open`, `ducknng_respondent_socket_open`, `ducknng_bus_socket_open`. One line each, wrapping `nng_pair0_open`, `nng_pub0_open`, etc. Also wrap `nng_sub0_socket_subscribe`, `nng_surveyor0_set_survey_time`, and the small handful of protocol-specific option setters.

3. **Parameterise the service layer.** `ducknng_service_create` takes a `protocol` field alongside `listen_url`. `ducknng_service_start` opens the right socket type. For pure data-plane patterns (PUB, PUSH, BUS broadcast), the worker loop is send-only or fire-and-forget; no method dispatcher runs. For reply-capable patterns (REP, RESPONDENT), the existing `ducknng_rep_worker_main` is generalised over `nng_ctx_recv`/`nng_ctx_send` (which is pattern-agnostic already) and dispatches through the method registry. For PAIR and BUS, where either side can send, the worker alternates or runs two aios concurrently.

4. **Parameterise the client layer.** `ducknng_open_socket(protocol)` accepts all eight strings. Each returns a `socket_id` tagged with its protocol. Protocol-specific operations become separate scalars keyed by what NNG actually supports:
   - `ducknng_subscribe(socket_id, topic)` — only valid on SUB sockets.
   - `ducknng_survey(socket_id, payload, deadline_ms) → TABLE(responder_id, ok, error, payload)` — surveys return multiple rows.
   - `ducknng_publish(socket_id, topic, payload)` — PUB.
   - `ducknng_push(socket_id, payload)` — PUSH.
   - `ducknng_pull(socket_id, timeout_ms) → TABLE(ok, error, payload)` — PULL.
   - `ducknng_send(socket_id, payload)` / `ducknng_recv(socket_id, timeout_ms)` — PAIR, BUS.
   Unsupported operations fail at bind time against the socket's declared protocol rather than at runtime with a generic NNG error.

5. **Document method-to-pattern validity.** `docs/protocol.md` needs a table: which of the eight patterns each built-in method runs on, and which patterns the envelope is defined for. The envelope itself is already pattern-agnostic (just a sequence of bytes), but a method that requires a reply cannot be advertised on a pattern that has no reply channel.

### SQL surface after the change

```sql
-- Listen on each pattern
SELECT ducknng_start_server('rpc',      'tcp://0:7777', protocol => 'rep');
SELECT ducknng_start_server('events',   'tcp://0:7778', protocol => 'pub');
SELECT ducknng_start_server('ingest',   'tcp://0:7779', protocol => 'pull');
SELECT ducknng_start_server('surveys',  'tcp://0:7780', protocol => 'surveyor');

-- Clients speak the matching pattern
SELECT ducknng_open_socket('sub') AS sid;
SELECT ducknng_subscribe(sid, 'metrics.');
SELECT * FROM ducknng_recv(sid, 5000);   -- returns one row per delivered event

SELECT ducknng_open_socket('surveyor') AS sid;
SELECT ducknng_dial_socket(sid, 'tcp://host:7780', 1000);
SELECT * FROM ducknng_survey(sid, 'status', 2000);   -- returns N responder rows
```

### Pattern x method compatibility matrix (initial)

| Pattern | Method dispatch | Method model | Notes |
| --- | --- | --- | --- |
| `req` (client) / `rep` (server) | Yes | Request / reply, one round trip. Current path. | Canonical RPC. |
| `surveyor` (client) / `respondent` (server) | Yes | One request, N replies, server-side deadline. | Session family cannot ride this; results are per-responder and bounded by deadline. |
| `pub` / `sub` | No (one-way) | Server emits events, clients subscribe by topic prefix. | Method "handlers" become event producers. Dispatcher is send-only. |
| `push` / `pull` | Partial | One request, no reply; load-balanced across PULL workers. | Appropriate for ingest-style methods. `response_mode = NONE`. |
| `pair` | Yes (symmetric) | Either side can send, single peer. | Useful for long-lived bidirectional RPC over one connection. |
| `bus` | No (broadcast) | Every peer gets every message. | Useful for cluster gossip; no method model attaches. |

The method registry already has `transport_pattern` and `accepted_request_flags` fields; expanding them to a bitmask and enforcing the compatibility matrix at registration time is a two-line code change plus the policy table.

## Method registration as the default unit of exposure; `exec` is the dangerous special case

The current server registers two built-in methods at startup: `manifest` and `exec` (`methods.c:192-200`). `manifest` is harmless. `exec` is not. It accepts any SQL string from any connected client, with no authentication, and executes it on `svc->rt->init_con` with the full privileges of the DuckDB process:

- `SELECT * FROM read_csv('/etc/shadow')` — file exfiltration.
- `COPY (SELECT ...) TO '/tmp/poc' (FORMAT 'csv')` — arbitrary file write.
- `INSTALL httpfs; LOAD httpfs; SELECT * FROM read_parquet('https://attacker/x')` — outbound network call.
- `INSTALL <anything>; LOAD <anything>` — code execution via extension load.
- `PRAGMA database_size` / `SHOW TABLES` — full schema enumeration.

Even with TLS, this is a remote code execution surface. `exec` being always-registered by default is the single largest security bug in the current design. The fix is already half-built — the registry supports arbitrary `register` and `unregister` — but the SQL API has no way to register methods, no way to declare a body with typed parameters, and no way to restrict which methods are reachable on which patterns.

### Principles

1. **Default surface is empty.** The extension loads with only `manifest` registered. Any SQL-reachable method, including `exec`, is an explicit act of the administrator running `SELECT ducknng_register_method(...)`. Services refuse to start with zero methods only if the administrator has not opted into the empty-server mode.
2. **Methods bind to typed argument schemas.** The argument schema is Arrow, declared at registration time and enforced by the dispatcher before the handler runs. Clients can send only the declared columns, and the server rejects anything else. This is the only durable defence against SQL injection: the wire cannot carry a SQL string unless the method explicitly asks for one.
3. **Bodies are one of four shapes.** A method body is either (a) a parameterised SQL template bound against the declared argument schema, (b) a registered table function name called with the declared arguments, (c) a registered view referenced `FROM`, or (d) a C handler registered at extension load. `exec` is shape (e): "run the argument as SQL," which is the only shape that breaks the typed-argument guarantee and therefore the only shape that requires explicit policy.
4. **Policy travels with the method.** `requires_auth`, `max_request_bytes`, `max_reply_bytes`, `max_rows`, `max_execution_ms`, `read_only`, `allowed_schemas`, `allowed_patterns` all live on the descriptor and are enforced by the dispatcher before the handler sees the call.

### Proposed SQL API

```sql
-- Default registration: SQL-template method with typed parameters
SELECT ducknng_register_method(
    name              => 'orders.search',
    family            => 'app.orders',
    summary           => 'Find orders by customer',
    request_schema    => '{"fields":[
        {"name":"customer_id","type":"int64","nullable":false},
        {"name":"since","type":"timestamp[us]","nullable":true}
    ]}',
    body_kind         => 'sql_template',
    body              => 'SELECT id, total, created_at
                          FROM orders
                          WHERE customer_id = $customer_id
                            AND ($since IS NULL OR created_at >= $since)',
    response_schema   => NULL,  -- inferred from the prepared statement
    allowed_patterns  => ['rep', 'respondent'],
    max_request_bytes => 4096,
    max_reply_bytes   => 16 * 1024 * 1024,
    max_rows          => 100000,
    max_execution_ms  => 5000,
    read_only         => true,
    requires_auth     => false
);

-- Table-function method: thin wrapper over an existing table function
SELECT ducknng_register_method(
    name              => 'analytics.daily_summary',
    body_kind         => 'table_function',
    body              => 'daily_summary',   -- name of an existing DuckDB table function
    request_schema    => '{"fields":[{"name":"day","type":"date32","nullable":false}]}',
    read_only         => true
);

-- View-backed method: the view already expresses the full query
SELECT ducknng_register_method(
    name              => 'catalog.tables',
    body_kind         => 'view',
    body              => 'my_catalog.public_tables',   -- schema.view_name
    request_schema    => '{"fields":[]}',
    read_only         => true
);

-- The dangerous special case: enable exec, guarded
SELECT ducknng_register_exec_method(
    name              => 'exec',
    requires_auth     => true,
    max_request_bytes => 64 * 1024,
    max_execution_ms  => 30000,
    read_only         => true,         -- rejects DDL, COPY TO, INSTALL, LOAD, PRAGMA write
    forbid_extensions => true,         -- rejects INSTALL / LOAD even outside DDL
    forbid_file_io    => true,         -- rejects COPY TO/FROM and file reader functions
    allowed_patterns  => ['rep']        -- not on surveyor, not on pub/sub
);
```

`ducknng_register_exec_method` is a distinct scalar, not a shape of `ducknng_register_method`, so that grep-level auditing can find every place the unbounded-SQL door has been opened without walking every method descriptor.

### Enforcement points

The dispatcher (`service.c:10-78`) gets new pre-handler gates, in order, driven entirely by descriptor fields:

1. Pattern gate — reject if the incoming message arrived on a pattern not in `allowed_patterns`.
2. Size gate — already present (`service.c:44`).
3. Auth gate — already present as a stub (`service.c:48`); needs a real identity resolver wired to TLS peer certs and/or an RPC-level auth method.
4. Schema gate — parse the incoming Arrow IPC payload, verify its schema matches the declared `request_schema` field-by-field. Extra columns rejected. Missing non-null columns rejected. Wrong types rejected.
5. Binding gate — for SQL-template bodies, bind each schema field by name into the prepared statement's parameter slots. A missing parameter name is a registration-time error, not a runtime error.
6. Execution gate — `max_execution_ms` enforced via `duckdb_interrupt` on a timer; `max_rows` enforced by inspecting `duckdb_result_chunk_count` and truncating with a warning; `read_only` enforced by running the statement on a read-only `duckdb_connection` created once per method at registration time.
7. Extension and file-I/O gates — for `exec` only, the pre-parse rejects `INSTALL`, `LOAD`, `COPY`, `PRAGMA` whose second token is a write, and any function call whose name is in the blocklist (`read_csv`, `read_parquet`, `read_json`, `write_csv`, `write_parquet`, external HTTP, etc.). This is best-effort; the real mitigation is `read_only` + `allowed_extensions = []`, but the textual gate catches the common cases early.

### Read-only connections and per-method isolation

DuckDB has native support for read-only database opens (`duckdb_open_ext` with `access_mode = READ_ONLY`). The cleanest model:

- The runtime holds the main database handle.
- At method registration time, for `read_only` methods, open a second read-only connection to the same database (or to an attached read-only snapshot). Cache it on the descriptor.
- The handler uses the method's cached connection, not `svc->rt->init_con`.
- For write-capable methods, use the runtime's write connection — but `read_only = false` is not the default; the administrator must opt in.

This also neatly solves the concurrency problem in `ducknng_method_exec_handler` (`methods.c:81-129`), which holds `svc->mu` across the full query execution and serialises every REP worker. With per-method read-only connections, read methods no longer contend on the write lock.

### Listing, updating, unregistering from SQL

```sql
SELECT * FROM ducknng_list_methods();   -- name, family, body_kind, allowed_patterns, policy flags
SELECT ducknng_unregister_method('orders.search');
SELECT ducknng_unregister_family('app.orders');   -- uses the existing family-unregister path
SELECT ducknng_disable_method('exec');           -- flips `disabled = 1` without removing
```

These are all thin wrappers over the registry calls that already exist in C (`ducknng_method_registry_unregister`, `ducknng_method_registry_unregister_family`). The only genuinely new C work is the `sql_template` handler, the `table_function` handler, the `view` handler, and the bounded-`exec` handler.

### How this composes with the other proposals

- **Arrow re-plumbing.** Registered methods whose body is `sql_template` use `duckdb_prepare` + `duckdb_bind_*` + `duckdb_execute_prepared_arrow` (`duckdb_capi/duckdb_extension.h:521`). The response stream flows through the same `duckdb_query_arrow_array` loop the general encode path uses.
- **Session family.** `query_open` takes a *method name* plus arguments, not raw SQL. A server that has not registered an `exec`-shape method cannot have a session opened against arbitrary SQL. `query_open` against a registered method simply starts the prepared statement with the bound arguments and returns a session id; `fetch` pulls batches from the corresponding `duckdb_pending_result`.
- **Codec framework.** Codecs operate on typed columns; typed columns come from the declared request and response schemas. The registration path is the right place to attach per-method codecs ("column `payload` on the response is encoded by `zstd_blob`").
- **Transport coverage.** `allowed_patterns` is how a method declares that it is reachable only on REQ/REP, or on SURVEY/RESPONDENT, or as a PUB event. The registration-time rejection means the server never advertises a method on a pattern that cannot carry it.

### Migration

There is no backward compatibility to preserve, so the migration is a single-PR change:

1. Delete the always-on `exec` registration in `ducknng_register_builtin_methods` (`methods.c:192-200`). Keep `manifest`.
2. Move the current `ducknng_method_exec_handler` body into a new `exec` body kind, register it only when the administrator calls `ducknng_register_exec_method`.
3. Add `ducknng_register_method`, `ducknng_unregister_method`, `ducknng_unregister_family`, `ducknng_disable_method`, `ducknng_list_methods` to the SQL surface.
4. Extend the descriptor struct with `allowed_patterns` (bitmask), `max_rows`, `max_execution_ms`, `read_only`, `forbid_extensions`, `forbid_file_io`, `body_kind`, `body` (owned string), `request_schema_json_owned` (distinction from the existing static `request_schema_json` field).
5. Update `docs/protocol.md`, `docs/registry.md`, and `docs/security.md` to reflect that `exec` is opt-in and that typed-argument methods are the default exposure.

Priority: this ranks with the Arrow re-plumbing and type-mapping reform — all three are load-bearing for what `ducknng` claims to be in its own docs, and all three are blocked from landing cleanly by the current codebase concentrating its surface area in two hand-rolled switches and a pair of always-on handlers.

### Updated simplification priority (full)

0. Re-plumb the Arrow path through `duckdb_query_arrow` / `duckdb_result_arrow_array` / `duckdb_arrow_scan`.
0a. Rewrite `docs/types.md` around delegation + extension metadata + preservation.
0b. Drop `want_result` from the `exec` request schema.
0c. Emit `statement_type` / `result_type` in the `exec` metadata reply as dictionary-encoded enums.
0d. **Delete the default `exec` registration; introduce `ducknng_register_method` + `ducknng_register_exec_method` with typed argument schemas and per-method policy.**
0e. **Parameterise the service and client layers over all eight NNG protocols; extend `ducknng_transport_pattern` to a bitmask and enforce per-method `allowed_patterns`.**
1. Delete raw/structured RPC-helper twins.
2. Collapse TLS overloads on `ducknng_start_server` and `ducknng_request_raw`.
3. Split `ducknng_sql_api.c` along servers/sockets/tls/rpc/frame lines.
4. Implement or remove the session family.
5. Decide the async story in or out; if in, add aio handles and `ducknng_aio_collect`.
6. Make `ducknng_open_socket` honest about the protocol set it accepts.
7. Unify allocator discipline.
8. Fix service teardown ownership.
9. Land the codec framework via Arrow extension types.
10. Surface pipe events as a readiness primitive.

## End-product target: a push/pull DuckDB mesh (homebrew MotherDuck)

This section is the north star the rest of the design should be measured against. If `ducknng` can run a head DuckDB that accepts a SQL query, dispatches shards or replicated sub-queries to a fleet of worker DuckDBs over NNG, collects their Arrow IPC results, and serves the aggregated answer back to the original client, then the project's reason for existing is proven. Every earlier recommendation in this review — the Arrow re-plumbing, the typed method registration, the full NNG protocol set, the aio primitives, the session family, the codec framework — is load-bearing for that target. Stating the target explicitly lets us verify each subsystem is sized for it and not for a narrower RPC-only view.

### The cluster, abstractly

There are two roles, and a node can hold either or both:

- **Head.** Accepts client queries over REQ/REP. Holds the catalog of known workers. Decides how to distribute each query. Dispatches sub-queries. Assembles the final answer from worker Arrow streams. Returns rows to the client.
- **Worker.** Hosts a DuckDB instance and some local data (Parquet/CSV/native). Exposes a narrow, typed RPC surface the head uses to request shard execution. May also run a local copy of the head role for peer-to-peer.

There is nothing hosted, no external control plane, no proprietary network. Discovery is either static configuration or a PUB/SUB announce-and-subscribe exchange over the same NNG mesh. A cluster can live entirely in `inproc://` for a single-process test, entirely in `ipc://` for a laptop-local demo, or across `tls+tcp://` for a real deployment. The same code path, different URLs.

### Query lifecycle

1. Client → head, REQ/REP, typed method `ducknng.query` with `{sql: utf8, options: json}`. Not `exec` — `exec` is not registered by default and should not be reachable from remote clients in this topology.
2. Head parses the SQL, looks up data placement in its worker catalog, picks a distribution strategy:
   - *Replicate-and-reduce*: same SQL on every worker, union the per-worker results, run a final local aggregation. Appropriate when every worker can answer the query against its own local data.
   - *Shard-and-reduce*: each worker receives a filtered sub-query bound to the shard it owns (e.g. a time range, a hash bucket, a tenant id). Appropriate when data is range- or hash-partitioned.
   - *Shuffle-and-reduce*: two-stage. Each worker emits partial state keyed by the shuffle key; partial state is pushed to the responsible worker for that key; each worker completes the aggregation for keys it owns; head unions. Required for GROUP BY on keys whose placement is not aligned with the input partition.
3. Head issues N sub-queries as async RPCs. This is where `ducknng_request_aio` + `ducknng_aio_collect` pay for themselves: the head fires N calls, then collects them as they complete. No serial loop.
4. Each worker receives a typed method call (e.g. `ducknng.execute_shard`), binds the arguments against its prepared statement, produces an Arrow IPC stream from DuckDB's native `duckdb_execute_prepared_arrow`, and returns it.
5. Head decodes each worker reply via `duckdb_arrow_scan`, registering the stream as a transient DuckDB table named `_ducknng_worker_<worker_id>`. At this point the final aggregation is an ordinary local DuckDB query — `SELECT ... FROM (SELECT * FROM _ducknng_worker_0 UNION ALL ... UNION ALL SELECT * FROM _ducknng_worker_N) GROUP BY ...` — and DuckDB's own optimiser runs the reduce stage.
6. Head returns the aggregated Arrow stream to the client over its own REQ/REP reply.

### Why each proposed primitive is load-bearing here

| Primitive from earlier sections | Role in the mesh |
| --- | --- |
| `duckdb_query_arrow` / `duckdb_result_arrow_array` on workers | Produce correct, full-type-range Arrow batches from arbitrary worker SQL with zero hand-coded type switches. |
| `duckdb_arrow_scan` on head | Register each worker's reply as a DuckDB table so the reduce stage is plain SQL. This is the single most important primitive; without it the head is a custom result-merger. |
| Typed method registration + non-default `exec` | Workers advertise only `execute_shard(shard_id, bind_vars, sql_template_ref)` and similar safe methods. The head cannot ask a worker to run arbitrary SQL unless the worker has opted in. Cluster security reduces to "did any worker register `exec`?" |
| Session family (`query_open` / `fetch` / `close`) | Large reduce stages stream: head opens one session per worker, interleaves `fetch` calls, and emits aggregated batches to the client without materialising any worker's full result. |
| `ducknng_request_aio` + `ducknng_aio_collect` | Scatter-gather. Without aio the head either blocks sequentially on each worker or spawns a thread per worker. With aio it fires N and collects the first-ready, which is exactly the scheduling the reduce stage wants. |
| `ducknng_pipe_events` | Head notices worker disconnects immediately instead of discovering them on the next `fetch` timeout. A disconnected worker's in-flight shard is re-dispatched; its session is cancelled; the reduce plan adapts. |
| Full NNG protocol set | PUB/SUB for worker-availability announcements and schema change fan-out. PUSH/PULL for work queues (head pushes shard descriptions, idle workers pull — natural load balancing without a scheduler). SURVEY/RESPONDENT for "ask every worker, give me everything within 500 ms" control queries. REQ/REP for the actual per-shard RPC because the mesh needs a reliable per-shard reply channel. |
| Codec framework via Arrow extension types | Dictionary-encoded ENUMs, UUID, JSON, HUGEINT survive the head-worker round trip without being downgraded to strings. User-defined codecs let a worker compress a large column once and let the head decode lazily at reduce time. |

### Concrete primitive interactions

#### Worker registration and health

Each worker, at startup, dials the head's control-plane SUB socket and publishes:

```sql
-- On worker:
SELECT ducknng_start_server('rpc', 'tcp://0:7800', protocol => 'rep');
SELECT ducknng_open_socket('pub') INTO sid;
SELECT ducknng_dial_socket(sid, 'tcp://head:7999', 1000);
SELECT ducknng_publish(sid, 'ducknng.worker.up', encode_worker_info('w-03', 'tcp://worker03:7800'));
```

Head is a subscriber:

```sql
-- On head, once at startup:
SELECT ducknng_start_server('control', 'tcp://0:7999', protocol => 'sub');
SELECT ducknng_subscribe('control', 'ducknng.worker.');

-- Worker catalog is a table updated by a background task (or by a table
-- function that drains the sub queue on each query):
CREATE TABLE worker_catalog(worker_id VARCHAR, url VARCHAR, last_seen TIMESTAMPTZ);
```

Pipe events from the head's REP socket feed the same catalog: a worker that disconnects flips `last_seen` to the disconnect time and the planner stops routing to it.

#### Typed worker method

```sql
-- On every worker, at startup:
SELECT ducknng_register_method(
    name              => 'ducknng.execute_shard',
    family            => 'ducknng.cluster',
    request_schema    => '{"fields":[
        {"name":"shard_id","type":"utf8","nullable":false},
        {"name":"template_id","type":"utf8","nullable":false},
        {"name":"args","type":"map<utf8, utf8>","nullable":false}
    ]}',
    body_kind         => 'sql_template_registry',
    body              => 'worker_templates',   -- a DuckDB table mapping template_id to SQL
    allowed_patterns  => ['rep'],
    max_rows          => 10000000,
    max_execution_ms  => 60000,
    read_only         => true
);
```

The worker does not expose `exec`. The set of SQL templates it will run is a table the administrator controls, and the head addresses templates by id. Adding a new query shape to the cluster is a coordinated act: register the template on each worker, restart nothing.

#### Head planner (replicate-and-reduce)

The planner is a DuckDB table function the client never sees:

```sql
CREATE OR REPLACE MACRO ducknng.plan_replicate(template_id, args) AS (
    WITH workers AS (
        SELECT worker_id, url FROM worker_catalog WHERE last_seen > now() - INTERVAL 30 SECOND
    ),
    aio AS (
        SELECT worker_id,
               ducknng_request_aio(url, ducknng_build_call('ducknng.execute_shard',
                    struct_pack(shard_id := worker_id, template_id := template_id, args := args)),
                    30000) AS aio_id
        FROM workers
    ),
    results AS (
        SELECT aio.worker_id, c.ok, c.error, c.payload
        FROM aio,
             LATERAL ducknng_aio_collect(ARRAY_AGG(aio.aio_id OVER ()), 30000) c
        WHERE c.aio_id = aio.aio_id
    )
    SELECT * FROM results
);
```

Then the reduce stage registers each payload and runs plain SQL:

```sql
-- Conceptually; real impl uses a table function that does this without manual loops.
SELECT ducknng_arrow_scan_blob('_worker_0', (SELECT payload FROM results WHERE worker_id = 'w-00'));
SELECT ducknng_arrow_scan_blob('_worker_1', (SELECT payload FROM results WHERE worker_id = 'w-01'));

SELECT region, SUM(total)
FROM (SELECT * FROM _worker_0 UNION ALL SELECT * FROM _worker_1)
GROUP BY region;
```

The proposed `ducknng_arrow_scan_blob(name, blob)` is a thin scalar around `duckdb_arrow_scan` plus an `ArrowArrayStream` built from an in-memory IPC blob. The same mechanism underlies `ducknng_query_rpc` in the single-node case; it's the natural primitive both paths share.

#### Head planner (shard-and-reduce)

Same shape, different dispatch. `ducknng.plan_shard` reads the shard catalog (which shard is on which worker) and sends each worker a different `args` map:

```sql
SELECT worker_id,
       ducknng_request_aio(url,
           ducknng_build_call('ducknng.execute_shard',
               struct_pack(shard_id := shard_id, template_id := 'orders.by_day',
                           args := map_from_entries([('day', day::VARCHAR)]))),
           30000)
FROM shard_catalog
JOIN worker_catalog USING (worker_id)
WHERE day BETWEEN '2026-01-01' AND '2026-01-31';
```

Each worker gets exactly the shards it owns; the head receives per-shard Arrow batches; the reduce stage unions them.

#### Shuffle (two-phase GROUP BY)

Uses PUSH/PULL for the shuffle, not SURVEY. Phase one: each worker computes partial aggregates keyed by the shuffle key and, for each output row, pushes the row to the worker owning that key's hash bucket. Phase two: each worker pulls all rows destined for it, completes aggregation, and replies to the head.

```sql
-- Phase 1 on every worker:
SELECT ducknng_push('shuffle_out', hash_bucket(key), partial_row_ipc) ...

-- Phase 2 on every worker:
SELECT ducknng_pull('shuffle_in', timeout_ms => 5000);
-- Scan pulled rows, run the final aggregate, return via REP.
```

PUSH/PULL earns its place: the head is not in the critical path of the shuffle; workers talk worker-to-worker. The head only kicks off phase one and collects phase-two results. NNG's load-balanced PUSH delivery is the right semantics because each hash bucket has exactly one owner.

### What is still missing to reach the end product

Ranked from largest to smallest:

1. **A planner.** The above sketches are SQL-level; a real planner needs to parse an incoming user SQL, detect which tables live in the worker catalog, decide replicate vs shard vs shuffle, and rewrite into the three macros above. This is probably a separate extension that depends on `ducknng` rather than living inside it.
2. **Template registry semantics.** `body_kind = 'sql_template_registry'` proposed above is a body shape not in the earlier section. It means "look up the template text in a DuckDB table by id." Cheap to add once the four core body kinds are in place, but it is the sixth body kind, not one of the first four.
3. **Partial-aggregate combine rules.** `SUM(col)` is a trivial reduce (sum of partial sums), `COUNT(col)` is too (sum of partial counts), `AVG(col)` requires `(sum_of_sums) / (sum_of_counts)`, quantiles require `reservoir-merge` or approximate algorithms, `COUNT(DISTINCT)` requires HyperLogLog exchange. DuckDB already has HLL internally; the planner needs to know the rewrite rules.
4. **Worker failure handling.** Head detects a worker dying mid-query (via pipe events). What happens: re-dispatch its shards to a live peer? Fail the query? Return a partial answer with a warning column? This is a deliberate policy choice — the simplest correct initial behaviour is "fail fast, let the client retry," and the infrastructure needed to implement it is only `ducknng_pipe_events` + a per-query dispatch table.
5. **Authentication across workers.** Worker trusts head, but does worker trust every client that reaches head? If not, the head needs to forward caller identity. The simplest path is "workers are on a private network reachable only by the head." The next path up is mutual TLS with client certificates terminated at the head and re-asserted to workers via a short-lived token. `requires_auth` on the worker method is the gate.
6. **Data placement catalog as a first-class table.** The examples above use `shard_catalog` and `worker_catalog` as plain tables. In practice you want `ducknng_register_worker(...)`, `ducknng_register_shard(...)`, `ducknng.workers`, `ducknng.shards` — and rules for keeping the two in sync as workers come and go.

None of these are novel research. All of them are reachable once the lower layers of this review are in place.

### This anchors the rest of the checklist

Re-reading the simplification priority with the mesh target in mind, the ordering is reinforced:

- The Arrow re-plumbing (item 0) is not an optimisation, it is the mesh's reduce stage. Without `duckdb_arrow_scan` on the head the project cannot be a cluster, because every sub-query reply would be decoded by a hand-written switch that rejects half of DuckDB's types.
- The typed method registration (0d) is not a nice-to-have security knob, it is the worker API. `execute_shard` exists; `exec` does not.
- The full NNG protocol set (0e) is not completionist, it is what allows worker-to-worker shuffle, worker-to-head announce, and head-to-workers survey to each pick the right pattern.
- The async aio primitives (item 5) are the head's entire dispatch model. Without them there is no scatter-gather, only serial blocking calls that scale inversely with cluster size.
- The session family (item 4) is what makes streaming reduce possible at all; without it, every shard must materialise its full result before the head can combine.
- The codec framework (item 9) keeps semantically rich DuckDB types alive across the wire — otherwise every GROUP BY over ENUMs returns strings and every UUID becomes `binary` with no tag.

The cluster is the single project-level acceptance test that tells you whether the lower layers are correctly shaped. A working demo — one head plus three `inproc://` workers running a scatter-gather `SELECT region, SUM(total) FROM sales GROUP BY region` on local Parquet shards — is the milestone the current tree should be steered toward. Every design argument in this review supports that milestone directly.

## What this review does not cover

Security posture beyond TLS transport (per-method authentication, caller identity binding from TLS peer material, session ownership enforcement under adversarial reconnects) is specified in `docs/security.md` and deserves its own review once the session family exists. The type-mapping matrix in `docs/types.md` is likewise out of scope here; the codec framework above is the mechanism for extending that matrix without rewriting it.
