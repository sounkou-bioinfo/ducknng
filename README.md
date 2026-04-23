
<!-- README.md is generated from README.Rmd. Please edit that file -->

# ducknng: a DuckDB binding to the NNG Scalability Protocols library and an Arrow IPC-based RPC framework

`ducknng` is the pure C DuckDB extension that implements that
binding/framework inside DuckDB.

It gives DuckDB SQL a `nanonext`-like messaging surface and uses a thin,
versioned RPC envelope with Arrow IPC payloads. The SQL ergonomics are
inspired by [`r-lib/nanonext`](https://github.com/r-lib/nanonext), and
the thin-envelope + Arrow IPC direction is informed by projects such as
[`sounkou-bioinfo/mangoro`](https://github.com/sounkou-bioinfo/mangoro).

Today the extension already includes:

- server lifecycle control through `ducknng_start_server()`,
  `ducknng_stop_server()`, and `ducknng_list_servers()`
- generic NNG socket handles for `bus`, `pair`, `poly`, `push`, `pull`,
  `pub`, `sub`, `req`, `rep`, `surveyor`, and `respondent`
- raw socket send/recv primitives plus req-style raw request/reply
  helpers
- raw aio helpers for socket send/recv, req-style request/reply futures,
  and the first raw unary RPC futures
- a low-level HTTP/HTTPS client helper for transport-adapter work
- explicit query-session helpers and TLS config handles

`ducknng` is also intentionally low-level. Long-lived runtime handles
are **manually managed**. In normal use that means you should stop
servers, close sockets, drop aio handles, drop TLS config handles, and
close query sessions explicitly rather than expecting a nanonext-style
GC/finalizer layer.

Transport is selected from the URL scheme. Today the NNG-facing
operations work across `inproc://`, `ipc://`, `tcp://`, and
`tls+tcp://`, and `ducknng_ncurl(...)` provides the first low-level
HTTP/HTTPS client slice. The generic socket layer is explicitly modeled
on `nanonext`: open a protocol-specific socket, dial or listen it, and
then use raw send/recv or aio helpers as appropriate. The higher-level
RPC helpers wrap manifest-declared request/reply methods, and the
session helpers wrap the fixed `query_open` / `fetch` / `close` /
`cancel` lifecycle over repeated request/reply exchanges rather than
acting as a generic abstraction over all socket patterns. The current
async RPC wrappers are also layered this way: they launch one ordinary
request/reply method call asynchronously and later return the collected
raw reply frame for decoding, rather than introducing a second
background-job protocol. The current server is still unauthenticated, so
the session family is implemented and usable but should still be treated
as experimental until session ownership is bound to a concrete
multi-client identity model. HTTP/HTTPS are the only current web-carrier
focus; `ws://` and `wss://` are intentionally deferred and are not
enabled in the current build.

## Getting started

Build the extension locally:

``` sh
make configure
make release
```

Load it into DuckDB, start a local IPC listener, inspect the built-in
manifest, and stop it again:

``` sql
LOAD 'build/release/ducknng.duckdb_extension';

SELECT ducknng_start_server(
  'sql0',
  'ipc:///tmp/ducknng_sql0.ipc',
  1,
  134217728,
  300000,
  0
);

SELECT * FROM ducknng_get_rpc_manifest('ipc:///tmp/ducknng_sql0.ipc', 0::UBIGINT);
SELECT ducknng_stop_server('sql0');
```

    ┌──────────────────────────────────────────────────────────────────────────────────────┐
    │ ducknng_start_server('sql0', 'ipc:///tmp/ducknng_sql0.ipc', 1, 134217728, 300000, 0) │
    │                                       boolean                                        │
    ├──────────────────────────────────────────────────────────────────────────────────────┤
    │ true                                                                                 │
    └──────────────────────────────────────────────────────────────────────────────────────┘
    ┌─────────┬─────────┬────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
    │   ok    │  error  │                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          manifest                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          │
    │ boolean │ varchar │                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          varchar                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           │
    ├─────────┼─────────┼────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
    │ true    │ NULL    │ {"server":{"name":"ducknng","version":"0.1.0","protocol_version":1},"methods":[{"name":"manifest","family":"control","summary":"Return the registry-derived manifest JSON","transport_pattern":"reqrep","request_payload_format":"none","response_payload_format":"json","response_mode":"metadata_only","session_behavior":"stateless","requires_auth":false,"requires_session":false,"opens_session":false,"closes_session":false,"mutates_state":false,"idempotent":true,"deprecated":false,"disabled":false,"accepted_request_flags":0,"emitted_reply_flags":4,"max_request_bytes":0,"max_reply_bytes":1048576,"version_introduced":1,"request_schema":null,"response_schema":{"type":"json"}},{"name":"query_open","family":"query","summary":"Open a server-side query session","transport_pattern":"reqrep","request_payload_format":"arrow_ipc_stream","response_payload_format":"json","response_mode":"session_open","session_behavior":"opens_session","requires_auth":false,"requires_session":false,"opens_session":true,"closes_session":false,"mutates_state":false,"idempotent":false,"deprecated":false,"disabled":false,"accepted_request_flags":0,"emitted_reply_flags":36,"max_request_bytes":16777216,"max_reply_bytes":1048576,"version_introduced":1,"request_schema":{"fields":[{"name":"sql","type":"utf8","nullable":false},{"name":"batch_rows","type":"uint64","nullable":true},{"name":"batch_bytes","type":"uint64","nullable":true}]},"response_schema":{"type":"json","session_open":true}},{"name":"fetch","family":"query","summary":"Fetch the next Arrow batch from an open session","transport_pattern":"reqrep","request_payload_format":"json","response_payload_format":"arrow_ipc_stream","response_mode":"rows","session_behavior":"requires_session","requires_auth":false,"requires_session":true,"opens_session":false,"closes_session":false,"mutates_state":false,"idempotent":false,"deprecated":false,"disabled":false,"accepted_request_flags":0,"emitted_reply_flags":29,"max_request_bytes":1048576,"max_reply_bytes":16777216,"version_introduced":1,"request_schema":{"type":"json","fields":[{"name":"session_id","type":"uint64","nullable":false}]},"response_schema":{"mode":"rows_or_control"}},{"name":"close","family":"query","summary":"Close an open or exhausted session","transport_pattern":"reqrep","request_payload_format":"json","response_payload_format":"json","response_mode":"metadata_only","session_behavior":"closes_session","requires_auth":false,"requires_session":true,"opens_session":false,"closes_session":true,"mutates_state":true,"idempotent":true,"deprecated":false,"disabled":false,"accepted_request_flags":0,"emitted_reply_flags":68,"max_request_bytes":1048576,"max_reply_bytes":1048576,"version_introduced":1,"request_schema":{"type":"json","fields":[{"name":"session_id","type":"uint64","nullable":false}]},"response_schema":{"type":"json"}},{"name":"cancel","family":"query","summary":"Cancel and close an open session","transport_pattern":"reqrep","request_payload_format":"json","response_payload_format":"json","response_mode":"metadata_only","session_behavior":"cancels_session","requires_auth":false,"requires_session":true,"opens_session":false,"closes_session":true,"mutates_state":true,"idempotent":false,"deprecated":false,"disabled":false,"accepted_request_flags":0,"emitted_reply_flags":196,"max_request_bytes":1048576,"max_reply_bytes":1048576,"version_introduced":1,"request_schema":{"type":"json","fields":[{"name":"session_id","type":"uint64","nullable":false}]},"response_schema":{"type":"json"}}]} │
    └─────────┴─────────┴────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
    ┌─────────────────────────────┐
    │ ducknng_stop_server('sql0') │
    │           boolean           │
    ├─────────────────────────────┤
    │ true                        │
    └─────────────────────────────┘

## Lifetime and manual cleanup

DuckDB’s extension API does give `ducknng` destroy callbacks for
internal function-registration, bind, and init state. What DuckDB SQL
does **not** give this project is a general-purpose GC/finalizer model
for long-lived SQL handles like R external pointer finalizers. So at the
public SQL surface, long-lived handles are manually managed.

You should explicitly clean up what you create:

- stop servers with `ducknng_stop_server(...)`
- close sockets with `ducknng_close_socket(...)`
- drop aio handles with `ducknng_aio_drop(...)`
- drop TLS config handles with `ducknng_drop_tls_config(...)`
- close query sessions with `ducknng_close_query(...)`

Important details:

- `ducknng_aio_cancel(...)` is cancellation control, not the destructor
- `ducknng_cancel_query(...)` is best-effort session control, not a
  replacement for the normal explicit close path
- runtime teardown is fallback cleanup, not the primary lifecycle model
  for a long-lived DuckDB process

The README should be enough to understand the public lifecycle
expectations. The files under `docs/` are mostly deeper
protocol/reference material and project design notes. If you want the
more detailed lifetime write-up, see `docs/lifetime.md`. For protocol
and transport reference details, see `docs/protocol.md`,
`docs/manifest.md`, `docs/security.md`, `docs/registry.md`,
`docs/transports.md`, `docs/http.md`, and `docs/types.md`. `NEWS.md`
summarizes notable landed changes. TLS already supports both file-backed
and in-memory PEM material through helpers such as
`ducknng_tls_config_from_files(...)`,
`ducknng_tls_config_from_pem(...)`, and
`ducknng_self_signed_tls_config(...)`.

## Function catalog

<details>
<summary>
Expand the full generated function catalog
</summary>

# Function Catalog

This file is generated from `function_catalog/functions.yaml`.

## Service Control

| name                   | kind   | arguments                                                                | returns   | description                         |
|------------------------|--------|--------------------------------------------------------------------------|-----------|-------------------------------------|
| `ducknng_start_server` | scalar | `name, listen, contexts, recv_max_bytes, session_idle_ms, tls_config_id` | `BOOLEAN` | Start a named ducknng REP listener. |
| `ducknng_stop_server`  | scalar | `name`                                                                   | `BOOLEAN` | Stop a named ducknng service.       |

## Introspection

| name                   | kind  | arguments | returns                                                                                                        | description                       |
|------------------------|-------|-----------|----------------------------------------------------------------------------------------------------------------|-----------------------------------|
| `ducknng_list_servers` | table |           | `TABLE(service_id UBIGINT, name VARCHAR, listen VARCHAR, contexts INTEGER, running BOOLEAN, sessions UBIGINT)` | List registered ducknng services. |

## Method Registry

| name                           | kind   | arguments | returns                                                                                                                                                                                                                                                                       | description                                                        |
|--------------------------------|--------|-----------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------|
| `ducknng_register_exec_method` | scalar |           | `BOOLEAN`                                                                                                                                                                                                                                                                     | Register the built-in exec RPC method explicitly.                  |
| `ducknng_unregister_method`    | scalar | `name`    | `BOOLEAN`                                                                                                                                                                                                                                                                     | Unregister a method from the runtime registry.                     |
| `ducknng_unregister_family`    | scalar | `family`  | `UBIGINT`                                                                                                                                                                                                                                                                     | Unregister all methods in a family and return the number removed.  |
| `ducknng_list_methods`         | table  |           | `TABLE(name VARCHAR, family VARCHAR, summary VARCHAR, transport_pattern VARCHAR, request_payload_format VARCHAR, response_payload_format VARCHAR, response_mode VARCHAR, request_schema_json VARCHAR, response_schema_json VARCHAR, requires_auth BOOLEAN, disabled BOOLEAN)` | List the currently registered RPC methods in the runtime registry. |

## Primitive Transport

| name                         | kind   | arguments                                       | returns                                                                                                                                                         | description                                                                                         |
|------------------------------|--------|-------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------|
| `ducknng_open_socket`        | scalar | `protocol`                                      | `UBIGINT`                                                                                                                                                       | Open a client socket handle for a supported NNG protocol.                                           |
| `ducknng_dial_socket`        | scalar | `socket_id, url, timeout_ms`                    | `BOOLEAN`                                                                                                                                                       | Dial a URL using an opened socket handle.                                                           |
| `ducknng_listen_socket`      | scalar | `socket_id, url, recv_max_bytes, tls_config_id` | `BOOLEAN`                                                                                                                                                       | Bind a socket handle to a listen URL and start its NNG listener.                                    |
| `ducknng_close_socket`       | scalar | `socket_id`                                     | `BOOLEAN`                                                                                                                                                       | Close a client socket handle.                                                                       |
| `ducknng_send_socket_raw`    | scalar | `socket_id, frame, timeout_ms`                  | `BOOLEAN`                                                                                                                                                       | Send one raw frame through an active socket handle.                                                 |
| `ducknng_recv_socket_raw`    | scalar | `socket_id, timeout_ms`                         | `BLOB`                                                                                                                                                          | Receive one raw frame from an active socket handle.                                                 |
| `ducknng_subscribe_socket`   | scalar | `socket_id, topic`                              | `BOOLEAN`                                                                                                                                                       | Register a raw topic prefix on a sub socket.                                                        |
| `ducknng_unsubscribe_socket` | scalar | `socket_id, topic`                              | `BOOLEAN`                                                                                                                                                       | Remove a raw topic prefix from a sub socket.                                                        |
| `ducknng_list_sockets`       | table  |                                                 | `TABLE(socket_id UBIGINT, protocol VARCHAR, url VARCHAR, open BOOLEAN, connected BOOLEAN, listening BOOLEAN, send_timeout_ms INTEGER, recv_timeout_ms INTEGER)` | List client socket handles in the runtime.                                                          |
| `ducknng_request`            | table  | `url, payload, timeout_ms, tls_config_id`       | `TABLE(ok BOOLEAN, error VARCHAR, payload BLOB)`                                                                                                                | Perform a one-shot raw request and return a structured result row.                                  |
| `ducknng_request_socket`     | table  | `socket_id, payload, timeout_ms`                | `TABLE(ok BOOLEAN, error VARCHAR, payload BLOB)`                                                                                                                | Perform a raw request through a previously dialed socket handle and return a structured result row. |
| `ducknng_request_raw`        | scalar | `url, payload, timeout_ms, tls_config_id`       | `BLOB`                                                                                                                                                          | Perform a one-shot raw request and return the raw reply frame bytes.                                |
| `ducknng_request_socket_raw` | scalar | `socket_id, payload, timeout_ms`                | `BLOB`                                                                                                                                                          | Perform a raw request through a dialed socket handle and return the raw reply frame bytes.          |
| `ducknng_decode_frame`       | table  | `frame`                                         | `TABLE(ok BOOLEAN, error VARCHAR, version UTINYINT, type UTINYINT, flags UINTEGER, type_name VARCHAR, name VARCHAR, payload BLOB, payload_text VARCHAR)`        | Decode a raw ducknng frame into envelope fields and extracted payload columns.                      |

## Transport Security

| name                             | kind   | arguments                                        | returns                                                                                                                                                                                                                 | description                                                                            |
|----------------------------------|--------|--------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------|
| `ducknng_list_tls_configs`       | table  |                                                  | `TABLE(tls_config_id UBIGINT, source VARCHAR, enabled BOOLEAN, has_cert_key_file BOOLEAN, has_ca_file BOOLEAN, has_cert_pem BOOLEAN, has_key_pem BOOLEAN, has_ca_pem BOOLEAN, has_password BOOLEAN, auth_mode INTEGER)` | List registered TLS config handles and the kinds of material they contain.             |
| `ducknng_drop_tls_config`        | scalar | `tls_config_id`                                  | `BOOLEAN`                                                                                                                                                                                                               | Remove a registered TLS config handle from the runtime.                                |
| `ducknng_self_signed_tls_config` | scalar | `common_name, valid_days, auth_mode`             | `UBIGINT`                                                                                                                                                                                                               | Generate a self-signed development certificate and register it as a TLS config handle. |
| `ducknng_tls_config_from_pem`    | scalar | `cert_pem, key_pem, ca_pem, password, auth_mode` | `UBIGINT`                                                                                                                                                                                                               | Register a TLS config handle from in-memory PEM material.                              |
| `ducknng_tls_config_from_files`  | scalar | `cert_key_file, ca_file, password, auth_mode`    | `UBIGINT`                                                                                                                                                                                                               | Register a TLS config handle from file-backed certificate material.                    |

## HTTP Transport

| name            | kind  | arguments                                                    | returns                                                                                                | description                                                         |
|-----------------|-------|--------------------------------------------------------------|--------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------|
| `ducknng_ncurl` | table | `url, method, headers_json, body, timeout_ms, tls_config_id` | `TABLE(ok BOOLEAN, status INTEGER, error VARCHAR, headers_json VARCHAR, body BLOB, body_text VARCHAR)` | Perform one HTTP or HTTPS request and return an in-band result row. |

## Async I/O

| name                               | kind   | arguments                               | returns                                                                                                                                                                                                               | description                                                                                                   |
|------------------------------------|--------|-----------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------|
| `ducknng_request_raw_aio`          | scalar | `url, frame, timeout_ms, tls_config_id` | `UBIGINT`                                                                                                                                                                                                             | Launch one raw req/rep roundtrip asynchronously and return a future-like aio handle id.                       |
| `ducknng_get_rpc_manifest_raw_aio` | scalar | `url, timeout_ms, tls_config_id`        | `UBIGINT`                                                                                                                                                                                                             | Launch one asynchronous manifest RPC request and return an aio handle id for the raw reply frame.             |
| `ducknng_run_rpc_raw_aio`          | scalar | `url, sql, timeout_ms, tls_config_id`   | `UBIGINT`                                                                                                                                                                                                             | Launch one asynchronous metadata-only exec RPC request and return an aio handle id for the raw reply frame.   |
| `ducknng_request_socket_raw_aio`   | scalar | `socket_id, frame, timeout_ms`          | `UBIGINT`                                                                                                                                                                                                             | Launch one raw req/rep roundtrip asynchronously on an existing req socket handle and return an aio handle id. |
| `ducknng_send_socket_raw_aio`      | scalar | `socket_id, frame, timeout_ms`          | `UBIGINT`                                                                                                                                                                                                             | Launch one raw socket send asynchronously and return an aio handle id.                                        |
| `ducknng_recv_socket_raw_aio`      | scalar | `socket_id, timeout_ms`                 | `UBIGINT`                                                                                                                                                                                                             | Launch one raw socket receive asynchronously and return an aio handle id.                                     |
| `ducknng_aio_ready`                | scalar | `aio_id`                                | `BOOLEAN`                                                                                                                                                                                                             | Return whether an aio handle has reached a terminal state.                                                    |
| `ducknng_aio_status`               | table  | `aio_id`                                | `TABLE(aio_id UBIGINT, exists BOOLEAN, kind VARCHAR, state VARCHAR, phase VARCHAR, terminal BOOLEAN, send_done BOOLEAN, send_ok BOOLEAN, recv_done BOOLEAN, recv_ok BOOLEAN, has_reply_frame BOOLEAN, error VARCHAR)` | Inspect the current or terminal status of one aio handle, including send-phase and recv-phase completion.     |
| `ducknng_aio_collect`              | table  | `aio_ids, wait_ms`                      | `TABLE(aio_id UBIGINT, ok BOOLEAN, error VARCHAR, frame BLOB)`                                                                                                                                                        | Wait for any requested aio handles to finish and return one row per newly collected terminal result.          |
| `ducknng_aio_cancel`               | scalar | `aio_id`                                | `BOOLEAN`                                                                                                                                                                                                             | Request cancellation of a pending aio handle.                                                                 |
| `ducknng_aio_drop`                 | scalar | `aio_id`                                | `BOOLEAN`                                                                                                                                                                                                             | Release a terminal aio handle from the runtime registry.                                                      |

## RPC Helper

| name                           | kind   | arguments                 | returns                                                                                               | description                                                                                   |
|--------------------------------|--------|---------------------------|-------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------|
| `ducknng_get_rpc_manifest`     | table  | `url, tls_config_id`      | `TABLE(ok BOOLEAN, error VARCHAR, manifest VARCHAR)`                                                  | Request the RPC manifest and return a structured result row.                                  |
| `ducknng_get_rpc_manifest_raw` | scalar | `url, tls_config_id`      | `BLOB`                                                                                                | Request the RPC manifest and return the raw reply frame as BLOB.                              |
| `ducknng_run_rpc`              | table  | `url, sql, tls_config_id` | `TABLE(ok BOOLEAN, error VARCHAR, rows_changed UBIGINT, statement_type INTEGER, result_type INTEGER)` | Execute a metadata-oriented RPC call and return a structured result row.                      |
| `ducknng_run_rpc_raw`          | scalar | `url, sql, tls_config_id` | `BLOB`                                                                                                | Execute the exec RPC and return the raw reply frame as BLOB.                                  |
| `ducknng_query_rpc`            | table  | `url, sql, tls_config_id` | `table`                                                                                               | Execute a row-returning RPC query and expose the unary Arrow IPC row reply as a DuckDB table. |

## RPC Session

| name                   | kind  | arguments                                                 | returns                                                                                                                                               | description                                                                                                    |
|------------------------|-------|-----------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------|
| `ducknng_open_query`   | table | `url, sql, batch_rows, batch_bytes, tls_config_id`        | `TABLE(ok BOOLEAN, error VARCHAR, session_id UBIGINT, state VARCHAR, next_method VARCHAR, control_json VARCHAR)`                                      | Open a server-side query session and return the JSON control metadata as a structured row.                     |
| `ducknng_fetch_query`  | table | `url, session_id, batch_rows, batch_bytes, tls_config_id` | `TABLE(ok BOOLEAN, error VARCHAR, session_id UBIGINT, state VARCHAR, next_method VARCHAR, control_json VARCHAR, payload BLOB, end_of_stream BOOLEAN)` | Fetch the next session reply and return either JSON control metadata or an Arrow IPC batch payload.            |
| `ducknng_close_query`  | table | `url, session_id, tls_config_id`                          | `TABLE(ok BOOLEAN, error VARCHAR, session_id UBIGINT, state VARCHAR, next_method VARCHAR, control_json VARCHAR)`                                      | Close a server-side query session and return the JSON control metadata as a structured row.                    |
| `ducknng_cancel_query` | table | `url, session_id, tls_config_id`                          | `TABLE(ok BOOLEAN, error VARCHAR, session_id UBIGINT, state VARCHAR, next_method VARCHAR, control_json VARCHAR)`                                      | Request cancellation for a server-side query session and return the JSON control metadata as a structured row. |

</details>

## Examples

### Start an IPC listener and inspect the registry

``` sql
LOAD 'build/release/ducknng.duckdb_extension';
SELECT ducknng_start_server(
  'sql0',                         -- service name
  'ipc:///tmp/ducknng_sql0.ipc', -- listen URL
  1,                              -- REP contexts
  134217728,                      -- recv_max_bytes
  300000,                         -- session_idle_ms
  0                               -- tls_config_id (0 means plaintext)
);

SELECT name, listen, contexts, running, sessions
FROM ducknng_list_servers();

SELECT ducknng_stop_server('sql0');
```

    ┌──────────────────────────────────────────────────────────────────────────────────────┐
    │ ducknng_start_server('sql0', 'ipc:///tmp/ducknng_sql0.ipc', 1, 134217728, 300000, 0) │
    │                                       boolean                                        │
    ├──────────────────────────────────────────────────────────────────────────────────────┤
    │ true                                                                                 │
    └──────────────────────────────────────────────────────────────────────────────────────┘
    ┌─────────┬─────────────────────────────┬──────────┬─────────┬──────────┐
    │  name   │           listen            │ contexts │ running │ sessions │
    │ varchar │           varchar           │  int32   │ boolean │  uint64  │
    ├─────────┼─────────────────────────────┼──────────┼─────────┼──────────┤
    │ sql0    │ ipc:///tmp/ducknng_sql0.ipc │        1 │ true    │        0 │
    └─────────┴─────────────────────────────┴──────────┴─────────┴──────────┘
    ┌─────────────────────────────┐
    │ ducknng_stop_server('sql0') │
    │           boolean           │
    ├─────────────────────────────┤
    │ true                        │
    └─────────────────────────────┘

### Request multiple REP contexts on one REP socket

``` sql
LOAD 'build/release/ducknng.duckdb_extension';
SELECT ducknng_start_server(
  'sql_multi',                    -- service name
  'ipc:///tmp/ducknng_sql_multi.ipc', -- listen URL
  3,                              -- REP contexts
  134217728,                      -- recv_max_bytes
  300000,                         -- session_idle_ms
  0                               -- tls_config_id (0 means plaintext)
);

SELECT name, contexts, running
FROM ducknng_list_servers()
WHERE name = 'sql_multi';

SELECT ducknng_stop_server('sql_multi');
```

    ┌────────────────────────────────────────────────────────────────────────────────────────────────┐
    │ ducknng_start_server('sql_multi', 'ipc:///tmp/ducknng_sql_multi.ipc', 3, 134217728, 300000, 0) │
    │                                            boolean                                             │
    ├────────────────────────────────────────────────────────────────────────────────────────────────┤
    │ true                                                                                           │
    └────────────────────────────────────────────────────────────────────────────────────────────────┘
    ┌───────────┬──────────┬─────────┐
    │   name    │ contexts │ running │
    │  varchar  │  int32   │ boolean │
    ├───────────┼──────────┼─────────┤
    │ sql_multi │        3 │ true    │
    └───────────┴──────────┴─────────┘
    ┌──────────────────────────────────┐
    │ ducknng_stop_server('sql_multi') │
    │             boolean              │
    ├──────────────────────────────────┤
    │ true                             │
    └──────────────────────────────────┘

### DuckDB can also act as a client

``` sql
LOAD 'build/release/ducknng.duckdb_extension';
-- Start a local ducknng service that the following client examples will talk to.
SELECT ducknng_start_server(
  'sql_client_demo',              -- service name
  'ipc:///tmp/ducknng_sql_client_demo.ipc', -- listen URL
  1,                              -- REP contexts
  134217728,                      -- recv_max_bytes
  300000,                         -- session_idle_ms
  0                               -- tls_config_id (0 means plaintext)
);

-- The default RPC surface is minimal: manifest is built in, exec is opt-in.
SELECT * FROM ducknng_get_rpc_manifest('ipc:///tmp/ducknng_sql_client_demo.ipc', 0::UBIGINT);
SELECT * FROM ducknng_list_methods();

-- Register exec explicitly before exposing SQL execution over RPC.
SELECT ducknng_register_exec_method();
SELECT * FROM ducknng_list_methods();
SELECT * FROM ducknng_get_rpc_manifest('ipc:///tmp/ducknng_sql_client_demo.ipc', 0::UBIGINT);

-- RPC helper: run non-row statements and keep errors in-band.
SELECT * FROM ducknng_run_rpc('ipc:///tmp/ducknng_sql_client_demo.ipc', 'CREATE TABLE IF NOT EXISTS client_side_demo(i INTEGER)', 0::UBIGINT);
SELECT * FROM ducknng_run_rpc('ipc:///tmp/ducknng_sql_client_demo.ipc', 'INSERT INTO client_side_demo VALUES (10), (11)', 0::UBIGINT);

-- RPC helper: fetch row results through the unary query path.
SELECT * FROM ducknng_query_rpc('ipc:///tmp/ducknng_sql_client_demo.ipc', 'SELECT i, i > 10 AS gt_10 FROM client_side_demo ORDER BY i', 0::UBIGINT);

-- Primitive transport layer: open a req socket handle, dial it, and inspect the registry.
SELECT ducknng_open_socket('req');
SELECT ducknng_dial_socket(1, 'ipc:///tmp/ducknng_sql_client_demo.ipc', 1000);
SELECT * FROM ducknng_list_sockets();

-- Primitive transport layer: send the built-in manifest request frame.
-- The hex literal here is the current wire-format request for the always-on manifest method.
SELECT *
FROM ducknng_request_socket(
  1::UBIGINT,                                        -- socket_id
  from_hex('01000000000000000000000000000000000000000000'), -- manifest request frame
  1000                                               -- timeout_ms
);

SELECT *
FROM ducknng_request(
  'ipc:///tmp/ducknng_sql_client_demo.ipc',          -- url
  from_hex('01000000000000000000000000000000000000000000'), -- manifest request frame
  1000,                                              -- timeout_ms
  0::UBIGINT                                         -- tls_config_id
);

-- Raw scalar forms now mean raw reply frames as BLOBs.
SELECT substr(
  hex(
    ducknng_request_socket_raw(
      1,                                             -- socket_id
      from_hex('01000000000000000000000000000000000000000000'), -- manifest request frame
      1000                                           -- timeout_ms
    )
  ),
  1,
  28
);

-- Decode raw manifest and exec replies into envelope fields plus extracted text payload.
SELECT ok, version, type_name, name, position('"name":"exec"' IN payload_text) > 0
FROM ducknng_decode_frame(ducknng_get_rpc_manifest_raw('ipc:///tmp/ducknng_sql_client_demo.ipc', 0::UBIGINT));

SELECT ok, type_name, name
FROM ducknng_decode_frame(
  ducknng_run_rpc_raw('ipc:///tmp/ducknng_sql_client_demo.ipc', 'CREATE TABLE IF NOT EXISTS client_side_demo(i INTEGER)', 0::UBIGINT)
);

-- The generic raw request helper can be decoded the same way.
SELECT ok, version, type_name, name, payload_text
FROM ducknng_decode_frame(
  ducknng_request_raw('ipc:///tmp/ducknng_sql_client_demo.ipc', from_hex('01000000000000000000000000000000000000000000'), 1000, 0::UBIGINT)
);

-- Close the client socket handle and stop the demo server.
SELECT ducknng_close_socket(1);
SELECT ducknng_stop_server('sql_client_demo');
```

    ┌────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
    │ ducknng_start_server('sql_client_demo', 'ipc:///tmp/ducknng_sql_client_demo.ipc', 1, 134217728, 300000, 0) │
    │                                                  boolean                                                   │
    ├────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
    │ true                                                                                                       │
    └────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
    ┌─────────┬─────────┬────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
    │   ok    │  error  │                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          manifest                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          │
    │ boolean │ varchar │                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          varchar                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           │
    ├─────────┼─────────┼────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
    │ true    │ NULL    │ {"server":{"name":"ducknng","version":"0.1.0","protocol_version":1},"methods":[{"name":"manifest","family":"control","summary":"Return the registry-derived manifest JSON","transport_pattern":"reqrep","request_payload_format":"none","response_payload_format":"json","response_mode":"metadata_only","session_behavior":"stateless","requires_auth":false,"requires_session":false,"opens_session":false,"closes_session":false,"mutates_state":false,"idempotent":true,"deprecated":false,"disabled":false,"accepted_request_flags":0,"emitted_reply_flags":4,"max_request_bytes":0,"max_reply_bytes":1048576,"version_introduced":1,"request_schema":null,"response_schema":{"type":"json"}},{"name":"query_open","family":"query","summary":"Open a server-side query session","transport_pattern":"reqrep","request_payload_format":"arrow_ipc_stream","response_payload_format":"json","response_mode":"session_open","session_behavior":"opens_session","requires_auth":false,"requires_session":false,"opens_session":true,"closes_session":false,"mutates_state":false,"idempotent":false,"deprecated":false,"disabled":false,"accepted_request_flags":0,"emitted_reply_flags":36,"max_request_bytes":16777216,"max_reply_bytes":1048576,"version_introduced":1,"request_schema":{"fields":[{"name":"sql","type":"utf8","nullable":false},{"name":"batch_rows","type":"uint64","nullable":true},{"name":"batch_bytes","type":"uint64","nullable":true}]},"response_schema":{"type":"json","session_open":true}},{"name":"fetch","family":"query","summary":"Fetch the next Arrow batch from an open session","transport_pattern":"reqrep","request_payload_format":"json","response_payload_format":"arrow_ipc_stream","response_mode":"rows","session_behavior":"requires_session","requires_auth":false,"requires_session":true,"opens_session":false,"closes_session":false,"mutates_state":false,"idempotent":false,"deprecated":false,"disabled":false,"accepted_request_flags":0,"emitted_reply_flags":29,"max_request_bytes":1048576,"max_reply_bytes":16777216,"version_introduced":1,"request_schema":{"type":"json","fields":[{"name":"session_id","type":"uint64","nullable":false}]},"response_schema":{"mode":"rows_or_control"}},{"name":"close","family":"query","summary":"Close an open or exhausted session","transport_pattern":"reqrep","request_payload_format":"json","response_payload_format":"json","response_mode":"metadata_only","session_behavior":"closes_session","requires_auth":false,"requires_session":true,"opens_session":false,"closes_session":true,"mutates_state":true,"idempotent":true,"deprecated":false,"disabled":false,"accepted_request_flags":0,"emitted_reply_flags":68,"max_request_bytes":1048576,"max_reply_bytes":1048576,"version_introduced":1,"request_schema":{"type":"json","fields":[{"name":"session_id","type":"uint64","nullable":false}]},"response_schema":{"type":"json"}},{"name":"cancel","family":"query","summary":"Cancel and close an open session","transport_pattern":"reqrep","request_payload_format":"json","response_payload_format":"json","response_mode":"metadata_only","session_behavior":"cancels_session","requires_auth":false,"requires_session":true,"opens_session":false,"closes_session":true,"mutates_state":true,"idempotent":false,"deprecated":false,"disabled":false,"accepted_request_flags":0,"emitted_reply_flags":196,"max_request_bytes":1048576,"max_reply_bytes":1048576,"version_introduced":1,"request_schema":{"type":"json","fields":[{"name":"session_id","type":"uint64","nullable":false}]},"response_schema":{"type":"json"}}]} │
    └─────────┴─────────┴────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
    ┌────────────┬─────────┬─────────────────────────────────────────────────┬───────────────────┬────────────────────────┬─────────────────────────┬───────────────┬─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┬─────────────────────────────────────┬───────────────┬──────────┐
    │    name    │ family  │                     summary                     │ transport_pattern │ request_payload_format │ response_payload_format │ response_mode │                                                                           request_schema_json                                                                           │        response_schema_json         │ requires_auth │ disabled │
    │  varchar   │ varchar │                     varchar                     │      varchar      │        varchar         │         varchar         │    varchar    │                                                                                 varchar                                                                                 │               varchar               │    boolean    │ boolean  │
    ├────────────┼─────────┼─────────────────────────────────────────────────┼───────────────────┼────────────────────────┼─────────────────────────┼───────────────┼─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┼─────────────────────────────────────┼───────────────┼──────────┤
    │ manifest   │ control │ Return the registry-derived manifest JSON       │ reqrep            │ none                   │ json                    │ metadata_only │ NULL                                                                                                                                                                    │ {"type":"json"}                     │ false         │ false    │
    │ query_open │ query   │ Open a server-side query session                │ reqrep            │ arrow_ipc_stream       │ json                    │ session_open  │ {"fields":[{"name":"sql","type":"utf8","nullable":false},{"name":"batch_rows","type":"uint64","nullable":true},{"name":"batch_bytes","type":"uint64","nullable":true}]} │ {"type":"json","session_open":true} │ false         │ false    │
    │ fetch      │ query   │ Fetch the next Arrow batch from an open session │ reqrep            │ json                   │ arrow_ipc_stream        │ rows          │ {"type":"json","fields":[{"name":"session_id","type":"uint64","nullable":false}]}                                                                                       │ {"mode":"rows_or_control"}          │ false         │ false    │
    │ close      │ query   │ Close an open or exhausted session              │ reqrep            │ json                   │ json                    │ metadata_only │ {"type":"json","fields":[{"name":"session_id","type":"uint64","nullable":false}]}                                                                                       │ {"type":"json"}                     │ false         │ false    │
    │ cancel     │ query   │ Cancel and close an open session                │ reqrep            │ json                   │ json                    │ metadata_only │ {"type":"json","fields":[{"name":"session_id","type":"uint64","nullable":false}]}                                                                                       │ {"type":"json"}                     │ false         │ false    │
    └────────────┴─────────┴─────────────────────────────────────────────────┴───────────────────┴────────────────────────┴─────────────────────────┴───────────────┴─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┴─────────────────────────────────────┴───────────────┴──────────┘
    ┌────────────────────────────────┐
    │ ducknng_register_exec_method() │
    │            boolean             │
    ├────────────────────────────────┤
    │ true                           │
    └────────────────────────────────┘
    ┌────────────┬─────────┬─────────────────────────────────────────────────┬───────────────────┬────────────────────────┬─────────────────────────┬──────────────────┬─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┬─────────────────────────────────────┬───────────────┬──────────┐
    │    name    │ family  │                     summary                     │ transport_pattern │ request_payload_format │ response_payload_format │  response_mode   │                                                                           request_schema_json                                                                           │        response_schema_json         │ requires_auth │ disabled │
    │  varchar   │ varchar │                     varchar                     │      varchar      │        varchar         │         varchar         │     varchar      │                                                                                 varchar                                                                                 │               varchar               │    boolean    │ boolean  │
    ├────────────┼─────────┼─────────────────────────────────────────────────┼───────────────────┼────────────────────────┼─────────────────────────┼──────────────────┼─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┼─────────────────────────────────────┼───────────────┼──────────┤
    │ manifest   │ control │ Return the registry-derived manifest JSON       │ reqrep            │ none                   │ json                    │ metadata_only    │ NULL                                                                                                                                                                    │ {"type":"json"}                     │ false         │ false    │
    │ query_open │ query   │ Open a server-side query session                │ reqrep            │ arrow_ipc_stream       │ json                    │ session_open     │ {"fields":[{"name":"sql","type":"utf8","nullable":false},{"name":"batch_rows","type":"uint64","nullable":true},{"name":"batch_bytes","type":"uint64","nullable":true}]} │ {"type":"json","session_open":true} │ false         │ false    │
    │ fetch      │ query   │ Fetch the next Arrow batch from an open session │ reqrep            │ json                   │ arrow_ipc_stream        │ rows             │ {"type":"json","fields":[{"name":"session_id","type":"uint64","nullable":false}]}                                                                                       │ {"mode":"rows_or_control"}          │ false         │ false    │
    │ close      │ query   │ Close an open or exhausted session              │ reqrep            │ json                   │ json                    │ metadata_only    │ {"type":"json","fields":[{"name":"session_id","type":"uint64","nullable":false}]}                                                                                       │ {"type":"json"}                     │ false         │ false    │
    │ cancel     │ query   │ Cancel and close an open session                │ reqrep            │ json                   │ json                    │ metadata_only    │ {"type":"json","fields":[{"name":"session_id","type":"uint64","nullable":false}]}                                                                                       │ {"type":"json"}                     │ false         │ false    │
    │ exec       │ sql     │ Execute SQL and return metadata or rows         │ reqrep            │ arrow_ipc_stream       │ arrow_ipc_stream        │ metadata_or_rows │ {"fields":[{"name":"sql","type":"utf8","nullable":false},{"name":"want_result","type":"bool","nullable":false}]}                                                        │ {"mode":"metadata_or_rows"}         │ false         │ false    │
    └────────────┴─────────┴─────────────────────────────────────────────────┴───────────────────┴────────────────────────┴─────────────────────────┴──────────────────┴─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┴─────────────────────────────────────┴───────────────┴──────────┘
    ┌─────────┬─────────┬──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
    │   ok    │  error  │                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             manifest                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             │
    │ boolean │ varchar │                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             varchar                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              │
    ├─────────┼─────────┼──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
    │ true    │ NULL    │ {"server":{"name":"ducknng","version":"0.1.0","protocol_version":1},"methods":[{"name":"manifest","family":"control","summary":"Return the registry-derived manifest JSON","transport_pattern":"reqrep","request_payload_format":"none","response_payload_format":"json","response_mode":"metadata_only","session_behavior":"stateless","requires_auth":false,"requires_session":false,"opens_session":false,"closes_session":false,"mutates_state":false,"idempotent":true,"deprecated":false,"disabled":false,"accepted_request_flags":0,"emitted_reply_flags":4,"max_request_bytes":0,"max_reply_bytes":1048576,"version_introduced":1,"request_schema":null,"response_schema":{"type":"json"}},{"name":"query_open","family":"query","summary":"Open a server-side query session","transport_pattern":"reqrep","request_payload_format":"arrow_ipc_stream","response_payload_format":"json","response_mode":"session_open","session_behavior":"opens_session","requires_auth":false,"requires_session":false,"opens_session":true,"closes_session":false,"mutates_state":false,"idempotent":false,"deprecated":false,"disabled":false,"accepted_request_flags":0,"emitted_reply_flags":36,"max_request_bytes":16777216,"max_reply_bytes":1048576,"version_introduced":1,"request_schema":{"fields":[{"name":"sql","type":"utf8","nullable":false},{"name":"batch_rows","type":"uint64","nullable":true},{"name":"batch_bytes","type":"uint64","nullable":true}]},"response_schema":{"type":"json","session_open":true}},{"name":"fetch","family":"query","summary":"Fetch the next Arrow batch from an open session","transport_pattern":"reqrep","request_payload_format":"json","response_payload_format":"arrow_ipc_stream","response_mode":"rows","session_behavior":"requires_session","requires_auth":false,"requires_session":true,"opens_session":false,"closes_session":false,"mutates_state":false,"idempotent":false,"deprecated":false,"disabled":false,"accepted_request_flags":0,"emitted_reply_flags":29,"max_request_bytes":1048576,"max_reply_bytes":16777216,"version_introduced":1,"request_schema":{"type":"json","fields":[{"name":"session_id","type":"uint64","nullable":false}]},"response_schema":{"mode":"rows_or_control"}},{"name":"close","family":"query","summary":"Close an open or exhausted session","transport_pattern":"reqrep","request_payload_format":"json","response_payload_format":"json","response_mode":"metadata_only","session_behavior":"closes_session","requires_auth":false,"requires_session":true,"opens_session":false,"closes_session":true,"mutates_state":true,"idempotent":true,"deprecated":false,"disabled":false,"accepted_request_flags":0,"emitted_reply_flags":68,"max_request_bytes":1048576,"max_reply_bytes":1048576,"version_introduced":1,"request_schema":{"type":"json","fields":[{"name":"session_id","type":"uint64","nullable":false}]},"response_schema":{"type":"json"}},{"name":"cancel","family":"query","summary":"Cancel and close an open session","transport_pattern":"reqrep","request_payload_format":"json","response_payload_format":"json","response_mode":"metadata_only","session_behavior":"cancels_session","requires_auth":false,"requires_session":true,"opens_session":false,"closes_session":true,"mutates_state":true,"idempotent":false,"deprecated":false,"disabled":false,"accepted_request_flags":0,"emitted_reply_flags":196,"max_request_bytes":1048576,"max_reply_bytes":1048576,"version_introduced":1,"request_schema":{"type":"json","fields":[{"name":"session_id","type":"uint64","nullable":false}]},"response_schema":{"type":"json"}},{"name":"exec","family":"sql","summary":"Execute SQL and return metadata or rows","transport_pattern":"reqrep","request_payload_format":"arrow_ipc_stream","response_payload_format":"arrow_ipc_stream","response_mode":"metadata_or_rows","session_behavior":"stateless","requires_auth":false,"requires_session":false,"opens_session":false,"closes_session":false,"mutates_state":true,"idempotent":false,"deprecated":false,"disabled":false,"accepted_request_flags":0,"emitted_reply_flags":11,"max_request_bytes":16777216,"max_reply_bytes":16777216,"version_introduced":1,"request_schema":{"fields":[{"name":"sql","type":"utf8","nullable":false},{"name":"want_result","type":"bool","nullable":false}]},"response_schema":{"mode":"metadata_or_rows"}}]} │
    └─────────┴─────────┴──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
    ┌─────────┬─────────┬──────────────┬────────────────┬─────────────┐
    │   ok    │  error  │ rows_changed │ statement_type │ result_type │
    │ boolean │ varchar │    uint64    │     int32      │    int32    │
    ├─────────┼─────────┼──────────────┼────────────────┼─────────────┤
    │ true    │ NULL    │            0 │              7 │           2 │
    └─────────┴─────────┴──────────────┴────────────────┴─────────────┘
    ┌─────────┬─────────┬──────────────┬────────────────┬─────────────┐
    │   ok    │  error  │ rows_changed │ statement_type │ result_type │
    │ boolean │ varchar │    uint64    │     int32      │    int32    │
    ├─────────┼─────────┼──────────────┼────────────────┼─────────────┤
    │ true    │ NULL    │            2 │              2 │           1 │
    └─────────┴─────────┴──────────────┴────────────────┴─────────────┘
    ┌───────┬─────────┐
    │   i   │  gt_10  │
    │ int32 │ boolean │
    ├───────┼─────────┤
    │    10 │ false   │
    │    11 │ true    │
    └───────┴─────────┘
    ┌────────────────────────────┐
    │ ducknng_open_socket('req') │
    │           uint64           │
    ├────────────────────────────┤
    │                          1 │
    └────────────────────────────┘
    ┌────────────────────────────────────────────────────────────────────────┐
    │ ducknng_dial_socket(1, 'ipc:///tmp/ducknng_sql_client_demo.ipc', 1000) │
    │                                boolean                                 │
    ├────────────────────────────────────────────────────────────────────────┤
    │ true                                                                   │
    └────────────────────────────────────────────────────────────────────────┘
    ┌───────────┬──────────┬────────────────────────────────────────┬─────────┬───────────┬───────────┬─────────────────┬─────────────────┐
    │ socket_id │ protocol │                  url                   │  open   │ connected │ listening │ send_timeout_ms │ recv_timeout_ms │
    │  uint64   │ varchar  │                varchar                 │ boolean │  boolean  │  boolean  │      int32      │      int32      │
    ├───────────┼──────────┼────────────────────────────────────────┼─────────┼───────────┼───────────┼─────────────────┼─────────────────┤
    │         1 │ req      │ ipc:///tmp/ducknng_sql_client_demo.ipc │ true    │ true      │ false     │            1000 │            1000 │
    └───────────┴──────────┴────────────────────────────────────────┴─────────┴───────────┴───────────┴─────────────────┴─────────────────┘
    ┌─────────┬─────────┬────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
    │   ok    │  error  │                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                payload                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 │
    │ boolean │ varchar │                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  blob                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  │
    ├─────────┼─────────┼────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
    │ true    │ NULL    │ \x01\x02\x04\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00\xA0\x10\x00\x00\x00\x00\x00\x00manifest{\x22server\x22:{\x22name\x22:\x22ducknng\x22,\x22version\x22:\x220.1.0\x22,\x22protocol_version\x22:1},\x22methods\x22:[{\x22name\x22:\x22manifest\x22,\x22family\x22:\x22control\x22,\x22summary\x22:\x22Return the registry-derived manifest JSON\x22,\x22transport_pattern\x22:\x22reqrep\x22,\x22request_payload_format\x22:\x22none\x22,\x22response_payload_format\x22:\x22json\x22,\x22response_mode\x22:\x22metadata_only\x22,\x22session_behavior\x22:\x22stateless\x22,\x22requires_auth\x22:false,\x22requires_session\x22:false,\x22opens_session\x22:false,\x22closes_session\x22:false,\x22mutates_state\x22:false,\x22idempotent\x22:true,\x22deprecated\x22:false,\x22disabled\x22:false,\x22accepted_request_flags\x22:0,\x22emitted_reply_flags\x22:4,\x22max_request_bytes\x22:0,\x22max_reply_bytes\x22:1048576,\x22version_introduced\x22:1,\x22request_schema\x22:null,\x22response_schema\x22:{\x22type\x22:\x22json\x22}},{\x22name\x22:\x22query_open\x22,\x22family\x22:\x22query\x22,\x22summary\x22:\x22Open a server-side query session\x22,\x22transport_pattern\x22:\x22reqrep\x22,\x22request_payload_format\x22:\x22arrow_ipc_stream\x22,\x22response_payload_format\x22:\x22json\x22,\x22response_mode\x22:\x22session_open\x22,\x22session_behavior\x22:\x22opens_session\x22,\x22requires_auth\x22:false,\x22requires_session\x22:false,\x22opens_session\x22:true,\x22closes_session\x22:false,\x22mutates_state\x22:false,\x22idempotent\x22:false,\x22deprecated\x22:false,\x22disabled\x22:false,\x22accepted_request_flags\x22:0,\x22emitted_reply_flags\x22:36,\x22max_request_bytes\x22:16777216,\x22max_reply_bytes\x22:1048576,\x22version_introduced\x22:1,\x22request_schema\x22:{\x22fields\x22:[{\x22name\x22:\x22sql\x22,\x22type\x22:\x22utf8\x22,\x22nullable\x22:false},{\x22name\x22:\x22batch_rows\x22,\x22type\x22:\x22uint64\x22,\x22nullable\x22:true},{\x22name\x22:\x22batch_bytes\x22,\x22type\x22:\x22uint64\x22,\x22nullable\x22:true}]},\x22response_schema\x22:{\x22type\x22:\x22json\x22,\x22session_open\x22:true}},{\x22name\x22:\x22fetch\x22,\x22family\x22:\x22query\x22,\x22summary\x22:\x22Fetch the next Arrow batch from an open session\x22,\x22transport_pattern\x22:\x22reqrep\x22,\x22request_payload_format\x22:\x22json\x22,\x22response_payload_format\x22:\x22arrow_ipc_stream\x22,\x22response_mode\x22:\x22rows\x22,\x22session_behavior\x22:\x22requires_session\x22,\x22requires_auth\x22:false,\x22requires_session\x22:true,\x22opens_session\x22:false,\x22closes_session\x22:false,\x22mutates_state\x22:false,\x22idempotent\x22:false,\x22deprecated\x22:false,\x22disabled\x22:false,\x22accepted_request_flags\x22:0,\x22emitted_reply_flags\x22:29,\x22max_request_bytes\x22:1048576,\x22max_reply_bytes\x22:16777216,\x22version_introduced\x22:1,\x22request_schema\x22:{\x22type\x22:\x22json\x22,\x22fields\x22:[{\x22name\x22:\x22session_id\x22,\x22type\x22:\x22uint64\x22,\x22nullable\x22:false}]},\x22response_schema\x22:{\x22mode\x22:\x22rows_or_control\x22}},{\x22name\x22:\x22close\x22,\x22family\x22:\x22query\x22,\x22summary\x22:\x22Close an open or exhausted session\x22,\x22transport_pattern\x22:\x22reqrep\x22,\x22request_payload_format\x22:\x22json\x22,\x22response_payload_format\x22:\x22json\x22,\x22response_mode\x22:\x22metadata_only\x22,\x22session_behavior\x22:\x22closes_session\x22,\x22requires_auth\x22:false,\x22requires_session\x22:true,\x22opens_session\x22:false,\x22closes_session\x22:true,\x22mutates_state\x22:true,\x22idempotent\x22:true,\x22deprecated\x22:false,\x22disabled\x22:false,\x22accepted_request_flags\x22:0,\x22emitted_reply_flags\x22:68,\x22max_request_bytes\x22:1048576,\x22max_reply_bytes\x22:1048576,\x22version_introduced\x22:1,\x22request_schema\x22:{\x22type\x22:\x22json\x22,\x22fields\x22:[{\x22name\x22:\x22session_id\x22,\x22type\x22:\x22uint64\x22,\x22nullable\x22:false}]},\x22response_schema\x22:{\x22type\x22:\x22json\x22}},{\x22name\x22:\x22cancel\x22,\x22family\x22:\x22query\x22,\x22summary\x22:\x22Cancel and close an open session\x22,\x22transport_pattern\x22:\x22reqrep\x22,\x22request_payload_format\x22:\x22json\x22,\x22response_payload_format\x22:\x22json\x22,\x22response_mode\x22:\x22metadata_only\x22,\x22session_behavior\x22:\x22cancels_session\x22,\x22requires_auth\x22:false,\x22requires_session\x22:true,\x22opens_session\x22:false,\x22closes_session\x22:true,\x22mutates_state\x22:true,\x22idempotent\x22:false,\x22deprecated\x22:false,\x22disabled\x22:false,\x22accepted_request_flags\x22:0,\x22emitted_reply_flags\x22:196,\x22max_request_bytes\x22:1048576,\x22max_reply_bytes\x22:1048576,\x22version_introduced\x22:1,\x22request_schema\x22:{\x22type\x22:\x22json\x22,\x22fields\x22:[{\x22name\x22:\x22session_id\x22,\x22type\x22:\x22uint64\x22,\x22nullable\x22:false}]},\x22response_schema\x22:{\x22type\x22:\x22json\x22}},{\x22name\x22:\x22exec\x22,\x22family\x22:\x22sql\x22,\x22summary\x22:\x22Execute SQL and return metadata or rows\x22,\x22transport_pattern\x22:\x22reqrep\x22,\x22request_payload_format\x22:\x22arrow_ipc_stream\x22,\x22response_payload_format\x22:\x22arrow_ipc_stream\x22,\x22response_mode\x22:\x22metadata_or_rows\x22,\x22session_behavior\x22:\x22stateless\x22,\x22requires_auth\x22:false,\x22requires_session\x22:false,\x22opens_session\x22:false,\x22closes_session\x22:false,\x22mutates_state\x22:true,\x22idempotent\x22:false,\x22deprecated\x22:false,\x22disabled\x22:false,\x22accepted_request_flags\x22:0,\x22emitted_reply_flags\x22:11,\x22max_request_bytes\x22:16777216,\x22max_reply_bytes\x22:16777216,\x22version_introduced\x22:1,\x22request_schema\x22:{\x22fields\x22:[{\x22name\x22:\x22sql\x22,\x22type\x22:\x22utf8\x22,\x22nullable\x22:false},{\x22name\x22:\x22want_result\x22,\x22type\x22:\x22bool\x22,\x22nullable\x22:false}]},\x22response_schema\x22:{\x22mode\x22:\x22metadata_or_rows\x22}}]} │
    └─────────┴─────────┴────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
    ┌─────────┬─────────┬────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
    │   ok    │  error  │                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                payload                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 │
    │ boolean │ varchar │                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  blob                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  │
    ├─────────┼─────────┼────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
    │ true    │ NULL    │ \x01\x02\x04\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00\xA0\x10\x00\x00\x00\x00\x00\x00manifest{\x22server\x22:{\x22name\x22:\x22ducknng\x22,\x22version\x22:\x220.1.0\x22,\x22protocol_version\x22:1},\x22methods\x22:[{\x22name\x22:\x22manifest\x22,\x22family\x22:\x22control\x22,\x22summary\x22:\x22Return the registry-derived manifest JSON\x22,\x22transport_pattern\x22:\x22reqrep\x22,\x22request_payload_format\x22:\x22none\x22,\x22response_payload_format\x22:\x22json\x22,\x22response_mode\x22:\x22metadata_only\x22,\x22session_behavior\x22:\x22stateless\x22,\x22requires_auth\x22:false,\x22requires_session\x22:false,\x22opens_session\x22:false,\x22closes_session\x22:false,\x22mutates_state\x22:false,\x22idempotent\x22:true,\x22deprecated\x22:false,\x22disabled\x22:false,\x22accepted_request_flags\x22:0,\x22emitted_reply_flags\x22:4,\x22max_request_bytes\x22:0,\x22max_reply_bytes\x22:1048576,\x22version_introduced\x22:1,\x22request_schema\x22:null,\x22response_schema\x22:{\x22type\x22:\x22json\x22}},{\x22name\x22:\x22query_open\x22,\x22family\x22:\x22query\x22,\x22summary\x22:\x22Open a server-side query session\x22,\x22transport_pattern\x22:\x22reqrep\x22,\x22request_payload_format\x22:\x22arrow_ipc_stream\x22,\x22response_payload_format\x22:\x22json\x22,\x22response_mode\x22:\x22session_open\x22,\x22session_behavior\x22:\x22opens_session\x22,\x22requires_auth\x22:false,\x22requires_session\x22:false,\x22opens_session\x22:true,\x22closes_session\x22:false,\x22mutates_state\x22:false,\x22idempotent\x22:false,\x22deprecated\x22:false,\x22disabled\x22:false,\x22accepted_request_flags\x22:0,\x22emitted_reply_flags\x22:36,\x22max_request_bytes\x22:16777216,\x22max_reply_bytes\x22:1048576,\x22version_introduced\x22:1,\x22request_schema\x22:{\x22fields\x22:[{\x22name\x22:\x22sql\x22,\x22type\x22:\x22utf8\x22,\x22nullable\x22:false},{\x22name\x22:\x22batch_rows\x22,\x22type\x22:\x22uint64\x22,\x22nullable\x22:true},{\x22name\x22:\x22batch_bytes\x22,\x22type\x22:\x22uint64\x22,\x22nullable\x22:true}]},\x22response_schema\x22:{\x22type\x22:\x22json\x22,\x22session_open\x22:true}},{\x22name\x22:\x22fetch\x22,\x22family\x22:\x22query\x22,\x22summary\x22:\x22Fetch the next Arrow batch from an open session\x22,\x22transport_pattern\x22:\x22reqrep\x22,\x22request_payload_format\x22:\x22json\x22,\x22response_payload_format\x22:\x22arrow_ipc_stream\x22,\x22response_mode\x22:\x22rows\x22,\x22session_behavior\x22:\x22requires_session\x22,\x22requires_auth\x22:false,\x22requires_session\x22:true,\x22opens_session\x22:false,\x22closes_session\x22:false,\x22mutates_state\x22:false,\x22idempotent\x22:false,\x22deprecated\x22:false,\x22disabled\x22:false,\x22accepted_request_flags\x22:0,\x22emitted_reply_flags\x22:29,\x22max_request_bytes\x22:1048576,\x22max_reply_bytes\x22:16777216,\x22version_introduced\x22:1,\x22request_schema\x22:{\x22type\x22:\x22json\x22,\x22fields\x22:[{\x22name\x22:\x22session_id\x22,\x22type\x22:\x22uint64\x22,\x22nullable\x22:false}]},\x22response_schema\x22:{\x22mode\x22:\x22rows_or_control\x22}},{\x22name\x22:\x22close\x22,\x22family\x22:\x22query\x22,\x22summary\x22:\x22Close an open or exhausted session\x22,\x22transport_pattern\x22:\x22reqrep\x22,\x22request_payload_format\x22:\x22json\x22,\x22response_payload_format\x22:\x22json\x22,\x22response_mode\x22:\x22metadata_only\x22,\x22session_behavior\x22:\x22closes_session\x22,\x22requires_auth\x22:false,\x22requires_session\x22:true,\x22opens_session\x22:false,\x22closes_session\x22:true,\x22mutates_state\x22:true,\x22idempotent\x22:true,\x22deprecated\x22:false,\x22disabled\x22:false,\x22accepted_request_flags\x22:0,\x22emitted_reply_flags\x22:68,\x22max_request_bytes\x22:1048576,\x22max_reply_bytes\x22:1048576,\x22version_introduced\x22:1,\x22request_schema\x22:{\x22type\x22:\x22json\x22,\x22fields\x22:[{\x22name\x22:\x22session_id\x22,\x22type\x22:\x22uint64\x22,\x22nullable\x22:false}]},\x22response_schema\x22:{\x22type\x22:\x22json\x22}},{\x22name\x22:\x22cancel\x22,\x22family\x22:\x22query\x22,\x22summary\x22:\x22Cancel and close an open session\x22,\x22transport_pattern\x22:\x22reqrep\x22,\x22request_payload_format\x22:\x22json\x22,\x22response_payload_format\x22:\x22json\x22,\x22response_mode\x22:\x22metadata_only\x22,\x22session_behavior\x22:\x22cancels_session\x22,\x22requires_auth\x22:false,\x22requires_session\x22:true,\x22opens_session\x22:false,\x22closes_session\x22:true,\x22mutates_state\x22:true,\x22idempotent\x22:false,\x22deprecated\x22:false,\x22disabled\x22:false,\x22accepted_request_flags\x22:0,\x22emitted_reply_flags\x22:196,\x22max_request_bytes\x22:1048576,\x22max_reply_bytes\x22:1048576,\x22version_introduced\x22:1,\x22request_schema\x22:{\x22type\x22:\x22json\x22,\x22fields\x22:[{\x22name\x22:\x22session_id\x22,\x22type\x22:\x22uint64\x22,\x22nullable\x22:false}]},\x22response_schema\x22:{\x22type\x22:\x22json\x22}},{\x22name\x22:\x22exec\x22,\x22family\x22:\x22sql\x22,\x22summary\x22:\x22Execute SQL and return metadata or rows\x22,\x22transport_pattern\x22:\x22reqrep\x22,\x22request_payload_format\x22:\x22arrow_ipc_stream\x22,\x22response_payload_format\x22:\x22arrow_ipc_stream\x22,\x22response_mode\x22:\x22metadata_or_rows\x22,\x22session_behavior\x22:\x22stateless\x22,\x22requires_auth\x22:false,\x22requires_session\x22:false,\x22opens_session\x22:false,\x22closes_session\x22:false,\x22mutates_state\x22:true,\x22idempotent\x22:false,\x22deprecated\x22:false,\x22disabled\x22:false,\x22accepted_request_flags\x22:0,\x22emitted_reply_flags\x22:11,\x22max_request_bytes\x22:16777216,\x22max_reply_bytes\x22:16777216,\x22version_introduced\x22:1,\x22request_schema\x22:{\x22fields\x22:[{\x22name\x22:\x22sql\x22,\x22type\x22:\x22utf8\x22,\x22nullable\x22:false},{\x22name\x22:\x22want_result\x22,\x22type\x22:\x22bool\x22,\x22nullable\x22:false}]},\x22response_schema\x22:{\x22mode\x22:\x22metadata_or_rows\x22}}]} │
    └─────────┴─────────┴────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
    ┌───────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
    │ substr(hex(ducknng_request_socket_raw(1, from_hex('01000000000000000000000000000000000000000000'), 1000)), 1, 28) │
    │                                                      varchar                                                      │
    ├───────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
    │ 0102040000000800000000000000                                                                                      │
    └───────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
    ┌─────────┬─────────┬───────────┬──────────┬──────────────────────────────────────────────────────┐
    │   ok    │ version │ type_name │   name   │ (main."position"(payload_text, '"name":"exec"') > 0) │
    │ boolean │  uint8  │  varchar  │ varchar  │                       boolean                        │
    ├─────────┼─────────┼───────────┼──────────┼──────────────────────────────────────────────────────┤
    │ true    │       1 │ result    │ manifest │ true                                                 │
    └─────────┴─────────┴───────────┴──────────┴──────────────────────────────────────────────────────┘
    ┌─────────┬───────────┬─────────┐
    │   ok    │ type_name │  name   │
    │ boolean │  varchar  │ varchar │
    ├─────────┼───────────┼─────────┤
    │ true    │ result    │ exec    │
    └─────────┴───────────┴─────────┘
    ┌─────────┬─────────┬───────────┬──────────┬──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
    │   ok    │ version │ type_name │   name   │                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           payload_text                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           │
    │ boolean │  uint8  │  varchar  │ varchar  │                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             varchar                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              │
    ├─────────┼─────────┼───────────┼──────────┼──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
    │ true    │       1 │ result    │ manifest │ {"server":{"name":"ducknng","version":"0.1.0","protocol_version":1},"methods":[{"name":"manifest","family":"control","summary":"Return the registry-derived manifest JSON","transport_pattern":"reqrep","request_payload_format":"none","response_payload_format":"json","response_mode":"metadata_only","session_behavior":"stateless","requires_auth":false,"requires_session":false,"opens_session":false,"closes_session":false,"mutates_state":false,"idempotent":true,"deprecated":false,"disabled":false,"accepted_request_flags":0,"emitted_reply_flags":4,"max_request_bytes":0,"max_reply_bytes":1048576,"version_introduced":1,"request_schema":null,"response_schema":{"type":"json"}},{"name":"query_open","family":"query","summary":"Open a server-side query session","transport_pattern":"reqrep","request_payload_format":"arrow_ipc_stream","response_payload_format":"json","response_mode":"session_open","session_behavior":"opens_session","requires_auth":false,"requires_session":false,"opens_session":true,"closes_session":false,"mutates_state":false,"idempotent":false,"deprecated":false,"disabled":false,"accepted_request_flags":0,"emitted_reply_flags":36,"max_request_bytes":16777216,"max_reply_bytes":1048576,"version_introduced":1,"request_schema":{"fields":[{"name":"sql","type":"utf8","nullable":false},{"name":"batch_rows","type":"uint64","nullable":true},{"name":"batch_bytes","type":"uint64","nullable":true}]},"response_schema":{"type":"json","session_open":true}},{"name":"fetch","family":"query","summary":"Fetch the next Arrow batch from an open session","transport_pattern":"reqrep","request_payload_format":"json","response_payload_format":"arrow_ipc_stream","response_mode":"rows","session_behavior":"requires_session","requires_auth":false,"requires_session":true,"opens_session":false,"closes_session":false,"mutates_state":false,"idempotent":false,"deprecated":false,"disabled":false,"accepted_request_flags":0,"emitted_reply_flags":29,"max_request_bytes":1048576,"max_reply_bytes":16777216,"version_introduced":1,"request_schema":{"type":"json","fields":[{"name":"session_id","type":"uint64","nullable":false}]},"response_schema":{"mode":"rows_or_control"}},{"name":"close","family":"query","summary":"Close an open or exhausted session","transport_pattern":"reqrep","request_payload_format":"json","response_payload_format":"json","response_mode":"metadata_only","session_behavior":"closes_session","requires_auth":false,"requires_session":true,"opens_session":false,"closes_session":true,"mutates_state":true,"idempotent":true,"deprecated":false,"disabled":false,"accepted_request_flags":0,"emitted_reply_flags":68,"max_request_bytes":1048576,"max_reply_bytes":1048576,"version_introduced":1,"request_schema":{"type":"json","fields":[{"name":"session_id","type":"uint64","nullable":false}]},"response_schema":{"type":"json"}},{"name":"cancel","family":"query","summary":"Cancel and close an open session","transport_pattern":"reqrep","request_payload_format":"json","response_payload_format":"json","response_mode":"metadata_only","session_behavior":"cancels_session","requires_auth":false,"requires_session":true,"opens_session":false,"closes_session":true,"mutates_state":true,"idempotent":false,"deprecated":false,"disabled":false,"accepted_request_flags":0,"emitted_reply_flags":196,"max_request_bytes":1048576,"max_reply_bytes":1048576,"version_introduced":1,"request_schema":{"type":"json","fields":[{"name":"session_id","type":"uint64","nullable":false}]},"response_schema":{"type":"json"}},{"name":"exec","family":"sql","summary":"Execute SQL and return metadata or rows","transport_pattern":"reqrep","request_payload_format":"arrow_ipc_stream","response_payload_format":"arrow_ipc_stream","response_mode":"metadata_or_rows","session_behavior":"stateless","requires_auth":false,"requires_session":false,"opens_session":false,"closes_session":false,"mutates_state":true,"idempotent":false,"deprecated":false,"disabled":false,"accepted_request_flags":0,"emitted_reply_flags":11,"max_request_bytes":16777216,"max_reply_bytes":16777216,"version_introduced":1,"request_schema":{"fields":[{"name":"sql","type":"utf8","nullable":false},{"name":"want_result","type":"bool","nullable":false}]},"response_schema":{"mode":"metadata_or_rows"}}]} │
    └─────────┴─────────┴───────────┴──────────┴──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
    ┌─────────────────────────┐
    │ ducknng_close_socket(1) │
    │         boolean         │
    ├─────────────────────────┤
    │ true                    │
    └─────────────────────────┘
    ┌────────────────────────────────────────┐
    │ ducknng_stop_server('sql_client_demo') │
    │                boolean                 │
    ├────────────────────────────────────────┤
    │ true                                   │
    └────────────────────────────────────────┘

### Use `ducknng_ncurl()` against a local `nanonext` HTTPS server

A tiny local R `nanonext` HTTPS server is started during README
rendering so this example runs without depending on the public internet.
The core server definition is shown below because the transport helper
and its carrier behavior are part of the example rather than hidden
setup. The hidden setup only launches the script in the background and
manages the temporary PID, log, and CA files needed while rendering.
`ducknng_ncurl(...)` is the low-level HTTP/HTTPS transport primitive;
the higher-level request, RPC, and session helpers have not been
auto-routed over `http://` and `https://` yet.

``` r
library(nanonext)

cert <- write_cert(cn = '127.0.0.1')
writeLines(cert$client[[1]], "/tmp/ducknng_readme_http_demo_ca.pem")

server <- http_server(
  url = 'https://127.0.0.1:18443',
  handlers = list(
    handler('/hello', function(req) {
      list(
        status = 200L,
        headers = c(
          'Content-Type' = 'text/plain',
          'X-Test' = 'hello'
        ),
        body = 'hello from nanonext https server'
      )
    }),
    handler('/echo', function(req) {
      list(
        status = 200L,
        headers = c(
          'Content-Type' = (
            req$headers[['Content-Type']] %||% 'application/octet-stream'
          ),
          'X-Test' = 'echo'
        ),
        body = req$body
      )
    }, method = 'POST')
  ),
  tls = tls_config(server = cert$server)
)
```

``` sql
LOAD 'build/release/ducknng.duckdb_extension';
-- Register a client TLS handle that trusts the self-signed HTTPS demo server.
SELECT ducknng_tls_config_from_files(
  NULL,                                     -- cert_key_file
  '/tmp/ducknng_readme_http_demo_ca.pem',   -- ca_file
  NULL,                                     -- password
  2                                         -- auth_mode = require certificate validation
);

-- Call /hello through ducknng_ncurl(): NULL method means GET and NULL body means no request body.
SELECT ok, status, error, body_text
FROM ducknng_ncurl(
  'https://127.0.0.1:18443/hello', -- url
  NULL,                            -- method
  NULL,                            -- headers_json
  NULL,                            -- body
  2000,                            -- timeout_ms
  1::UBIGINT                       -- tls_config_id
);

-- POST can send raw bytes while still exposing HTTP headers and status in-band.
SET VARIABLE echo_headers =
  '[{"name":"Content-Type","value":"application/octet-stream"}]';

SELECT
  ok,
  status,
  error,
  hex(body) AS body_hex,
  position('X-Test' IN headers_json) > 0 AS has_header
FROM ducknng_ncurl(
  'https://127.0.0.1:18443/echo', -- url
  'POST',                         -- method
  getvariable('echo_headers'),    -- headers_json
  from_hex('01020304'),           -- body
  2000,                           -- timeout_ms
  1::UBIGINT                      -- tls_config_id
);

-- Drop the temporary client TLS handle after the HTTPS checks complete.
SELECT ducknng_drop_tls_config(1);
```

    ┌──────────────────────────────────────────────────────────────────────────────────────┐
    │ ducknng_tls_config_from_files(NULL, '/tmp/ducknng_readme_http_demo_ca.pem', NULL, 2) │
    │                                        uint64                                        │
    ├──────────────────────────────────────────────────────────────────────────────────────┤
    │                                                                                    1 │
    └──────────────────────────────────────────────────────────────────────────────────────┘
    ┌─────────┬────────┬─────────┬──────────────────────────────────┐
    │   ok    │ status │  error  │            body_text             │
    │ boolean │ int32  │ varchar │             varchar              │
    ├─────────┼────────┼─────────┼──────────────────────────────────┤
    │ true    │    200 │ NULL    │ hello from nanonext https server │
    └─────────┴────────┴─────────┴──────────────────────────────────┘
    ┌─────────┬────────┬─────────┬──────────┬────────────┐
    │   ok    │ status │  error  │ body_hex │ has_header │
    │ boolean │ int32  │ varchar │ varchar  │  boolean   │
    ├─────────┼────────┼─────────┼──────────┼────────────┤
    │ true    │    200 │ NULL    │ 01020304 │ true       │
    └─────────┴────────┴─────────┴──────────┴────────────┘
    ┌────────────────────────────┐
    │ ducknng_drop_tls_config(1) │
    │          boolean           │
    ├────────────────────────────┤
    │ true                       │
    └────────────────────────────┘

### Launch raw socket send/recv airos and inspect send status explicitly

``` sql
LOAD 'build/release/ducknng.duckdb_extension';
-- Open one listening pair socket and one dialed peer.
SELECT ducknng_open_socket('pair');
SELECT ducknng_listen_socket(1, 'ipc:///tmp/ducknng_sql_pair_aio_demo.ipc', 134217728, 0::UBIGINT);
SELECT ducknng_open_socket('pair');
SELECT ducknng_dial_socket(2, 'ipc:///tmp/ducknng_sql_pair_aio_demo.ipc', 1000);

-- Start one async receive and one async send.
CREATE TEMP TABLE pair_recv AS
SELECT ducknng_recv_socket_raw_aio(1::UBIGINT, 1000) AS recv_aio;

CREATE TEMP TABLE pair_send AS
SELECT ducknng_send_socket_raw_aio(2::UBIGINT, from_hex('6173796e632072657175657374'), 1000) AS send_aio;

-- Collect the terminal send result. Send-only airos succeed with frame = NULL.
SELECT aio_id, ok, frame IS NULL AS no_frame
FROM ducknng_aio_collect((SELECT list_value(send_aio) FROM pair_send), 1000);

-- Collect the received raw frame from the peer side.
SELECT aio_id, ok, hex(frame) = '6173796E632072657175657374' AS got_payload
FROM ducknng_aio_collect((SELECT list_value(recv_aio) FROM pair_recv), 1000);

-- Inspect whether the send phase completed successfully.
SELECT kind, state, phase, send_done, send_ok, recv_done, recv_ok IS NULL AS recv_ok_is_null
FROM ducknng_aio_status((SELECT send_aio FROM pair_send));

-- Clean up the terminal aio handles, temp state, and socket handles.
SELECT ducknng_aio_drop((SELECT send_aio FROM pair_send));
SELECT ducknng_aio_drop((SELECT recv_aio FROM pair_recv));
DROP TABLE pair_send;
DROP TABLE pair_recv;
SELECT ducknng_close_socket(2);
SELECT ducknng_close_socket(1);
```

    ┌─────────────────────────────┐
    │ ducknng_open_socket('pair') │
    │           uint64            │
    ├─────────────────────────────┤
    │                           1 │
    └─────────────────────────────┘
    ┌───────────────────────────────────────────────────────────────────────────────────────────────────────┐
    │ ducknng_listen_socket(1, 'ipc:///tmp/ducknng_sql_pair_aio_demo.ipc', 134217728, CAST(0 AS "UBIGINT")) │
    │                                                boolean                                                │
    ├───────────────────────────────────────────────────────────────────────────────────────────────────────┤
    │ true                                                                                                  │
    └───────────────────────────────────────────────────────────────────────────────────────────────────────┘
    ┌─────────────────────────────┐
    │ ducknng_open_socket('pair') │
    │           uint64            │
    ├─────────────────────────────┤
    │                           2 │
    └─────────────────────────────┘
    ┌──────────────────────────────────────────────────────────────────────────┐
    │ ducknng_dial_socket(2, 'ipc:///tmp/ducknng_sql_pair_aio_demo.ipc', 1000) │
    │                                 boolean                                  │
    ├──────────────────────────────────────────────────────────────────────────┤
    │ true                                                                     │
    └──────────────────────────────────────────────────────────────────────────┘
    ┌────────┬─────────┬──────────┐
    │ aio_id │   ok    │ no_frame │
    │ uint64 │ boolean │ boolean  │
    ├────────┼─────────┼──────────┤
    │      2 │ true    │ true     │
    └────────┴─────────┴──────────┘
    ┌────────┬─────────┬─────────────┐
    │ aio_id │   ok    │ got_payload │
    │ uint64 │ boolean │   boolean   │
    ├────────┼─────────┼─────────────┤
    │      1 │ true    │ true        │
    └────────┴─────────┴─────────────┘
    ┌─────────┬───────────┬─────────┬───────────┬─────────┬───────────┬─────────────────┐
    │  kind   │   state   │  phase  │ send_done │ send_ok │ recv_done │ recv_ok_is_null │
    │ varchar │  varchar  │ varchar │  boolean  │ boolean │  boolean  │     boolean     │
    ├─────────┼───────────┼─────────┼───────────┼─────────┼───────────┼─────────────────┤
    │ send    │ collected │ send    │ true      │ true    │ false     │ true            │
    └─────────┴───────────┴─────────┴───────────┴─────────┴───────────┴─────────────────┘
    ┌────────────────────────────────────────────────────┐
    │ ducknng_aio_drop((SELECT send_aio FROM pair_send)) │
    │                      boolean                       │
    ├────────────────────────────────────────────────────┤
    │ true                                               │
    └────────────────────────────────────────────────────┘
    ┌────────────────────────────────────────────────────┐
    │ ducknng_aio_drop((SELECT recv_aio FROM pair_recv)) │
    │                      boolean                       │
    ├────────────────────────────────────────────────────┤
    │ true                                               │
    └────────────────────────────────────────────────────┘
    ┌─────────────────────────┐
    │ ducknng_close_socket(2) │
    │         boolean         │
    ├─────────────────────────┤
    │ true                    │
    └─────────────────────────┘
    ┌─────────────────────────┐
    │ ducknng_close_socket(1) │
    │         boolean         │
    ├─────────────────────────┤
    │ true                    │
    └─────────────────────────┘

### Push one raw message through `push` / `pull`

``` sql
LOAD 'build/release/ducknng.duckdb_extension';
-- Open one pull socket, listen on it, then dial it from a push peer.
SELECT ducknng_open_socket('pull');
SELECT ducknng_listen_socket(1, 'ipc:///tmp/ducknng_sql_pushpull_demo.ipc', 134217728, 0::UBIGINT);
SELECT ducknng_open_socket('push');
SELECT ducknng_dial_socket(2, 'ipc:///tmp/ducknng_sql_pushpull_demo.ipc', 1000);

-- Receive asynchronously on pull so the send side can return immediately.
CREATE TEMP TABLE pushpull_recv AS
SELECT ducknng_recv_socket_raw_aio(1::UBIGINT, 1000) AS recv_aio;

SELECT ducknng_send_socket_raw(2::UBIGINT, from_hex('707573682d70756c6c'), 1000) AS sent;

SELECT ok, hex(frame) = '707573682D70756C6C' AS got_payload
FROM ducknng_aio_collect((SELECT list_value(recv_aio) FROM pushpull_recv), 1000);

SELECT ducknng_aio_drop((SELECT recv_aio FROM pushpull_recv));
DROP TABLE pushpull_recv;
SELECT ducknng_close_socket(2);
SELECT ducknng_close_socket(1);
```

    ┌─────────────────────────────┐
    │ ducknng_open_socket('pull') │
    │           uint64            │
    ├─────────────────────────────┤
    │                           1 │
    └─────────────────────────────┘
    ┌───────────────────────────────────────────────────────────────────────────────────────────────────────┐
    │ ducknng_listen_socket(1, 'ipc:///tmp/ducknng_sql_pushpull_demo.ipc', 134217728, CAST(0 AS "UBIGINT")) │
    │                                                boolean                                                │
    ├───────────────────────────────────────────────────────────────────────────────────────────────────────┤
    │ true                                                                                                  │
    └───────────────────────────────────────────────────────────────────────────────────────────────────────┘
    ┌─────────────────────────────┐
    │ ducknng_open_socket('push') │
    │           uint64            │
    ├─────────────────────────────┤
    │                           2 │
    └─────────────────────────────┘
    ┌──────────────────────────────────────────────────────────────────────────┐
    │ ducknng_dial_socket(2, 'ipc:///tmp/ducknng_sql_pushpull_demo.ipc', 1000) │
    │                                 boolean                                  │
    ├──────────────────────────────────────────────────────────────────────────┤
    │ true                                                                     │
    └──────────────────────────────────────────────────────────────────────────┘
    ┌─────────┐
    │  sent   │
    │ boolean │
    ├─────────┤
    │ true    │
    └─────────┘
    ┌─────────┬─────────────┐
    │   ok    │ got_payload │
    │ boolean │   boolean   │
    ├─────────┼─────────────┤
    │ true    │ true        │
    └─────────┴─────────────┘
    ┌────────────────────────────────────────────────────────┐
    │ ducknng_aio_drop((SELECT recv_aio FROM pushpull_recv)) │
    │                        boolean                         │
    ├────────────────────────────────────────────────────────┤
    │ true                                                   │
    └────────────────────────────────────────────────────────┘
    ┌─────────────────────────┐
    │ ducknng_close_socket(2) │
    │         boolean         │
    ├─────────────────────────┤
    │ true                    │
    └─────────────────────────┘
    ┌─────────────────────────┐
    │ ducknng_close_socket(1) │
    │         boolean         │
    ├─────────────────────────┤
    │ true                    │
    └─────────────────────────┘

### Publish one raw message through `pub` / `sub`

``` sql
LOAD 'build/release/ducknng.duckdb_extension';
-- Open one publisher, then subscribe from a sub peer to all topics.
SELECT ducknng_open_socket('pub');
SELECT ducknng_listen_socket(1, 'ipc:///tmp/ducknng_sql_pubsub_demo.ipc', 134217728, 0::UBIGINT);
SELECT ducknng_open_socket('sub');
SELECT ducknng_subscribe_socket(2::UBIGINT, from_hex(''));
SELECT ducknng_dial_socket(2, 'ipc:///tmp/ducknng_sql_pubsub_demo.ipc', 1000);

CREATE TEMP TABLE pubsub_recv AS
SELECT ducknng_recv_socket_raw_aio(2::UBIGINT, 1000) AS recv_aio;

SELECT ducknng_send_socket_raw(1::UBIGINT, from_hex('7075622d737562'), 1000) AS sent;

SELECT ok, hex(frame) = '7075622D737562' AS got_payload
FROM ducknng_aio_collect((SELECT list_value(recv_aio) FROM pubsub_recv), 1000);

SELECT ducknng_aio_drop((SELECT recv_aio FROM pubsub_recv));
DROP TABLE pubsub_recv;
SELECT ducknng_close_socket(2);
SELECT ducknng_close_socket(1);
```

    ┌────────────────────────────┐
    │ ducknng_open_socket('pub') │
    │           uint64           │
    ├────────────────────────────┤
    │                          1 │
    └────────────────────────────┘
    ┌─────────────────────────────────────────────────────────────────────────────────────────────────────┐
    │ ducknng_listen_socket(1, 'ipc:///tmp/ducknng_sql_pubsub_demo.ipc', 134217728, CAST(0 AS "UBIGINT")) │
    │                                               boolean                                               │
    ├─────────────────────────────────────────────────────────────────────────────────────────────────────┤
    │ true                                                                                                │
    └─────────────────────────────────────────────────────────────────────────────────────────────────────┘
    ┌────────────────────────────┐
    │ ducknng_open_socket('sub') │
    │           uint64           │
    ├────────────────────────────┤
    │                          2 │
    └────────────────────────────┘
    ┌──────────────────────────────────────────────────────────────┐
    │ ducknng_subscribe_socket(CAST(2 AS "UBIGINT"), from_hex('')) │
    │                           boolean                            │
    ├──────────────────────────────────────────────────────────────┤
    │ true                                                         │
    └──────────────────────────────────────────────────────────────┘
    ┌────────────────────────────────────────────────────────────────────────┐
    │ ducknng_dial_socket(2, 'ipc:///tmp/ducknng_sql_pubsub_demo.ipc', 1000) │
    │                                boolean                                 │
    ├────────────────────────────────────────────────────────────────────────┤
    │ true                                                                   │
    └────────────────────────────────────────────────────────────────────────┘
    ┌─────────┐
    │  sent   │
    │ boolean │
    ├─────────┤
    │ true    │
    └─────────┘
    ┌─────────┬─────────────┐
    │   ok    │ got_payload │
    │ boolean │   boolean   │
    ├─────────┼─────────────┤
    │ true    │ true        │
    └─────────┴─────────────┘
    ┌──────────────────────────────────────────────────────┐
    │ ducknng_aio_drop((SELECT recv_aio FROM pubsub_recv)) │
    │                       boolean                        │
    ├──────────────────────────────────────────────────────┤
    │ true                                                 │
    └──────────────────────────────────────────────────────┘
    ┌─────────────────────────┐
    │ ducknng_close_socket(2) │
    │         boolean         │
    ├─────────────────────────┤
    │ true                    │
    └─────────────────────────┘
    ┌─────────────────────────┐
    │ ducknng_close_socket(1) │
    │         boolean         │
    ├─────────────────────────┤
    │ true                    │
    └─────────────────────────┘

### Exchange one survey and one response through `surveyor` / `respondent`

``` sql
LOAD 'build/release/ducknng.duckdb_extension';
-- Open one respondent listener and one surveyor peer.
SELECT ducknng_open_socket('respondent');
SELECT ducknng_listen_socket(1, 'ipc:///tmp/ducknng_sql_survey_demo.ipc', 134217728, 0::UBIGINT);
SELECT ducknng_open_socket('surveyor');
SELECT ducknng_dial_socket(2, 'ipc:///tmp/ducknng_sql_survey_demo.ipc', 1000);

-- Receive the survey on the respondent side first.
CREATE TEMP TABLE respondent_recv AS
SELECT ducknng_recv_socket_raw_aio(1::UBIGINT, 1000) AS recv_aio;

SELECT ducknng_send_socket_raw(2::UBIGINT, from_hex('737572766579'), 1000) AS sent_survey;

SELECT ok, hex(frame) = '737572766579' AS got_survey
FROM ducknng_aio_collect((SELECT list_value(recv_aio) FROM respondent_recv), 1000);

-- Then receive the respondent reply back on the surveyor side.
CREATE TEMP TABLE surveyor_recv AS
SELECT ducknng_recv_socket_raw_aio(2::UBIGINT, 1000) AS recv_aio;

SELECT ducknng_send_socket_raw(1::UBIGINT, from_hex('726573706f6e7365'), 1000) AS sent_response;

SELECT ok, hex(frame) = '726573706F6E7365' AS got_response
FROM ducknng_aio_collect((SELECT list_value(recv_aio) FROM surveyor_recv), 1000);

SELECT ducknng_aio_drop((SELECT recv_aio FROM respondent_recv));
SELECT ducknng_aio_drop((SELECT recv_aio FROM surveyor_recv));
DROP TABLE respondent_recv;
DROP TABLE surveyor_recv;
SELECT ducknng_close_socket(2);
SELECT ducknng_close_socket(1);
```

    ┌───────────────────────────────────┐
    │ ducknng_open_socket('respondent') │
    │              uint64               │
    ├───────────────────────────────────┤
    │                                 1 │
    └───────────────────────────────────┘
    ┌─────────────────────────────────────────────────────────────────────────────────────────────────────┐
    │ ducknng_listen_socket(1, 'ipc:///tmp/ducknng_sql_survey_demo.ipc', 134217728, CAST(0 AS "UBIGINT")) │
    │                                               boolean                                               │
    ├─────────────────────────────────────────────────────────────────────────────────────────────────────┤
    │ true                                                                                                │
    └─────────────────────────────────────────────────────────────────────────────────────────────────────┘
    ┌─────────────────────────────────┐
    │ ducknng_open_socket('surveyor') │
    │             uint64              │
    ├─────────────────────────────────┤
    │                               2 │
    └─────────────────────────────────┘
    ┌────────────────────────────────────────────────────────────────────────┐
    │ ducknng_dial_socket(2, 'ipc:///tmp/ducknng_sql_survey_demo.ipc', 1000) │
    │                                boolean                                 │
    ├────────────────────────────────────────────────────────────────────────┤
    │ true                                                                   │
    └────────────────────────────────────────────────────────────────────────┘
    ┌─────────────┐
    │ sent_survey │
    │   boolean   │
    ├─────────────┤
    │ true        │
    └─────────────┘
    ┌─────────┬────────────┐
    │   ok    │ got_survey │
    │ boolean │  boolean   │
    ├─────────┼────────────┤
    │ true    │ true       │
    └─────────┴────────────┘
    ┌───────────────┐
    │ sent_response │
    │    boolean    │
    ├───────────────┤
    │ true          │
    └───────────────┘
    ┌─────────┬──────────────┐
    │   ok    │ got_response │
    │ boolean │   boolean    │
    ├─────────┼──────────────┤
    │ true    │ true         │
    └─────────┴──────────────┘
    ┌──────────────────────────────────────────────────────────┐
    │ ducknng_aio_drop((SELECT recv_aio FROM respondent_recv)) │
    │                         boolean                          │
    ├──────────────────────────────────────────────────────────┤
    │ true                                                     │
    └──────────────────────────────────────────────────────────┘
    ┌────────────────────────────────────────────────────────┐
    │ ducknng_aio_drop((SELECT recv_aio FROM surveyor_recv)) │
    │                        boolean                         │
    ├────────────────────────────────────────────────────────┤
    │ true                                                   │
    └────────────────────────────────────────────────────────┘
    ┌─────────────────────────┐
    │ ducknng_close_socket(2) │
    │         boolean         │
    ├─────────────────────────┤
    │ true                    │
    └─────────────────────────┘
    ┌─────────────────────────┐
    │ ducknng_close_socket(1) │
    │         boolean         │
    ├─────────────────────────┤
    │ true                    │
    └─────────────────────────┘

### Launch raw requests asynchronously and collect the reply frames later

``` sql
LOAD 'build/release/ducknng.duckdb_extension';
-- Start a local listener that the aio requests will target.
SELECT ducknng_start_server(
  'sql_aio_demo',                -- service name
  'ipc:///tmp/ducknng_sql_aio_demo.ipc', -- listen URL
  1,                             -- REP contexts
  134217728,                     -- recv_max_bytes
  300000,                        -- session_idle_ms
  0                              -- tls_config_id (0 means plaintext)
);

-- Launch two raw request/reply futures and keep their aio ids in one temp row.
-- timeout_ms bounds the pending network operation itself; it is separate from the later wait_ms passed to ducknng_aio_collect().
-- The hex literal below is the built-in manifest request frame in the current wire format.
CREATE TEMP TABLE aio_demo AS
SELECT
  ducknng_request_raw_aio(
    'ipc:///tmp/ducknng_sql_aio_demo.ipc', -- url
    from_hex('01000000000000000000000000000000000000000000'), -- manifest request frame
    1000,                                  -- timeout_ms
    0::UBIGINT                             -- tls_config_id
  ) AS aio1,
  ducknng_request_raw_aio(
    'ipc:///tmp/ducknng_sql_aio_demo.ipc', -- url
    from_hex('01000000000000000000000000000000000000000000'), -- manifest request frame
    1000,                                  -- timeout_ms
    0::UBIGINT                             -- tls_config_id
  ) AS aio2;

-- Confirm that both aio launches returned runtime handle ids.
SELECT aio1 > 0 AS aio1_started, aio2 > aio1 AS aio2_started_after_aio1
FROM aio_demo;

-- Wait for terminal results and return the full reply frames for later decoding.
SELECT aio_id, ok, octet_length(frame) > 0 AS has_frame
FROM ducknng_aio_collect((SELECT list_value(aio1, aio2) FROM aio_demo), 1000)
ORDER BY aio_id;

-- A collected aio is terminal, so ready() stays true until the handle is dropped.
SELECT ducknng_aio_ready(aio1) AS aio1_ready, ducknng_aio_ready(aio2) AS aio2_ready
FROM aio_demo;

-- Drop the terminal aio handles explicitly because SQL has no destructor hook.
SELECT ducknng_aio_drop(aio1) AND ducknng_aio_drop(aio2) AS dropped
FROM aio_demo;

-- Remove the temp state and stop the demo listener.
DROP TABLE aio_demo;
SELECT ducknng_stop_server('sql_aio_demo');
```

    ┌──────────────────────────────────────────────────────────────────────────────────────────────────────┐
    │ ducknng_start_server('sql_aio_demo', 'ipc:///tmp/ducknng_sql_aio_demo.ipc', 1, 134217728, 300000, 0) │
    │                                               boolean                                                │
    ├──────────────────────────────────────────────────────────────────────────────────────────────────────┤
    │ true                                                                                                 │
    └──────────────────────────────────────────────────────────────────────────────────────────────────────┘
    ┌──────────────┬─────────────────────────┐
    │ aio1_started │ aio2_started_after_aio1 │
    │   boolean    │         boolean         │
    ├──────────────┼─────────────────────────┤
    │ true         │ true                    │
    └──────────────┴─────────────────────────┘
    ┌────────┬─────────┬───────────┐
    │ aio_id │   ok    │ has_frame │
    │ uint64 │ boolean │  boolean  │
    ├────────┼─────────┼───────────┤
    │      1 │ true    │ true      │
    │      2 │ true    │ true      │
    └────────┴─────────┴───────────┘
    ┌────────────┬────────────┐
    │ aio1_ready │ aio2_ready │
    │  boolean   │  boolean   │
    ├────────────┼────────────┤
    │ true       │ true       │
    └────────────┴────────────┘
    ┌─────────┐
    │ dropped │
    │ boolean │
    ├─────────┤
    │ true    │
    └─────────┘
    ┌─────────────────────────────────────┐
    │ ducknng_stop_server('sql_aio_demo') │
    │               boolean               │
    ├─────────────────────────────────────┤
    │ true                                │
    └─────────────────────────────────────┘

### Launch unary RPC calls asynchronously and decode the replies later

These helpers sit above the same request/reply aio substrate as
`ducknng_request_raw_aio(...)`, but they build the manifest and exec
request frames for you. They still collect raw reply frames later, so
decoding remains explicit and honest.

``` sql
LOAD 'build/release/ducknng.duckdb_extension';
-- Start a local listener for the async RPC wrapper demo.
SELECT ducknng_start_server(
  'sql_rpc_aio_demo',            -- service name
  'ipc:///tmp/ducknng_sql_rpc_aio_demo.ipc', -- listen URL
  1,                             -- REP contexts
  134217728,                     -- recv_max_bytes
  300000,                        -- session_idle_ms
  0                              -- tls_config_id (0 means plaintext)
);

-- Register exec so the later async exec wrapper has a real method to call.
SELECT ducknng_register_exec_method();

-- Launch one manifest request and one metadata-only exec request asynchronously.
SET VARIABLE manifest_aio = ducknng_get_rpc_manifest_raw_aio(
  'ipc:///tmp/ducknng_sql_rpc_aio_demo.ipc', -- url
  1000,                                      -- timeout_ms
  0::UBIGINT                                 -- tls_config_id
);

SET VARIABLE exec_aio = ducknng_run_rpc_raw_aio(
  'ipc:///tmp/ducknng_sql_rpc_aio_demo.ipc', -- url
  'CREATE TABLE IF NOT EXISTS rpc_aio_demo_t(i INTEGER)', -- sql
  1000,                                      -- timeout_ms
  0::UBIGINT                                 -- tls_config_id
);

SELECT getvariable('manifest_aio') > 0 AS manifest_aio_started,
       getvariable('exec_aio') > getvariable('manifest_aio') AS exec_aio_started_after_manifest
;

-- Collect both terminal results. The collected values are still raw frames.
CREATE TEMP TABLE rpc_aio_collect AS
SELECT *
FROM ducknng_aio_collect(list_value(getvariable('manifest_aio'), getvariable('exec_aio')), 1000);

-- Store the collected raw frames explicitly before decoding them.
SET VARIABLE manifest_frame = (
  SELECT frame
  FROM rpc_aio_collect
  WHERE aio_id = getvariable('manifest_aio')
);

SET VARIABLE exec_frame = (
  SELECT frame
  FROM rpc_aio_collect
  WHERE aio_id = getvariable('exec_aio')
);

-- Decode the manifest reply frame explicitly after collection.
SELECT ok, type_name, name, position('"name":"exec"' IN payload_text) > 0 AS has_exec
FROM ducknng_decode_frame(getvariable('manifest_frame'));

-- Decode the async exec reply frame the same way.
SELECT ok, type_name, name
FROM ducknng_decode_frame(getvariable('exec_frame'));

-- Drop the terminal aio handles, remove the temp state, and stop the demo server.
SELECT ducknng_aio_drop(getvariable('manifest_aio')) AND ducknng_aio_drop(getvariable('exec_aio')) AS dropped;
DROP TABLE rpc_aio_collect;
SELECT ducknng_stop_server('sql_rpc_aio_demo');
```

    ┌──────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
    │ ducknng_start_server('sql_rpc_aio_demo', 'ipc:///tmp/ducknng_sql_rpc_aio_demo.ipc', 1, 134217728, 300000, 0) │
    │                                                   boolean                                                    │
    ├──────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
    │ true                                                                                                         │
    └──────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
    ┌────────────────────────────────┐
    │ ducknng_register_exec_method() │
    │            boolean             │
    ├────────────────────────────────┤
    │ true                           │
    └────────────────────────────────┘
    ┌──────────────────────┬─────────────────────────────────┐
    │ manifest_aio_started │ exec_aio_started_after_manifest │
    │       boolean        │             boolean             │
    ├──────────────────────┼─────────────────────────────────┤
    │ true                 │ true                            │
    └──────────────────────┴─────────────────────────────────┘
    ┌─────────┬───────────┬──────────┬──────────┐
    │   ok    │ type_name │   name   │ has_exec │
    │ boolean │  varchar  │ varchar  │ boolean  │
    ├─────────┼───────────┼──────────┼──────────┤
    │ true    │ result    │ manifest │ true     │
    └─────────┴───────────┴──────────┴──────────┘
    ┌─────────┬───────────┬─────────┐
    │   ok    │ type_name │  name   │
    │ boolean │  varchar  │ varchar │
    ├─────────┼───────────┼─────────┤
    │ true    │ result    │ exec    │
    └─────────┴───────────┴─────────┘
    ┌─────────┐
    │ dropped │
    │ boolean │
    ├─────────┤
    │ true    │
    └─────────┘
    ┌─────────────────────────────────────────┐
    │ ducknng_stop_server('sql_rpc_aio_demo') │
    │                 boolean                 │
    ├─────────────────────────────────────────┤
    │ true                                    │
    └─────────────────────────────────────────┘

### Open, fetch, and close a query session explicitly

``` sql
LOAD 'build/release/ducknng.duckdb_extension';
-- Start a listener that will own the server-side query session.
SELECT ducknng_start_server(
  'sql_session_demo',            -- service name
  'ipc:///tmp/ducknng_sql_session_demo.ipc', -- listen URL
  1,                             -- REP contexts
  134217728,                     -- recv_max_bytes
  300000,                        -- session_idle_ms
  0                              -- tls_config_id (0 means plaintext)
);

-- Open one server-owned query session.
-- batch_rows = 0 and batch_bytes = 0 mean "use the server defaults" for this demo.
SET VARIABLE session_id = (
  SELECT session_id
  FROM ducknng_open_query(
    'ipc:///tmp/ducknng_sql_session_demo.ipc', -- url
    'SELECT 1 AS id UNION ALL SELECT 2 AS id ORDER BY id', -- SQL text to run remotely
    0::UBIGINT,                                -- batch_rows
    0::UBIGINT,                                -- batch_bytes
    0::UBIGINT                                 -- tls_config_id
  )
);

SELECT getvariable('session_id') AS opened_session_id;

-- The first fetch returns one Arrow IPC batch in payload.
SELECT ok, session_id, state IS NULL AS state_is_null, octet_length(payload) > 0 AS has_payload, end_of_stream
FROM ducknng_fetch_query(
  'ipc:///tmp/ducknng_sql_session_demo.ipc', -- url
  getvariable('session_id')::UBIGINT,        -- session_id returned by open_query
  0::UBIGINT,                                -- batch_rows override
  0::UBIGINT,                                -- batch_bytes override
  0::UBIGINT                                 -- tls_config_id
);

-- The second fetch returns JSON control metadata saying the session is exhausted.
SELECT ok, session_id, state, payload IS NULL AS no_payload, end_of_stream
FROM ducknng_fetch_query(
  'ipc:///tmp/ducknng_sql_session_demo.ipc', -- url
  getvariable('session_id')::UBIGINT,        -- session_id returned by open_query
  0::UBIGINT,                                -- batch_rows override
  0::UBIGINT,                                -- batch_bytes override
  0::UBIGINT                                 -- tls_config_id
);

-- Close the exhausted session explicitly.
SELECT *
FROM ducknng_close_query(
  'ipc:///tmp/ducknng_sql_session_demo.ipc', -- url
  getvariable('session_id')::UBIGINT,        -- session_id returned by open_query
  0::UBIGINT                                 -- tls_config_id
);

-- Stop the demo listener after the session is closed.
SELECT ducknng_stop_server('sql_session_demo');
```

    ┌──────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
    │ ducknng_start_server('sql_session_demo', 'ipc:///tmp/ducknng_sql_session_demo.ipc', 1, 134217728, 300000, 0) │
    │                                                   boolean                                                    │
    ├──────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
    │ true                                                                                                         │
    └──────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
    ┌───────────────────┐
    │ opened_session_id │
    │      uint64       │
    ├───────────────────┤
    │                 1 │
    └───────────────────┘
    ┌─────────┬────────────┬───────────────┬─────────────┬───────────────┐
    │   ok    │ session_id │ state_is_null │ has_payload │ end_of_stream │
    │ boolean │   uint64   │    boolean    │   boolean   │    boolean    │
    ├─────────┼────────────┼───────────────┼─────────────┼───────────────┤
    │ true    │          1 │ true          │ true        │ false         │
    └─────────┴────────────┴───────────────┴─────────────┴───────────────┘
    ┌─────────┬────────────┬───────────┬────────────┬───────────────┐
    │   ok    │ session_id │   state   │ no_payload │ end_of_stream │
    │ boolean │   uint64   │  varchar  │  boolean   │    boolean    │
    ├─────────┼────────────┼───────────┼────────────┼───────────────┤
    │ true    │          1 │ exhausted │ true       │ true          │
    └─────────┴────────────┴───────────┴────────────┴───────────────┘
    ┌─────────┬─────────┬────────────┬─────────┬─────────────┬───────────────────────────────────┐
    │   ok    │  error  │ session_id │  state  │ next_method │           control_json            │
    │ boolean │ varchar │   uint64   │ varchar │   varchar   │              varchar              │
    ├─────────┼─────────┼────────────┼─────────┼─────────────┼───────────────────────────────────┤
    │ true    │ NULL    │          1 │ closed  │ NULL        │ {"session_id":1,"state":"closed"} │
    └─────────┴─────────┴────────────┴─────────┴─────────────┴───────────────────────────────────┘
    ┌─────────────────────────────────────────┐
    │ ducknng_stop_server('sql_session_demo') │
    │                 boolean                 │
    ├─────────────────────────────────────────┤
    │ true                                    │
    └─────────────────────────────────────────┘

### `tls+tcp://` with a self-signed development TLS config

``` sql
LOAD 'build/release/ducknng.duckdb_extension';
-- Generate a self-signed loopback certificate and keep it as a runtime TLS handle.
SELECT ducknng_self_signed_tls_config('127.0.0.1', 365, 0);

-- Start a TLS listener using the generated config handle.
SELECT ducknng_start_server(
  'tls_demo_self',               -- service name
  'tls+tcp://127.0.0.1:45453',   -- listen URL
  1,                             -- REP contexts
  134217728,                     -- recv_max_bytes
  300000,                        -- session_idle_ms
  1                              -- tls_config_id returned above
);

-- Send the built-in manifest request frame over TLS and decode the reply.
SELECT ok, type_name, name, position('"name":"exec"' IN payload_text) > 0
FROM ducknng_decode_frame(
  ducknng_request_raw(
    'tls+tcp://127.0.0.1:45453',                -- url
    from_hex('01000000000000000000000000000000000000000000'), -- manifest request frame
    1000,                                       -- timeout_ms
    1                                           -- tls_config_id
  )
);

-- Clean up the TLS demo server and config handle.
SELECT ducknng_stop_server('tls_demo_self');
SELECT ducknng_drop_tls_config(1);
```

    ┌─────────────────────────────────────────────────────┐
    │ ducknng_self_signed_tls_config('127.0.0.1', 365, 0) │
    │                       uint64                        │
    ├─────────────────────────────────────────────────────┤
    │                                                   1 │
    └─────────────────────────────────────────────────────┘
    ┌─────────────────────────────────────────────────────────────────────────────────────────────┐
    │ ducknng_start_server('tls_demo_self', 'tls+tcp://127.0.0.1:45453', 1, 134217728, 300000, 1) │
    │                                           boolean                                           │
    ├─────────────────────────────────────────────────────────────────────────────────────────────┤
    │ true                                                                                        │
    └─────────────────────────────────────────────────────────────────────────────────────────────┘
    ┌─────────┬───────────┬──────────┬──────────────────────────────────────────────────────┐
    │   ok    │ type_name │   name   │ (main."position"(payload_text, '"name":"exec"') > 0) │
    │ boolean │  varchar  │ varchar  │                       boolean                        │
    ├─────────┼───────────┼──────────┼──────────────────────────────────────────────────────┤
    │ true    │ result    │ manifest │ false                                                │
    └─────────┴───────────┴──────────┴──────────────────────────────────────────────────────┘
    ┌──────────────────────────────────────┐
    │ ducknng_stop_server('tls_demo_self') │
    │               boolean                │
    ├──────────────────────────────────────┤
    │ true                                 │
    └──────────────────────────────────────┘
    ┌────────────────────────────┐
    │ ducknng_drop_tls_config(1) │
    │          boolean           │
    ├────────────────────────────┤
    │ true                       │
    └────────────────────────────┘

### `tls+tcp://` from file-backed certificate material

``` sql
LOAD 'build/release/ducknng.duckdb_extension';
-- Register a file-backed TLS config using committed loopback test certificates.
SELECT ducknng_tls_config_from_files(
  'test/certs/loopback-cert-key.pem', -- cert_key_file
  'test/certs/loopback-ca.pem',       -- ca_file
  NULL,                               -- password
  0                                   -- auth_mode
);

-- Start a TLS listener with that file-backed config.
SELECT ducknng_start_server(
  'tls_demo_files',              -- service name
  'tls+tcp://127.0.0.1:45454',   -- listen URL
  1,                             -- REP contexts
  134217728,                     -- recv_max_bytes
  300000,                        -- session_idle_ms
  1                              -- tls_config_id returned above
);

-- Inspect the manifest reply over TLS using the same config handle on the client side.
SELECT ok, type_name, name, position('"name":"exec"' IN payload_text) > 0
FROM ducknng_decode_frame(
  ducknng_request_raw(
    'tls+tcp://127.0.0.1:45454',                -- url
    from_hex('01000000000000000000000000000000000000000000'), -- manifest request frame
    1000,                                       -- timeout_ms
    1                                           -- tls_config_id
  )
);

-- Clean up the file-backed TLS demo server and config handle.
SELECT ducknng_stop_server('tls_demo_files');
SELECT ducknng_drop_tls_config(1);
```

    ┌──────────────────────────────────────────────────────────────────────────────────────────────────────────┐
    │ ducknng_tls_config_from_files('test/certs/loopback-cert-key.pem', 'test/certs/loopback-ca.pem', NULL, 0) │
    │                                                  uint64                                                  │
    ├──────────────────────────────────────────────────────────────────────────────────────────────────────────┤
    │                                                                                                        1 │
    └──────────────────────────────────────────────────────────────────────────────────────────────────────────┘
    ┌──────────────────────────────────────────────────────────────────────────────────────────────┐
    │ ducknng_start_server('tls_demo_files', 'tls+tcp://127.0.0.1:45454', 1, 134217728, 300000, 1) │
    │                                           boolean                                            │
    ├──────────────────────────────────────────────────────────────────────────────────────────────┤
    │ true                                                                                         │
    └──────────────────────────────────────────────────────────────────────────────────────────────┘
    ┌─────────┬───────────┬──────────┬──────────────────────────────────────────────────────┐
    │   ok    │ type_name │   name   │ (main."position"(payload_text, '"name":"exec"') > 0) │
    │ boolean │  varchar  │ varchar  │                       boolean                        │
    ├─────────┼───────────┼──────────┼──────────────────────────────────────────────────────┤
    │ true    │ result    │ manifest │ false                                                │
    └─────────┴───────────┴──────────┴──────────────────────────────────────────────────────┘
    ┌───────────────────────────────────────┐
    │ ducknng_stop_server('tls_demo_files') │
    │                boolean                │
    ├───────────────────────────────────────┤
    │ true                                  │
    └───────────────────────────────────────┘
    ┌────────────────────────────┐
    │ ducknng_drop_tls_config(1) │
    │          boolean           │
    ├────────────────────────────┤
    │ true                       │
    └────────────────────────────┘

### REQ/REP `EXEC` via `nanonext` as an interop example

This example is easier to follow as a short sequence: define the frame
helpers, define the retry helpers, start a local server, run a few
remote `exec` requests, inspect the same table locally, and then clean
up.

#### Frame encoding and decoding helpers

``` r
# Little-endian helpers for the versioned ducknng RPC envelope.
write_u32le <- function(x) {
  writeBin(as.integer(x), raw(), size = 4L, endian = "little")
}

write_u64le <- function(x) {
  x <- as.double(x)
  c(write_u32le(x %% 2^32), write_u32le(floor(x / 2^32)))
}

read_u32le_bytes <- function(x) {
  sum(as.double(as.integer(x)) * 256^(0:3))
}

read_u64le_bytes <- function(x) {
  read_u32le_bytes(x[1:4]) + 2^32 * read_u32le_bytes(x[5:8])
}

read_u32le <- function(buf, offset) {
  read_u32le_bytes(buf[offset + 0:3])
}

read_u64le <- function(buf, offset) {
  read_u64le_bytes(buf[offset + 0:7])
}

# Encode the Arrow IPC payload carried by the built-in exec RPC method.
encode_ducknng_exec_payload <- function(sql, want_result = FALSE) {
  raw_con <- rawConnection(raw(), open = "r+")
  on.exit(close(raw_con), add = TRUE)

  nanoarrow::write_nanoarrow(
    data.frame(sql = sql, want_result = want_result),
    raw_con
  )

  rawConnectionValue(raw_con)
}

# Build a version 1 RPC call frame for the named method.
encode_ducknng_call_frame <- function(method, payload, flags = 0L) {
  method_name <- charToRaw(method)

  c(
    as.raw(1L),
    as.raw(1L),
    write_u32le(flags),
    write_u32le(length(method_name)),
    write_u32le(0),
    write_u64le(length(payload)),
    method_name,
    payload
  )
}

encode_ducknng_exec_request <- function(sql, want_result = FALSE) {
  encode_ducknng_call_frame(
    method = "exec",
    payload = encode_ducknng_exec_payload(sql, want_result = want_result)
  )
}

# Decode a reply envelope and parse its Arrow IPC payload when present.
decode_ducknng_exec_reply <- function(buf) {
  header_size <- 23L

  if (!is.raw(buf) || length(buf) < (header_size - 1L)) {
    stop("ducknng reply frame was empty or truncated", call. = FALSE)
  }

  name_len <- read_u32le(buf, 7)
  error_len <- read_u32le(buf, 11)
  payload_len <- read_u64le(buf, 15)

  name_start <- header_size
  error_start <- name_start + name_len
  payload_start <- error_start + error_len
  payload_end <- payload_start + payload_len - 1L

  payload <- if (payload_len > 0) buf[payload_start:payload_end] else raw()

  list(
    version = as.integer(buf[1]),
    type = as.integer(buf[2]),
    flags = read_u32le(buf, 3),
    method = rawToChar(buf[name_start:(error_start - 1L)]),
    error = if (error_len > 0) rawToChar(buf[error_start:(payload_start - 1L)]) else "",
    payload = payload,
    data = if (payload_len > 0) as.data.frame(nanoarrow::read_nanoarrow(payload)) else NULL
  )
}
```

#### REQ/REP retry helpers

``` r
# Send and receive with the same retry style used in mangoro examples.
send_ducknng_frame <- function(socket, frame, timeout_ms = 1000L, max_attempts = 20L) {
  attempt <- 1L
  send_result <- nanonext::send(socket, frame, mode = "raw", block = timeout_ms)

  while (nanonext::is_error_value(send_result) && attempt < max_attempts) {
    Sys.sleep(0.25)
    send_result <- nanonext::send(socket, frame, mode = "raw", block = timeout_ms)
    attempt <- attempt + 1L
  }

  send_result
}

recv_ducknng_frame <- function(socket, timeout_ms = 1000L, max_attempts = 20L) {
  attempt <- 1L
  response <- nanonext::recv(socket, mode = "raw", block = timeout_ms)

  while (
    (
      nanonext::is_error_value(response) ||
      !is.raw(response) ||
      length(response) < 22L
    ) &&
    attempt < max_attempts
  ) {
    Sys.sleep(0.25)
    response <- nanonext::recv(socket, mode = "raw", block = timeout_ms)
    attempt <- attempt + 1L
  }

  response
}

run_ducknng_req <- function(socket, frame, timeout_ms = 1000L) {
  send_ducknng_frame(socket, frame, timeout_ms = timeout_ms)
  decode_ducknng_exec_reply(
    recv_ducknng_frame(socket, timeout_ms = timeout_ms)
  )
}

# Wait for an IPC listener path to appear before dialing it.
wait_for_ducknng_listener <- function(path, timeout_secs = 10, interval_secs = 0.1) {
  deadline <- Sys.time() + timeout_secs

  while (Sys.time() < deadline) {
    if (file.exists(path)) {
      return(invisible(TRUE))
    }
    Sys.sleep(interval_secs)
  }

  stop("ducknng IPC listener did not become ready in time", call. = FALSE)
}
```

#### Start a local ducknng server and dial a req socket

``` r
# Start a ducknng server in this R session, then talk to it over a req socket.
ext_path <- normalizePath("build/release/ducknng.duckdb_extension")
ipc_path <- tempfile(pattern = "ducknng_readme_exec_", tmpdir = "/tmp", fileext = ".ipc")
ipc_url <- paste0("ipc://", ipc_path)

db_driver <- duckdb::duckdb(
  dbdir = ":memory:",
  config = list(allow_unsigned_extensions = "true")
)
db_con <- DBI::dbConnect(db_driver)

DBI::dbGetQuery(db_con, "SELECT 42 AS ok")
#>   ok
#> 1 42
DBI::dbExecute(db_con, sprintf("LOAD '%s'", ext_path))
#> [1] 0
DBI::dbGetQuery(
  db_con,
  sprintf(
    "SELECT ducknng_start_server('sql_exec', '%s', 1, 134217728, 300000, 0::UBIGINT)",
    ipc_url
  )
)
#>   ducknng_start_server('sql_exec', 'ipc:///tmp/ducknng_readme_exec_1eb23657d3b691.ipc', 1, 134217728, 300000, CAST(0 AS "UBIGINT"))
#> 1                                                                                                                              TRUE
DBI::dbGetQuery(db_con, "SELECT ducknng_register_exec_method()")
#>   ducknng_register_exec_method()
#> 1                           TRUE

wait_for_ducknng_listener(ipc_path)
req <- nanonext::socket("req", dial = ipc_url)
```

#### Create and insert rows remotely

``` r
# Create a table remotely and inspect the metadata reply.
create_reply <- run_ducknng_req(
  req,
  encode_ducknng_exec_request("CREATE TABLE ducknng_exec_demo(i INTEGER)")
)
create_reply$data
#>   rows_changed statement_type result_type
#> 1            0              7           2

# Insert rows remotely and inspect the second metadata reply.
insert_reply <- run_ducknng_req(
  req,
  encode_ducknng_exec_request("INSERT INTO ducknng_exec_demo VALUES (42), (99)")
)
insert_reply$data
#>   rows_changed statement_type result_type
#> 1            2              2           1
```

#### Query rows remotely with `want_result = TRUE`

``` r
select_reply <- run_ducknng_req(
  req,
  encode_ducknng_exec_request(
    "SELECT i, i > 50 AS gt_50 FROM ducknng_exec_demo ORDER BY i",
    want_result = TRUE
  )
)
select_reply$data
#>    i gt_50
#> 1 42 FALSE
#> 2 99  TRUE
```

#### Inspect the same table locally through DuckDB

``` r
server_rows <- DBI::dbGetQuery(db_con, "SELECT * FROM ducknng_exec_demo ORDER BY i")
server_rows
#>    i
#> 1 42
#> 2 99
```

#### Stop the server and clean up

``` r
DBI::dbGetQuery(db_con, "SELECT ducknng_stop_server('sql_exec')")
#>   ducknng_stop_server('sql_exec')
#> 1                            TRUE
close(req)
DBI::dbDisconnect(db_con)
unlink(ipc_path)
```

## References

- [NNG](https://nng.nanomsg.org/) for the underlying messaging library
  and transport family.
- [`r-lib/nanonext`](https://github.com/r-lib/nanonext) for the main
  client/server ergonomics reference.
- [`sounkou-bioinfo/mangoro`](https://github.com/sounkou-bioinfo/mangoro)
  for the thin-envelope + Arrow IPC RPC direction.
- [DuckDB C API](https://duckdb.org/docs/stable/clients/c/api) for the
  extension and SQL integration boundary.
- [Apache Arrow
  IPC](https://arrow.apache.org/docs/format/Columnar.html#serialization-and-interprocess-communication-ipc)
  for the tabular payload format.
