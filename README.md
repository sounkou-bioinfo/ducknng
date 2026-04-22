
<!-- README.md is generated from README.Rmd. Please edit that file -->

# ducknng

`ducknng` is a pure C DuckDB extension for DuckDB + NNG REQ/REP interop.

Its SQL-facing client and server interface is explicitly modeled over
[`r-lib/nanonext`](https://github.com/r-lib/nanonext), taking req/rep
sockets, dial/listen semantics, context-oriented NNG usage, and a
practical messaging surface as the reference point for what should feel
natural from DuckDB SQL. Its RPC framing and tabular payload direction
are likewise informed by Arrow IPC based RPC work such as
[`sounkou-bioinfo/mangoro`](https://github.com/sounkou-bioinfo/mangoro)
and related projects, where a thin envelope is kept separate from Arrow
IPC request and reply payloads instead of being buried inside one-off
method-specific binaries.

At the moment, `ducknng` already exposes a low-level transport layer
with socket-open, dial, close, and raw request primitives, alongside
SQL-visible server lifecycle control and runtime introspection through
`ducknng_start_server()`, `ducknng_stop_server()`, and
`ducknng_list_servers()`. That transport surface is operation-oriented
rather than transport-specific: transport is autodetected from the URL
scheme in the same general style as `nanonext`, so the same API can work
with `inproc://`, `ipc://`, `tcp://`, and `tls+tcp://` without spawning
a separate helper family for each scheme. On the server side it runs one
REP socket with one or more REP contexts, and on top of that transport
it already supports a working RPC request/reply path with manifest
discovery, metadata-oriented execution, and unary row-returning
execution over the current raw wire and Arrow IPC model.

The next documented protocol slice is the session query family:
`query_open`, `fetch`, `close`, and `cancel`. In the current contract,
`query_open` opens one server-owned query session and returns JSON
control metadata, `fetch` is the only method that may return streamed
row batches, `close` is the normal explicit cleanup path, and `cancel`
is best-effort rather than a guarantee of immediate interruption. Until
a real owner-identity model is implemented, that family should still be
treated as development scaffolding rather than as a production-safe
multi-client surface.

The transport-security utility layer now has a runtime model for TLS
configuration handles. Those handles can be assembled from file-backed
certificate material, from in-memory PEM strings, or from self-signed
development certificates generated on demand. The actual `tls+tcp://`
transport wiring remains isolated behind `ducknng_nng_compat.c`, but it
is now enabled for real loopback listener and one-shot client-request
paths so the public API no longer needs a separate transport-specific
helper family. This follows the same broad direction as `nanonext`,
which exposes TLS helper facilities backed by mbedTLS rather than
forcing every client to hand-roll certificate setup outside the
transport layer.

## Functions

# Function Catalog

This file is generated from `function_catalog/functions.yaml`.

## Service Control

| name                   | kind   | arguments                                                                                                                                                                                                | returns   | description                         |
|------------------------|--------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-----------|-------------------------------------|
| `ducknng_start_server` | scalar | `name, listen, contexts, recv_max_bytes, session_idle_ms, tls_cert_key_file, tls_ca_file, tls_auth_mode) or ducknng_start_server(name, listen, contexts, recv_max_bytes, session_idle_ms, tls_config_id` | `BOOLEAN` | Start a named ducknng REP listener. |
| `ducknng_stop_server`  | scalar | `name`                                                                                                                                                                                                   | `BOOLEAN` | Stop a named ducknng service.       |

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

| name                         | kind   | arguments                        | returns                                                                                                                                                  | description                                                                                         |
|------------------------------|--------|----------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------|
| `ducknng_open_socket`        | scalar | `protocol`                       | `UBIGINT`                                                                                                                                                | Open a client socket handle for a supported protocol.                                               |
| `ducknng_dial_socket`        | scalar | `socket_id, url, timeout_ms`     | `BOOLEAN`                                                                                                                                                | Dial a URL using an opened socket handle.                                                           |
| `ducknng_close_socket`       | scalar | `socket_id`                      | `BOOLEAN`                                                                                                                                                | Close a client socket handle.                                                                       |
| `ducknng_list_sockets`       | table  |                                  | `TABLE(socket_id UBIGINT, protocol VARCHAR, url VARCHAR, open BOOLEAN, connected BOOLEAN, send_timeout_ms INTEGER, recv_timeout_ms INTEGER)`             | List client socket handles in the runtime.                                                          |
| `ducknng_request`            | table  | `url, payload, timeout_ms`       | `TABLE(ok BOOLEAN, error VARCHAR, payload BLOB)`                                                                                                         | Perform a one-shot raw request and return a structured result row.                                  |
| `ducknng_request_socket`     | table  | `socket_id, payload, timeout_ms` | `TABLE(ok BOOLEAN, error VARCHAR, payload BLOB)`                                                                                                         | Perform a raw request through a previously dialed socket handle and return a structured result row. |
| `ducknng_request_raw`        | scalar | `url, payload, timeout_ms`       | `BLOB`                                                                                                                                                   | Perform a one-shot raw request and return the raw reply frame bytes.                                |
| `ducknng_request_socket_raw` | scalar | `socket_id, payload, timeout_ms` | `BLOB`                                                                                                                                                   | Perform a raw request through a dialed socket handle and return the raw reply frame bytes.          |
| `ducknng_decode_frame`       | table  | `frame`                          | `TABLE(ok BOOLEAN, error VARCHAR, version UTINYINT, type UTINYINT, flags UINTEGER, type_name VARCHAR, name VARCHAR, payload BLOB, payload_text VARCHAR)` | Decode a raw ducknng frame into envelope fields and extracted payload columns.                      |

## Transport Security

| name                             | kind   | arguments                                        | returns                                                                                                                                                                                                                 | description                                                                            |
|----------------------------------|--------|--------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------|
| `ducknng_list_tls_configs`       | table  |                                                  | `TABLE(tls_config_id UBIGINT, source VARCHAR, enabled BOOLEAN, has_cert_key_file BOOLEAN, has_ca_file BOOLEAN, has_cert_pem BOOLEAN, has_key_pem BOOLEAN, has_ca_pem BOOLEAN, has_password BOOLEAN, auth_mode INTEGER)` | List registered TLS config handles and the kinds of material they contain.             |
| `ducknng_drop_tls_config`        | scalar | `tls_config_id`                                  | `BOOLEAN`                                                                                                                                                                                                               | Remove a registered TLS config handle from the runtime.                                |
| `ducknng_self_signed_tls_config` | scalar | `common_name, valid_days, auth_mode`             | `UBIGINT`                                                                                                                                                                                                               | Generate a self-signed development certificate and register it as a TLS config handle. |
| `ducknng_tls_config_from_pem`    | scalar | `cert_pem, key_pem, ca_pem, password, auth_mode` | `UBIGINT`                                                                                                                                                                                                               | Register a TLS config handle from in-memory PEM material.                              |
| `ducknng_tls_config_from_files`  | scalar | `cert_key_file, ca_file, password, auth_mode`    | `UBIGINT`                                                                                                                                                                                                               | Register a TLS config handle from file-backed certificate material.                    |

## RPC Helper

| name                           | kind   | arguments  | returns                                                                                               | description                                                                                   |
|--------------------------------|--------|------------|-------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------|
| `ducknng_get_rpc_manifest`     | table  | `url`      | `TABLE(ok BOOLEAN, error VARCHAR, manifest VARCHAR)`                                                  | Request the RPC manifest and return a structured result row.                                  |
| `ducknng_get_rpc_manifest_raw` | scalar | `url`      | `BLOB`                                                                                                | Request the RPC manifest and return the raw reply frame as BLOB.                              |
| `ducknng_run_rpc`              | table  | `url, sql` | `TABLE(ok BOOLEAN, error VARCHAR, rows_changed UBIGINT, statement_type INTEGER, result_type INTEGER)` | Execute a metadata-oriented RPC call and return a structured result row.                      |
| `ducknng_run_rpc_raw`          | scalar | `url, sql` | `BLOB`                                                                                                | Execute the exec RPC and return the raw reply frame as BLOB.                                  |
| `ducknng_query_rpc`            | table  | `url, sql` | `table`                                                                                               | Execute a row-returning RPC query and expose the unary Arrow IPC row reply as a DuckDB table. |

## Build

``` sh
make configure
make release
```

See also `NEWS.md` for the current implementation status and planned
next steps, and `docs/protocol.md`, `docs/manifest.md`,
`docs/security.md`, `docs/registry.md`, and `docs/types.md` for the
binding transport, TLS, and session/query-family contract. Those design
docs also define the current TLS direction: certificate and key material
can be accepted either from files or from in-memory PEM content, with
helper utilities for development certificate generation and
client/server TLS configuration rather than a file-only setup path.

## Examples

### Start an IPC listener and inspect the registry

``` sql
LOAD '/root/ducknng/build/release/ducknng.duckdb_extension';
SELECT ducknng_start_server(
  'sql0',                         -- service name
  'ipc:///tmp/ducknng_sql0.ipc', -- listen URL
  1,                              -- REP contexts
  134217728,                      -- recv_max_bytes
  300000,                         -- session_idle_ms
  NULL,                           -- tls_cert_key_file
  NULL,                           -- tls_ca_file
  NULL                            -- tls_auth_mode
);

SELECT name, listen, contexts, running, sessions
FROM ducknng_list_servers();

SELECT ducknng_stop_server('sql0');
```

    ┌─────────────────────────────────────────────────────────────────────────────────────────────────────┐
    │ ducknng_start_server('sql0', 'ipc:///tmp/ducknng_sql0.ipc', 1, 134217728, 300000, NULL, NULL, NULL) │
    │                                               boolean                                               │
    ├─────────────────────────────────────────────────────────────────────────────────────────────────────┤
    │ true                                                                                                │
    └─────────────────────────────────────────────────────────────────────────────────────────────────────┘
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
LOAD '/root/ducknng/build/release/ducknng.duckdb_extension';
SELECT ducknng_start_server(
  'sql_multi',                    -- service name
  'ipc:///tmp/ducknng_sql_multi.ipc', -- listen URL
  3,                              -- REP contexts
  134217728,                      -- recv_max_bytes
  300000,                         -- session_idle_ms
  NULL,                           -- tls_cert_key_file
  NULL,                           -- tls_ca_file
  NULL                            -- tls_auth_mode
);

SELECT name, contexts, running
FROM ducknng_list_servers()
WHERE name = 'sql_multi';

SELECT ducknng_stop_server('sql_multi');
```

    ┌───────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
    │ ducknng_start_server('sql_multi', 'ipc:///tmp/ducknng_sql_multi.ipc', 3, 134217728, 300000, NULL, NULL, NULL) │
    │                                                    boolean                                                    │
    ├───────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
    │ true                                                                                                          │
    └───────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
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
LOAD '/root/ducknng/build/release/ducknng.duckdb_extension';
-- Start a local ducknng service that the following client examples will talk to.
SELECT ducknng_start_server(
  'sql_client_demo',              -- service name
  'ipc:///tmp/ducknng_sql_client_demo.ipc', -- listen URL
  1,                              -- REP contexts
  134217728,                      -- recv_max_bytes
  300000,                         -- session_idle_ms
  NULL,                           -- tls_cert_key_file
  NULL,                           -- tls_ca_file
  NULL                            -- tls_auth_mode
);

-- The default RPC surface is minimal: manifest is built in, exec is opt-in.
SELECT * FROM ducknng_get_rpc_manifest('ipc:///tmp/ducknng_sql_client_demo.ipc');
SELECT * FROM ducknng_list_methods();

-- Register exec explicitly before exposing SQL execution over RPC.
SELECT ducknng_register_exec_method();
SELECT * FROM ducknng_list_methods();
SELECT * FROM ducknng_get_rpc_manifest('ipc:///tmp/ducknng_sql_client_demo.ipc');

-- RPC helper: run non-row statements and keep errors in-band.
SELECT * FROM ducknng_run_rpc('ipc:///tmp/ducknng_sql_client_demo.ipc', 'CREATE TABLE IF NOT EXISTS client_side_demo(i INTEGER)');
SELECT * FROM ducknng_run_rpc('ipc:///tmp/ducknng_sql_client_demo.ipc', 'INSERT INTO client_side_demo VALUES (10), (11)');

-- RPC helper: fetch row results through the unary query path.
SELECT * FROM ducknng_query_rpc('ipc:///tmp/ducknng_sql_client_demo.ipc', 'SELECT i, i > 10 AS gt_10 FROM client_side_demo ORDER BY i');

-- Primitive transport layer: open a socket handle, dial it, and inspect the registry.
SELECT ducknng_open_socket('req');
SELECT ducknng_dial_socket(1, 'ipc:///tmp/ducknng_sql_client_demo.ipc', 1000);
SELECT * FROM ducknng_list_sockets();

-- Primitive transport layer: send a raw manifest frame and inspect the reply prefix.
SELECT * FROM ducknng_request_socket(1::UBIGINT, from_hex('01000000000000000000000000000000000000000000'), 1000);
SELECT * FROM ducknng_request('ipc:///tmp/ducknng_sql_client_demo.ipc', from_hex('01000000000000000000000000000000000000000000'), 1000);

-- Raw scalar forms now mean raw reply frames as BLOBs.
SELECT substr(hex(ducknng_request_socket_raw(1, from_hex('01000000000000000000000000000000000000000000'), 1000)), 1, 28);

-- Decode raw manifest and exec replies into envelope fields plus extracted text payload.
SELECT ok, version, type_name, name, position('"name":"exec"' IN payload_text) > 0
FROM ducknng_decode_frame(ducknng_get_rpc_manifest_raw('ipc:///tmp/ducknng_sql_client_demo.ipc'));

SELECT ok, type_name, name
FROM ducknng_decode_frame(
  ducknng_run_rpc_raw('ipc:///tmp/ducknng_sql_client_demo.ipc', 'CREATE TABLE IF NOT EXISTS client_side_demo(i INTEGER)')
);

-- The generic raw request helper can be decoded the same way.
SELECT ok, version, type_name, name, payload_text
FROM ducknng_decode_frame(
  ducknng_request_raw('ipc:///tmp/ducknng_sql_client_demo.ipc', from_hex('01000000000000000000000000000000000000000000'), 1000)
);

-- Close the client socket handle and stop the demo server.
SELECT ducknng_close_socket(1);
SELECT ducknng_stop_server('sql_client_demo');
```

    ┌───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
    │ ducknng_start_server('sql_client_demo', 'ipc:///tmp/ducknng_sql_client_demo.ipc', 1, 134217728, 300000, NULL, NULL, NULL) │
    │                                                          boolean                                                          │
    ├───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
    │ true                                                                                                                      │
    └───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
    ┌─────────┬─────────┬──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
    │   ok    │  error  │                                                                                                                                                                                                                                                                                                                                               manifest                                                                                                                                                                                                                                                                                                                                               │
    │ boolean │ varchar │                                                                                                                                                                                                                                                                                                                                               varchar                                                                                                                                                                                                                                                                                                                                                │
    ├─────────┼─────────┼──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
    │ true    │ NULL    │ {"server":{"name":"ducknng","version":"0.1.0","protocol_version":1},"methods":[{"name":"manifest","family":"control","summary":"Return the registry-derived manifest JSON","transport_pattern":"reqrep","request_payload_format":"none","response_payload_format":"json","response_mode":"metadata_only","session_behavior":"stateless","requires_auth":false,"requires_session":false,"opens_session":false,"closes_session":false,"mutates_state":false,"idempotent":true,"deprecated":false,"disabled":false,"accepted_request_flags":0,"emitted_reply_flags":4,"max_request_bytes":0,"max_reply_bytes":1048576,"version_introduced":1,"request_schema":null,"response_schema":{"type":"json"}}]} │
    └─────────┴─────────┴──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
    ┌──────────┬─────────┬───────────────────────────────────────────┬───────────────────┬────────────────────────┬─────────────────────────┬───────────────┬─────────────────────┬──────────────────────┬───────────────┬──────────┐
    │   name   │ family  │                  summary                  │ transport_pattern │ request_payload_format │ response_payload_format │ response_mode │ request_schema_json │ response_schema_json │ requires_auth │ disabled │
    │ varchar  │ varchar │                  varchar                  │      varchar      │        varchar         │         varchar         │    varchar    │       varchar       │       varchar        │    boolean    │ boolean  │
    ├──────────┼─────────┼───────────────────────────────────────────┼───────────────────┼────────────────────────┼─────────────────────────┼───────────────┼─────────────────────┼──────────────────────┼───────────────┼──────────┤
    │ manifest │ control │ Return the registry-derived manifest JSON │ reqrep            │ none                   │ json                    │ metadata_only │ NULL                │ {"type":"json"}      │ false         │ false    │
    └──────────┴─────────┴───────────────────────────────────────────┴───────────────────┴────────────────────────┴─────────────────────────┴───────────────┴─────────────────────┴──────────────────────┴───────────────┴──────────┘
    ┌────────────────────────────────┐
    │ ducknng_register_exec_method() │
    │            boolean             │
    ├────────────────────────────────┤
    │ true                           │
    └────────────────────────────────┘
    ┌──────────┬─────────┬───────────────────────────────────────────┬───────────────────┬────────────────────────┬─────────────────────────┬──────────────────┬──────────────────────────────────────────────────────────────────────────────────────────────────────────────────┬─────────────────────────────┬───────────────┬──────────┐
    │   name   │ family  │                  summary                  │ transport_pattern │ request_payload_format │ response_payload_format │  response_mode   │                                               request_schema_json                                                │    response_schema_json     │ requires_auth │ disabled │
    │ varchar  │ varchar │                  varchar                  │      varchar      │        varchar         │         varchar         │     varchar      │                                                     varchar                                                      │           varchar           │    boolean    │ boolean  │
    ├──────────┼─────────┼───────────────────────────────────────────┼───────────────────┼────────────────────────┼─────────────────────────┼──────────────────┼──────────────────────────────────────────────────────────────────────────────────────────────────────────────────┼─────────────────────────────┼───────────────┼──────────┤
    │ manifest │ control │ Return the registry-derived manifest JSON │ reqrep            │ none                   │ json                    │ metadata_only    │ NULL                                                                                                             │ {"type":"json"}             │ false         │ false    │
    │ exec     │ sql     │ Execute SQL and return metadata or rows   │ reqrep            │ arrow_ipc_stream       │ arrow_ipc_stream        │ metadata_or_rows │ {"fields":[{"name":"sql","type":"utf8","nullable":false},{"name":"want_result","type":"bool","nullable":false}]} │ {"mode":"metadata_or_rows"} │ false         │ false    │
    └──────────┴─────────┴───────────────────────────────────────────┴───────────────────┴────────────────────────┴─────────────────────────┴──────────────────┴──────────────────────────────────────────────────────────────────────────────────────────────────────────────────┴─────────────────────────────┴───────────────┴──────────┘
    ┌─────────┬─────────┬────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
    │   ok    │  error  │                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  manifest                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  │
    │ boolean │ varchar │                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  varchar                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   │
    ├─────────┼─────────┼────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
    │ true    │ NULL    │ {"server":{"name":"ducknng","version":"0.1.0","protocol_version":1},"methods":[{"name":"manifest","family":"control","summary":"Return the registry-derived manifest JSON","transport_pattern":"reqrep","request_payload_format":"none","response_payload_format":"json","response_mode":"metadata_only","session_behavior":"stateless","requires_auth":false,"requires_session":false,"opens_session":false,"closes_session":false,"mutates_state":false,"idempotent":true,"deprecated":false,"disabled":false,"accepted_request_flags":0,"emitted_reply_flags":4,"max_request_bytes":0,"max_reply_bytes":1048576,"version_introduced":1,"request_schema":null,"response_schema":{"type":"json"}},{"name":"exec","family":"sql","summary":"Execute SQL and return metadata or rows","transport_pattern":"reqrep","request_payload_format":"arrow_ipc_stream","response_payload_format":"arrow_ipc_stream","response_mode":"metadata_or_rows","session_behavior":"stateless","requires_auth":false,"requires_session":false,"opens_session":false,"closes_session":false,"mutates_state":true,"idempotent":false,"deprecated":false,"disabled":false,"accepted_request_flags":0,"emitted_reply_flags":11,"max_request_bytes":16777216,"max_reply_bytes":16777216,"version_introduced":1,"request_schema":{"fields":[{"name":"sql","type":"utf8","nullable":false},{"name":"want_result","type":"bool","nullable":false}]},"response_schema":{"mode":"metadata_or_rows"}}]} │
    └─────────┴─────────┴────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
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
    ┌───────────┬──────────┬────────────────────────────────────────┬─────────┬───────────┬─────────────────┬─────────────────┐
    │ socket_id │ protocol │                  url                   │  open   │ connected │ send_timeout_ms │ recv_timeout_ms │
    │  uint64   │ varchar  │                varchar                 │ boolean │  boolean  │      int32      │      int32      │
    ├───────────┼──────────┼────────────────────────────────────────┼─────────┼───────────┼─────────────────┼─────────────────┤
    │         1 │ req      │ ipc:///tmp/ducknng_sql_client_demo.ipc │ true    │ true      │            1000 │            1000 │
    └───────────┴──────────┴────────────────────────────────────────┴─────────┴───────────┴─────────────────┴─────────────────┘
    ┌─────────┬─────────┬────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
    │   ok    │  error  │                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              payload                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               │
    │ boolean │ varchar │                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                blob                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                │
    ├─────────┼─────────┼────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
    │ true    │ NULL    │ \x01\x02\x04\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00\x8A\x05\x00\x00\x00\x00\x00\x00manifest{\x22server\x22:{\x22name\x22:\x22ducknng\x22,\x22version\x22:\x220.1.0\x22,\x22protocol_version\x22:1},\x22methods\x22:[{\x22name\x22:\x22manifest\x22,\x22family\x22:\x22control\x22,\x22summary\x22:\x22Return the registry-derived manifest JSON\x22,\x22transport_pattern\x22:\x22reqrep\x22,\x22request_payload_format\x22:\x22none\x22,\x22response_payload_format\x22:\x22json\x22,\x22response_mode\x22:\x22metadata_only\x22,\x22session_behavior\x22:\x22stateless\x22,\x22requires_auth\x22:false,\x22requires_session\x22:false,\x22opens_session\x22:false,\x22closes_session\x22:false,\x22mutates_state\x22:false,\x22idempotent\x22:true,\x22deprecated\x22:false,\x22disabled\x22:false,\x22accepted_request_flags\x22:0,\x22emitted_reply_flags\x22:4,\x22max_request_bytes\x22:0,\x22max_reply_bytes\x22:1048576,\x22version_introduced\x22:1,\x22request_schema\x22:null,\x22response_schema\x22:{\x22type\x22:\x22json\x22}},{\x22name\x22:\x22exec\x22,\x22family\x22:\x22sql\x22,\x22summary\x22:\x22Execute SQL and return metadata or rows\x22,\x22transport_pattern\x22:\x22reqrep\x22,\x22request_payload_format\x22:\x22arrow_ipc_stream\x22,\x22response_payload_format\x22:\x22arrow_ipc_stream\x22,\x22response_mode\x22:\x22metadata_or_rows\x22,\x22session_behavior\x22:\x22stateless\x22,\x22requires_auth\x22:false,\x22requires_session\x22:false,\x22opens_session\x22:false,\x22closes_session\x22:false,\x22mutates_state\x22:true,\x22idempotent\x22:false,\x22deprecated\x22:false,\x22disabled\x22:false,\x22accepted_request_flags\x22:0,\x22emitted_reply_flags\x22:11,\x22max_request_bytes\x22:16777216,\x22max_reply_bytes\x22:16777216,\x22version_introduced\x22:1,\x22request_schema\x22:{\x22fields\x22:[{\x22name\x22:\x22sql\x22,\x22type\x22:\x22utf8\x22,\x22nullable\x22:false},{\x22name\x22:\x22want_result\x22,\x22type\x22:\x22bool\x22,\x22nullable\x22:false}]},\x22response_schema\x22:{\x22mode\x22:\x22metadata_or_rows\x22}}]} │
    └─────────┴─────────┴────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
    ┌─────────┬─────────┬────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
    │   ok    │  error  │                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              payload                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               │
    │ boolean │ varchar │                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                blob                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                │
    ├─────────┼─────────┼────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
    │ true    │ NULL    │ \x01\x02\x04\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00\x8A\x05\x00\x00\x00\x00\x00\x00manifest{\x22server\x22:{\x22name\x22:\x22ducknng\x22,\x22version\x22:\x220.1.0\x22,\x22protocol_version\x22:1},\x22methods\x22:[{\x22name\x22:\x22manifest\x22,\x22family\x22:\x22control\x22,\x22summary\x22:\x22Return the registry-derived manifest JSON\x22,\x22transport_pattern\x22:\x22reqrep\x22,\x22request_payload_format\x22:\x22none\x22,\x22response_payload_format\x22:\x22json\x22,\x22response_mode\x22:\x22metadata_only\x22,\x22session_behavior\x22:\x22stateless\x22,\x22requires_auth\x22:false,\x22requires_session\x22:false,\x22opens_session\x22:false,\x22closes_session\x22:false,\x22mutates_state\x22:false,\x22idempotent\x22:true,\x22deprecated\x22:false,\x22disabled\x22:false,\x22accepted_request_flags\x22:0,\x22emitted_reply_flags\x22:4,\x22max_request_bytes\x22:0,\x22max_reply_bytes\x22:1048576,\x22version_introduced\x22:1,\x22request_schema\x22:null,\x22response_schema\x22:{\x22type\x22:\x22json\x22}},{\x22name\x22:\x22exec\x22,\x22family\x22:\x22sql\x22,\x22summary\x22:\x22Execute SQL and return metadata or rows\x22,\x22transport_pattern\x22:\x22reqrep\x22,\x22request_payload_format\x22:\x22arrow_ipc_stream\x22,\x22response_payload_format\x22:\x22arrow_ipc_stream\x22,\x22response_mode\x22:\x22metadata_or_rows\x22,\x22session_behavior\x22:\x22stateless\x22,\x22requires_auth\x22:false,\x22requires_session\x22:false,\x22opens_session\x22:false,\x22closes_session\x22:false,\x22mutates_state\x22:true,\x22idempotent\x22:false,\x22deprecated\x22:false,\x22disabled\x22:false,\x22accepted_request_flags\x22:0,\x22emitted_reply_flags\x22:11,\x22max_request_bytes\x22:16777216,\x22max_reply_bytes\x22:16777216,\x22version_introduced\x22:1,\x22request_schema\x22:{\x22fields\x22:[{\x22name\x22:\x22sql\x22,\x22type\x22:\x22utf8\x22,\x22nullable\x22:false},{\x22name\x22:\x22want_result\x22,\x22type\x22:\x22bool\x22,\x22nullable\x22:false}]},\x22response_schema\x22:{\x22mode\x22:\x22metadata_or_rows\x22}}]} │
    └─────────┴─────────┴────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
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
    ┌─────────┬─────────┬───────────┬──────────┬────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
    │   ok    │ version │ type_name │   name   │                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                payload_text                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                │
    │ boolean │  uint8  │  varchar  │ varchar  │                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  varchar                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   │
    ├─────────┼─────────┼───────────┼──────────┼────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
    │ true    │       1 │ result    │ manifest │ {"server":{"name":"ducknng","version":"0.1.0","protocol_version":1},"methods":[{"name":"manifest","family":"control","summary":"Return the registry-derived manifest JSON","transport_pattern":"reqrep","request_payload_format":"none","response_payload_format":"json","response_mode":"metadata_only","session_behavior":"stateless","requires_auth":false,"requires_session":false,"opens_session":false,"closes_session":false,"mutates_state":false,"idempotent":true,"deprecated":false,"disabled":false,"accepted_request_flags":0,"emitted_reply_flags":4,"max_request_bytes":0,"max_reply_bytes":1048576,"version_introduced":1,"request_schema":null,"response_schema":{"type":"json"}},{"name":"exec","family":"sql","summary":"Execute SQL and return metadata or rows","transport_pattern":"reqrep","request_payload_format":"arrow_ipc_stream","response_payload_format":"arrow_ipc_stream","response_mode":"metadata_or_rows","session_behavior":"stateless","requires_auth":false,"requires_session":false,"opens_session":false,"closes_session":false,"mutates_state":true,"idempotent":false,"deprecated":false,"disabled":false,"accepted_request_flags":0,"emitted_reply_flags":11,"max_request_bytes":16777216,"max_reply_bytes":16777216,"version_introduced":1,"request_schema":{"fields":[{"name":"sql","type":"utf8","nullable":false},{"name":"want_result","type":"bool","nullable":false}]},"response_schema":{"mode":"metadata_or_rows"}}]} │
    └─────────┴─────────┴───────────┴──────────┴────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
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

### `tls+tcp://` with a self-signed development TLS config

``` sql
LOAD '/root/ducknng/build/release/ducknng.duckdb_extension';
-- Generate a self-signed loopback certificate and keep it as a runtime TLS handle.
SELECT ducknng_self_signed_tls_config('127.0.0.1', 365, 0);

-- Start a TLS listener using the generated config handle.
SELECT ducknng_start_server(
  'tls_demo_self',
  'tls+tcp://127.0.0.1:45453',
  1,
  134217728,
  300000,
  1
);

-- Send a raw manifest request over TLS and decode the reply frame.
SELECT ok, type_name, name, position('"name":"exec"' IN payload_text) > 0
FROM ducknng_decode_frame(
  ducknng_request_raw(
    'tls+tcp://127.0.0.1:45453',
    from_hex('01000000000000000000000000000000000000000000'),
    1000,
    1
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
LOAD '/root/ducknng/build/release/ducknng.duckdb_extension';
-- Register a file-backed TLS config using committed loopback test certificates.
SELECT ducknng_tls_config_from_files(
  'test/certs/loopback-cert-key.pem',
  'test/certs/loopback-ca.pem',
  NULL,
  0
);

-- Start a TLS listener with that file-backed config.
SELECT ducknng_start_server(
  'tls_demo_files',
  'tls+tcp://127.0.0.1:45454',
  1,
  134217728,
  300000,
  1
);

-- Inspect the manifest reply over TLS using the same config handle on the client side.
SELECT ok, type_name, name, position('"name":"exec"' IN payload_text) > 0
FROM ducknng_decode_frame(
  ducknng_request_raw(
    'tls+tcp://127.0.0.1:45454',
    from_hex('01000000000000000000000000000000000000000000'),
    1000,
    1
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
    "SELECT ducknng_start_server('sql_exec', '%s', 1, 134217728, 300000, NULL, NULL, NULL)",
    ipc_url
  )
)
#>   ducknng_start_server('sql_exec', 'ipc:///tmp/ducknng_readme_exec_f3341538f691d.ipc', 1, 134217728, 300000, NULL, NULL, NULL)
#> 1                                                                                                                         TRUE
DBI::dbGetQuery(db_con, "SELECT ducknng_register_exec_method()")
#>   ducknng_register_exec_method()
#> 1                           TRUE

wait_for_ducknng_listener(ipc_path)
req <- nanonext::socket("req", dial = ipc_url)

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

# Request result rows directly with want_result = TRUE.
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

# Inspect the same table locally through the DuckDB connection that owns the server.
server_rows <- DBI::dbGetQuery(db_con, "SELECT * FROM ducknng_exec_demo ORDER BY i")
server_rows
#>    i
#> 1 42
#> 2 99

# Stop the server and clean up the socket and temporary IPC path.
DBI::dbGetQuery(db_con, "SELECT ducknng_stop_server('sql_exec')")
#>   ducknng_stop_server('sql_exec')
#> 1                            TRUE
close(req)
DBI::dbDisconnect(db_con)
unlink(ipc_path)
```
