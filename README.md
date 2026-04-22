
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
`ducknng_list_servers()`. On the server side it runs one REP socket with
one or more REP contexts, and on top of that transport it already
supports a working RPC request/reply path with manifest discovery,
metadata-oriented execution, and unary row-returning execution over the
current raw wire and Arrow IPC model.

The next documented protocol slice is the session query family:
`query_open`, `fetch`, `close`, and `cancel`. In the current contract,
`query_open` opens one server-owned query session and returns JSON
control metadata, `fetch` is the only method that may return streamed
row batches, `close` is the normal explicit cleanup path, and `cancel`
is best-effort rather than a guarantee of immediate interruption. Until
a real owner-identity model is implemented, that family should still be
treated as scaffolding for loopback or development use rather than as a
production-safe multi-client surface.

## Functions

# Function Catalog

This file is generated from `function_catalog/functions.yaml`.

| name                           | kind   | returns                                                                                                                                      | implemented | description                                                                                         |
|--------------------------------|--------|----------------------------------------------------------------------------------------------------------------------------------------------|-------------|-----------------------------------------------------------------------------------------------------|
| `ducknng_start_server`         | scalar | `BOOLEAN`                                                                                                                                    | yes         | Start a named ducknng REP listener.                                                                 |
| `ducknng_stop_server`          | scalar | `BOOLEAN`                                                                                                                                    | yes         | Stop a named ducknng service.                                                                       |
| `ducknng_list_servers`         | table  | `TABLE(service_id UBIGINT, name VARCHAR, listen VARCHAR, contexts INTEGER, running BOOLEAN, sessions UBIGINT)`                               | yes         | List registered ducknng services.                                                                   |
| `ducknng_open_socket`          | scalar | `UBIGINT`                                                                                                                                    | yes         | Open a client socket handle for a supported protocol.                                               |
| `ducknng_dial_socket`          | scalar | `BOOLEAN`                                                                                                                                    | yes         | Dial a URL using an opened socket handle.                                                           |
| `ducknng_close_socket`         | scalar | `BOOLEAN`                                                                                                                                    | yes         | Close a client socket handle.                                                                       |
| `ducknng_list_sockets`         | table  | `TABLE(socket_id UBIGINT, protocol VARCHAR, url VARCHAR, open BOOLEAN, connected BOOLEAN, send_timeout_ms INTEGER, recv_timeout_ms INTEGER)` | yes         | List client socket handles in the runtime.                                                          |
| `ducknng_request`              | table  | `TABLE(ok BOOLEAN, error VARCHAR, payload BLOB)`                                                                                             | yes         | Perform a one-shot raw request and return a structured result row.                                  |
| `ducknng_request_socket`       | table  | `TABLE(ok BOOLEAN, error VARCHAR, payload BLOB)`                                                                                             | yes         | Perform a raw request through a previously dialed socket handle and return a structured result row. |
| `ducknng_get_rpc_manifest`     | table  | `TABLE(ok BOOLEAN, error VARCHAR, manifest VARCHAR)`                                                                                         | yes         | Request the RPC manifest and return a structured result row.                                        |
| `ducknng_get_rpc_manifest_raw` | scalar | `VARCHAR`                                                                                                                                    | yes         | Request the RPC manifest and return only the manifest payload as VARCHAR.                           |
| `ducknng_run_rpc`              | table  | `TABLE(ok BOOLEAN, error VARCHAR, rows_changed UBIGINT, statement_type INTEGER, result_type INTEGER)`                                        | yes         | Execute a metadata-oriented RPC call and return a structured result row.                            |
| `ducknng_run_rpc_raw`          | scalar | `UBIGINT`                                                                                                                                    | yes         | Execute a metadata-oriented RPC call and return only rows_changed.                                  |
| `ducknng_query_rpc`            | table  | `table`                                                                                                                                      | yes         | Execute a row-returning RPC query and expose the unary Arrow IPC row reply as a DuckDB table.       |
| `ducknng_request_raw`          | scalar | `BLOB`                                                                                                                                       | yes         | Perform a one-shot raw request and return the raw reply bytes.                                      |
| `ducknng_request_socket_raw`   | scalar | `BLOB`                                                                                                                                       | yes         | Perform a raw request through a dialed socket handle and return the raw reply bytes.                |

## Build

``` sh
make configure
make release
```

See also `NEWS.md` for the current implementation status and planned
next steps, and `docs/protocol.md`, `docs/manifest.md`,
`docs/security.md`, `docs/registry.md`, and `docs/types.md` for the
binding session/query-family contract.

## Examples

### Start an IPC listener and inspect the registry

``` sql
LOAD '/root/ducknng/build/release/ducknng.duckdb_extension';
SELECT ducknng_start_server(
  'sql0',
  'ipc:///tmp/ducknng_sql0.ipc',
  1,
  134217728,
  300000,
  NULL,
  NULL,
  NULL
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
  'sql_multi',
  'ipc:///tmp/ducknng_sql_multi.ipc',
  3,
  134217728,
  300000,
  NULL,
  NULL,
  NULL
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
  'sql_client_demo',
  'ipc:///tmp/ducknng_sql_client_demo.ipc',
  1,
  134217728,
  300000,
  NULL,
  NULL,
  NULL
);

-- RPC helper: fetch the manifest as a structured status row.
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

-- Raw scalar forms remain available when only the payload bytes are wanted.
SELECT substr(hex(ducknng_request_socket_raw(1, from_hex('01000000000000000000000000000000000000000000'), 1000)), 1, 28);
SELECT position('"name":"exec"' IN ducknng_get_rpc_manifest_raw('ipc:///tmp/ducknng_sql_client_demo.ipc')) > 0;
SELECT ducknng_run_rpc_raw('ipc:///tmp/ducknng_sql_client_demo.ipc', 'CREATE TABLE IF NOT EXISTS client_side_demo(i INTEGER)');

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
    ┌────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
    │ (main."position"(ducknng_get_rpc_manifest_raw('ipc:///tmp/ducknng_sql_client_demo.ipc'), '"name":"exec"') > 0) │
    │                                                    boolean                                                     │
    ├────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
    │ true                                                                                                           │
    └────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
    ┌─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
    │ ducknng_run_rpc_raw('ipc:///tmp/ducknng_sql_client_demo.ipc', 'CREATE TABLE IF NOT EXISTS client_side_demo(i INTEGER)') │
    │                                                         uint64                                                          │
    ├─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
    │                                                                                                                       0 │
    └─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
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

### REQ/REP `EXEC` via `nanonext` as an interop example

``` r
suppressWarnings(suppressPackageStartupMessages({
  library(DBI)
  library(duckdb)
  library(nanonext)
  library(nanoarrow)
}))

# Little-endian integer encoders for the versioned ducknng RPC envelope.
u32le <- function(x) writeBin(as.integer(x), raw(), size = 4L, endian = "little")
u64le <- function(x) {
  x <- as.double(x)
  c(u32le(x %% 2^32), u32le(floor(x / 2^32)))
}
le_u32_from_raw <- function(x) sum(as.double(as.integer(x)) * 256^(0:3))
le_u64_from_raw <- function(x) le_u32_from_raw(x[1:4]) + 2^32 * le_u32_from_raw(x[5:8])
read_u32le <- function(buf, offset) le_u32_from_raw(buf[offset + 0:3])
read_u64le <- function(buf, offset) le_u64_from_raw(buf[offset + 0:7])

# Serialize a one-row exec request as Arrow IPC using nanoarrow.
encode_ducknng_exec_request <- function(sql, want_result = FALSE) {
  con <- rawConnection(raw(), open = "r+")
  on.exit(close(con))
  nanoarrow::write_nanoarrow(
    data.frame(sql = sql, want_result = want_result),
    con
  )
  payload <- rawConnectionValue(con)
  name <- charToRaw("exec")
  c(
    as.raw(1L),                  # envelope version
    as.raw(1L),                  # message type: call
    u32le(0),                    # flags
    u32le(length(name)),         # method-name length
    u32le(0),                    # error-string length on requests
    u64le(length(payload)),      # payload length
    name,
    payload
  )
}

# Decode a reply envelope and, when present, parse the Arrow IPC payload.
decode_ducknng_exec_reply <- function(buf) {
  name_len <- read_u32le(buf, 7)
  error_len <- read_u32le(buf, 11)
  payload_len <- read_u64le(buf, 15)
  name_start <- 23L
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

# Use a temporary IPC path and launch a DuckDB-backed ducknng server in a child process.
ext_path <- normalizePath("build/release/ducknng.duckdb_extension")
ipc_path <- tempfile(pattern = "ducknng_readme_exec_", tmpdir = "/tmp", fileext = ".ipc")
ipc_url <- paste0("ipc://", ipc_path)

server_job <- parallel::mcparallel({
  drv <- duckdb::duckdb(config = list(allow_unsigned_extensions = "true"))
  con <- DBI::dbConnect(drv, dbdir = ":memory:")
  DBI::dbExecute(con, sprintf("LOAD '%s'", ext_path))
  DBI::dbGetQuery(con, sprintf(
    "SELECT ducknng_start_server('sql_exec', '%s', 1, 134217728, 300000, NULL, NULL, NULL)",
    ipc_url
  ))
  Sys.sleep(2)
  rows <- DBI::dbGetQuery(con, "SELECT * FROM ducknng_exec_demo ORDER BY i")
  DBI::dbGetQuery(con, "SELECT ducknng_stop_server('sql_exec')")
  DBI::dbDisconnect(con, shutdown = TRUE)
  rows
})

# Give the listener a moment to come up, then dial it with a normal req socket.
Sys.sleep(1)
req <- nanonext::socket("req", dial = ipc_url, autostart = NA)

# Create a table remotely and inspect the reply metadata.
nanonext::send(req, encode_ducknng_exec_request("CREATE TABLE ducknng_exec_demo(i INTEGER)"), mode = "raw", block = 1000L)
#> [1] 0
create_reply <- decode_ducknng_exec_reply(nanonext::recv(req, mode = "raw", block = 1000L))
create_reply$data
#>   rows_changed statement_type result_type
#> 1            0              7           2

# Insert rows remotely and inspect the second reply.
nanonext::send(req, encode_ducknng_exec_request("INSERT INTO ducknng_exec_demo VALUES (42), (99)"), mode = "raw", block = 1000L)
#> [1] 0
insert_reply <- decode_ducknng_exec_reply(nanonext::recv(req, mode = "raw", block = 1000L))
insert_reply$data
#>   rows_changed statement_type result_type
#> 1            2              2           1

# Request result rows directly with want_result = TRUE.
nanonext::send(
  req,
  encode_ducknng_exec_request("SELECT i, i > 50 AS gt_50 FROM ducknng_exec_demo ORDER BY i", want_result = TRUE),
  mode = "raw",
  block = 1000L
)
#> [1] 0
select_reply <- decode_ducknng_exec_reply(nanonext::recv(req, mode = "raw", block = 1000L))
select_reply$data
#>    i gt_50
#> 1 42 FALSE
#> 2 99  TRUE

# Collect the server-side query result from the child process.
server_rows <- parallel::mccollect(server_job)[[1]]
server_rows
#>    i
#> 1 42
#> 2 99

close(req)
```
