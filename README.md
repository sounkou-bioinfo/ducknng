
<!-- README.md is generated from README.Rmd. Please edit that file -->

# ducknng

`ducknng` is a pure C DuckDB extension for DuckDB + NNG REQ/REP interop.

Its SQL-facing client and server interface is explicitly modeled over
[`r-lib/nanonext`](https://github.com/r-lib/nanonext): req/rep sockets,
dial/listen semantics, context-oriented NNG usage, and a practical
messaging surface that can be used directly from DuckDB SQL. Its RPC
framing and tabular payload direction are also explicitly informed by
Arrow IPC based RPC work such as
[`sounkou-bioinfo/mangoro`](https://github.com/sounkou-bioinfo/mangoro)
and related projects, where a thin envelope is kept separate from Arrow
IPC request and reply payloads.

Today it provides:

- named server start/stop from SQL
- registry introspection with `ducknng_servers()`
- one REP socket with one or more REP contexts
- a working `EXEC` request/reply path over the raw wire protocol

The documented next protocol slice is the session query family:
`query_open`, `fetch`, `close`, and `cancel`. In the current docs
contract, `query_open` opens one server-owned query session and returns
JSON control metadata, `fetch` is the only method that may return
streamed row batches, `close` is the normal explicit cleanup path, and
`cancel` is best-effort rather than a guarantee of immediate
interruption. Until a real owner-identity model is implemented, that
family should be treated as scaffolding for loopback or development use
rather than production-safe multi-client exposure.

## Functions

# Function Catalog

This file is generated from `function_catalog/functions.yaml`.

| name                      | kind   | returns                                                                                                                                      | implemented | description                                                                                                            |
|---------------------------|--------|----------------------------------------------------------------------------------------------------------------------------------------------|-------------|------------------------------------------------------------------------------------------------------------------------|
| `ducknng_server_start`    | scalar | `BOOLEAN`                                                                                                                                    | yes         | Start a named ducknng REP listener for SQL serving on an NNG URL.                                                      |
| `ducknng_server_stop`     | scalar | `BOOLEAN`                                                                                                                                    | yes         | Stop a named ducknng service and tear down its listener and worker thread.                                             |
| `ducknng_servers`         | table  | `TABLE(service_id UBIGINT, name VARCHAR, listen VARCHAR, contexts INTEGER, running BOOLEAN, sessions UBIGINT)`                               | yes         | List registered ducknng services in the current DuckDB database runtime.                                               |
| `ducknng_sessions`        | table  | `TABLE(session_id UBIGINT, batch_no UBIGINT, eos BOOLEAN, last_touch_ms UBIGINT)`                                                            | no          | List active query sessions for a named ducknng service.                                                                |
| `ducknng_remote_exec`     | scalar | `UBIGINT`                                                                                                                                    | yes         | Send an EXEC request over the real wire protocol and return rows changed from the remote reply metadata.               |
| `ducknng_remote_manifest` | scalar | `VARCHAR`                                                                                                                                    | yes         | Request the remote ducknng manifest JSON from another ducknng-compatible service.                                      |
| `ducknng_socket`          | scalar | `UBIGINT`                                                                                                                                    | yes         | Open a client socket handle for a supported NNG protocol family.                                                       |
| `ducknng_dial`            | scalar | `BOOLEAN`                                                                                                                                    | yes         | Associate a client socket handle with a remote URL using req-style timeout semantics.                                  |
| `ducknng_close`           | scalar | `BOOLEAN`                                                                                                                                    | yes         | Close a client socket handle and release its runtime state.                                                            |
| `ducknng_sockets`         | table  | `TABLE(socket_id UBIGINT, protocol VARCHAR, url VARCHAR, open BOOLEAN, connected BOOLEAN, send_timeout_ms INTEGER, recv_timeout_ms INTEGER)` | yes         | List client socket handles registered in the current DuckDB runtime.                                                   |
| `ducknng_request`         | scalar | `BLOB`                                                                                                                                       | yes         | Perform a one-shot req-style raw request and return the raw reply bytes.                                               |
| `ducknng_request_socket`  | scalar | `BLOB`                                                                                                                                       | yes         | Perform a req-style raw request using a previously dialed client socket handle and return the raw reply bytes.         |
| `ducknng_remote`          | table  | `table`                                                                                                                                      | yes         | Execute a remote row-returning query over REQ/REP and expose the unary Arrow IPC row reply as a DuckDB table function. |

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
SELECT ducknng_server_start(
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
FROM ducknng_servers();

SELECT ducknng_server_stop('sql0');
```

    ┌─────────────────────────────────────────────────────────────────────────────────────────────────────┐
    │ ducknng_server_start('sql0', 'ipc:///tmp/ducknng_sql0.ipc', 1, 134217728, 300000, NULL, NULL, NULL) │
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
    │ ducknng_server_stop('sql0') │
    │           boolean           │
    ├─────────────────────────────┤
    │ true                        │
    └─────────────────────────────┘

### Request multiple REP contexts on one REP socket

``` sql
LOAD '/root/ducknng/build/release/ducknng.duckdb_extension';
SELECT ducknng_server_start(
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
FROM ducknng_servers()
WHERE name = 'sql_multi';

SELECT ducknng_server_stop('sql_multi');
```

    ┌───────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
    │ ducknng_server_start('sql_multi', 'ipc:///tmp/ducknng_sql_multi.ipc', 3, 134217728, 300000, NULL, NULL, NULL) │
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
    │ ducknng_server_stop('sql_multi') │
    │             boolean              │
    ├──────────────────────────────────┤
    │ true                             │
    └──────────────────────────────────┘

### DuckDB can also act as a client

``` sql
LOAD '/root/ducknng/build/release/ducknng.duckdb_extension';
-- Start a local ducknng service that the following client examples will talk to.
SELECT ducknng_server_start(
  'sql_client_demo',
  'ipc:///tmp/ducknng_sql_client_demo.ipc',
  1,
  134217728,
  300000,
  NULL,
  NULL,
  NULL
);

-- Inspect the remote manifest over REQ/REP.
SELECT position('"name":"exec"' IN ducknng_remote_manifest('ipc:///tmp/ducknng_sql_client_demo.ipc')) > 0;

-- Run non-row statements through the metadata-oriented helper.
SELECT ducknng_remote_exec('ipc:///tmp/ducknng_sql_client_demo.ipc', 'CREATE TABLE client_side_demo(i INTEGER)');
SELECT ducknng_remote_exec('ipc:///tmp/ducknng_sql_client_demo.ipc', 'INSERT INTO client_side_demo VALUES (10), (11)');

-- Fetch row results through the table-function client path.
SELECT * FROM ducknng_remote('ipc:///tmp/ducknng_sql_client_demo.ipc', 'SELECT i, i > 10 AS gt_10 FROM client_side_demo ORDER BY i');

-- Open a req-style client handle, dial the service, and inspect the handle registry.
SELECT ducknng_socket('req');
SELECT ducknng_dial(1, 'ipc:///tmp/ducknng_sql_client_demo.ipc', 1000);
SELECT * FROM ducknng_sockets();

-- Send a raw manifest frame through the socket-handle API and inspect the reply prefix.
SELECT substr(hex(ducknng_request_socket(1, from_hex('01000000000000000000000000000000000000000000'), 1000)), 1, 28);

-- Close the client socket handle and stop the demo server.
SELECT ducknng_close(1);
SELECT ducknng_server_stop('sql_client_demo');
```

    ┌───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
    │ ducknng_server_start('sql_client_demo', 'ipc:///tmp/ducknng_sql_client_demo.ipc', 1, 134217728, 300000, NULL, NULL, NULL) │
    │                                                          boolean                                                          │
    ├───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
    │ true                                                                                                                      │
    └───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
    ┌───────────────────────────────────────────────────────────────────────────────────────────────────────────┐
    │ (main."position"(ducknng_remote_manifest('ipc:///tmp/ducknng_sql_client_demo.ipc'), '"name":"exec"') > 0) │
    │                                                  boolean                                                  │
    ├───────────────────────────────────────────────────────────────────────────────────────────────────────────┤
    │ true                                                                                                      │
    └───────────────────────────────────────────────────────────────────────────────────────────────────────────┘
    ┌───────────────────────────────────────────────────────────────────────────────────────────────────────────┐
    │ ducknng_remote_exec('ipc:///tmp/ducknng_sql_client_demo.ipc', 'CREATE TABLE client_side_demo(i INTEGER)') │
    │                                                  uint64                                                   │
    ├───────────────────────────────────────────────────────────────────────────────────────────────────────────┤
    │                                                                                                         0 │
    └───────────────────────────────────────────────────────────────────────────────────────────────────────────┘
    ┌─────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
    │ ducknng_remote_exec('ipc:///tmp/ducknng_sql_client_demo.ipc', 'INSERT INTO client_side_demo VALUES (10), (11)') │
    │                                                     uint64                                                      │
    ├─────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
    │                                                                                                               2 │
    └─────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
    ┌───────┬─────────┐
    │   i   │  gt_10  │
    │ int32 │ boolean │
    ├───────┼─────────┤
    │    10 │ false   │
    │    11 │ true    │
    └───────┴─────────┘
    ┌───────────────────────┐
    │ ducknng_socket('req') │
    │        uint64         │
    ├───────────────────────┤
    │                     1 │
    └───────────────────────┘
    ┌─────────────────────────────────────────────────────────────────┐
    │ ducknng_dial(1, 'ipc:///tmp/ducknng_sql_client_demo.ipc', 1000) │
    │                             boolean                             │
    ├─────────────────────────────────────────────────────────────────┤
    │ true                                                            │
    └─────────────────────────────────────────────────────────────────┘
    ┌───────────┬──────────┬────────────────────────────────────────┬─────────┬───────────┬─────────────────┬─────────────────┐
    │ socket_id │ protocol │                  url                   │  open   │ connected │ send_timeout_ms │ recv_timeout_ms │
    │  uint64   │ varchar  │                varchar                 │ boolean │  boolean  │      int32      │      int32      │
    ├───────────┼──────────┼────────────────────────────────────────┼─────────┼───────────┼─────────────────┼─────────────────┤
    │         1 │ req      │ ipc:///tmp/ducknng_sql_client_demo.ipc │ true    │ true      │            1000 │            1000 │
    └───────────┴──────────┴────────────────────────────────────────┴─────────┴───────────┴─────────────────┴─────────────────┘
    ┌───────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
    │ substr(hex(ducknng_request_socket(1, from_hex('01000000000000000000000000000000000000000000'), 1000)), 1, 28) │
    │                                                    varchar                                                    │
    ├───────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
    │ 0102040000000800000000000000                                                                                  │
    └───────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
    ┌──────────────────┐
    │ ducknng_close(1) │
    │     boolean      │
    ├──────────────────┤
    │ true             │
    └──────────────────┘
    ┌────────────────────────────────────────┐
    │ ducknng_server_stop('sql_client_demo') │
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
    "SELECT ducknng_server_start('sql_exec', '%s', 1, 134217728, 300000, NULL, NULL, NULL)",
    ipc_url
  ))
  Sys.sleep(2)
  rows <- DBI::dbGetQuery(con, "SELECT * FROM ducknng_exec_demo ORDER BY i")
  DBI::dbGetQuery(con, "SELECT ducknng_server_stop('sql_exec')")
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
