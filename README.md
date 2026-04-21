
<!-- README.md is generated from README.Rmd. Please edit that file -->

# ducknng

`ducknng` is a pure C DuckDB extension for DuckDB + NNG REQ/REP interop.

Today it provides:

- named server start/stop from SQL
- registry introspection with `ducknng_servers()`
- one REP socket with one or more REP contexts
- a working `EXEC` request/reply path over the raw wire protocol

## Functions

# Function Catalog

This file is generated from `function_catalog/functions.yaml`.

| name                   | kind   | returns                                                                                                        | phase | implemented | description                                                                                       |
|------------------------|--------|----------------------------------------------------------------------------------------------------------------|------:|-------------|---------------------------------------------------------------------------------------------------|
| `ducknng_server_start` | scalar | `BOOLEAN`                                                                                                      |     1 | yes         | Start a named ducknng REP listener for SQL serving on an NNG URL.                                 |
| `ducknng_server_stop`  | scalar | `BOOLEAN`                                                                                                      |     1 | yes         | Stop a named ducknng service and tear down its listener and worker thread.                        |
| `ducknng_servers`      | table  | `TABLE(service_id UBIGINT, name VARCHAR, listen VARCHAR, contexts INTEGER, running BOOLEAN, sessions UBIGINT)` |     1 | yes         | List registered ducknng services in the current DuckDB database runtime.                          |
| `ducknng_sessions`     | table  | `TABLE(session_id UBIGINT, batch_no UBIGINT, eos BOOLEAN, last_touch_ms UBIGINT)`                              |     3 | no          | List active query sessions for a named ducknng service.                                           |
| `ducknng_remote_exec`  | scalar | `UBIGINT`                                                                                                      |     2 | no          | Send an EXEC request over the real wire protocol and return rows changed.                         |
| `ducknng_remote`       | table  | `table`                                                                                                        |     3 | no          | Execute a remote query over REQ/REP and stream Arrow IPC batches back as a DuckDB table function. |

## Build

``` sh
make configure
make release
```

See also `NEWS.md` for the current implementation status and planned
next steps.

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
#> [1] rows_changed   statement_type result_type   
#> <0 rows> (or 0-length row.names)

# Insert rows remotely and inspect the second reply.
nanonext::send(req, encode_ducknng_exec_request("INSERT INTO ducknng_exec_demo VALUES (42), (99)"), mode = "raw", block = 1000L)
#> [1] 0
insert_reply <- decode_ducknng_exec_reply(nanonext::recv(req, mode = "raw", block = 1000L))
insert_reply$data
#> [1] rows_changed   statement_type result_type   
#> <0 rows> (or 0-length row.names)

# Collect the server-side query result from the child process.
server_rows <- parallel::mccollect(server_job)[[1]]
server_rows
#>    i
#> 1 42
#> 2 99

close(req)
```
