# Function Catalog

This file is generated from `function_catalog/functions.yaml`.

| name | kind | returns | implemented | description |
|---|---|---|---|---|
| `ducknng_server_start` | scalar | `BOOLEAN` | yes | Start a named ducknng REP listener for SQL serving on an NNG URL. |
| `ducknng_server_stop` | scalar | `BOOLEAN` | yes | Stop a named ducknng service and tear down its listener and worker thread. |
| `ducknng_servers` | table | `TABLE(service_id UBIGINT, name VARCHAR, listen VARCHAR, contexts INTEGER, running BOOLEAN, sessions UBIGINT)` | yes | List registered ducknng services in the current DuckDB database runtime. |
| `ducknng_sessions` | table | `TABLE(session_id UBIGINT, batch_no UBIGINT, eos BOOLEAN, last_touch_ms UBIGINT)` | no | List active query sessions for a named ducknng service. |
| `ducknng_remote_exec` | scalar | `UBIGINT` | yes | Send an EXEC request over the real wire protocol and return rows changed from the remote reply metadata. |
| `ducknng_remote_manifest` | scalar | `VARCHAR` | yes | Request the remote ducknng manifest JSON from another ducknng-compatible service. |
| `ducknng_socket` | scalar | `UBIGINT` | yes | Open a client socket handle for a supported NNG protocol family. |
| `ducknng_dial` | scalar | `BOOLEAN` | yes | Associate a client socket handle with a remote URL using req-style timeout semantics. |
| `ducknng_close` | scalar | `BOOLEAN` | yes | Close a client socket handle and release its runtime state. |
| `ducknng_sockets` | table | `TABLE(socket_id UBIGINT, protocol VARCHAR, url VARCHAR, open BOOLEAN, connected BOOLEAN, send_timeout_ms INTEGER, recv_timeout_ms INTEGER)` | yes | List client socket handles registered in the current DuckDB runtime. |
| `ducknng_request` | scalar | `BLOB` | yes | Perform a one-shot req-style raw request and return the raw reply bytes. |
| `ducknng_request_socket` | scalar | `BLOB` | yes | Perform a req-style raw request using a previously dialed client socket handle and return the raw reply bytes. |
| `ducknng_remote` | table | `table` | yes | Execute a remote row-returning query over REQ/REP and expose the unary Arrow IPC row reply as a DuckDB table function. |
