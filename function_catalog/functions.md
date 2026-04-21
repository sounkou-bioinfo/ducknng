# Function Catalog

This file is generated from `function_catalog/functions.yaml`.

| name | kind | returns | phase | implemented | description |
|---|---|---|---:|---|---|
| `ducknng_server_start` | scalar | `BOOLEAN` | 1 | yes | Start a named ducknng REP listener for SQL serving on an NNG URL. |
| `ducknng_server_stop` | scalar | `BOOLEAN` | 1 | yes | Stop a named ducknng service and tear down its listener and worker thread. |
| `ducknng_servers` | table | `TABLE(service_id UBIGINT, name VARCHAR, listen VARCHAR, contexts INTEGER, running BOOLEAN, sessions UBIGINT)` | 1 | yes | List registered ducknng services in the current DuckDB database runtime. |
| `ducknng_sessions` | table | `TABLE(session_id UBIGINT, batch_no UBIGINT, eos BOOLEAN, last_touch_ms UBIGINT)` | 3 | no | List active query sessions for a named ducknng service. |
| `ducknng_remote_exec` | scalar | `UBIGINT` | 2 | yes | Send an EXEC request over the real wire protocol and return rows changed from the remote reply metadata. |
| `ducknng_remote_manifest` | scalar | `VARCHAR` | 2 | yes | Request the remote ducknng manifest JSON from another ducknng-compatible service. |
| `ducknng_remote` | table | `table` | 3 | no | Execute a remote query over REQ/REP and stream Arrow IPC batches back as a DuckDB table function. |
