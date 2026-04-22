# Function Catalog

This file is generated from `function_catalog/functions.yaml`.

| name | kind | returns | implemented | description |
|---|---|---|---|---|
| `ducknng_start_server` | scalar | `BOOLEAN` | yes | Start a named ducknng REP listener. |
| `ducknng_stop_server` | scalar | `BOOLEAN` | yes | Stop a named ducknng service. |
| `ducknng_list_servers` | table | `TABLE(service_id UBIGINT, name VARCHAR, listen VARCHAR, contexts INTEGER, running BOOLEAN, sessions UBIGINT)` | yes | List registered ducknng services. |
| `ducknng_open_socket` | scalar | `UBIGINT` | yes | Open a client socket handle for a supported protocol. |
| `ducknng_dial_socket` | scalar | `BOOLEAN` | yes | Dial a URL using an opened socket handle. |
| `ducknng_close_socket` | scalar | `BOOLEAN` | yes | Close a client socket handle. |
| `ducknng_list_sockets` | table | `TABLE(socket_id UBIGINT, protocol VARCHAR, url VARCHAR, open BOOLEAN, connected BOOLEAN, send_timeout_ms INTEGER, recv_timeout_ms INTEGER)` | yes | List client socket handles in the runtime. |
| `ducknng_request` | table | `TABLE(ok BOOLEAN, error VARCHAR, payload BLOB)` | yes | Perform a one-shot raw request and return a structured result row. |
| `ducknng_request_socket` | table | `TABLE(ok BOOLEAN, error VARCHAR, payload BLOB)` | yes | Perform a raw request through a previously dialed socket handle and return a structured result row. |
| `ducknng_get_rpc_manifest` | table | `TABLE(ok BOOLEAN, error VARCHAR, manifest VARCHAR)` | yes | Request the RPC manifest and return a structured result row. |
| `ducknng_get_rpc_manifest_raw` | scalar | `VARCHAR` | yes | Request the RPC manifest and return only the manifest payload as VARCHAR. |
| `ducknng_run_rpc` | table | `TABLE(ok BOOLEAN, error VARCHAR, rows_changed UBIGINT, statement_type INTEGER, result_type INTEGER)` | yes | Execute a metadata-oriented RPC call and return a structured result row. |
| `ducknng_run_rpc_raw` | scalar | `UBIGINT` | yes | Execute a metadata-oriented RPC call and return only rows_changed. |
| `ducknng_query_rpc` | table | `table` | yes | Execute a row-returning RPC query and expose the unary Arrow IPC row reply as a DuckDB table. |
| `ducknng_request_raw` | scalar | `BLOB` | yes | Perform a one-shot raw request and return the raw reply bytes. |
| `ducknng_request_socket_raw` | scalar | `BLOB` | yes | Perform a raw request through a dialed socket handle and return the raw reply bytes. |
