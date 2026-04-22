# Function Catalog

This file is generated from `function_catalog/functions.yaml`.

## Service Control

| name | kind | arguments | returns | description |
|---|---|---|---|---|
| `ducknng_start_server` | scalar | `name, listen, contexts, recv_max_bytes, session_idle_ms, tls_cert_key_file, tls_ca_file, tls_auth_mode` | `BOOLEAN` | Start a named ducknng REP listener. |
| `ducknng_stop_server` | scalar | `name` | `BOOLEAN` | Stop a named ducknng service. |

## Introspection

| name | kind | arguments | returns | description |
|---|---|---|---|---|
| `ducknng_list_servers` | table | `` | `TABLE(service_id UBIGINT, name VARCHAR, listen VARCHAR, contexts INTEGER, running BOOLEAN, sessions UBIGINT)` | List registered ducknng services. |

## Primitive Transport

| name | kind | arguments | returns | description |
|---|---|---|---|---|
| `ducknng_open_socket` | scalar | `protocol` | `UBIGINT` | Open a client socket handle for a supported protocol. |
| `ducknng_dial_socket` | scalar | `socket_id, url, timeout_ms` | `BOOLEAN` | Dial a URL using an opened socket handle. |
| `ducknng_close_socket` | scalar | `socket_id` | `BOOLEAN` | Close a client socket handle. |
| `ducknng_list_sockets` | table | `` | `TABLE(socket_id UBIGINT, protocol VARCHAR, url VARCHAR, open BOOLEAN, connected BOOLEAN, send_timeout_ms INTEGER, recv_timeout_ms INTEGER)` | List client socket handles in the runtime. |
| `ducknng_request` | table | `url, payload, timeout_ms` | `TABLE(ok BOOLEAN, error VARCHAR, payload BLOB)` | Perform a one-shot raw request and return a structured result row. |
| `ducknng_request_socket` | table | `socket_id, payload, timeout_ms` | `TABLE(ok BOOLEAN, error VARCHAR, payload BLOB)` | Perform a raw request through a previously dialed socket handle and return a structured result row. |
| `ducknng_request_raw` | scalar | `url, payload, timeout_ms` | `BLOB` | Perform a one-shot raw request and return the raw reply frame bytes. |
| `ducknng_request_socket_raw` | scalar | `socket_id, payload, timeout_ms` | `BLOB` | Perform a raw request through a dialed socket handle and return the raw reply frame bytes. |
| `ducknng_decode_frame` | table | `frame` | `TABLE(ok BOOLEAN, error VARCHAR, version UTINYINT, type UTINYINT, flags UINTEGER, type_name VARCHAR, name VARCHAR, payload BLOB, payload_text VARCHAR)` | Decode a raw ducknng frame into envelope fields and extracted payload columns. |

## RPC Helper

| name | kind | arguments | returns | description |
|---|---|---|---|---|
| `ducknng_get_rpc_manifest` | table | `url` | `TABLE(ok BOOLEAN, error VARCHAR, manifest VARCHAR)` | Request the RPC manifest and return a structured result row. |
| `ducknng_get_rpc_manifest_raw` | scalar | `url` | `BLOB` | Request the RPC manifest and return the raw reply frame as BLOB. |
| `ducknng_run_rpc` | table | `url, sql` | `TABLE(ok BOOLEAN, error VARCHAR, rows_changed UBIGINT, statement_type INTEGER, result_type INTEGER)` | Execute a metadata-oriented RPC call and return a structured result row. |
| `ducknng_run_rpc_raw` | scalar | `url, sql` | `BLOB` | Execute the exec RPC and return the raw reply frame as BLOB. |
| `ducknng_query_rpc` | table | `url, sql` | `table` | Execute a row-returning RPC query and expose the unary Arrow IPC row reply as a DuckDB table. |
