# Function Catalog

This file is generated from `function_catalog/functions.yaml`.

## Service Control

| name | kind | arguments | returns | description |
|---|---|---|---|---|
| `ducknng_start_server` | scalar | `name, listen, contexts, recv_max_bytes, session_idle_ms, tls_config_id` | `BOOLEAN` | Start a named ducknng REP listener. |
| `ducknng_stop_server` | scalar | `name` | `BOOLEAN` | Stop a named ducknng service. |

## Introspection

| name | kind | arguments | returns | description |
|---|---|---|---|---|
| `ducknng_list_servers` | table |  | `TABLE(service_id UBIGINT, name VARCHAR, listen VARCHAR, contexts INTEGER, running BOOLEAN, sessions UBIGINT)` | List registered ducknng services. |

## Method Registry

| name | kind | arguments | returns | description |
|---|---|---|---|---|
| `ducknng_register_exec_method` | scalar |  | `BOOLEAN` | Register the built-in exec RPC method explicitly. |
| `ducknng_unregister_method` | scalar | `name` | `BOOLEAN` | Unregister a method from the runtime registry. |
| `ducknng_unregister_family` | scalar | `family` | `UBIGINT` | Unregister all methods in a family and return the number removed. |
| `ducknng_list_methods` | table |  | `TABLE(name VARCHAR, family VARCHAR, summary VARCHAR, transport_pattern VARCHAR, request_payload_format VARCHAR, response_payload_format VARCHAR, response_mode VARCHAR, request_schema_json VARCHAR, response_schema_json VARCHAR, requires_auth BOOLEAN, disabled BOOLEAN)` | List the currently registered RPC methods in the runtime registry. |

## Primitive Transport

| name | kind | arguments | returns | description |
|---|---|---|---|---|
| `ducknng_open_socket` | scalar | `protocol` | `UBIGINT` | Open a client socket handle for a supported protocol. |
| `ducknng_dial_socket` | scalar | `socket_id, url, timeout_ms` | `BOOLEAN` | Dial a URL using an opened socket handle. |
| `ducknng_close_socket` | scalar | `socket_id` | `BOOLEAN` | Close a client socket handle. |
| `ducknng_list_sockets` | table |  | `TABLE(socket_id UBIGINT, protocol VARCHAR, url VARCHAR, open BOOLEAN, connected BOOLEAN, send_timeout_ms INTEGER, recv_timeout_ms INTEGER)` | List client socket handles in the runtime. |
| `ducknng_request` | table | `url, payload, timeout_ms, tls_config_id` | `TABLE(ok BOOLEAN, error VARCHAR, payload BLOB)` | Perform a one-shot raw request and return a structured result row. |
| `ducknng_request_socket` | table | `socket_id, payload, timeout_ms` | `TABLE(ok BOOLEAN, error VARCHAR, payload BLOB)` | Perform a raw request through a previously dialed socket handle and return a structured result row. |
| `ducknng_request_raw` | scalar | `url, payload, timeout_ms, tls_config_id` | `BLOB` | Perform a one-shot raw request and return the raw reply frame bytes. |
| `ducknng_request_socket_raw` | scalar | `socket_id, payload, timeout_ms` | `BLOB` | Perform a raw request through a dialed socket handle and return the raw reply frame bytes. |
| `ducknng_decode_frame` | table | `frame` | `TABLE(ok BOOLEAN, error VARCHAR, version UTINYINT, type UTINYINT, flags UINTEGER, type_name VARCHAR, name VARCHAR, payload BLOB, payload_text VARCHAR)` | Decode a raw ducknng frame into envelope fields and extracted payload columns. |

## Transport Security

| name | kind | arguments | returns | description |
|---|---|---|---|---|
| `ducknng_list_tls_configs` | table |  | `TABLE(tls_config_id UBIGINT, source VARCHAR, enabled BOOLEAN, has_cert_key_file BOOLEAN, has_ca_file BOOLEAN, has_cert_pem BOOLEAN, has_key_pem BOOLEAN, has_ca_pem BOOLEAN, has_password BOOLEAN, auth_mode INTEGER)` | List registered TLS config handles and the kinds of material they contain. |
| `ducknng_drop_tls_config` | scalar | `tls_config_id` | `BOOLEAN` | Remove a registered TLS config handle from the runtime. |
| `ducknng_self_signed_tls_config` | scalar | `common_name, valid_days, auth_mode` | `UBIGINT` | Generate a self-signed development certificate and register it as a TLS config handle. |
| `ducknng_tls_config_from_pem` | scalar | `cert_pem, key_pem, ca_pem, password, auth_mode` | `UBIGINT` | Register a TLS config handle from in-memory PEM material. |
| `ducknng_tls_config_from_files` | scalar | `cert_key_file, ca_file, password, auth_mode` | `UBIGINT` | Register a TLS config handle from file-backed certificate material. |

## Async I/O

| name | kind | arguments | returns | description |
|---|---|---|---|---|
| `ducknng_request_raw_aio` | scalar | `url, frame, timeout_ms, tls_config_id` | `UBIGINT` | Launch one raw req/rep roundtrip asynchronously and return a future-like aio handle id. |
| `ducknng_request_socket_raw_aio` | scalar | `socket_id, frame, timeout_ms` | `UBIGINT` | Launch one raw req/rep roundtrip asynchronously on an existing socket handle and return an aio handle id. |
| `ducknng_aio_ready` | scalar | `aio_id` | `BOOLEAN` | Return whether an aio handle has reached a terminal state. |
| `ducknng_aio_collect` | table | `aio_ids, wait_ms` | `TABLE(aio_id UBIGINT, ok BOOLEAN, error VARCHAR, frame BLOB)` | Wait for any requested aio handles to finish and return one row per newly collected terminal result. |
| `ducknng_aio_cancel` | scalar | `aio_id` | `BOOLEAN` | Request cancellation of a pending aio handle. |
| `ducknng_aio_drop` | scalar | `aio_id` | `BOOLEAN` | Release a terminal aio handle from the runtime registry. |

## RPC Helper

| name | kind | arguments | returns | description |
|---|---|---|---|---|
| `ducknng_get_rpc_manifest` | table | `url, tls_config_id` | `TABLE(ok BOOLEAN, error VARCHAR, manifest VARCHAR)` | Request the RPC manifest and return a structured result row. |
| `ducknng_get_rpc_manifest_raw` | scalar | `url, tls_config_id` | `BLOB` | Request the RPC manifest and return the raw reply frame as BLOB. |
| `ducknng_run_rpc` | table | `url, sql, tls_config_id` | `TABLE(ok BOOLEAN, error VARCHAR, rows_changed UBIGINT, statement_type INTEGER, result_type INTEGER)` | Execute a metadata-oriented RPC call and return a structured result row. |
| `ducknng_run_rpc_raw` | scalar | `url, sql, tls_config_id` | `BLOB` | Execute the exec RPC and return the raw reply frame as BLOB. |
| `ducknng_query_rpc` | table | `url, sql, tls_config_id` | `table` | Execute a row-returning RPC query and expose the unary Arrow IPC row reply as a DuckDB table. |

## RPC Session

| name | kind | arguments | returns | description |
|---|---|---|---|---|
| `ducknng_open_query` | table | `url, sql, batch_rows, batch_bytes, tls_config_id` | `TABLE(ok BOOLEAN, error VARCHAR, session_id UBIGINT, state VARCHAR, next_method VARCHAR, control_json VARCHAR)` | Open a server-side query session and return the JSON control metadata as a structured row. |
| `ducknng_fetch_query` | table | `url, session_id, batch_rows, batch_bytes, tls_config_id` | `TABLE(ok BOOLEAN, error VARCHAR, session_id UBIGINT, state VARCHAR, next_method VARCHAR, control_json VARCHAR, payload BLOB, end_of_stream BOOLEAN)` | Fetch the next session reply and return either JSON control metadata or an Arrow IPC batch payload. |
| `ducknng_close_query` | table | `url, session_id, tls_config_id` | `TABLE(ok BOOLEAN, error VARCHAR, session_id UBIGINT, state VARCHAR, next_method VARCHAR, control_json VARCHAR)` | Close a server-side query session and return the JSON control metadata as a structured row. |
| `ducknng_cancel_query` | table | `url, session_id, tls_config_id` | `TABLE(ok BOOLEAN, error VARCHAR, session_id UBIGINT, state VARCHAR, next_method VARCHAR, control_json VARCHAR)` | Cancel and close a server-side query session and return the JSON control metadata as a structured row. |
