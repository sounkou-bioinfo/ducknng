# ducknng body codec providers

`ducknng` keeps transport bytes, protocol frames, and method payloads separate. The body codec provider layer is the opt-in serialization/deserialization layer for raw HTTP bodies and other content-type-tagged `BLOB` values. It is inspired by `nanonext` serialization providers, but it is keyed by media type instead of R object class and it is not part of the framed RPC method registry.

The raw HTTP primitive remains `ducknng_ncurl(...)`. It returns status, headers, `body BLOB`, and a best-effort UTF-8 `body_text` column without interpreting the response by `Content-Type`. Parsed body behavior is explicit:

```sql
SELECT * FROM ducknng_list_codecs();
SELECT * FROM ducknng_parse_body(body, content_type);
SELECT * FROM ducknng_ncurl_table(url, method, headers_json, body, timeout_ms, tls_config_id);
```

`ducknng_parse_body(...)` parses an existing `BLOB` using the supplied content type. `ducknng_ncurl_table(...)` performs one HTTP/HTTPS request, requires a 2xx response status, extracts the response `Content-Type`, and parses the response body into a DuckDB table. Use `ducknng_ncurl(...)` instead when you need to inspect non-2xx responses, response headers, or raw bytes.

The initial built-in providers are deliberately conservative. Unknown or missing content types fall back to raw `BLOB` output. `text/*` bodies are exposed as `VARCHAR` only when the bytes are valid UTF-8 text. `application/vnd.ducknng.frame` is decoded with the same envelope shape as `ducknng_decode_frame(...)`. Arrow IPC stream bytes are decoded through nanoarrow and the same stable manual DuckDB vector mapping used by `ducknng_query_rpc(...)`.

JSON, CSV, TSV, and Parquet providers delegate parsing to DuckDB itself. The implementation writes the body to a temporary file, runs the appropriate DuckDB reader through the runtime init-connection execution gate, then serializes the reader result through the existing Arrow IPC row mapping before returning rows to the caller. This lets DuckDB own format-specific inference and parsing while keeping the extension on the stable C API path.

Reader-backed codecs currently fail fast inside service-owned SQL, such as SQL executed through remote `exec` / `ducknng_query_rpc(...)`, because that path already owns the same runtime init-connection execution gate on the current request thread. Raw, text, frame, and direct Arrow IPC decoding do not need a nested DuckDB reader. Lifting this limitation requires a broader per-session/per-request connection model rather than re-entering the shared init connection from inside an active request.

The current provider list is built in, not user-extensible. Future `ducknng_register_codec(...)`-style hooks remain a design question because user-defined callbacks need clear ownership, security, error, and ABI rules. Until that is settled, provider names and content-type matching should stay narrow, documented, and covered by tests.

The HTTP server RPC endpoint does not use this provider layer to accept arbitrary web payloads. `ducknng_start_http_server(...)` still accepts only `POST` requests whose body is one complete `application/vnd.ducknng.frame` envelope. The provider layer is for local parsing and client-side HTTP convenience, not a second server protocol.
