# ducknng body codec providers

`ducknng` keeps transport bytes, protocol frames, and method payloads separate. The body codec provider layer is the opt-in serialization/deserialization layer for raw HTTP bodies and other content-type-tagged `BLOB` values. It is inspired by `nanonext` serialization providers, but it is keyed by media type instead of R object class and it is not part of the framed RPC method registry.

The raw HTTP primitives remain `ducknng_ncurl(...)` and its async companion `ducknng_ncurl_aio(...)` / `ducknng_ncurl_aio_collect(...)`. They return status, headers, `body BLOB`, and a best-effort UTF-8 `body_text` column without interpreting the response by `Content-Type`. Parsed body behavior is explicit:

```sql
SELECT * FROM ducknng_list_codecs();
SELECT * FROM ducknng_parse_body(body, content_type);
SELECT * FROM ducknng_ncurl_table(url, method, headers_json, body, timeout_ms, tls_config_id);
```

`ducknng_parse_body(...)` parses an existing `BLOB` using the supplied content type. `ducknng_ncurl_table(...)` performs one HTTP/HTTPS request, requires a 2xx response status, extracts the response `Content-Type`, and parses the response body into a DuckDB table. Use `ducknng_ncurl(...)` instead when you need to inspect non-2xx responses, response headers, or raw bytes.

The initial built-in providers are deliberately conservative. Unknown or missing content types fall back to raw `BLOB` output. `text/*` bodies are exposed as `VARCHAR` only when the bytes are valid UTF-8 text. `application/vnd.ducknng.frame` is decoded with the same envelope shape as `ducknng_decode_frame(...)`. Arrow IPC stream bytes are decoded through nanoarrow and the same stable manual DuckDB vector mapping used by `ducknng_query_rpc(...)`.

JSON bodies are parsed in memory through DuckDB's JSON scalar functions (`json_structure(...)` and `from_json(...)`) and then serialized through the existing Arrow IPC row mapping before returning rows to the caller. This avoids temporary files while still letting DuckDB own JSON type inference and conversion.

CSV, TSV, and Parquet are recognized media types but do not have parsing providers enabled yet. DuckDB's standard CSV and Parquet readers are path/file-system oriented; using them efficiently for BLOB bodies needs a scalarfs-style memory filesystem or a stable C extension hook that can expose an in-memory file to DuckDB readers. Until that lands, these media types use the generic `body BLOB` fallback instead of spilling to temporary files.

JSON parsing currently fails fast inside service-owned SQL, such as SQL executed through remote `exec` / `ducknng_query_rpc(...)`, because that path already owns the same runtime init-connection execution gate on the current request thread. Raw, text, CSV/TSV/Parquet fallback, frame, and direct Arrow IPC decoding do not need a nested DuckDB query. Lifting the JSON limitation requires a broader per-session/per-request connection model rather than re-entering the shared init connection from inside an active request.

The current provider list is built in, not user-extensible. Future `ducknng_register_codec(...)`-style hooks remain a design question because user-defined callbacks need clear ownership, security, error, and ABI rules. Until that is settled, provider names and content-type matching should stay narrow, documented, and covered by tests.

The HTTP server RPC endpoint does not use this provider layer to accept arbitrary web payloads. `ducknng_start_http_server(...)` still accepts only `POST` requests whose body is one complete `application/vnd.ducknng.frame` envelope. The provider layer is for local parsing and client-side HTTP convenience, not a second server protocol.
