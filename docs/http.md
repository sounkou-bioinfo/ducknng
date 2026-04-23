# ducknng HTTP and HTTPS transport adapter contract

This document defines the first concrete HTTP and HTTPS transport design for `ducknng`. It is binding for future work on `ducknng_start_http_server(...)` and for the already-landed `ducknng_ncurl(...)` client helper. The purpose of the document is to pin the intended SQL surface and carrier semantics now, before implementation pressure causes the project to grow a second RPC surface that drifts away from the existing manifest, session, and Arrow contracts.

The governing rule is the same one stated in `docs/protocol.md` and `docs/transports.md`: HTTP is a transport adapter, not a second protocol. It changes how the bytes travel. It does not change the registry-backed method set, the query-session lifecycle, or the Arrow-versus-JSON payload contract.

## Scope

The initial HTTP adapter is deliberately narrow. It is a carrier for the existing `ducknng` envelope and method registry. It is not a generic web framework, not a browser asset server, not a WebSocket toolkit, and not an excuse to create path-specific copies of `manifest`, `exec`, `query_open`, `fetch`, `close`, or `cancel`. Anything broader than framed RPC carriage belongs to later work and must be justified separately.

The current generic socket surface remains NNG-only. `ducknng_open_socket(...)`, `ducknng_listen_socket(...)`, `ducknng_send_socket_raw(...)`, `ducknng_recv_socket_raw(...)`, and the corresponding socket AIO helpers model NNG socket patterns and do not generalize to HTTP. The HTTP family instead gets its own low-level client helper, `ducknng_ncurl(...)`, while the existing request, RPC, and session helpers are expected to route by URL scheme once the HTTP adapter exists.

## Current versus planned SQL surface

The shipped low-level client entry point is:

```sql
ducknng_ncurl(url, method, headers_json, body, timeout_ms, tls_config_id)
```

It is modeled ergonomically on `nanonext::ncurl()` while staying faithful to DuckDB SQL conventions and the project preference for in-band error tables.

The implemented return shape is:

```text
TABLE(
  ok BOOLEAN,
  status INTEGER,
  error VARCHAR,
  headers_json VARCHAR,
  body BLOB,
  body_text VARCHAR
)
```

`ok` means the HTTP transport operation completed and a response was received. It does not mean the response status was 2xx. `status` is the HTTP status code when present. `error` is reserved for local client, connection, timeout, TLS, or adapter failures. `headers_json` is the response header block in a canonical JSON form that preserves order and duplicates. `body` is the raw response body. `body_text` is a best-effort UTF-8 decoding of `body` and is `NULL` when the body is absent or not valid text.

The request-side `headers_json` argument uses the same canonical JSON shape for symmetry. The preferred contract is an array of objects such as `[{"name":"Content-Type","value":"application/json"}]` rather than a plain JSON object, because HTTP header names may repeat and order sometimes matters operationally.

The concrete planned server entry point is:

```sql
ducknng_start_http_server(name, listen, recv_max_bytes, session_idle_ms, tls_config_id)
```

The arguments intentionally stay close to `ducknng_start_server(...)`.

`name` is the runtime service name. `listen` is a full HTTP or HTTPS endpoint URL such as `http://127.0.0.1:8080/_ducknng` or `https://127.0.0.1:8443/_ducknng`. For the HTTP adapter, the path component is semantically meaningful: it is the RPC mount path. `recv_max_bytes`, `session_idle_ms`, and `tls_config_id` retain their current meanings. `tls_config_id = 0::UBIGINT` means plaintext for `http://` and no explicit TLS configuration for `https://`; callers that need certificate validation or custom trust material should supply an explicit TLS handle.

The matching stop and introspection path should remain generic rather than adding HTTP-specific variants. The intended direction is that `ducknng_stop_server(name)` continues to stop a named service regardless of transport family, and `ducknng_list_servers()` becomes transport-family aware when the HTTP adapter is implemented. That keeps the public lifecycle surface compact.

`ducknng_ncurl(...)` is transport-local and not manifest-derived. It is meant for generic HTTP interactions, adapter debugging, and future interoperability helpers. It is not the only planned route to `ducknng` RPC over HTTP.

## Operation-oriented routing by URL scheme

Once HTTP transport is implemented, the existing request, RPC, and session helpers should remain operation-oriented and route by URL scheme instead of forcing callers to learn a second RPC client API.

That means URLs like `http://127.0.0.1:8080/_ducknng` and `https://127.0.0.1:8443/_ducknng` should eventually be accepted by the existing helpers:

- `ducknng_request(...)`
- `ducknng_request_raw(...)`
- `ducknng_get_rpc_manifest(...)`
- `ducknng_get_rpc_manifest_raw(...)`
- `ducknng_run_rpc(...)`
- `ducknng_run_rpc_raw(...)`
- `ducknng_query_rpc(...)`
- `ducknng_open_query(...)`
- `ducknng_fetch_query(...)`
- `ducknng_close_query(...)`
- `ducknng_cancel_query(...)`
- later, the corresponding AIO request helpers

In other words, `ducknng_ncurl(...)` is the generic HTTP primitive, while the higher-level `ducknng` RPC and session helpers keep their current names and learn to use the HTTP carrier automatically when the URL scheme is `http` or `https`.

## Initial HTTP carrier contract

The first HTTP adapter should expose one RPC endpoint at the exact path encoded in the `listen` URL. If the server is started on `http://127.0.0.1:8080/_ducknng`, then the RPC endpoint is `POST /_ducknng`. The same rule applies to HTTPS.

The initial binding is frame-over-HTTP. The request body is one complete `ducknng` frame. The response body is one complete `ducknng` frame whenever the adapter successfully reaches the registry-backed dispatcher and obtains a protocol-level reply.

The normative media type for both request and response is:

```text
application/vnd.ducknng.frame
```

The HTTP adapter is therefore a carrier for the existing `ducknng` envelope, not a replacement for it. The HTTP body is not raw SQL text, not ad hoc JSON RPC, and not a path-based method binding. It is the same versioned frame that today travels over NNG.

The initial method contract is intentionally narrow. `POST` is the only accepted method on the RPC endpoint. The adapter should reject other methods with `405 Method Not Allowed`. It should reject an unsupported request `Content-Type` with `415 Unsupported Media Type`. It should reject an oversized request with `413 Payload Too Large`. It should reject a malformed frame with `400 Bad Request`. Path mismatches are `404 Not Found`.

Once the adapter has accepted a request as a valid `ducknng` frame and handed it to the dispatcher, protocol-level success and protocol-level failure both travel back as ordinary `ducknng` frames. In that state, HTTP status code `200 OK` is the correct outer status even when the inner `ducknng` reply frame is an error frame. HTTP 4xx and 5xx responses are reserved for adapter-level failures that occur before a valid `ducknng` reply frame exists.

This distinction is essential. It keeps application errors inside the `ducknng` protocol, where existing clients already know how to decode them, instead of spreading method failure semantics across two unrelated status systems.

## Sessions and Arrow record batches over HTTP

The HTTP adapter does not alter the session contract. It carries the same methods and payloads that already exist over the NNG carrier.

`query_open` still accepts an Arrow IPC payload containing exactly one logical request row with `sql` and optional batch controls. Over HTTP, that Arrow IPC payload remains the payload inside a `ducknng` request frame, and that frame becomes the HTTP request body.

`fetch` still accepts JSON control metadata keyed by `session_id`. Over HTTP, that JSON control payload remains the payload inside a `ducknng` request frame, and that frame becomes the HTTP request body.

`fetch` still returns either Arrow IPC row data or JSON control metadata. When rows are returned, the Arrow IPC bytes remain inside the `ducknng` reply frame payload exactly as they do over NNG. The HTTP response body is therefore still one `ducknng` frame whose payload contains Arrow IPC record-batch bytes. When only control metadata is returned, the HTTP response body is one `ducknng` frame whose payload contains JSON.

`close` and `cancel` retain the same JSON control request and response shapes. They do not become path-specialized HTTP endpoints and they do not gain alternate payload encodings just because the outer carrier is HTTP.

This means Arrow record batches remain Arrow record batches. They do not become JSON arrays, text tables, or bespoke HTTP chunking formats in the first HTTP adapter. Session ids remain session ids. They do not migrate into path segments or query parameters. The same state machine in `docs/protocol.md` continues to govern `query_open`, `fetch`, `close`, and `cancel`.

## TLS and security

The HTTP adapter inherits the same trust model and ownership rules described in `docs/security.md`. It does not introduce a second authentication model. HTTPS uses the same TLS handle model already established for the NNG transport direction, and session ownership remains a protocol-level concern rather than a carrier-local shortcut.

A successful HTTPS deployment therefore still needs deliberate TLS configuration and still does not make bare `session_id` ownership rules acceptable. The transport may encrypt the bytes, but the multi-client session contract remains the same problem it already is over NNG.

## Deferred items

The following are explicitly not part of the first HTTP adapter contract.

Human-friendly convenience routes such as `GET /manifest` are deferred. They may later be added as transport conveniences that internally map onto the same registry-backed methods, but they are not part of the first binding.

WebSocket, SSE, NDJSON, browser asset serving, and mixed HTTP-plus-static routing are deferred. They belong to broader web-toolkit work and should not be smuggled into the first RPC carrier implementation.

A dedicated `ducknng_ncurl_aio(...)` or persistent HTTP session handle surface is also deferred. The natural future direction is to offer them in a way that mirrors the already-landed `ducknng` AIO model and the `nanonext` ergonomic reference, but the first priority is a correct synchronous transport adapter that preserves the existing manifest, session, and Arrow contracts.
