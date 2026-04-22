#include "ducknng_sql_api.h"
#include "ducknng_ipc_in.h"
#include "ducknng_ipc_out.h"
#include "ducknng_nng_compat.h"
#include "ducknng_runtime.h"
#include "ducknng_service.h"
#include "ducknng_util.h"
#include "ducknng_wire.h"
#include <string.h>

DUCKDB_EXTENSION_EXTERN

typedef struct {
    ducknng_runtime *rt;
} ducknng_sql_context;

typedef struct {
    uint64_t service_id;
    char *name;
    char *listen;
    int32_t contexts;
    bool running;
    uint64_t sessions;
} ducknng_server_row;

typedef struct {
    ducknng_server_row *rows;
    idx_t row_count;
} ducknng_servers_bind_data;

typedef struct {
    ducknng_servers_bind_data *bind;
    idx_t offset;
} ducknng_servers_init_data;

typedef struct {
    uint64_t socket_id;
    char *protocol;
    char *url;
    bool open;
    bool connected;
    int32_t send_timeout_ms;
    int32_t recv_timeout_ms;
} ducknng_socket_row;

typedef struct {
    ducknng_socket_row *rows;
    idx_t row_count;
} ducknng_sockets_bind_data;

typedef struct {
    ducknng_sockets_bind_data *bind;
    idx_t offset;
} ducknng_sockets_init_data;

typedef struct {
    struct ArrowSchema schema;
    struct ArrowArray array;
    idx_t row_count;
} ducknng_remote_bind_data;

typedef struct {
    ducknng_remote_bind_data *bind;
    idx_t offset;
} ducknng_remote_init_data;

static int arg_is_null(duckdb_vector vec, idx_t row) {
    uint64_t *validity = duckdb_vector_get_validity(vec);
    return validity && !duckdb_validity_row_is_valid(validity, row);
}
static char *arg_varchar_dup(duckdb_vector vec, idx_t row) {
    duckdb_string_t *data = (duckdb_string_t *)duckdb_vector_get_data(vec);
    const char *src;
    uint32_t len;
    char *out;
    if (arg_is_null(vec, row)) return NULL;
    src = duckdb_string_t_data(&data[row]);
    len = duckdb_string_t_length(data[row]);
    out = (char *)duckdb_malloc((size_t)len + 1);
    if (!out) return NULL;
    if (len) memcpy(out, src, len);
    out[len] = '\0';
    return out;
}
static uint8_t *arg_blob_dup(duckdb_vector vec, idx_t row, idx_t *len_out) {
    duckdb_string_t *data = (duckdb_string_t *)duckdb_vector_get_data(vec);
    const char *src;
    uint32_t len;
    uint8_t *out;
    if (len_out) *len_out = 0;
    if (arg_is_null(vec, row)) return NULL;
    src = duckdb_string_t_data(&data[row]);
    len = duckdb_string_t_length(data[row]);
    out = (uint8_t *)duckdb_malloc((size_t)len);
    if (!out && len > 0) return NULL;
    if (len) memcpy(out, src, len);
    if (len_out) *len_out = (idx_t)len;
    return out;
}
static int32_t arg_int32(duckdb_vector vec, idx_t row, int32_t dflt) {
    int32_t *data = (int32_t *)duckdb_vector_get_data(vec);
    if (arg_is_null(vec, row)) return dflt;
    return data[row];
}
static uint64_t arg_u64(duckdb_vector vec, idx_t row, uint64_t dflt) {
    uint64_t *data = (uint64_t *)duckdb_vector_get_data(vec);
    if (arg_is_null(vec, row)) return dflt;
    return data[row];
}
static void set_null(duckdb_vector vec, idx_t row) {
    uint64_t *validity;
    duckdb_vector_ensure_validity_writable(vec);
    validity = duckdb_vector_get_validity(vec);
    duckdb_validity_set_row_invalid(validity, row);
}
static void assign_blob(duckdb_vector vec, idx_t row, const uint8_t *data, idx_t len) {
    duckdb_vector_assign_string_element_len(vec, row, (const char *)data, len);
}

static void destroy_servers_bind_data(void *ptr) {
    ducknng_servers_bind_data *data = (ducknng_servers_bind_data *)ptr;
    idx_t i;
    if (!data) return;
    for (i = 0; i < data->row_count; i++) {
        if (data->rows[i].name) duckdb_free(data->rows[i].name);
        if (data->rows[i].listen) duckdb_free(data->rows[i].listen);
    }
    if (data->rows) duckdb_free(data->rows);
    duckdb_free(data);
}

static void destroy_servers_init_data(void *ptr) {
    ducknng_servers_init_data *data = (ducknng_servers_init_data *)ptr;
    if (data) duckdb_free(data);
}

static void destroy_sockets_bind_data(void *ptr) {
    ducknng_sockets_bind_data *data = (ducknng_sockets_bind_data *)ptr;
    idx_t i;
    if (!data) return;
    for (i = 0; i < data->row_count; i++) {
        if (data->rows[i].protocol) duckdb_free(data->rows[i].protocol);
        if (data->rows[i].url) duckdb_free(data->rows[i].url);
    }
    if (data->rows) duckdb_free(data->rows);
    duckdb_free(data);
}

static void destroy_sockets_init_data(void *ptr) {
    ducknng_sockets_init_data *data = (ducknng_sockets_init_data *)ptr;
    if (data) duckdb_free(data);
}

static void destroy_remote_bind_data(void *ptr) {
    ducknng_remote_bind_data *data = (ducknng_remote_bind_data *)ptr;
    if (!data) return;
    if (data->array.release) ArrowArrayRelease(&data->array);
    if (data->schema.release) ArrowSchemaRelease(&data->schema);
    duckdb_free(data);
}

static void destroy_remote_init_data(void *ptr) {
    ducknng_remote_init_data *data = (ducknng_remote_init_data *)ptr;
    if (data) duckdb_free(data);
}

static void ducknng_server_start_scalar(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
    idx_t count = duckdb_data_chunk_get_size(input);
    idx_t row;
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_scalar_function_get_extra_info(info);
    bool *out = (bool *)duckdb_vector_get_data(output);
    for (row = 0; row < count; row++) {
        char *name = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 0), row);
        char *listen = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 1), row);
        int contexts = arg_int32(duckdb_data_chunk_get_vector(input, 2), row, 1);
        uint64_t recv_max = arg_u64(duckdb_data_chunk_get_vector(input, 3), row, 134217728ULL);
        uint64_t idle_ms = arg_u64(duckdb_data_chunk_get_vector(input, 4), row, 300000ULL);
        char *cert = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 5), row);
        char *ca = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 6), row);
        int auth_mode = arg_int32(duckdb_data_chunk_get_vector(input, 7), row, 0);
        ducknng_service *svc;
        char *errmsg = NULL;
        if (!ctx || !ctx->rt || !name || !listen) {
            if (name) duckdb_free(name);
            if (listen) duckdb_free(listen);
            if (cert) duckdb_free(cert);
            if (ca) duckdb_free(ca);
            duckdb_scalar_function_set_error(info, "ducknng: invalid start arguments");
            return;
        }
        if (!name[0] || !listen[0]) {
            duckdb_free(name);
            duckdb_free(listen);
            if (cert) duckdb_free(cert);
            if (ca) duckdb_free(ca);
            duckdb_scalar_function_set_error(info, "ducknng: service name and listen URL must be non-empty");
            return;
        }
        if (contexts < 1) {
            duckdb_free(name);
            duckdb_free(listen);
            if (cert) duckdb_free(cert);
            if (ca) duckdb_free(ca);
            duckdb_scalar_function_set_error(info, "ducknng: contexts must be >= 1");
            return;
        }
        if (recv_max == 0 || idle_ms == 0) {
            duckdb_free(name);
            duckdb_free(listen);
            if (cert) duckdb_free(cert);
            if (ca) duckdb_free(ca);
            duckdb_scalar_function_set_error(info, "ducknng: recv_max_bytes and session_idle_ms must be > 0");
            return;
        }
        if (auth_mode < 0) {
            duckdb_free(name);
            duckdb_free(listen);
            if (cert) duckdb_free(cert);
            if (ca) duckdb_free(ca);
            duckdb_scalar_function_set_error(info, "ducknng: tls_auth_mode must be >= 0");
            return;
        }
        svc = ducknng_service_create(ctx->rt, name, listen, contexts, (size_t)recv_max, idle_ms, cert, ca, auth_mode);
        duckdb_free(name);
        duckdb_free(listen);
        if (cert) duckdb_free(cert);
        if (ca) duckdb_free(ca);
        if (!svc) {
            duckdb_scalar_function_set_error(info, "ducknng: failed to allocate service");
            return;
        }
        if (ducknng_runtime_add_service(ctx->rt, svc, &errmsg) != 0) {
            ducknng_service_destroy(svc);
            duckdb_scalar_function_set_error(info, errmsg ? errmsg : "ducknng: failed to add service");
            if (errmsg) duckdb_free(errmsg);
            return;
        }
        if (ducknng_service_start(svc, &errmsg) != 0) {
            ducknng_runtime_remove_service(ctx->rt, svc->name);
            ducknng_service_destroy(svc);
            duckdb_scalar_function_set_error(info, errmsg ? errmsg : "ducknng: failed to start service");
            if (errmsg) duckdb_free(errmsg);
            return;
        }
        out[row] = true;
    }
}

static void ducknng_server_stop_scalar(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
    idx_t count = duckdb_data_chunk_get_size(input);
    idx_t row;
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_scalar_function_get_extra_info(info);
    bool *out = (bool *)duckdb_vector_get_data(output);
    for (row = 0; row < count; row++) {
        char *name = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 0), row);
        ducknng_service *svc;
        if (!ctx || !ctx->rt || !name) {
            if (name) duckdb_free(name);
            duckdb_scalar_function_set_error(info, "ducknng: invalid stop arguments");
            return;
        }
        svc = ducknng_runtime_remove_service(ctx->rt, name);
        duckdb_free(name);
        if (!svc) {
            duckdb_scalar_function_set_error(info, "ducknng: service not found");
            return;
        }
        ducknng_service_stop(svc, NULL);
        ducknng_service_destroy(svc);
        out[row] = true;
    }
}

static nng_msg *ducknng_client_manifest_request(void) {
    return ducknng_build_reply(DUCKNNG_RPC_MANIFEST, NULL, 0, NULL, NULL, 0);
}

static nng_msg *ducknng_client_exec_request(const char *sql, int want_result, char **errmsg) {
    uint8_t *payload = NULL;
    size_t payload_len = 0;
    nng_msg *msg;
    if (ducknng_exec_request_to_ipc(sql, want_result, &payload, &payload_len, errmsg) != 0) return NULL;
    msg = ducknng_build_reply(DUCKNNG_RPC_CALL, "exec", 0, NULL, payload, (uint64_t)payload_len);
    duckdb_free(payload);
    if (!msg && errmsg && !*errmsg) *errmsg = ducknng_strdup("ducknng: failed to allocate exec request message");
    return msg;
}

static char *ducknng_frame_error_detail(const ducknng_frame *frame, const char *fallback) {
    char *detail;
    if (!frame || !frame->error || frame->error_len == 0) return ducknng_strdup(fallback);
    detail = (char *)duckdb_malloc((size_t)frame->error_len + 1);
    if (!detail) return ducknng_strdup("ducknng: out of memory decoding remote error");
    memcpy(detail, frame->error, (size_t)frame->error_len);
    detail[frame->error_len] = '\0';
    return detail;
}

static int ducknng_client_open_req_socket(const char *url, int timeout_ms, nng_socket *out, char **errmsg) {
    int rv;
    if (!url || !url[0] || !out) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: client URL is required");
        return -1;
    }
    rv = ducknng_req_socket_open(out);
    if (rv != 0) {
        if (errmsg) *errmsg = ducknng_strdup(ducknng_nng_strerror(rv));
        return -1;
    }
    rv = ducknng_socket_set_timeout_ms(*out, timeout_ms, timeout_ms);
    if (rv != 0) {
        if (errmsg) *errmsg = ducknng_strdup(ducknng_nng_strerror(rv));
        ducknng_socket_close(*out);
        return -1;
    }
    rv = ducknng_socket_dial(*out, url);
    if (rv != 0) {
        if (errmsg) *errmsg = ducknng_strdup(ducknng_nng_strerror(rv));
        ducknng_socket_close(*out);
        return -1;
    }
    return 0;
}

static nng_msg *ducknng_client_roundtrip(const char *url, nng_msg *req, int timeout_ms, char **errmsg) {
    nng_socket sock;
    nng_msg *resp = NULL;
    int rv;
    memset(&sock, 0, sizeof(sock));
    if (!req) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: request message is required");
        return NULL;
    }
    if (ducknng_client_open_req_socket(url, timeout_ms, &sock, errmsg) != 0) {
        nng_msg_free(req);
        return NULL;
    }
    rv = ducknng_req_transact(sock, req, &resp);
    ducknng_socket_close(sock);
    if (rv != 0) {
        if (errmsg) *errmsg = ducknng_strdup(ducknng_nng_strerror(rv));
        return NULL;
    }
    return resp;
}

static nng_msg *ducknng_client_roundtrip_raw(const char *url, const uint8_t *payload, size_t payload_len,
    int timeout_ms, char **errmsg) {
    nng_msg *req = NULL;
    int rv = nng_msg_alloc(&req, payload_len);
    if (rv != 0) {
        if (errmsg) *errmsg = ducknng_strdup(ducknng_nng_strerror(rv));
        return NULL;
    }
    if (payload_len) memcpy(nng_msg_body(req), payload, payload_len);
    return ducknng_client_roundtrip(url, req, timeout_ms, errmsg);
}

static void ducknng_remote_manifest_scalar(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
    idx_t count = duckdb_data_chunk_get_size(input);
    idx_t row;
    for (row = 0; row < count; row++) {
        char *url = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 0), row);
        char *errmsg = NULL;
        nng_msg *resp_msg;
        ducknng_frame frame;
        if (!url) {
            duckdb_scalar_function_set_error(info, "ducknng: remote manifest URL must not be NULL");
            return;
        }
        resp_msg = ducknng_client_roundtrip(url, ducknng_client_manifest_request(), 5000, &errmsg);
        duckdb_free(url);
        if (!resp_msg) {
            duckdb_scalar_function_set_error(info, errmsg ? errmsg : "ducknng: manifest request failed");
            if (errmsg) duckdb_free(errmsg);
            return;
        }
        if (ducknng_decode_request(resp_msg, &frame) != 0) {
            nng_msg_free(resp_msg);
            duckdb_scalar_function_set_error(info, "ducknng: invalid manifest response envelope");
            return;
        }
        if (frame.type == DUCKNNG_RPC_ERROR) {
            char *detail = ducknng_frame_error_detail(&frame, "ducknng: manifest request failed");
            nng_msg_free(resp_msg);
            duckdb_scalar_function_set_error(info, detail ? detail : "ducknng: manifest request failed");
            if (detail) duckdb_free(detail);
            return;
        }
        if (frame.type != DUCKNNG_RPC_RESULT || !(frame.flags & DUCKNNG_RPC_FLAG_PAYLOAD_JSON)) {
            nng_msg_free(resp_msg);
            duckdb_scalar_function_set_error(info, "ducknng: manifest response was not JSON result payload");
            return;
        }
        duckdb_vector_assign_string_element_len(output, row, (const char *)frame.payload, (idx_t)frame.payload_len);
        nng_msg_free(resp_msg);
    }
}

static void ducknng_remote_exec_scalar(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
    idx_t count = duckdb_data_chunk_get_size(input);
    idx_t row;
    uint64_t *out = (uint64_t *)duckdb_vector_get_data(output);
    for (row = 0; row < count; row++) {
        char *url = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 0), row);
        char *sql = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 1), row);
        char *errmsg = NULL;
        nng_msg *resp_msg;
        ducknng_frame frame;
        uint64_t rows_changed = 0;
        uint32_t statement_type = 0;
        uint32_t result_type = 0;
        if (!url || !sql) {
            if (url) duckdb_free(url);
            if (sql) duckdb_free(sql);
            duckdb_scalar_function_set_error(info, "ducknng: remote exec URL and SQL must not be NULL");
            return;
        }
        resp_msg = ducknng_client_roundtrip(url, ducknng_client_exec_request(sql, 0, &errmsg), 5000, &errmsg);
        duckdb_free(url);
        duckdb_free(sql);
        if (!resp_msg) {
            duckdb_scalar_function_set_error(info, errmsg ? errmsg : "ducknng: remote exec request failed");
            if (errmsg) duckdb_free(errmsg);
            return;
        }
        if (ducknng_decode_request(resp_msg, &frame) != 0) {
            nng_msg_free(resp_msg);
            duckdb_scalar_function_set_error(info, "ducknng: invalid remote exec response envelope");
            return;
        }
        if (frame.type == DUCKNNG_RPC_ERROR) {
            char *detail = ducknng_frame_error_detail(&frame, "ducknng: remote exec request failed");
            nng_msg_free(resp_msg);
            duckdb_scalar_function_set_error(info, detail ? detail : "ducknng: remote exec request failed");
            if (detail) duckdb_free(detail);
            return;
        }
        if (frame.type != DUCKNNG_RPC_RESULT || !(frame.flags & DUCKNNG_RPC_FLAG_RESULT_METADATA)) {
            nng_msg_free(resp_msg);
            duckdb_scalar_function_set_error(info, "ducknng: remote exec expected metadata reply");
            return;
        }
        if (ducknng_decode_exec_metadata_payload(frame.payload, (size_t)frame.payload_len,
                &rows_changed, &statement_type, &result_type, &errmsg) != 0) {
            nng_msg_free(resp_msg);
            duckdb_scalar_function_set_error(info, errmsg ? errmsg : "ducknng: failed to decode remote exec metadata");
            if (errmsg) duckdb_free(errmsg);
            return;
        }
        (void)statement_type;
        (void)result_type;
        out[row] = rows_changed;
        nng_msg_free(resp_msg);
    }
}

static void ducknng_socket_scalar(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
    idx_t count = duckdb_data_chunk_get_size(input);
    idx_t row;
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_scalar_function_get_extra_info(info);
    uint64_t *out = (uint64_t *)duckdb_vector_get_data(output);
    for (row = 0; row < count; row++) {
        char *protocol = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 0), row);
        ducknng_client_socket *sock;
        char *errmsg = NULL;
        int rv;
        if (!ctx || !ctx->rt || !protocol) {
            if (protocol) duckdb_free(protocol);
            duckdb_scalar_function_set_error(info, "ducknng: socket protocol is required");
            return;
        }
        if (strcmp(protocol, "req") != 0) {
            duckdb_free(protocol);
            duckdb_scalar_function_set_error(info, "ducknng: only req client sockets are implemented");
            return;
        }
        sock = (ducknng_client_socket *)duckdb_malloc(sizeof(*sock));
        if (!sock) {
            duckdb_free(protocol);
            duckdb_scalar_function_set_error(info, "ducknng: out of memory allocating client socket");
            return;
        }
        memset(sock, 0, sizeof(*sock));
        sock->protocol = protocol;
        sock->send_timeout_ms = 5000;
        sock->recv_timeout_ms = 5000;
        rv = ducknng_req_socket_open(&sock->sock);
        if (rv != 0) {
            duckdb_free(sock->protocol);
            duckdb_free(sock);
            duckdb_scalar_function_set_error(info, ducknng_nng_strerror(rv));
            return;
        }
        rv = ducknng_ctx_open(&sock->ctx, sock->sock);
        if (rv != 0) {
            ducknng_socket_close(sock->sock);
            duckdb_free(sock->protocol);
            duckdb_free(sock);
            duckdb_scalar_function_set_error(info, ducknng_nng_strerror(rv));
            return;
        }
        sock->has_ctx = 1;
        sock->open = 1;
        if (ducknng_runtime_add_client_socket(ctx->rt, sock, &errmsg) != 0) {
            ducknng_socket_close(sock->sock);
            duckdb_free(sock->protocol);
            duckdb_free(sock);
            duckdb_scalar_function_set_error(info, errmsg ? errmsg : "ducknng: failed to register client socket");
            if (errmsg) duckdb_free(errmsg);
            return;
        }
        out[row] = sock->socket_id;
    }
}

static void ducknng_dial_scalar(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
    idx_t count = duckdb_data_chunk_get_size(input);
    idx_t row;
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_scalar_function_get_extra_info(info);
    bool *out = (bool *)duckdb_vector_get_data(output);
    for (row = 0; row < count; row++) {
        uint64_t socket_id = arg_u64(duckdb_data_chunk_get_vector(input, 0), row, 0);
        char *url = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 1), row);
        int32_t timeout_ms = arg_int32(duckdb_data_chunk_get_vector(input, 2), row, 5000);
        ducknng_client_socket *sock;
        int rv;
        if (!ctx || !ctx->rt || socket_id == 0 || !url) {
            if (url) duckdb_free(url);
            duckdb_scalar_function_set_error(info, "ducknng: socket id and URL are required");
            return;
        }
        sock = ducknng_runtime_find_client_socket(ctx->rt, socket_id);
        if (!sock || !sock->open) {
            duckdb_free(url);
            duckdb_scalar_function_set_error(info, "ducknng: client socket not found");
            return;
        }
        if (strcmp(sock->protocol, "req") != 0) {
            duckdb_free(url);
            duckdb_scalar_function_set_error(info, "ducknng: only req client sockets are implemented");
            return;
        }
        rv = ducknng_socket_set_timeout_ms(sock->sock, timeout_ms, timeout_ms);
        if (rv != 0) {
            duckdb_free(url);
            duckdb_scalar_function_set_error(info, ducknng_nng_strerror(rv));
            return;
        }
        rv = ducknng_socket_dial(sock->sock, url);
        if (rv != 0) {
            duckdb_free(url);
            duckdb_scalar_function_set_error(info, ducknng_nng_strerror(rv));
            return;
        }
        if (sock->url) duckdb_free(sock->url);
        sock->url = url;
        sock->connected = 1;
        sock->send_timeout_ms = timeout_ms;
        sock->recv_timeout_ms = timeout_ms;
        out[row] = true;
    }
}

static void ducknng_close_scalar(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
    idx_t count = duckdb_data_chunk_get_size(input);
    idx_t row;
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_scalar_function_get_extra_info(info);
    bool *out = (bool *)duckdb_vector_get_data(output);
    for (row = 0; row < count; row++) {
        uint64_t socket_id = arg_u64(duckdb_data_chunk_get_vector(input, 0), row, 0);
        ducknng_client_socket *sock;
        if (!ctx || !ctx->rt || socket_id == 0) {
            duckdb_scalar_function_set_error(info, "ducknng: socket id is required");
            return;
        }
        sock = ducknng_runtime_remove_client_socket(ctx->rt, socket_id);
        if (!sock) {
            duckdb_scalar_function_set_error(info, "ducknng: client socket not found");
            return;
        }
        if (sock->has_ctx) ducknng_ctx_close(sock->ctx);
        if (sock->open) ducknng_socket_close(sock->sock);
        if (sock->protocol) duckdb_free(sock->protocol);
        if (sock->url) duckdb_free(sock->url);
        if (sock->pending_request) duckdb_free(sock->pending_request);
        if (sock->pending_reply) duckdb_free(sock->pending_reply);
        duckdb_free(sock);
        out[row] = true;
    }
}

static void ducknng_send_scalar(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
    idx_t count = duckdb_data_chunk_get_size(input);
    idx_t row;
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_scalar_function_get_extra_info(info);
    bool *out = (bool *)duckdb_vector_get_data(output);
    for (row = 0; row < count; row++) {
        uint64_t socket_id = arg_u64(duckdb_data_chunk_get_vector(input, 0), row, 0);
        idx_t payload_len = 0;
        uint8_t *payload = arg_blob_dup(duckdb_data_chunk_get_vector(input, 1), row, &payload_len);
        ducknng_client_socket *sock;
        if (!ctx || !ctx->rt || socket_id == 0 || (!payload && payload_len > 0)) {
            if (payload) duckdb_free(payload);
            duckdb_scalar_function_set_error(info, "ducknng: socket id and payload are required");
            return;
        }
        sock = ducknng_runtime_find_client_socket(ctx->rt, socket_id);
        if (!sock || !sock->open || !sock->connected) {
            if (payload) duckdb_free(payload);
            duckdb_scalar_function_set_error(info, "ducknng: connected client socket not found");
            return;
        }
        if (sock->pending_request) {
            duckdb_free(sock->pending_request);
            sock->pending_request = NULL;
            sock->pending_request_len = 0;
        }
        if (sock->pending_reply) {
            duckdb_free(sock->pending_reply);
            sock->pending_reply = NULL;
            sock->pending_reply_len = 0;
        }
        if (payload_len > 0) {
            sock->pending_request = payload;
            sock->pending_request_len = (size_t)payload_len;
        } else if (payload) {
            duckdb_free(payload);
        }
        out[row] = true;
    }
}

static void ducknng_recv_scalar(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
    idx_t count = duckdb_data_chunk_get_size(input);
    idx_t row;
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_scalar_function_get_extra_info(info);
    for (row = 0; row < count; row++) {
        uint64_t socket_id = arg_u64(duckdb_data_chunk_get_vector(input, 0), row, 0);
        int32_t timeout_ms = arg_int32(duckdb_data_chunk_get_vector(input, 1), row, 5000);
        ducknng_client_socket *sock;
        nng_msg *msg = NULL;
        char *errmsg = NULL;
        if (!ctx || !ctx->rt || socket_id == 0) {
            duckdb_scalar_function_set_error(info, "ducknng: socket id is required");
            return;
        }
        sock = ducknng_runtime_find_client_socket(ctx->rt, socket_id);
        if (!sock || !sock->open || !sock->connected) {
            duckdb_scalar_function_set_error(info, "ducknng: connected client socket not found");
            return;
        }
        if (!sock->pending_reply) {
            if (!sock->url || !sock->pending_request) {
                duckdb_scalar_function_set_error(info, "ducknng: no pending request for recv");
                return;
            }
            msg = ducknng_client_roundtrip_raw(sock->url, sock->pending_request, sock->pending_request_len,
                timeout_ms, &errmsg);
            if (!msg) {
                duckdb_scalar_function_set_error(info, errmsg ? errmsg : "ducknng: recv request failed");
                if (errmsg) duckdb_free(errmsg);
                return;
            }
            sock->pending_reply_len = (size_t)nng_msg_len(msg);
            sock->pending_reply = (uint8_t *)duckdb_malloc(sock->pending_reply_len);
            if (!sock->pending_reply && sock->pending_reply_len > 0) {
                nng_msg_free(msg);
                duckdb_scalar_function_set_error(info, "ducknng: out of memory buffering reply");
                return;
            }
            if (sock->pending_reply_len) memcpy(sock->pending_reply, nng_msg_body(msg), sock->pending_reply_len);
            nng_msg_free(msg);
            duckdb_free(sock->pending_request);
            sock->pending_request = NULL;
            sock->pending_request_len = 0;
        }
        assign_blob(output, row, sock->pending_reply, (idx_t)sock->pending_reply_len);
        duckdb_free(sock->pending_reply);
        sock->pending_reply = NULL;
        sock->pending_reply_len = 0;
        sock->recv_timeout_ms = timeout_ms;
    }
}

static void ducknng_request_scalar(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
    idx_t count = duckdb_data_chunk_get_size(input);
    idx_t row;
    for (row = 0; row < count; row++) {
        char *url = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 0), row);
        idx_t payload_len = 0;
        uint8_t *payload = arg_blob_dup(duckdb_data_chunk_get_vector(input, 1), row, &payload_len);
        int32_t timeout_ms = arg_int32(duckdb_data_chunk_get_vector(input, 2), row, 5000);
        nng_msg *resp = NULL;
        char *errmsg = NULL;
        if (!url || (!payload && payload_len > 0)) {
            if (url) duckdb_free(url);
            if (payload) duckdb_free(payload);
            duckdb_scalar_function_set_error(info, "ducknng: request URL and payload are required");
            return;
        }
        resp = ducknng_client_roundtrip_raw(url, payload, (size_t)payload_len, timeout_ms, &errmsg);
        duckdb_free(url);
        if (payload) duckdb_free(payload);
        if (!resp) {
            duckdb_scalar_function_set_error(info, errmsg ? errmsg : "ducknng: request failed");
            if (errmsg) duckdb_free(errmsg);
            return;
        }
        assign_blob(output, row, (const uint8_t *)nng_msg_body(resp), (idx_t)nng_msg_len(resp));
        nng_msg_free(resp);
    }
}

static void ducknng_request_socket_scalar(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
    idx_t count = duckdb_data_chunk_get_size(input);
    idx_t row;
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_scalar_function_get_extra_info(info);
    for (row = 0; row < count; row++) {
        uint64_t socket_id = arg_u64(duckdb_data_chunk_get_vector(input, 0), row, 0);
        idx_t payload_len = 0;
        uint8_t *payload = arg_blob_dup(duckdb_data_chunk_get_vector(input, 1), row, &payload_len);
        int32_t timeout_ms = arg_int32(duckdb_data_chunk_get_vector(input, 2), row, 5000);
        ducknng_client_socket *sock;
        nng_msg *resp = NULL;
        char *errmsg = NULL;
        if (!ctx || !ctx->rt || socket_id == 0 || (!payload && payload_len > 0)) {
            if (payload) duckdb_free(payload);
            duckdb_scalar_function_set_error(info, "ducknng: socket id and payload are required");
            return;
        }
        sock = ducknng_runtime_find_client_socket(ctx->rt, socket_id);
        if (!sock || !sock->open || !sock->connected || !sock->url) {
            if (payload) duckdb_free(payload);
            duckdb_scalar_function_set_error(info, "ducknng: connected client socket not found");
            return;
        }
        resp = ducknng_client_roundtrip_raw(sock->url, payload, (size_t)payload_len, timeout_ms, &errmsg);
        if (payload) duckdb_free(payload);
        if (!resp) {
            duckdb_scalar_function_set_error(info, errmsg ? errmsg : "ducknng: socket request failed");
            if (errmsg) duckdb_free(errmsg);
            return;
        }
        assign_blob(output, row, (const uint8_t *)nng_msg_body(resp), (idx_t)nng_msg_len(resp));
        nng_msg_free(resp);
    }
}

static int ducknng_arrow_schema_to_logical_type(const struct ArrowSchema *schema,
    duckdb_logical_type *out_type, char **errmsg) {
    struct ArrowSchemaView schema_view;
    struct ArrowError error;
    memset(&schema_view, 0, sizeof(schema_view));
    memset(&error, 0, sizeof(error));
    if (!schema || !out_type) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: missing Arrow schema for remote table binding");
        return -1;
    }
    if (ArrowSchemaViewInit(&schema_view, schema, &error) != NANOARROW_OK) {
        if (errmsg) *errmsg = ducknng_strdup(error.message);
        return -1;
    }
    switch (schema_view.storage_type) {
        case NANOARROW_TYPE_BOOL:
            *out_type = duckdb_create_logical_type(DUCKDB_TYPE_BOOLEAN);
            return 0;
        case NANOARROW_TYPE_INT8:
            *out_type = duckdb_create_logical_type(DUCKDB_TYPE_TINYINT);
            return 0;
        case NANOARROW_TYPE_INT16:
            *out_type = duckdb_create_logical_type(DUCKDB_TYPE_SMALLINT);
            return 0;
        case NANOARROW_TYPE_INT32:
            *out_type = duckdb_create_logical_type(DUCKDB_TYPE_INTEGER);
            return 0;
        case NANOARROW_TYPE_INT64:
            *out_type = duckdb_create_logical_type(DUCKDB_TYPE_BIGINT);
            return 0;
        case NANOARROW_TYPE_UINT8:
            *out_type = duckdb_create_logical_type(DUCKDB_TYPE_UTINYINT);
            return 0;
        case NANOARROW_TYPE_UINT16:
            *out_type = duckdb_create_logical_type(DUCKDB_TYPE_USMALLINT);
            return 0;
        case NANOARROW_TYPE_UINT32:
            *out_type = duckdb_create_logical_type(DUCKDB_TYPE_UINTEGER);
            return 0;
        case NANOARROW_TYPE_UINT64:
            *out_type = duckdb_create_logical_type(DUCKDB_TYPE_UBIGINT);
            return 0;
        case NANOARROW_TYPE_FLOAT:
            *out_type = duckdb_create_logical_type(DUCKDB_TYPE_FLOAT);
            return 0;
        case NANOARROW_TYPE_DOUBLE:
            *out_type = duckdb_create_logical_type(DUCKDB_TYPE_DOUBLE);
            return 0;
        case NANOARROW_TYPE_STRING:
            *out_type = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
            return 0;
        case NANOARROW_TYPE_BINARY:
            *out_type = duckdb_create_logical_type(DUCKDB_TYPE_BLOB);
            return 0;
        default:
            if (errmsg) *errmsg = ducknng_strdup(
                "ducknng: remote unary row replies currently support BOOLEAN, signed/unsigned integers, FLOAT/DOUBLE, VARCHAR, and BLOB only");
            return -1;
    }
}

static int ducknng_remote_assign_column(duckdb_vector vec, struct ArrowArrayView *col_view,
    const struct ArrowSchema *col_schema, idx_t src_offset, idx_t count, char **errmsg) {
    struct ArrowSchemaView schema_view;
    struct ArrowError error;
    idx_t i;
    memset(&schema_view, 0, sizeof(schema_view));
    memset(&error, 0, sizeof(error));
    if (ArrowSchemaViewInit(&schema_view, col_schema, &error) != NANOARROW_OK) {
        if (errmsg) *errmsg = ducknng_strdup(error.message);
        return -1;
    }
    switch (schema_view.storage_type) {
        case NANOARROW_TYPE_BOOL: {
            bool *out = (bool *)duckdb_vector_get_data(vec);
            for (i = 0; i < count; i++) {
                if (ArrowArrayViewIsNull(col_view, src_offset + i)) set_null(vec, i);
                else out[i] = ArrowArrayViewGetIntUnsafe(col_view, src_offset + i) != 0;
            }
            return 0;
        }
        case NANOARROW_TYPE_INT8: {
            int8_t *out = (int8_t *)duckdb_vector_get_data(vec);
            for (i = 0; i < count; i++) {
                if (ArrowArrayViewIsNull(col_view, src_offset + i)) set_null(vec, i);
                else out[i] = (int8_t)ArrowArrayViewGetIntUnsafe(col_view, src_offset + i);
            }
            return 0;
        }
        case NANOARROW_TYPE_INT16: {
            int16_t *out = (int16_t *)duckdb_vector_get_data(vec);
            for (i = 0; i < count; i++) {
                if (ArrowArrayViewIsNull(col_view, src_offset + i)) set_null(vec, i);
                else out[i] = (int16_t)ArrowArrayViewGetIntUnsafe(col_view, src_offset + i);
            }
            return 0;
        }
        case NANOARROW_TYPE_INT32: {
            int32_t *out = (int32_t *)duckdb_vector_get_data(vec);
            for (i = 0; i < count; i++) {
                if (ArrowArrayViewIsNull(col_view, src_offset + i)) set_null(vec, i);
                else out[i] = (int32_t)ArrowArrayViewGetIntUnsafe(col_view, src_offset + i);
            }
            return 0;
        }
        case NANOARROW_TYPE_INT64: {
            int64_t *out = (int64_t *)duckdb_vector_get_data(vec);
            for (i = 0; i < count; i++) {
                if (ArrowArrayViewIsNull(col_view, src_offset + i)) set_null(vec, i);
                else out[i] = (int64_t)ArrowArrayViewGetIntUnsafe(col_view, src_offset + i);
            }
            return 0;
        }
        case NANOARROW_TYPE_UINT8: {
            uint8_t *out = (uint8_t *)duckdb_vector_get_data(vec);
            for (i = 0; i < count; i++) {
                if (ArrowArrayViewIsNull(col_view, src_offset + i)) set_null(vec, i);
                else out[i] = (uint8_t)ArrowArrayViewGetUIntUnsafe(col_view, src_offset + i);
            }
            return 0;
        }
        case NANOARROW_TYPE_UINT16: {
            uint16_t *out = (uint16_t *)duckdb_vector_get_data(vec);
            for (i = 0; i < count; i++) {
                if (ArrowArrayViewIsNull(col_view, src_offset + i)) set_null(vec, i);
                else out[i] = (uint16_t)ArrowArrayViewGetUIntUnsafe(col_view, src_offset + i);
            }
            return 0;
        }
        case NANOARROW_TYPE_UINT32: {
            uint32_t *out = (uint32_t *)duckdb_vector_get_data(vec);
            for (i = 0; i < count; i++) {
                if (ArrowArrayViewIsNull(col_view, src_offset + i)) set_null(vec, i);
                else out[i] = (uint32_t)ArrowArrayViewGetUIntUnsafe(col_view, src_offset + i);
            }
            return 0;
        }
        case NANOARROW_TYPE_UINT64: {
            uint64_t *out = (uint64_t *)duckdb_vector_get_data(vec);
            for (i = 0; i < count; i++) {
                if (ArrowArrayViewIsNull(col_view, src_offset + i)) set_null(vec, i);
                else out[i] = (uint64_t)ArrowArrayViewGetUIntUnsafe(col_view, src_offset + i);
            }
            return 0;
        }
        case NANOARROW_TYPE_FLOAT: {
            float *out = (float *)duckdb_vector_get_data(vec);
            for (i = 0; i < count; i++) {
                if (ArrowArrayViewIsNull(col_view, src_offset + i)) set_null(vec, i);
                else out[i] = (float)ArrowArrayViewGetDoubleUnsafe(col_view, src_offset + i);
            }
            return 0;
        }
        case NANOARROW_TYPE_DOUBLE: {
            double *out = (double *)duckdb_vector_get_data(vec);
            for (i = 0; i < count; i++) {
                if (ArrowArrayViewIsNull(col_view, src_offset + i)) set_null(vec, i);
                else out[i] = ArrowArrayViewGetDoubleUnsafe(col_view, src_offset + i);
            }
            return 0;
        }
        case NANOARROW_TYPE_STRING: {
            for (i = 0; i < count; i++) {
                struct ArrowStringView value;
                if (ArrowArrayViewIsNull(col_view, src_offset + i)) set_null(vec, i);
                else {
                    value = ArrowArrayViewGetStringUnsafe(col_view, src_offset + i);
                    duckdb_vector_assign_string_element_len(vec, i, value.data, (idx_t)value.size_bytes);
                }
            }
            return 0;
        }
        case NANOARROW_TYPE_BINARY: {
            for (i = 0; i < count; i++) {
                struct ArrowBufferView value;
                if (ArrowArrayViewIsNull(col_view, src_offset + i)) set_null(vec, i);
                else {
                    value = ArrowArrayViewGetBytesUnsafe(col_view, src_offset + i);
                    assign_blob(vec, i, value.data.data, (idx_t)value.size_bytes);
                }
            }
            return 0;
        }
        default:
            if (errmsg) *errmsg = ducknng_strdup("ducknng: unsupported Arrow type in remote row reply");
            return -1;
    }
}

static void ducknng_remote_bind(duckdb_bind_info info) {
    ducknng_remote_bind_data *bind;
    duckdb_value url_val;
    duckdb_value sql_val;
    char *url;
    char *sql;
    char *errmsg = NULL;
    nng_msg *resp_msg;
    ducknng_frame frame;
    idx_t col;
    if (duckdb_bind_get_parameter_count(info) != 2) {
        duckdb_bind_set_error(info, "ducknng: ducknng_remote(url, sql) requires exactly two parameters");
        return;
    }
    url_val = duckdb_bind_get_parameter(info, 0);
    sql_val = duckdb_bind_get_parameter(info, 1);
    url = duckdb_get_varchar(url_val);
    sql = duckdb_get_varchar(sql_val);
    duckdb_destroy_value(&url_val);
    duckdb_destroy_value(&sql_val);
    if (!url || !sql || !url[0] || !sql[0]) {
        if (url) duckdb_free(url);
        if (sql) duckdb_free(sql);
        duckdb_bind_set_error(info, "ducknng: ducknng_remote(url, sql) requires non-empty url and sql");
        return;
    }
    resp_msg = ducknng_client_roundtrip(url, ducknng_client_exec_request(sql, 1, &errmsg), 5000, &errmsg);
    duckdb_free(url);
    duckdb_free(sql);
    if (!resp_msg) {
        duckdb_bind_set_error(info, errmsg ? errmsg : "ducknng: remote request failed");
        if (errmsg) duckdb_free(errmsg);
        return;
    }
    if (ducknng_decode_request(resp_msg, &frame) != 0) {
        nng_msg_free(resp_msg);
        duckdb_bind_set_error(info, "ducknng: invalid remote response envelope");
        return;
    }
    if (frame.type == DUCKNNG_RPC_ERROR) {
        char *detail = ducknng_frame_error_detail(&frame, "ducknng: remote request failed");
        nng_msg_free(resp_msg);
        duckdb_bind_set_error(info, detail ? detail : "ducknng: remote request failed");
        if (detail) duckdb_free(detail);
        return;
    }
    if (frame.type != DUCKNNG_RPC_RESULT || !(frame.flags & DUCKNNG_RPC_FLAG_PAYLOAD_ARROW_STREAM) ||
        !(frame.flags & DUCKNNG_RPC_FLAG_RESULT_ROWS)) {
        nng_msg_free(resp_msg);
        duckdb_bind_set_error(info, "ducknng: remote query did not return row payloads");
        return;
    }
    bind = (ducknng_remote_bind_data *)duckdb_malloc(sizeof(*bind));
    if (!bind) {
        nng_msg_free(resp_msg);
        duckdb_bind_set_error(info, "ducknng: out of memory");
        return;
    }
    memset(bind, 0, sizeof(*bind));
    if (ducknng_decode_ipc_table_payload(frame.payload, (size_t)frame.payload_len, &bind->schema, &bind->array, &errmsg) != 0) {
        nng_msg_free(resp_msg);
        duckdb_free(bind);
        duckdb_bind_set_error(info, errmsg ? errmsg : "ducknng: failed to decode remote Arrow row payload");
        if (errmsg) duckdb_free(errmsg);
        return;
    }
    nng_msg_free(resp_msg);
    bind->row_count = (idx_t)bind->array.length;
    if (bind->schema.n_children < 0 || bind->schema.n_children != bind->array.n_children) {
        destroy_remote_bind_data(bind);
        duckdb_bind_set_error(info, "ducknng: invalid remote Arrow row schema");
        return;
    }
    for (col = 0; col < (idx_t)bind->schema.n_children; col++) {
        duckdb_logical_type type;
        const char *name = bind->schema.children[col] && bind->schema.children[col]->name ? bind->schema.children[col]->name : "";
        if (ducknng_arrow_schema_to_logical_type(bind->schema.children[col], &type, &errmsg) != 0) {
            destroy_remote_bind_data(bind);
            duckdb_bind_set_error(info, errmsg ? errmsg : "ducknng: unsupported remote Arrow type");
            if (errmsg) duckdb_free(errmsg);
            return;
        }
        duckdb_bind_add_result_column(info, name, type);
        duckdb_destroy_logical_type(&type);
    }
    duckdb_bind_set_bind_data(info, bind, destroy_remote_bind_data);
    duckdb_bind_set_cardinality(info, bind->row_count, true);
}

static void ducknng_remote_init(duckdb_init_info info) {
    ducknng_remote_bind_data *bind = (ducknng_remote_bind_data *)duckdb_init_get_bind_data(info);
    ducknng_remote_init_data *init = (ducknng_remote_init_data *)duckdb_malloc(sizeof(*init));
    if (!init) {
        duckdb_init_set_error(info, "ducknng: out of memory");
        return;
    }
    init->bind = bind;
    init->offset = 0;
    duckdb_init_set_max_threads(info, 1);
    duckdb_init_set_init_data(info, init, destroy_remote_init_data);
}

static void ducknng_remote_scan(duckdb_function_info info, duckdb_data_chunk output) {
    ducknng_remote_init_data *init = (ducknng_remote_init_data *)duckdb_function_get_init_data(info);
    ducknng_remote_bind_data *bind;
    struct ArrowArrayView view;
    struct ArrowError error;
    idx_t remaining;
    idx_t chunk_size;
    idx_t col;
    if (!init || !init->bind || init->offset >= init->bind->row_count) {
        duckdb_data_chunk_set_size(output, 0);
        return;
    }
    bind = init->bind;
    memset(&view, 0, sizeof(view));
    memset(&error, 0, sizeof(error));
    if (ArrowArrayViewInitFromSchema(&view, &bind->schema, &error) != NANOARROW_OK ||
        ArrowArrayViewSetArray(&view, &bind->array, &error) != NANOARROW_OK) {
        duckdb_function_set_error(info, error.message[0] ? error.message : "ducknng: failed to view remote Arrow row payload");
        ArrowArrayViewReset(&view);
        return;
    }
    remaining = bind->row_count - init->offset;
    chunk_size = remaining > duckdb_vector_size() ? duckdb_vector_size() : remaining;
    for (col = 0; col < (idx_t)bind->schema.n_children; col++) {
        duckdb_vector vec = duckdb_data_chunk_get_vector(output, col);
        if (ducknng_remote_assign_column(vec, view.children[col], bind->schema.children[col], init->offset, chunk_size, NULL) != 0) {
            duckdb_function_set_error(info, "ducknng: failed to decode remote Arrow row payload");
            ArrowArrayViewReset(&view);
            return;
        }
    }
    init->offset += chunk_size;
    duckdb_data_chunk_set_size(output, chunk_size);
    ArrowArrayViewReset(&view);
}

static int register_scalar(duckdb_connection con, const char *name, idx_t nparams,
    duckdb_scalar_function_t fn, ducknng_sql_context *ctx, duckdb_type *param_types,
    duckdb_type return_type_id) {
    duckdb_scalar_function f = duckdb_create_scalar_function();
    idx_t i;
    duckdb_logical_type ret_type;
    if (!f) return 0;
    duckdb_scalar_function_set_name(f, name);
    for (i = 0; i < nparams; i++) {
        duckdb_logical_type t = duckdb_create_logical_type(param_types[i]);
        duckdb_scalar_function_add_parameter(f, t);
        duckdb_destroy_logical_type(&t);
    }
    ret_type = duckdb_create_logical_type(return_type_id);
    duckdb_scalar_function_set_return_type(f, ret_type);
    duckdb_destroy_logical_type(&ret_type);
    duckdb_scalar_function_set_function(f, fn);
    duckdb_scalar_function_set_special_handling(f);
    duckdb_scalar_function_set_extra_info(f, ctx, NULL);
    if (duckdb_register_scalar_function(con, f) == DuckDBError) { duckdb_destroy_scalar_function(&f); return 0; }
    duckdb_destroy_scalar_function(&f);
    return 1;
}

static void ducknng_servers_bind(duckdb_bind_info info) {
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_bind_get_extra_info(info);
    ducknng_servers_bind_data *bind;
    duckdb_logical_type type;
    size_t i;
    if (!ctx || !ctx->rt) {
        duckdb_bind_set_error(info, "ducknng: missing runtime");
        return;
    }
    bind = (ducknng_servers_bind_data *)duckdb_malloc(sizeof(*bind));
    if (!bind) {
        duckdb_bind_set_error(info, "ducknng: out of memory");
        return;
    }
    memset(bind, 0, sizeof(*bind));

    ducknng_mutex_lock(&ctx->rt->mu);
    bind->row_count = (idx_t)ctx->rt->service_count;
    if (bind->row_count > 0) {
        bind->rows = (ducknng_server_row *)duckdb_malloc(sizeof(*bind->rows) * (size_t)bind->row_count);
        if (!bind->rows) {
            ducknng_mutex_unlock(&ctx->rt->mu);
            duckdb_free(bind);
            duckdb_bind_set_error(info, "ducknng: out of memory");
            return;
        }
        memset(bind->rows, 0, sizeof(*bind->rows) * (size_t)bind->row_count);
        for (i = 0; i < (size_t)bind->row_count; i++) {
            ducknng_service *svc = ctx->rt->services[i];
            bind->rows[i].service_id = svc ? svc->service_id : 0;
            bind->rows[i].name = svc && svc->name ? ducknng_strdup(svc->name) : NULL;
            bind->rows[i].listen = svc && ducknng_service_resolved_listen(svc) ? ducknng_strdup(ducknng_service_resolved_listen(svc)) : NULL;
            bind->rows[i].contexts = svc ? svc->ncontexts : 0;
            bind->rows[i].running = svc ? (bool)svc->running : false;
            bind->rows[i].sessions = svc ? (uint64_t)svc->session_count : 0;
        }
    }
    ducknng_mutex_unlock(&ctx->rt->mu);

    type = duckdb_create_logical_type(DUCKDB_TYPE_UBIGINT);
    duckdb_bind_add_result_column(info, "service_id", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
    duckdb_bind_add_result_column(info, "name", type);
    duckdb_bind_add_result_column(info, "listen", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_INTEGER);
    duckdb_bind_add_result_column(info, "contexts", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_BOOLEAN);
    duckdb_bind_add_result_column(info, "running", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_UBIGINT);
    duckdb_bind_add_result_column(info, "sessions", type);
    duckdb_destroy_logical_type(&type);

    duckdb_bind_set_bind_data(info, bind, destroy_servers_bind_data);
    duckdb_bind_set_cardinality(info, bind->row_count, true);
}

static void ducknng_servers_init(duckdb_init_info info) {
    ducknng_servers_bind_data *bind = (ducknng_servers_bind_data *)duckdb_init_get_bind_data(info);
    ducknng_servers_init_data *init = (ducknng_servers_init_data *)duckdb_malloc(sizeof(*init));
    if (!init) {
        duckdb_init_set_error(info, "ducknng: out of memory");
        return;
    }
    init->bind = bind;
    init->offset = 0;
    duckdb_init_set_max_threads(info, 1);
    duckdb_init_set_init_data(info, init, destroy_servers_init_data);
}

static void ducknng_servers_scan(duckdb_function_info info, duckdb_data_chunk output) {
    ducknng_servers_init_data *init = (ducknng_servers_init_data *)duckdb_function_get_init_data(info);
    ducknng_servers_bind_data *bind;
    idx_t remaining;
    idx_t chunk_size;
    idx_t i;
    duckdb_vector vec_service_id;
    duckdb_vector vec_name;
    duckdb_vector vec_listen;
    duckdb_vector vec_contexts;
    duckdb_vector vec_running;
    duckdb_vector vec_sessions;
    uint64_t *service_ids;
    int32_t *contexts;
    bool *running;
    uint64_t *sessions;
    if (!init || !init->bind || init->offset >= init->bind->row_count) {
        duckdb_data_chunk_set_size(output, 0);
        return;
    }
    bind = init->bind;
    remaining = bind->row_count - init->offset;
    chunk_size = remaining > duckdb_vector_size() ? duckdb_vector_size() : remaining;

    vec_service_id = duckdb_data_chunk_get_vector(output, 0);
    vec_name = duckdb_data_chunk_get_vector(output, 1);
    vec_listen = duckdb_data_chunk_get_vector(output, 2);
    vec_contexts = duckdb_data_chunk_get_vector(output, 3);
    vec_running = duckdb_data_chunk_get_vector(output, 4);
    vec_sessions = duckdb_data_chunk_get_vector(output, 5);

    service_ids = (uint64_t *)duckdb_vector_get_data(vec_service_id);
    contexts = (int32_t *)duckdb_vector_get_data(vec_contexts);
    running = (bool *)duckdb_vector_get_data(vec_running);
    sessions = (uint64_t *)duckdb_vector_get_data(vec_sessions);

    for (i = 0; i < chunk_size; i++) {
        ducknng_server_row *row = &bind->rows[init->offset + i];
        service_ids[i] = row->service_id;
        contexts[i] = row->contexts;
        running[i] = row->running;
        sessions[i] = row->sessions;
        if (row->name) duckdb_vector_assign_string_element(vec_name, i, row->name);
        else set_null(vec_name, i);
        if (row->listen) duckdb_vector_assign_string_element(vec_listen, i, row->listen);
        else set_null(vec_listen, i);
    }
    init->offset += chunk_size;
    duckdb_data_chunk_set_size(output, chunk_size);
}

static void ducknng_sockets_bind(duckdb_bind_info info) {
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_bind_get_extra_info(info);
    ducknng_sockets_bind_data *bind;
    duckdb_logical_type type;
    size_t i;
    if (!ctx || !ctx->rt) {
        duckdb_bind_set_error(info, "ducknng: missing runtime");
        return;
    }
    bind = (ducknng_sockets_bind_data *)duckdb_malloc(sizeof(*bind));
    if (!bind) {
        duckdb_bind_set_error(info, "ducknng: out of memory");
        return;
    }
    memset(bind, 0, sizeof(*bind));
    ducknng_mutex_lock(&ctx->rt->mu);
    bind->row_count = (idx_t)ctx->rt->client_socket_count;
    if (bind->row_count > 0) {
        bind->rows = (ducknng_socket_row *)duckdb_malloc(sizeof(*bind->rows) * (size_t)bind->row_count);
        if (!bind->rows) {
            ducknng_mutex_unlock(&ctx->rt->mu);
            duckdb_free(bind);
            duckdb_bind_set_error(info, "ducknng: out of memory");
            return;
        }
        memset(bind->rows, 0, sizeof(*bind->rows) * (size_t)bind->row_count);
        for (i = 0; i < (size_t)bind->row_count; i++) {
            ducknng_client_socket *sock = ctx->rt->client_sockets[i];
            bind->rows[i].socket_id = sock ? sock->socket_id : 0;
            bind->rows[i].protocol = sock && sock->protocol ? ducknng_strdup(sock->protocol) : NULL;
            bind->rows[i].url = sock && sock->url ? ducknng_strdup(sock->url) : NULL;
            bind->rows[i].open = sock ? (bool)sock->open : false;
            bind->rows[i].connected = sock ? (bool)sock->connected : false;
            bind->rows[i].send_timeout_ms = sock ? sock->send_timeout_ms : 0;
            bind->rows[i].recv_timeout_ms = sock ? sock->recv_timeout_ms : 0;
        }
    }
    ducknng_mutex_unlock(&ctx->rt->mu);
    type = duckdb_create_logical_type(DUCKDB_TYPE_UBIGINT);
    duckdb_bind_add_result_column(info, "socket_id", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
    duckdb_bind_add_result_column(info, "protocol", type);
    duckdb_bind_add_result_column(info, "url", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_BOOLEAN);
    duckdb_bind_add_result_column(info, "open", type);
    duckdb_bind_add_result_column(info, "connected", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_INTEGER);
    duckdb_bind_add_result_column(info, "send_timeout_ms", type);
    duckdb_bind_add_result_column(info, "recv_timeout_ms", type);
    duckdb_destroy_logical_type(&type);
    duckdb_bind_set_bind_data(info, bind, destroy_sockets_bind_data);
    duckdb_bind_set_cardinality(info, bind->row_count, true);
}

static void ducknng_sockets_init(duckdb_init_info info) {
    ducknng_sockets_bind_data *bind = (ducknng_sockets_bind_data *)duckdb_init_get_bind_data(info);
    ducknng_sockets_init_data *init = (ducknng_sockets_init_data *)duckdb_malloc(sizeof(*init));
    if (!init) {
        duckdb_init_set_error(info, "ducknng: out of memory");
        return;
    }
    init->bind = bind;
    init->offset = 0;
    duckdb_init_set_max_threads(info, 1);
    duckdb_init_set_init_data(info, init, destroy_sockets_init_data);
}

static void ducknng_sockets_scan(duckdb_function_info info, duckdb_data_chunk output) {
    ducknng_sockets_init_data *init = (ducknng_sockets_init_data *)duckdb_function_get_init_data(info);
    ducknng_sockets_bind_data *bind;
    idx_t remaining;
    idx_t chunk_size;
    idx_t i;
    duckdb_vector vec_socket_id;
    duckdb_vector vec_protocol;
    duckdb_vector vec_url;
    duckdb_vector vec_open;
    duckdb_vector vec_connected;
    duckdb_vector vec_send_timeout_ms;
    duckdb_vector vec_recv_timeout_ms;
    uint64_t *socket_ids;
    bool *open;
    bool *connected;
    int32_t *send_timeout_ms;
    int32_t *recv_timeout_ms;
    if (!init || !init->bind || init->offset >= init->bind->row_count) {
        duckdb_data_chunk_set_size(output, 0);
        return;
    }
    bind = init->bind;
    remaining = bind->row_count - init->offset;
    chunk_size = remaining > duckdb_vector_size() ? duckdb_vector_size() : remaining;
    vec_socket_id = duckdb_data_chunk_get_vector(output, 0);
    vec_protocol = duckdb_data_chunk_get_vector(output, 1);
    vec_url = duckdb_data_chunk_get_vector(output, 2);
    vec_open = duckdb_data_chunk_get_vector(output, 3);
    vec_connected = duckdb_data_chunk_get_vector(output, 4);
    vec_send_timeout_ms = duckdb_data_chunk_get_vector(output, 5);
    vec_recv_timeout_ms = duckdb_data_chunk_get_vector(output, 6);
    socket_ids = (uint64_t *)duckdb_vector_get_data(vec_socket_id);
    open = (bool *)duckdb_vector_get_data(vec_open);
    connected = (bool *)duckdb_vector_get_data(vec_connected);
    send_timeout_ms = (int32_t *)duckdb_vector_get_data(vec_send_timeout_ms);
    recv_timeout_ms = (int32_t *)duckdb_vector_get_data(vec_recv_timeout_ms);
    for (i = 0; i < chunk_size; i++) {
        ducknng_socket_row *row = &bind->rows[init->offset + i];
        socket_ids[i] = row->socket_id;
        open[i] = row->open;
        connected[i] = row->connected;
        send_timeout_ms[i] = row->send_timeout_ms;
        recv_timeout_ms[i] = row->recv_timeout_ms;
        if (row->protocol) duckdb_vector_assign_string_element(vec_protocol, i, row->protocol);
        else set_null(vec_protocol, i);
        if (row->url) duckdb_vector_assign_string_element(vec_url, i, row->url);
        else set_null(vec_url, i);
    }
    init->offset += chunk_size;
    duckdb_data_chunk_set_size(output, chunk_size);
}

static int register_servers_table(duckdb_connection con, ducknng_sql_context *ctx) {
    duckdb_table_function tf;
    if (!ctx || !ctx->rt) return 0;
    tf = duckdb_create_table_function();
    if (!tf) return 0;
    duckdb_table_function_set_name(tf, "ducknng_servers");
    duckdb_table_function_set_extra_info(tf, ctx, NULL);
    duckdb_table_function_set_bind(tf, ducknng_servers_bind);
    duckdb_table_function_set_init(tf, ducknng_servers_init);
    duckdb_table_function_set_function(tf, ducknng_servers_scan);
    if (duckdb_register_table_function(con, tf) == DuckDBError) {
        duckdb_destroy_table_function(&tf);
        return 0;
    }
    duckdb_destroy_table_function(&tf);
    return 1;
}

static int register_sockets_table(duckdb_connection con, ducknng_sql_context *ctx) {
    duckdb_table_function tf;
    if (!ctx || !ctx->rt) return 0;
    tf = duckdb_create_table_function();
    if (!tf) return 0;
    duckdb_table_function_set_name(tf, "ducknng_sockets");
    duckdb_table_function_set_extra_info(tf, ctx, NULL);
    duckdb_table_function_set_bind(tf, ducknng_sockets_bind);
    duckdb_table_function_set_init(tf, ducknng_sockets_init);
    duckdb_table_function_set_function(tf, ducknng_sockets_scan);
    if (duckdb_register_table_function(con, tf) == DuckDBError) {
        duckdb_destroy_table_function(&tf);
        return 0;
    }
    duckdb_destroy_table_function(&tf);
    return 1;
}

static int register_remote_table(duckdb_connection con, ducknng_sql_context *ctx) {
    duckdb_table_function tf;
    duckdb_logical_type type;
    if (!ctx || !ctx->rt) return 0;
    tf = duckdb_create_table_function();
    if (!tf) return 0;
    duckdb_table_function_set_name(tf, "ducknng_remote");
    type = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
    duckdb_table_function_add_parameter(tf, type);
    duckdb_table_function_add_parameter(tf, type);
    duckdb_destroy_logical_type(&type);
    duckdb_table_function_set_extra_info(tf, ctx, NULL);
    duckdb_table_function_set_bind(tf, ducknng_remote_bind);
    duckdb_table_function_set_init(tf, ducknng_remote_init);
    duckdb_table_function_set_function(tf, ducknng_remote_scan);
    if (duckdb_register_table_function(con, tf) == DuckDBError) {
        duckdb_destroy_table_function(&tf);
        return 0;
    }
    duckdb_destroy_table_function(&tf);
    return 1;
}

int ducknng_register_sql_api(duckdb_connection connection, ducknng_runtime *rt) {
    static ducknng_sql_context ctx;
    duckdb_type start_types[8] = {DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_INTEGER, DUCKDB_TYPE_UBIGINT,
        DUCKDB_TYPE_UBIGINT, DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_INTEGER};
    duckdb_type stop_types[1] = {DUCKDB_TYPE_VARCHAR};
    duckdb_type remote_exec_types[2] = {DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_VARCHAR};
    duckdb_type remote_manifest_types[1] = {DUCKDB_TYPE_VARCHAR};
    duckdb_type socket_types[1] = {DUCKDB_TYPE_VARCHAR};
    duckdb_type dial_types[3] = {DUCKDB_TYPE_UBIGINT, DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_INTEGER};
    duckdb_type close_types[1] = {DUCKDB_TYPE_UBIGINT};
    duckdb_type request_socket_types[3] = {DUCKDB_TYPE_UBIGINT, DUCKDB_TYPE_BLOB, DUCKDB_TYPE_INTEGER};
    duckdb_type request_types[3] = {DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_BLOB, DUCKDB_TYPE_INTEGER};
    ctx.rt = rt;
    if (!register_scalar(connection, "ducknng_server_start", 8, ducknng_server_start_scalar, &ctx, start_types, DUCKDB_TYPE_BOOLEAN)) return 0;
    if (!register_scalar(connection, "ducknng_server_stop", 1, ducknng_server_stop_scalar, &ctx, stop_types, DUCKDB_TYPE_BOOLEAN)) return 0;
    if (!register_scalar(connection, "ducknng_remote_exec", 2, ducknng_remote_exec_scalar, &ctx, remote_exec_types, DUCKDB_TYPE_UBIGINT)) return 0;
    if (!register_scalar(connection, "ducknng_remote_manifest", 1, ducknng_remote_manifest_scalar, &ctx, remote_manifest_types, DUCKDB_TYPE_VARCHAR)) return 0;
    if (!register_scalar(connection, "ducknng_socket", 1, ducknng_socket_scalar, &ctx, socket_types, DUCKDB_TYPE_UBIGINT)) return 0;
    if (!register_scalar(connection, "ducknng_dial", 3, ducknng_dial_scalar, &ctx, dial_types, DUCKDB_TYPE_BOOLEAN)) return 0;
    if (!register_scalar(connection, "ducknng_close", 1, ducknng_close_scalar, &ctx, close_types, DUCKDB_TYPE_BOOLEAN)) return 0;
    if (!register_scalar(connection, "ducknng_request_socket", 3, ducknng_request_socket_scalar, &ctx, request_socket_types, DUCKDB_TYPE_BLOB)) return 0;
    if (!register_scalar(connection, "ducknng_request", 3, ducknng_request_scalar, &ctx, request_types, DUCKDB_TYPE_BLOB)) return 0;
    if (!register_servers_table(connection, &ctx)) return 0;
    if (!register_sockets_table(connection, &ctx)) return 0;
    if (!register_remote_table(connection, &ctx)) return 0;
    return 1;
}
