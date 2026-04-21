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

static nng_msg *ducknng_client_roundtrip(const char *url, nng_msg *req, char **errmsg) {
    nng_socket sock;
    nng_msg *resp = NULL;
    int rv;
    memset(&sock, 0, sizeof(sock));
    if (!url || !url[0] || !req) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: client URL and request are required");
        if (req) nng_msg_free(req);
        return NULL;
    }
    rv = ducknng_req_socket_open(&sock);
    if (rv != 0) {
        if (errmsg) *errmsg = ducknng_strdup(ducknng_nng_strerror(rv));
        nng_msg_free(req);
        return NULL;
    }
    rv = ducknng_req_dial(sock, url, 5000);
    if (rv != 0) {
        if (errmsg) *errmsg = ducknng_strdup(ducknng_nng_strerror(rv));
        ducknng_socket_close(sock);
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
        resp_msg = ducknng_client_roundtrip(url, ducknng_client_manifest_request(), &errmsg);
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
            char *detail = (char *)duckdb_malloc((size_t)frame.error_len + 1);
            if (!detail) {
                nng_msg_free(resp_msg);
                duckdb_scalar_function_set_error(info, "ducknng: out of memory decoding manifest error");
                return;
            }
            memcpy(detail, frame.error, (size_t)frame.error_len);
            detail[frame.error_len] = '\0';
            nng_msg_free(resp_msg);
            duckdb_scalar_function_set_error(info, detail);
            duckdb_free(detail);
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
        resp_msg = ducknng_client_roundtrip(url, ducknng_client_exec_request(sql, 0, &errmsg), &errmsg);
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
            char *detail = (char *)duckdb_malloc((size_t)frame.error_len + 1);
            if (!detail) {
                nng_msg_free(resp_msg);
                duckdb_scalar_function_set_error(info, "ducknng: out of memory decoding remote exec error");
                return;
            }
            memcpy(detail, frame.error, (size_t)frame.error_len);
            detail[frame.error_len] = '\0';
            nng_msg_free(resp_msg);
            duckdb_scalar_function_set_error(info, detail);
            duckdb_free(detail);
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

int ducknng_register_sql_api(duckdb_connection connection, ducknng_runtime *rt) {
    static ducknng_sql_context ctx;
    duckdb_type start_types[8] = {DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_INTEGER, DUCKDB_TYPE_UBIGINT,
        DUCKDB_TYPE_UBIGINT, DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_INTEGER};
    duckdb_type stop_types[1] = {DUCKDB_TYPE_VARCHAR};
    duckdb_type remote_exec_types[2] = {DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_VARCHAR};
    duckdb_type remote_manifest_types[1] = {DUCKDB_TYPE_VARCHAR};
    ctx.rt = rt;
    if (!register_scalar(connection, "ducknng_server_start", 8, ducknng_server_start_scalar, &ctx, start_types, DUCKDB_TYPE_BOOLEAN)) return 0;
    if (!register_scalar(connection, "ducknng_server_stop", 1, ducknng_server_stop_scalar, &ctx, stop_types, DUCKDB_TYPE_BOOLEAN)) return 0;
    if (!register_scalar(connection, "ducknng_remote_exec", 2, ducknng_remote_exec_scalar, &ctx, remote_exec_types, DUCKDB_TYPE_UBIGINT)) return 0;
    if (!register_scalar(connection, "ducknng_remote_manifest", 1, ducknng_remote_manifest_scalar, &ctx, remote_manifest_types, DUCKDB_TYPE_VARCHAR)) return 0;
    if (!register_servers_table(connection, &ctx)) return 0;
    return 1;
}
