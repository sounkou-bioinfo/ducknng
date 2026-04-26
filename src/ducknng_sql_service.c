#include "ducknng_sql_shared.h"
#include "ducknng_service.h"
#include "ducknng_util.h"
#include <stdatomic.h>
#include <string.h>

DUCKDB_EXTENSION_EXTERN

typedef struct {
    uint64_t service_id;
    char *name;
    char *listen;
    int32_t contexts;
    bool running;
    uint64_t sessions;
    uint64_t active_pipes;
    uint64_t max_open_sessions;
    uint64_t max_active_pipes;
    bool tls_enabled;
    int32_t tls_auth_mode;
    bool peer_identity_required;
    bool peer_allowlist_active;
    uint64_t peer_allowlist_count;
    bool ip_allowlist_active;
    uint64_t ip_allowlist_count;
    bool sql_authorizer_active;
} ducknng_server_row;

typedef struct {
    ducknng_server_row *rows;
    idx_t row_count;
} ducknng_servers_bind_data;

typedef struct {
    ducknng_servers_bind_data *bind;
    idx_t offset;
} ducknng_servers_init_data;

static void set_null(duckdb_vector vec, idx_t row) {
    uint64_t *validity;
    duckdb_vector_ensure_validity_writable(vec);
    validity = duckdb_vector_get_validity(vec);
    duckdb_validity_set_row_invalid(validity, row);
}

static void destroy_sql_context_extra(void *data) {
    if (data) duckdb_free(data);
}

static ducknng_sql_context *ducknng_dup_sql_context(const ducknng_sql_context *ctx) {
    ducknng_sql_context *copy;
    if (!ctx) return NULL;
    copy = (ducknng_sql_context *)duckdb_malloc(sizeof(*copy));
    if (!copy) return NULL;
    *copy = *ctx;
    return copy;
}

static int ducknng_set_table_sql_context(duckdb_table_function tf, const ducknng_sql_context *ctx) {
    ducknng_sql_context *copy = ducknng_dup_sql_context(ctx);
    if (!copy) return 0;
    duckdb_table_function_set_extra_info(tf, copy, destroy_sql_context_extra);
    return 1;
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
            bind->rows[i].sessions = svc ? (uint64_t)atomic_load_explicit(&svc->session_count_visible, memory_order_acquire) : 0;
            bind->rows[i].active_pipes = svc ? (uint64_t)ducknng_service_active_pipe_count(svc) : 0;
            bind->rows[i].max_open_sessions = svc ? ducknng_service_max_open_sessions(svc) : 0;
            bind->rows[i].max_active_pipes = svc ? ducknng_service_max_active_pipes(svc) : 0;
            bind->rows[i].tls_enabled = svc ? (bool)svc->tls_enabled : false;
            bind->rows[i].tls_auth_mode = svc ? svc->tls_opts.auth_mode : 0;
            bind->rows[i].peer_identity_required = svc ? (bool)ducknng_service_requires_peer_identity(svc) : false;
            bind->rows[i].peer_allowlist_active = svc ? (bool)ducknng_service_peer_allowlist_active(svc) : false;
            bind->rows[i].peer_allowlist_count = svc ? (uint64_t)ducknng_service_peer_allowlist_count(svc) : 0;
            bind->rows[i].ip_allowlist_active = svc ? (bool)ducknng_service_ip_allowlist_active(svc) : false;
            bind->rows[i].ip_allowlist_count = svc ? (uint64_t)ducknng_service_ip_allowlist_count(svc) : 0;
            bind->rows[i].sql_authorizer_active = svc ? (bool)ducknng_service_authorizer_active(svc) : false;
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
    duckdb_bind_add_result_column(info, "active_pipes", type);
    duckdb_bind_add_result_column(info, "max_open_sessions", type);
    duckdb_bind_add_result_column(info, "max_active_pipes", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_BOOLEAN);
    duckdb_bind_add_result_column(info, "tls_enabled", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_INTEGER);
    duckdb_bind_add_result_column(info, "tls_auth_mode", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_BOOLEAN);
    duckdb_bind_add_result_column(info, "peer_identity_required", type);
    duckdb_bind_add_result_column(info, "peer_allowlist_active", type);
    duckdb_bind_add_result_column(info, "ip_allowlist_active", type);
    duckdb_bind_add_result_column(info, "sql_authorizer_active", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_UBIGINT);
    duckdb_bind_add_result_column(info, "peer_allowlist_count", type);
    duckdb_bind_add_result_column(info, "ip_allowlist_count", type);
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
    duckdb_vector vec_active_pipes;
    duckdb_vector vec_max_open_sessions;
    duckdb_vector vec_max_active_pipes;
    duckdb_vector vec_tls_enabled;
    duckdb_vector vec_tls_auth_mode;
    duckdb_vector vec_peer_identity_required;
    duckdb_vector vec_peer_allowlist_active;
    duckdb_vector vec_ip_allowlist_active;
    duckdb_vector vec_sql_authorizer_active;
    duckdb_vector vec_peer_allowlist_count;
    duckdb_vector vec_ip_allowlist_count;
    uint64_t *service_ids;
    int32_t *contexts;
    bool *running;
    uint64_t *sessions;
    uint64_t *active_pipes;
    uint64_t *max_open_sessions;
    uint64_t *max_active_pipes;
    bool *tls_enabled;
    int32_t *tls_auth_mode;
    bool *peer_identity_required;
    bool *peer_allowlist_active;
    bool *ip_allowlist_active;
    bool *sql_authorizer_active;
    uint64_t *peer_allowlist_count;
    uint64_t *ip_allowlist_count;
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
    vec_active_pipes = duckdb_data_chunk_get_vector(output, 6);
    vec_max_open_sessions = duckdb_data_chunk_get_vector(output, 7);
    vec_max_active_pipes = duckdb_data_chunk_get_vector(output, 8);
    vec_tls_enabled = duckdb_data_chunk_get_vector(output, 9);
    vec_tls_auth_mode = duckdb_data_chunk_get_vector(output, 10);
    vec_peer_identity_required = duckdb_data_chunk_get_vector(output, 11);
    vec_peer_allowlist_active = duckdb_data_chunk_get_vector(output, 12);
    vec_ip_allowlist_active = duckdb_data_chunk_get_vector(output, 13);
    vec_sql_authorizer_active = duckdb_data_chunk_get_vector(output, 14);
    vec_peer_allowlist_count = duckdb_data_chunk_get_vector(output, 15);
    vec_ip_allowlist_count = duckdb_data_chunk_get_vector(output, 16);

    service_ids = (uint64_t *)duckdb_vector_get_data(vec_service_id);
    contexts = (int32_t *)duckdb_vector_get_data(vec_contexts);
    running = (bool *)duckdb_vector_get_data(vec_running);
    sessions = (uint64_t *)duckdb_vector_get_data(vec_sessions);
    active_pipes = (uint64_t *)duckdb_vector_get_data(vec_active_pipes);
    max_open_sessions = (uint64_t *)duckdb_vector_get_data(vec_max_open_sessions);
    max_active_pipes = (uint64_t *)duckdb_vector_get_data(vec_max_active_pipes);
    tls_enabled = (bool *)duckdb_vector_get_data(vec_tls_enabled);
    tls_auth_mode = (int32_t *)duckdb_vector_get_data(vec_tls_auth_mode);
    peer_identity_required = (bool *)duckdb_vector_get_data(vec_peer_identity_required);
    peer_allowlist_active = (bool *)duckdb_vector_get_data(vec_peer_allowlist_active);
    ip_allowlist_active = (bool *)duckdb_vector_get_data(vec_ip_allowlist_active);
    sql_authorizer_active = (bool *)duckdb_vector_get_data(vec_sql_authorizer_active);
    peer_allowlist_count = (uint64_t *)duckdb_vector_get_data(vec_peer_allowlist_count);
    ip_allowlist_count = (uint64_t *)duckdb_vector_get_data(vec_ip_allowlist_count);

    for (i = 0; i < chunk_size; i++) {
        ducknng_server_row *row = &bind->rows[init->offset + i];
        service_ids[i] = row->service_id;
        contexts[i] = row->contexts;
        running[i] = row->running;
        sessions[i] = row->sessions;
        active_pipes[i] = row->active_pipes;
        max_open_sessions[i] = row->max_open_sessions;
        max_active_pipes[i] = row->max_active_pipes;
        tls_enabled[i] = row->tls_enabled;
        tls_auth_mode[i] = row->tls_auth_mode;
        peer_identity_required[i] = row->peer_identity_required;
        peer_allowlist_active[i] = row->peer_allowlist_active;
        ip_allowlist_active[i] = row->ip_allowlist_active;
        sql_authorizer_active[i] = row->sql_authorizer_active;
        peer_allowlist_count[i] = row->peer_allowlist_count;
        ip_allowlist_count[i] = row->ip_allowlist_count;
        if (row->name) duckdb_vector_assign_string_element(vec_name, i, row->name);
        else set_null(vec_name, i);
        if (row->listen) duckdb_vector_assign_string_element(vec_listen, i, row->listen);
        else set_null(vec_listen, i);
    }
    init->offset += chunk_size;
    duckdb_data_chunk_set_size(output, chunk_size);
}

int ducknng_register_sql_service(duckdb_connection con, ducknng_sql_context *ctx) {
    duckdb_table_function tf;
    if (!ctx || !ctx->rt) return 0;
    tf = duckdb_create_table_function();
    if (!tf) return 0;
    duckdb_table_function_set_name(tf, "ducknng_list_servers");
    if (!ducknng_set_table_sql_context(tf, ctx)) { duckdb_destroy_table_function(&tf); return 0; }
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
