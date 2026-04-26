#include "ducknng_sql_shared.h"
#include "ducknng_service.h"
#include "ducknng_transport.h"
#include "ducknng_util.h"
#include <string.h>

DUCKDB_EXTENSION_EXTERN

typedef struct {
    idx_t offset;
} ducknng_monitor_init_data;

typedef struct {
    uint64_t seq;
    uint64_t ts_ms;
    uint64_t pipe_id;
    char *service_name;
    char *listen;
    char *transport_family;
    char *scheme;
    char *event;
    int admitted;
    char *remote_addr;
    char *remote_ip;
    int32_t remote_port;
    char *peer_identity;
} ducknng_monitor_row;

typedef struct {
    ducknng_monitor_row *rows;
    idx_t row_count;
} ducknng_monitor_bind_data;

typedef struct {
    uint64_t pipe_id;
    uint64_t opened_ms;
    char *service_name;
    char *listen;
    char *transport_family;
    char *scheme;
    char *remote_addr;
    char *remote_ip;
    int32_t remote_port;
    char *peer_identity;
} ducknng_pipe_row;

typedef struct {
    ducknng_pipe_row *rows;
    idx_t row_count;
} ducknng_pipes_bind_data;

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

static void ducknng_monitor_row_reset(ducknng_monitor_row *row) {
    if (!row) return;
    if (row->service_name) duckdb_free(row->service_name);
    if (row->listen) duckdb_free(row->listen);
    if (row->transport_family) duckdb_free(row->transport_family);
    if (row->scheme) duckdb_free(row->scheme);
    if (row->event) duckdb_free(row->event);
    if (row->remote_addr) duckdb_free(row->remote_addr);
    if (row->remote_ip) duckdb_free(row->remote_ip);
    if (row->peer_identity) duckdb_free(row->peer_identity);
    memset(row, 0, sizeof(*row));
}

static void destroy_monitor_bind_data(void *ptr) {
    ducknng_monitor_bind_data *data = (ducknng_monitor_bind_data *)ptr;
    idx_t i;
    if (!data) return;
    for (i = 0; i < data->row_count; i++) ducknng_monitor_row_reset(&data->rows[i]);
    if (data->rows) duckdb_free(data->rows);
    duckdb_free(data);
}

static void ducknng_pipe_row_reset(ducknng_pipe_row *row) {
    if (!row) return;
    if (row->service_name) duckdb_free(row->service_name);
    if (row->listen) duckdb_free(row->listen);
    if (row->transport_family) duckdb_free(row->transport_family);
    if (row->scheme) duckdb_free(row->scheme);
    if (row->remote_addr) duckdb_free(row->remote_addr);
    if (row->remote_ip) duckdb_free(row->remote_ip);
    if (row->peer_identity) duckdb_free(row->peer_identity);
    memset(row, 0, sizeof(*row));
}

static void destroy_pipes_bind_data(void *ptr) {
    ducknng_pipes_bind_data *data = (ducknng_pipes_bind_data *)ptr;
    idx_t i;
    if (!data) return;
    for (i = 0; i < data->row_count; i++) ducknng_pipe_row_reset(&data->rows[i]);
    if (data->rows) duckdb_free(data->rows);
    duckdb_free(data);
}

static void destroy_monitor_init_data(void *ptr) {
    if (ptr) duckdb_free(ptr);
}

static char *ducknng_bind_varchar_parameter(duckdb_bind_info info, idx_t idx) {
    duckdb_value val = duckdb_bind_get_parameter(info, idx);
    char *out = duckdb_get_varchar(val);
    duckdb_destroy_value(&val);
    return out;
}

static uint64_t ducknng_bind_u64_parameter(duckdb_bind_info info, idx_t idx, uint64_t dflt) {
    duckdb_value val = duckdb_bind_get_parameter(info, idx);
    uint64_t out = val ? (uint64_t)duckdb_get_uint64(val) : dflt;
    duckdb_destroy_value(&val);
    return out;
}

static int ducknng_monitor_parse_service(duckdb_bind_info info, ducknng_sql_context *ctx, idx_t name_idx,
    char **out_name, ducknng_service **out_svc, ducknng_transport_url *out_parsed, const char **out_listen) {
    char *name;
    ducknng_service *svc;
    const char *listen;
    char *parse_err = NULL;
    if (out_name) *out_name = NULL;
    if (out_svc) *out_svc = NULL;
    if (out_listen) *out_listen = NULL;
    if (!ctx || !ctx->rt) {
        duckdb_bind_set_error(info, "ducknng: missing runtime");
        return -1;
    }
    name = ducknng_bind_varchar_parameter(info, name_idx);
    if (!name || !name[0]) {
        if (name) duckdb_free(name);
        duckdb_bind_set_error(info, "ducknng: service name is required");
        return -1;
    }
    svc = ducknng_runtime_find_service(ctx->rt, name);
    if (!svc) {
        duckdb_free(name);
        duckdb_bind_set_error(info, "ducknng: service not found");
        return -1;
    }
    listen = ducknng_service_resolved_listen(svc);
    if (out_parsed) {
        ducknng_transport_url_init(out_parsed);
        if (listen && ducknng_transport_url_parse(listen, out_parsed, &parse_err) != 0) {
            if (parse_err) duckdb_free(parse_err);
        }
    }
    if (out_name) *out_name = name;
    else duckdb_free(name);
    if (out_svc) *out_svc = svc;
    if (out_listen) *out_listen = listen;
    return 0;
}

static void ducknng_read_monitor_bind(duckdb_bind_info info) {
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_bind_get_extra_info(info);
    ducknng_monitor_bind_data *bind = NULL;
    ducknng_service *svc = NULL;
    ducknng_pipe_event *events = NULL;
    size_t event_count = 0;
    char *name = NULL;
    char *errmsg = NULL;
    uint64_t after_seq;
    uint64_t max_events;
    ducknng_transport_url parsed;
    char *parse_err = NULL;
    const char *listen = NULL;
    size_t i;
    duckdb_logical_type type;

    if (!ctx || !ctx->rt) {
        duckdb_bind_set_error(info, "ducknng: missing runtime");
        return;
    }
    if (duckdb_bind_get_parameter_count(info) != 3) {
        duckdb_bind_set_error(info, "ducknng: ducknng_read_monitor(name, after_seq, max_events) requires exactly three parameters");
        return;
    }
    name = ducknng_bind_varchar_parameter(info, 0);
    after_seq = ducknng_bind_u64_parameter(info, 1, 0);
    max_events = ducknng_bind_u64_parameter(info, 2, 0);
    if (!name || !name[0]) {
        if (name) duckdb_free(name);
        duckdb_bind_set_error(info, "ducknng: service name is required");
        return;
    }
    svc = ducknng_runtime_find_service(ctx->rt, name);
    if (!svc) {
        duckdb_free(name);
        duckdb_bind_set_error(info, "ducknng: service not found");
        return;
    }
    listen = ducknng_service_resolved_listen(svc);
    ducknng_transport_url_init(&parsed);
    if (listen && ducknng_transport_url_parse(listen, &parsed, &parse_err) != 0) {
        if (parse_err) { duckdb_free(parse_err); parse_err = NULL; }
    }
    if (ducknng_service_pipe_events_snapshot(svc, after_seq, max_events, &events, &event_count, &errmsg) != 0) {
        duckdb_free(name);
        duckdb_bind_set_error(info, errmsg ? errmsg : "ducknng: failed to snapshot pipe monitor events");
        if (errmsg) duckdb_free(errmsg);
        return;
    }

    bind = (ducknng_monitor_bind_data *)duckdb_malloc(sizeof(*bind));
    if (!bind) {
        ducknng_service_pipe_events_free(events, event_count);
        duckdb_free(name);
        duckdb_bind_set_error(info, "ducknng: out of memory");
        return;
    }
    memset(bind, 0, sizeof(*bind));
    bind->row_count = (idx_t)event_count;
    if (event_count > 0) {
        bind->rows = (ducknng_monitor_row *)duckdb_malloc(sizeof(*bind->rows) * event_count);
        if (!bind->rows) {
            ducknng_service_pipe_events_free(events, event_count);
            duckdb_free(name);
            duckdb_free(bind);
            duckdb_bind_set_error(info, "ducknng: out of memory");
            return;
        }
        memset(bind->rows, 0, sizeof(*bind->rows) * event_count);
        for (i = 0; i < event_count; i++) {
            bind->rows[i].seq = events[i].seq;
            bind->rows[i].ts_ms = events[i].ts_ms;
            bind->rows[i].pipe_id = events[i].pipe_id;
            bind->rows[i].service_name = ducknng_strdup(name);
            bind->rows[i].listen = listen ? ducknng_strdup(listen) : NULL;
            bind->rows[i].transport_family = ducknng_strdup(ducknng_transport_family_name(parsed.family));
            bind->rows[i].scheme = ducknng_strdup(ducknng_transport_scheme_name(parsed.scheme));
            bind->rows[i].event = events[i].event ? ducknng_strdup(events[i].event) : NULL;
            bind->rows[i].admitted = events[i].admitted;
            bind->rows[i].remote_addr = events[i].remote_addr ? ducknng_strdup(events[i].remote_addr) : NULL;
            bind->rows[i].remote_ip = events[i].remote_ip ? ducknng_strdup(events[i].remote_ip) : NULL;
            bind->rows[i].remote_port = events[i].remote_port;
            bind->rows[i].peer_identity = events[i].peer_identity ? ducknng_strdup(events[i].peer_identity) : NULL;
        }
    }
    ducknng_service_pipe_events_free(events, event_count);
    duckdb_free(name);

    type = duckdb_create_logical_type(DUCKDB_TYPE_UBIGINT);
    duckdb_bind_add_result_column(info, "seq", type);
    duckdb_bind_add_result_column(info, "ts_ms", type);
    duckdb_bind_add_result_column(info, "pipe_id", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
    duckdb_bind_add_result_column(info, "service_name", type);
    duckdb_bind_add_result_column(info, "listen", type);
    duckdb_bind_add_result_column(info, "transport_family", type);
    duckdb_bind_add_result_column(info, "scheme", type);
    duckdb_bind_add_result_column(info, "event", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_BOOLEAN);
    duckdb_bind_add_result_column(info, "admitted", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
    duckdb_bind_add_result_column(info, "remote_addr", type);
    duckdb_bind_add_result_column(info, "remote_ip", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_INTEGER);
    duckdb_bind_add_result_column(info, "remote_port", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
    duckdb_bind_add_result_column(info, "peer_identity", type);
    duckdb_destroy_logical_type(&type);

    duckdb_bind_set_bind_data(info, bind, destroy_monitor_bind_data);
    duckdb_bind_set_cardinality(info, bind->row_count, true);
}

static void ducknng_read_monitor_init(duckdb_init_info info) {
    ducknng_monitor_init_data *init = (ducknng_monitor_init_data *)duckdb_malloc(sizeof(*init));
    if (!init) {
        duckdb_init_set_error(info, "ducknng: out of memory");
        return;
    }
    init->offset = 0;
    duckdb_init_set_max_threads(info, 1);
    duckdb_init_set_init_data(info, init, destroy_monitor_init_data);
}

static void ducknng_read_monitor_scan(duckdb_function_info info, duckdb_data_chunk output) {
    ducknng_monitor_init_data *init = (ducknng_monitor_init_data *)duckdb_function_get_init_data(info);
    ducknng_monitor_bind_data *bind = (ducknng_monitor_bind_data *)duckdb_function_get_bind_data(info);
    idx_t remaining;
    idx_t chunk_size;
    idx_t i;
    if (!init || !bind || init->offset >= bind->row_count) {
        duckdb_data_chunk_set_size(output, 0);
        return;
    }
    remaining = bind->row_count - init->offset;
    chunk_size = remaining > duckdb_vector_size() ? duckdb_vector_size() : remaining;
    for (i = 0; i < chunk_size; i++) {
        ducknng_monitor_row *row = &bind->rows[init->offset + i];
        ((uint64_t *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 0)))[i] = row->seq;
        ((uint64_t *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 1)))[i] = row->ts_ms;
        ((uint64_t *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 2)))[i] = row->pipe_id;
        if (row->service_name) duckdb_vector_assign_string_element(duckdb_data_chunk_get_vector(output, 3), i, row->service_name); else set_null(duckdb_data_chunk_get_vector(output, 3), i);
        if (row->listen) duckdb_vector_assign_string_element(duckdb_data_chunk_get_vector(output, 4), i, row->listen); else set_null(duckdb_data_chunk_get_vector(output, 4), i);
        if (row->transport_family) duckdb_vector_assign_string_element(duckdb_data_chunk_get_vector(output, 5), i, row->transport_family); else set_null(duckdb_data_chunk_get_vector(output, 5), i);
        if (row->scheme) duckdb_vector_assign_string_element(duckdb_data_chunk_get_vector(output, 6), i, row->scheme); else set_null(duckdb_data_chunk_get_vector(output, 6), i);
        if (row->event) duckdb_vector_assign_string_element(duckdb_data_chunk_get_vector(output, 7), i, row->event); else set_null(duckdb_data_chunk_get_vector(output, 7), i);
        if (row->admitted < 0) set_null(duckdb_data_chunk_get_vector(output, 8), i);
        else ((bool *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 8)))[i] = row->admitted ? true : false;
        if (row->remote_addr) duckdb_vector_assign_string_element(duckdb_data_chunk_get_vector(output, 9), i, row->remote_addr); else set_null(duckdb_data_chunk_get_vector(output, 9), i);
        if (row->remote_ip) duckdb_vector_assign_string_element(duckdb_data_chunk_get_vector(output, 10), i, row->remote_ip); else set_null(duckdb_data_chunk_get_vector(output, 10), i);
        ((int32_t *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 11)))[i] = row->remote_port;
        if (row->peer_identity) duckdb_vector_assign_string_element(duckdb_data_chunk_get_vector(output, 12), i, row->peer_identity); else set_null(duckdb_data_chunk_get_vector(output, 12), i);
    }
    init->offset += chunk_size;
    duckdb_data_chunk_set_size(output, chunk_size);
}

static void ducknng_list_pipes_bind(duckdb_bind_info info) {
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_bind_get_extra_info(info);
    ducknng_pipes_bind_data *bind = NULL;
    ducknng_service *svc = NULL;
    ducknng_pipe_state *states = NULL;
    size_t state_count = 0;
    char *name = NULL;
    char *errmsg = NULL;
    ducknng_transport_url parsed;
    const char *listen = NULL;
    size_t i;
    duckdb_logical_type type;

    if (duckdb_bind_get_parameter_count(info) != 1) {
        duckdb_bind_set_error(info, "ducknng: ducknng_list_pipes(name) requires exactly one parameter");
        return;
    }
    if (ducknng_monitor_parse_service(info, ctx, 0, &name, &svc, &parsed, &listen) != 0) return;
    if (ducknng_service_pipe_states_snapshot(svc, &states, &state_count, &errmsg) != 0) {
        duckdb_free(name);
        duckdb_bind_set_error(info, errmsg ? errmsg : "ducknng: failed to snapshot active pipes");
        if (errmsg) duckdb_free(errmsg);
        return;
    }
    bind = (ducknng_pipes_bind_data *)duckdb_malloc(sizeof(*bind));
    if (!bind) {
        ducknng_service_pipe_states_free(states, state_count);
        duckdb_free(name);
        duckdb_bind_set_error(info, "ducknng: out of memory");
        return;
    }
    memset(bind, 0, sizeof(*bind));
    bind->row_count = (idx_t)state_count;
    if (state_count > 0) {
        bind->rows = (ducknng_pipe_row *)duckdb_malloc(sizeof(*bind->rows) * state_count);
        if (!bind->rows) {
            ducknng_service_pipe_states_free(states, state_count);
            duckdb_free(name);
            duckdb_free(bind);
            duckdb_bind_set_error(info, "ducknng: out of memory");
            return;
        }
        memset(bind->rows, 0, sizeof(*bind->rows) * state_count);
        for (i = 0; i < state_count; i++) {
            bind->rows[i].pipe_id = states[i].pipe_id;
            bind->rows[i].opened_ms = states[i].opened_ms;
            bind->rows[i].service_name = ducknng_strdup(name);
            bind->rows[i].listen = listen ? ducknng_strdup(listen) : NULL;
            bind->rows[i].transport_family = ducknng_strdup(ducknng_transport_family_name(parsed.family));
            bind->rows[i].scheme = ducknng_strdup(ducknng_transport_scheme_name(parsed.scheme));
            bind->rows[i].remote_addr = states[i].remote_addr ? ducknng_strdup(states[i].remote_addr) : NULL;
            bind->rows[i].remote_ip = states[i].remote_ip ? ducknng_strdup(states[i].remote_ip) : NULL;
            bind->rows[i].remote_port = states[i].remote_port;
            bind->rows[i].peer_identity = states[i].peer_identity ? ducknng_strdup(states[i].peer_identity) : NULL;
        }
    }
    ducknng_service_pipe_states_free(states, state_count);
    duckdb_free(name);

    type = duckdb_create_logical_type(DUCKDB_TYPE_UBIGINT);
    duckdb_bind_add_result_column(info, "pipe_id", type);
    duckdb_bind_add_result_column(info, "opened_ms", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
    duckdb_bind_add_result_column(info, "service_name", type);
    duckdb_bind_add_result_column(info, "listen", type);
    duckdb_bind_add_result_column(info, "transport_family", type);
    duckdb_bind_add_result_column(info, "scheme", type);
    duckdb_bind_add_result_column(info, "remote_addr", type);
    duckdb_bind_add_result_column(info, "remote_ip", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_INTEGER);
    duckdb_bind_add_result_column(info, "remote_port", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
    duckdb_bind_add_result_column(info, "peer_identity", type);
    duckdb_destroy_logical_type(&type);

    duckdb_bind_set_bind_data(info, bind, destroy_pipes_bind_data);
    duckdb_bind_set_cardinality(info, bind->row_count, true);
}

static void ducknng_list_pipes_scan(duckdb_function_info info, duckdb_data_chunk output) {
    ducknng_monitor_init_data *init = (ducknng_monitor_init_data *)duckdb_function_get_init_data(info);
    ducknng_pipes_bind_data *bind = (ducknng_pipes_bind_data *)duckdb_function_get_bind_data(info);
    idx_t remaining;
    idx_t chunk_size;
    idx_t i;
    if (!init || !bind || init->offset >= bind->row_count) {
        duckdb_data_chunk_set_size(output, 0);
        return;
    }
    remaining = bind->row_count - init->offset;
    chunk_size = remaining > duckdb_vector_size() ? duckdb_vector_size() : remaining;
    for (i = 0; i < chunk_size; i++) {
        ducknng_pipe_row *row = &bind->rows[init->offset + i];
        ((uint64_t *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 0)))[i] = row->pipe_id;
        ((uint64_t *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 1)))[i] = row->opened_ms;
        if (row->service_name) duckdb_vector_assign_string_element(duckdb_data_chunk_get_vector(output, 2), i, row->service_name); else set_null(duckdb_data_chunk_get_vector(output, 2), i);
        if (row->listen) duckdb_vector_assign_string_element(duckdb_data_chunk_get_vector(output, 3), i, row->listen); else set_null(duckdb_data_chunk_get_vector(output, 3), i);
        if (row->transport_family) duckdb_vector_assign_string_element(duckdb_data_chunk_get_vector(output, 4), i, row->transport_family); else set_null(duckdb_data_chunk_get_vector(output, 4), i);
        if (row->scheme) duckdb_vector_assign_string_element(duckdb_data_chunk_get_vector(output, 5), i, row->scheme); else set_null(duckdb_data_chunk_get_vector(output, 5), i);
        if (row->remote_addr) duckdb_vector_assign_string_element(duckdb_data_chunk_get_vector(output, 6), i, row->remote_addr); else set_null(duckdb_data_chunk_get_vector(output, 6), i);
        if (row->remote_ip) duckdb_vector_assign_string_element(duckdb_data_chunk_get_vector(output, 7), i, row->remote_ip); else set_null(duckdb_data_chunk_get_vector(output, 7), i);
        ((int32_t *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 8)))[i] = row->remote_port;
        if (row->peer_identity) duckdb_vector_assign_string_element(duckdb_data_chunk_get_vector(output, 9), i, row->peer_identity); else set_null(duckdb_data_chunk_get_vector(output, 9), i);
    }
    init->offset += chunk_size;
    duckdb_data_chunk_set_size(output, chunk_size);
}

static int register_monitor_table(duckdb_connection con, ducknng_sql_context *ctx) {
    duckdb_table_function tf;
    duckdb_logical_type type_varchar;
    duckdb_logical_type type_u64;
    if (!ctx || !ctx->rt) return 0;
    tf = duckdb_create_table_function();
    if (!tf) return 0;
    duckdb_table_function_set_name(tf, "ducknng_read_monitor");
    type_varchar = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
    type_u64 = duckdb_create_logical_type(DUCKDB_TYPE_UBIGINT);
    duckdb_table_function_add_parameter(tf, type_varchar);
    duckdb_table_function_add_parameter(tf, type_u64);
    duckdb_table_function_add_parameter(tf, type_u64);
    duckdb_destroy_logical_type(&type_varchar);
    duckdb_destroy_logical_type(&type_u64);
    if (!ducknng_set_table_sql_context(tf, ctx)) { duckdb_destroy_table_function(&tf); return 0; }
    duckdb_table_function_set_bind(tf, ducknng_read_monitor_bind);
    duckdb_table_function_set_init(tf, ducknng_read_monitor_init);
    duckdb_table_function_set_function(tf, ducknng_read_monitor_scan);
    if (duckdb_register_table_function(con, tf) == DuckDBError) {
        duckdb_destroy_table_function(&tf);
        return 0;
    }
    duckdb_destroy_table_function(&tf);
    return 1;
}

static int register_pipes_table(duckdb_connection con, ducknng_sql_context *ctx) {
    duckdb_table_function tf;
    duckdb_logical_type type_varchar;
    if (!ctx || !ctx->rt) return 0;
    tf = duckdb_create_table_function();
    if (!tf) return 0;
    duckdb_table_function_set_name(tf, "ducknng_list_pipes");
    type_varchar = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
    duckdb_table_function_add_parameter(tf, type_varchar);
    duckdb_destroy_logical_type(&type_varchar);
    if (!ducknng_set_table_sql_context(tf, ctx)) { duckdb_destroy_table_function(&tf); return 0; }
    duckdb_table_function_set_bind(tf, ducknng_list_pipes_bind);
    duckdb_table_function_set_init(tf, ducknng_read_monitor_init);
    duckdb_table_function_set_function(tf, ducknng_list_pipes_scan);
    if (duckdb_register_table_function(con, tf) == DuckDBError) {
        duckdb_destroy_table_function(&tf);
        return 0;
    }
    duckdb_destroy_table_function(&tf);
    return 1;
}

int ducknng_register_sql_monitor(duckdb_connection con, ducknng_sql_context *ctx) {
    if (!register_monitor_table(con, ctx)) return 0;
    if (!register_pipes_table(con, ctx)) return 0;
    return 1;
}
