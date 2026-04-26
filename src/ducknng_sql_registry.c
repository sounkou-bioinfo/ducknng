#include "ducknng_sql_shared.h"
#include "ducknng_manifest.h"
#include "ducknng_registry.h"
#include "ducknng_runtime.h"
#include "ducknng_service.h"
#include "ducknng_util.h"
#include <stdatomic.h>
#include <string.h>

DUCKDB_EXTENSION_EXTERN

typedef struct {
    char *name;
    char *family;
    char *summary;
    char *transport_pattern;
    char *request_payload_format;
    char *response_payload_format;
    char *response_mode;
    bool requires_auth;
    bool disabled;
    char *request_schema_json;
    char *response_schema_json;
} ducknng_method_row;

typedef struct {
    ducknng_method_row *rows;
    idx_t row_count;
} ducknng_methods_bind_data;

typedef struct {
    ducknng_methods_bind_data *bind;
    idx_t offset;
} ducknng_methods_init_data;



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
    memcpy(out, src, len);
    out[len] = '\0';
    return out;
}

static bool arg_bool(duckdb_vector vec, idx_t row, bool dflt) {
    bool *data = (bool *)duckdb_vector_get_data(vec);
    if (arg_is_null(vec, row)) return dflt;
    return data[row];
}

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

static int ducknng_set_scalar_sql_context(duckdb_scalar_function fn, const ducknng_sql_context *ctx) {
    ducknng_sql_context *copy = ducknng_dup_sql_context(ctx);
    if (!copy) return 0;
    duckdb_scalar_function_set_extra_info(fn, copy, destroy_sql_context_extra);
    return 1;
}

static int ducknng_set_table_sql_context(duckdb_table_function tf, const ducknng_sql_context *ctx) {
    ducknng_sql_context *copy = ducknng_dup_sql_context(ctx);
    if (!copy) return 0;
    duckdb_table_function_set_extra_info(tf, copy, destroy_sql_context_extra);
    return 1;
}

static int ducknng_reject_scalar_inside_authorizer(duckdb_function_info info, ducknng_sql_context *ctx) {
    if (ctx && ctx->rt && ducknng_runtime_current_thread_authorizer_context_get(ctx->rt)) {
        duckdb_scalar_function_set_error(info, "ducknng: ducknng client and lifecycle functions cannot run inside a SQL authorizer callback");
        return 1;
    }
    return 0;
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
    duckdb_scalar_function_set_volatile(f);
    if (!ducknng_set_scalar_sql_context(f, ctx)) { duckdb_destroy_scalar_function(&f); return 0; }
    if (duckdb_register_scalar_function(con, f) == DuckDBError) {
        duckdb_destroy_scalar_function(&f);
        return 0;
    }
    duckdb_destroy_scalar_function(&f);
    return 1;
}

static void destroy_methods_bind_data(void *ptr) {
    ducknng_methods_bind_data *data = (ducknng_methods_bind_data *)ptr;
    idx_t i;
    if (!data) return;
    for (i = 0; i < data->row_count; i++) {
        if (data->rows[i].name) duckdb_free(data->rows[i].name);
        if (data->rows[i].family) duckdb_free(data->rows[i].family);
        if (data->rows[i].summary) duckdb_free(data->rows[i].summary);
        if (data->rows[i].transport_pattern) duckdb_free(data->rows[i].transport_pattern);
        if (data->rows[i].request_payload_format) duckdb_free(data->rows[i].request_payload_format);
        if (data->rows[i].response_payload_format) duckdb_free(data->rows[i].response_payload_format);
        if (data->rows[i].response_mode) duckdb_free(data->rows[i].response_mode);
        if (data->rows[i].request_schema_json) duckdb_free(data->rows[i].request_schema_json);
        if (data->rows[i].response_schema_json) duckdb_free(data->rows[i].response_schema_json);
    }
    if (data->rows) duckdb_free(data->rows);
    duckdb_free(data);
}

static void destroy_methods_init_data(void *ptr) {
    ducknng_methods_init_data *data = (ducknng_methods_init_data *)ptr;
    if (data) duckdb_free(data);
}


static void ducknng_register_exec_method_scalar(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
    idx_t count = duckdb_data_chunk_get_size(input);
    idx_t ncols = duckdb_data_chunk_get_column_count(input);
    idx_t row;
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_scalar_function_get_extra_info(info);
    if (ducknng_reject_scalar_inside_authorizer(info, ctx)) return;
    bool *out = (bool *)duckdb_vector_get_data(output);
    char *errmsg = NULL;
    if (!ctx || !ctx->rt) {
        duckdb_scalar_function_set_error(info, "ducknng: missing runtime");
        return;
    }
    for (row = 0; row < count; row++) {
        int requires_auth = ncols > 0 ? (arg_bool(duckdb_data_chunk_get_vector(input, 0), row, false) ? 1 : 0) : 0;
        ducknng_mutex_lock(&ctx->rt->mu);
        if (!ducknng_register_exec_method_with_auth(ctx->rt, requires_auth, &errmsg)) {
            ducknng_mutex_unlock(&ctx->rt->mu);
            duckdb_scalar_function_set_error(info, errmsg ? errmsg : "ducknng: failed to register exec method");
            if (errmsg) duckdb_free(errmsg);
            return;
        }
        ducknng_mutex_unlock(&ctx->rt->mu);
        out[row] = true;
    }
}

static int ducknng_method_descriptor_sessionful(const ducknng_method_descriptor *method) {
    if (!method) return 0;
    return method->session_behavior != DUCKNNG_SESSION_STATELESS ||
        method->requires_session || method->opens_session || method->closes_session;
}

static size_t ducknng_runtime_visible_session_count_locked(ducknng_runtime *rt) {
    size_t i;
    size_t total = 0;
    if (!rt) return 0;
    for (i = 0; i < rt->service_count; i++) {
        ducknng_service *svc = rt->services[i];
        if (svc) total += atomic_load_explicit(&svc->session_count_visible, memory_order_acquire);
    }
    return total;
}

static int ducknng_registry_family_sessionful_locked(const ducknng_method_registry *registry, const char *family) {
    size_t i;
    if (!registry || !family || !family[0]) return 0;
    for (i = 0; i < registry->method_count; i++) {
        const ducknng_method_descriptor *method = registry->methods[i];
        if (method && method->family && strcmp(method->family, family) == 0 &&
            ducknng_method_descriptor_sessionful(method)) return 1;
    }
    return 0;
}

static void ducknng_set_method_auth_scalar(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
    idx_t count = duckdb_data_chunk_get_size(input);
    idx_t row;
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_scalar_function_get_extra_info(info);
    if (ducknng_reject_scalar_inside_authorizer(info, ctx)) return;
    bool *out = (bool *)duckdb_vector_get_data(output);
    for (row = 0; row < count; row++) {
        char *name = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 0), row);
        int requires_auth = arg_bool(duckdb_data_chunk_get_vector(input, 1), row, false) ? 1 : 0;
        char *errmsg = NULL;
        int ok;
        if (!ctx || !ctx->rt || !name || !name[0]) {
            if (name) duckdb_free(name);
            duckdb_scalar_function_set_error(info, "ducknng: method name is required");
            return;
        }
        ducknng_mutex_lock(&ctx->rt->mu);
        ok = ducknng_method_registry_set_requires_auth(&ctx->rt->registry, name, requires_auth, &errmsg);
        ducknng_mutex_unlock(&ctx->rt->mu);
        duckdb_free(name);
        if (!ok) {
            duckdb_scalar_function_set_error(info, errmsg ? errmsg : "ducknng: failed to update method auth policy");
            if (errmsg) duckdb_free(errmsg);
            return;
        }
        out[row] = true;
    }
}

static void ducknng_unregister_method_scalar(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
    idx_t count = duckdb_data_chunk_get_size(input);
    idx_t row;
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_scalar_function_get_extra_info(info);
    if (ducknng_reject_scalar_inside_authorizer(info, ctx)) return;
    bool *out = (bool *)duckdb_vector_get_data(output);
    for (row = 0; row < count; row++) {
        char *name = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 0), row);
        int ok;
        if (!ctx || !ctx->rt || !name || !name[0]) {
            if (name) duckdb_free(name);
            duckdb_scalar_function_set_error(info, "ducknng: method name is required");
            return;
        }
        if (strcmp(name, "manifest") == 0) {
            duckdb_free(name);
            duckdb_scalar_function_set_error(info, "ducknng: manifest cannot be unregistered");
            return;
        }
        ducknng_mutex_lock(&ctx->rt->mu);
        {
            const ducknng_method_descriptor *method = ducknng_method_registry_find(&ctx->rt->registry,
                (const uint8_t *)name, (uint32_t)strlen(name));
            if (!method) {
                ducknng_mutex_unlock(&ctx->rt->mu);
                duckdb_free(name);
                duckdb_scalar_function_set_error(info, "ducknng: method not found");
                return;
            }
            if (ducknng_method_descriptor_sessionful(method) &&
                ducknng_runtime_visible_session_count_locked(ctx->rt) > 0) {
                ducknng_mutex_unlock(&ctx->rt->mu);
                duckdb_free(name);
                duckdb_scalar_function_set_error(info,
                    "ducknng: cannot unregister sessionful method while sessions are open");
                return;
            }
        }
        ok = ducknng_method_registry_unregister(&ctx->rt->registry, name);
        ducknng_mutex_unlock(&ctx->rt->mu);
        duckdb_free(name);
        if (!ok) {
            duckdb_scalar_function_set_error(info, "ducknng: method not found");
            return;
        }
        out[row] = true;
    }
}

static void ducknng_unregister_family_scalar(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
    idx_t count = duckdb_data_chunk_get_size(input);
    idx_t row;
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_scalar_function_get_extra_info(info);
    if (ducknng_reject_scalar_inside_authorizer(info, ctx)) return;
    uint64_t *out = (uint64_t *)duckdb_vector_get_data(output);
    for (row = 0; row < count; row++) {
        char *family = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 0), row);
        size_t removed;
        if (!ctx || !ctx->rt || !family || !family[0]) {
            if (family) duckdb_free(family);
            duckdb_scalar_function_set_error(info, "ducknng: method family is required");
            return;
        }
        if (strcmp(family, "control") == 0) {
            duckdb_free(family);
            duckdb_scalar_function_set_error(info, "ducknng: control family cannot be unregistered");
            return;
        }
        ducknng_mutex_lock(&ctx->rt->mu);
        if (ducknng_registry_family_sessionful_locked(&ctx->rt->registry, family) &&
            ducknng_runtime_visible_session_count_locked(ctx->rt) > 0) {
            ducknng_mutex_unlock(&ctx->rt->mu);
            duckdb_free(family);
            duckdb_scalar_function_set_error(info,
                "ducknng: cannot unregister sessionful method family while sessions are open");
            return;
        }
        removed = ducknng_method_registry_unregister_family(&ctx->rt->registry, family);
        ducknng_mutex_unlock(&ctx->rt->mu);
        duckdb_free(family);
        out[row] = (uint64_t)removed;
    }
}


static void ducknng_methods_bind(duckdb_bind_info info) {
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_bind_get_extra_info(info);
    ducknng_methods_bind_data *bind;
    duckdb_logical_type type;
    size_t i;
    if (!ctx || !ctx->rt) {
        duckdb_bind_set_error(info, "ducknng: missing runtime");
        return;
    }
    bind = (ducknng_methods_bind_data *)duckdb_malloc(sizeof(*bind));
    if (!bind) {
        duckdb_bind_set_error(info, "ducknng: out of memory");
        return;
    }
    memset(bind, 0, sizeof(*bind));

    ducknng_mutex_lock(&ctx->rt->mu);
    bind->row_count = (idx_t)ctx->rt->registry.method_count;
    if (bind->row_count > 0) {
        bind->rows = (ducknng_method_row *)duckdb_malloc(sizeof(*bind->rows) * (size_t)bind->row_count);
        if (!bind->rows) {
            ducknng_mutex_unlock(&ctx->rt->mu);
            duckdb_free(bind);
            duckdb_bind_set_error(info, "ducknng: out of memory");
            return;
        }
        memset(bind->rows, 0, sizeof(*bind->rows) * (size_t)bind->row_count);
        for (i = 0; i < (size_t)bind->row_count; i++) {
            const ducknng_method_descriptor *m = ctx->rt->registry.methods[i];
            bind->rows[i].name = m && m->name ? ducknng_strdup(m->name) : NULL;
            bind->rows[i].family = m && m->family ? ducknng_strdup(m->family) : NULL;
            bind->rows[i].summary = m && m->summary ? ducknng_strdup(m->summary) : NULL;
            bind->rows[i].transport_pattern = m ? ducknng_strdup(ducknng_transport_pattern_name(m->transport_pattern)) : NULL;
            bind->rows[i].request_payload_format = m ? ducknng_strdup(ducknng_payload_format_name(m->request_payload_format)) : NULL;
            bind->rows[i].response_payload_format = m ? ducknng_strdup(ducknng_payload_format_name(m->response_payload_format)) : NULL;
            bind->rows[i].response_mode = m ? ducknng_strdup(ducknng_response_mode_name(m->response_mode)) : NULL;
            bind->rows[i].requires_auth = m ? (bool)m->requires_auth : false;
            bind->rows[i].disabled = m ? (bool)m->disabled : false;
            bind->rows[i].request_schema_json = m && m->request_schema_json ? ducknng_strdup(m->request_schema_json) : NULL;
            bind->rows[i].response_schema_json = m && m->response_schema_json ? ducknng_strdup(m->response_schema_json) : NULL;
        }
    }
    ducknng_mutex_unlock(&ctx->rt->mu);

    type = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
    duckdb_bind_add_result_column(info, "name", type);
    duckdb_bind_add_result_column(info, "family", type);
    duckdb_bind_add_result_column(info, "summary", type);
    duckdb_bind_add_result_column(info, "transport_pattern", type);
    duckdb_bind_add_result_column(info, "request_payload_format", type);
    duckdb_bind_add_result_column(info, "response_payload_format", type);
    duckdb_bind_add_result_column(info, "response_mode", type);
    duckdb_bind_add_result_column(info, "request_schema_json", type);
    duckdb_bind_add_result_column(info, "response_schema_json", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_BOOLEAN);
    duckdb_bind_add_result_column(info, "requires_auth", type);
    duckdb_bind_add_result_column(info, "disabled", type);
    duckdb_destroy_logical_type(&type);
    duckdb_bind_set_bind_data(info, bind, destroy_methods_bind_data);
}

static void ducknng_methods_init(duckdb_init_info info) {
    ducknng_methods_bind_data *bind = (ducknng_methods_bind_data *)duckdb_init_get_bind_data(info);
    ducknng_methods_init_data *init = (ducknng_methods_init_data *)duckdb_malloc(sizeof(*init));
    if (!init) {
        duckdb_init_set_error(info, "ducknng: out of memory");
        return;
    }
    init->bind = bind;
    init->offset = 0;
    duckdb_init_set_max_threads(info, 1);
    duckdb_init_set_init_data(info, init, destroy_methods_init_data);
}

static void ducknng_methods_scan(duckdb_function_info info, duckdb_data_chunk output) {
    ducknng_methods_init_data *init = (ducknng_methods_init_data *)duckdb_function_get_init_data(info);
    ducknng_methods_bind_data *bind;
    idx_t remaining;
    idx_t chunk_size;
    idx_t i;
    bool *requires_auth;
    bool *disabled;
    if (!init || !init->bind || init->offset >= init->bind->row_count) {
        duckdb_data_chunk_set_size(output, 0);
        return;
    }
    bind = init->bind;
    remaining = bind->row_count - init->offset;
    chunk_size = remaining > duckdb_vector_size() ? duckdb_vector_size() : remaining;
    requires_auth = (bool *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 9));
    disabled = (bool *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 10));
    for (i = 0; i < chunk_size; i++) {
        ducknng_method_row *row = &bind->rows[init->offset + i];
        if (row->name) duckdb_vector_assign_string_element(duckdb_data_chunk_get_vector(output, 0), i, row->name); else set_null(duckdb_data_chunk_get_vector(output, 0), i);
        if (row->family) duckdb_vector_assign_string_element(duckdb_data_chunk_get_vector(output, 1), i, row->family); else set_null(duckdb_data_chunk_get_vector(output, 1), i);
        if (row->summary) duckdb_vector_assign_string_element(duckdb_data_chunk_get_vector(output, 2), i, row->summary); else set_null(duckdb_data_chunk_get_vector(output, 2), i);
        if (row->transport_pattern) duckdb_vector_assign_string_element(duckdb_data_chunk_get_vector(output, 3), i, row->transport_pattern); else set_null(duckdb_data_chunk_get_vector(output, 3), i);
        if (row->request_payload_format) duckdb_vector_assign_string_element(duckdb_data_chunk_get_vector(output, 4), i, row->request_payload_format); else set_null(duckdb_data_chunk_get_vector(output, 4), i);
        if (row->response_payload_format) duckdb_vector_assign_string_element(duckdb_data_chunk_get_vector(output, 5), i, row->response_payload_format); else set_null(duckdb_data_chunk_get_vector(output, 5), i);
        if (row->response_mode) duckdb_vector_assign_string_element(duckdb_data_chunk_get_vector(output, 6), i, row->response_mode); else set_null(duckdb_data_chunk_get_vector(output, 6), i);
        if (row->request_schema_json) duckdb_vector_assign_string_element(duckdb_data_chunk_get_vector(output, 7), i, row->request_schema_json); else set_null(duckdb_data_chunk_get_vector(output, 7), i);
        if (row->response_schema_json) duckdb_vector_assign_string_element(duckdb_data_chunk_get_vector(output, 8), i, row->response_schema_json); else set_null(duckdb_data_chunk_get_vector(output, 8), i);
        requires_auth[i] = row->requires_auth;
        disabled[i] = row->disabled;
    }
    init->offset += chunk_size;
    duckdb_data_chunk_set_size(output, chunk_size);
}

static int register_named_methods_table(duckdb_connection con, ducknng_sql_context *ctx, const char *name) {
    duckdb_table_function tf;
    if (!ctx || !ctx->rt) return 0;
    tf = duckdb_create_table_function();
    if (!tf) return 0;
    duckdb_table_function_set_name(tf, name);
    if (!ducknng_set_table_sql_context(tf, ctx)) { duckdb_destroy_table_function(&tf); return 0; }
    duckdb_table_function_set_bind(tf, ducknng_methods_bind);
    duckdb_table_function_set_init(tf, ducknng_methods_init);
    duckdb_table_function_set_function(tf, ducknng_methods_scan);
    if (duckdb_register_table_function(con, tf) == DuckDBError) {
        duckdb_destroy_table_function(&tf);
        return 0;
    }
    duckdb_destroy_table_function(&tf);
    return 1;
}



int ducknng_register_sql_registry(duckdb_connection con, ducknng_sql_context *ctx) {
    duckdb_type method_name_types[1] = {DUCKDB_TYPE_VARCHAR};
    duckdb_type method_auth_types[2] = {DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_BOOLEAN};
    duckdb_type method_family_types[1] = {DUCKDB_TYPE_VARCHAR};
    duckdb_type register_exec_auth_types[1] = {DUCKDB_TYPE_BOOLEAN};
    if (!register_scalar(con, "ducknng_register_exec_method", 0, ducknng_register_exec_method_scalar, ctx, NULL, DUCKDB_TYPE_BOOLEAN)) return 0;
    if (!register_scalar(con, "ducknng_register_exec_method", 1, ducknng_register_exec_method_scalar, ctx, register_exec_auth_types, DUCKDB_TYPE_BOOLEAN)) return 0;
    if (!register_scalar(con, "ducknng_set_method_auth", 2, ducknng_set_method_auth_scalar, ctx, method_auth_types, DUCKDB_TYPE_BOOLEAN)) return 0;
    if (!register_scalar(con, "ducknng_unregister_method", 1, ducknng_unregister_method_scalar, ctx, method_name_types, DUCKDB_TYPE_BOOLEAN)) return 0;
    if (!register_scalar(con, "ducknng_unregister_family", 1, ducknng_unregister_family_scalar, ctx, method_family_types, DUCKDB_TYPE_UBIGINT)) return 0;
    if (!register_named_methods_table(con, ctx, "ducknng_list_methods")) return 0;
    return 1;
}
