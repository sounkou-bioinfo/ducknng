#include "ducknng_sql_shared.h"
#include "ducknng_http_compat.h"
#include "ducknng_ipc_in.h"
#include "ducknng_ipc_out.h"
#include "ducknng_runtime.h"
#include "ducknng_util.h"
#include "ducknng_wire.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

DUCKDB_EXTENSION_EXTERN

typedef struct {
    bool ok;
    char *error;
    uint8_t version;
    uint8_t type;
    uint32_t flags;
    char *type_name;
    char *name;
    char *payload_text;
    uint8_t *payload;
    idx_t payload_len;
} ducknng_frame_decode_bind_data;

typedef enum ducknng_body_codec_kind {
    DUCKNNG_BODY_CODEC_RAW = 0,
    DUCKNNG_BODY_CODEC_TEXT = 1,
    DUCKNNG_BODY_CODEC_JSON = 2,
    DUCKNNG_BODY_CODEC_CSV = 3,
    DUCKNNG_BODY_CODEC_TSV = 4,
    DUCKNNG_BODY_CODEC_PARQUET = 5,
    DUCKNNG_BODY_CODEC_ARROW_IPC = 6,
    DUCKNNG_BODY_CODEC_DUCKNNG_FRAME = 7
} ducknng_body_codec_kind;

typedef enum ducknng_body_result_kind {
    DUCKNNG_BODY_RESULT_SINGLE = 1,
    DUCKNNG_BODY_RESULT_ARROW = 2,
    DUCKNNG_BODY_RESULT_FRAME = 3
} ducknng_body_result_kind;

typedef struct {
    int result_kind;
    int codec_kind;
    char *provider;
    char *media_type;
    uint8_t *raw;
    idx_t raw_len;
    char *text;
    struct ArrowSchema schema;
    struct ArrowArray array;
    idx_t row_count;
    ducknng_frame_decode_bind_data frame;
} ducknng_body_parse_bind_data;

typedef struct {
    ducknng_body_parse_bind_data *bind;
    idx_t offset;
    int emitted;
} ducknng_body_parse_init_data;

typedef struct {
    const char *provider;
    const char *media_types;
    const char *output;
    const char *notes;
} ducknng_codec_static_row;

typedef struct {
    const ducknng_codec_static_row *rows;
    idx_t row_count;
} ducknng_codecs_bind_data;

typedef struct {
    ducknng_codecs_bind_data *bind;
    idx_t offset;
} ducknng_codecs_init_data;

typedef struct {
    idx_t emitted;
} ducknng_single_row_init_data;

static int arg_is_null(duckdb_vector vec, idx_t row) {
    uint64_t *validity = duckdb_vector_get_validity(vec);
    return validity && !duckdb_validity_row_is_valid(validity, row);
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

static void destroy_single_row_init_data(void *ptr) {
    ducknng_single_row_init_data *data = (ducknng_single_row_init_data *)ptr;
    if (data) duckdb_free(data);
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

static int ducknng_sql_inside_authorizer(ducknng_sql_context *ctx) {
    return ctx && ctx->rt && ducknng_runtime_current_thread_authorizer_context_get(ctx->rt) != NULL;
}

static int ducknng_reject_table_inside_authorizer(duckdb_bind_info info, ducknng_sql_context *ctx) {
    if (!ducknng_sql_inside_authorizer(ctx)) return 0;
    duckdb_bind_set_error(info, "ducknng: ducknng client and lifecycle functions cannot run inside a SQL authorizer callback");
    return 1;
}

static char *ducknng_dup_bytes(const uint8_t *data, size_t len) {
    char *out = (char *)duckdb_malloc(len + 1);
    if (!out) return NULL;
    if (len) memcpy(out, data, len);
    out[len] = '\0';
    return out;
}
static int ducknng_bytes_look_text(const uint8_t *data, size_t len) {
    size_t i = 0;
    if (len == 0) return 1;
    if (!data) return 0;
    while (i < len) {
        uint8_t b = data[i];
        if (b == 0) return 0;
        if (b < 0x20) {
            if (b != '\n' && b != '\r' && b != '\t') return 0;
            i++;
        } else if (b < 0x80) {
            i++;
        } else if ((b & 0xe0) == 0xc0) {
            if (i + 1 >= len || (data[i + 1] & 0xc0) != 0x80 || b < 0xc2) return 0;
            i += 2;
        } else if ((b & 0xf0) == 0xe0) {
            uint8_t b1;
            if (i + 2 >= len || (data[i + 1] & 0xc0) != 0x80 || (data[i + 2] & 0xc0) != 0x80) return 0;
            b1 = data[i + 1];
            if (b == 0xe0 && b1 < 0xa0) return 0;
            if (b == 0xed && b1 >= 0xa0) return 0;
            i += 3;
        } else if ((b & 0xf8) == 0xf0) {
            uint8_t b1;
            if (i + 3 >= len || (data[i + 1] & 0xc0) != 0x80 ||
                (data[i + 2] & 0xc0) != 0x80 || (data[i + 3] & 0xc0) != 0x80) return 0;
            b1 = data[i + 1];
            if (b == 0xf0 && b1 < 0x90) return 0;
            if (b > 0xf4 || (b == 0xf4 && b1 >= 0x90)) return 0;
            i += 4;
        } else {
            return 0;
        }
    }
    return 1;
}
static const char *ducknng_rpc_type_name(uint8_t type) {
    switch (type) {
    case DUCKNNG_RPC_MANIFEST: return "manifest";
    case DUCKNNG_RPC_CALL: return "call";
    case DUCKNNG_RPC_RESULT: return "result";
    case DUCKNNG_RPC_ERROR: return "error";
    case DUCKNNG_RPC_EVENT: return "event";
    default: return "unknown";
    }
}
static int ducknng_ascii_tolower_int(int c) {
    return (c >= 'A' && c <= 'Z') ? c + ('a' - 'A') : c;
}

static int ducknng_ascii_ieq(const char *a, const char *b) {
    if (!a || !b) return 0;
    while (*a && *b) {
        if (ducknng_ascii_tolower_int((unsigned char)*a) != ducknng_ascii_tolower_int((unsigned char)*b)) return 0;
        a++;
        b++;
    }
    return *a == '\0' && *b == '\0';
}

static int ducknng_ascii_istarts_with(const char *s, const char *prefix) {
    if (!s || !prefix) return 0;
    while (*prefix) {
        if (!*s || ducknng_ascii_tolower_int((unsigned char)*s) != ducknng_ascii_tolower_int((unsigned char)*prefix)) return 0;
        s++;
        prefix++;
    }
    return 1;
}

static int ducknng_ascii_iends_with(const char *s, const char *suffix) {
    size_t slen;
    size_t suffix_len;
    if (!s || !suffix) return 0;
    slen = strlen(s);
    suffix_len = strlen(suffix);
    if (suffix_len > slen) return 0;
    return ducknng_ascii_ieq(s + slen - suffix_len, suffix);
}

static char *ducknng_normalize_media_type(const char *content_type) {
    const char *start;
    const char *end;
    char *out;
    size_t len;
    size_t i;
    if (!content_type) return ducknng_strdup("");
    start = content_type;
    while (*start == ' ' || *start == '\t' || *start == '\r' || *start == '\n') start++;
    end = start;
    while (*end && *end != ';' && *end != ' ' && *end != '\t' && *end != '\r' && *end != '\n') end++;
    while (end > start && (end[-1] == ' ' || end[-1] == '\t')) end--;
    len = (size_t)(end - start);
    out = (char *)duckdb_malloc(len + 1);
    if (!out) return NULL;
    for (i = 0; i < len; i++) out[i] = (char)ducknng_ascii_tolower_int((unsigned char)start[i]);
    out[len] = '\0';
    return out;
}

static const char *ducknng_body_codec_provider_name(int codec_kind) {
    switch (codec_kind) {
        case DUCKNNG_BODY_CODEC_TEXT: return "text";
        case DUCKNNG_BODY_CODEC_JSON: return "json";
        case DUCKNNG_BODY_CODEC_CSV: return "csv";
        case DUCKNNG_BODY_CODEC_TSV: return "tsv";
        case DUCKNNG_BODY_CODEC_PARQUET: return "parquet";
        case DUCKNNG_BODY_CODEC_ARROW_IPC: return "arrow_ipc";
        case DUCKNNG_BODY_CODEC_DUCKNNG_FRAME: return "ducknng_frame";
        case DUCKNNG_BODY_CODEC_RAW:
        default: return "raw";
    }
}

static int ducknng_body_codec_for_media_type(const char *media_type) {
    if (!media_type || !media_type[0]) return DUCKNNG_BODY_CODEC_RAW;
    if (ducknng_ascii_ieq(media_type, "application/vnd.ducknng.frame")) return DUCKNNG_BODY_CODEC_DUCKNNG_FRAME;
    if (ducknng_ascii_ieq(media_type, "application/vnd.apache.arrow.stream") ||
        ducknng_ascii_ieq(media_type, "application/vnd.apache.arrow.ipc") ||
        ducknng_ascii_ieq(media_type, "application/arrow")) return DUCKNNG_BODY_CODEC_ARROW_IPC;
    if (ducknng_ascii_ieq(media_type, "application/vnd.apache.parquet") ||
        ducknng_ascii_ieq(media_type, "application/parquet")) return DUCKNNG_BODY_CODEC_PARQUET;
    if (ducknng_ascii_ieq(media_type, "text/csv") || ducknng_ascii_ieq(media_type, "application/csv")) return DUCKNNG_BODY_CODEC_CSV;
    if (ducknng_ascii_ieq(media_type, "text/tab-separated-values")) return DUCKNNG_BODY_CODEC_TSV;
    if (ducknng_ascii_ieq(media_type, "application/json") ||
        ducknng_ascii_ieq(media_type, "application/x-ndjson") ||
        ducknng_ascii_iends_with(media_type, "+json")) return DUCKNNG_BODY_CODEC_JSON;
    if (ducknng_ascii_istarts_with(media_type, "text/")) return DUCKNNG_BODY_CODEC_TEXT;
    return DUCKNNG_BODY_CODEC_RAW;
}

static char *ducknng_sql_quote_literal(const char *s) {
    size_t need = 3;
    size_t i;
    char *out;
    char *p;
    if (!s) return ducknng_strdup("NULL");
    for (i = 0; s[i]; i++) need += s[i] == '\'' ? 2 : 1;
    out = (char *)duckdb_malloc(need);
    if (!out) return NULL;
    p = out;
    *p++ = '\'';
    for (i = 0; s[i]; i++) {
        if (s[i] == '\'') *p++ = '\'';
        *p++ = s[i];
    }
    *p++ = '\'';
    *p = '\0';
    return out;
}

static char *ducknng_headers_json_get_header(const char *headers_json, const char *wanted_name) {
    const char *p = headers_json;
    char *last_value = NULL;
    if (!headers_json || !wanted_name) return NULL;
    while ((p = strstr(p, "{\"name\":")) != NULL) {
        const char *name_start;
        const char *name_end;
        const char *value_key;
        const char *value_start;
        const char *value_end;
        char *name;
        char *value;
        p += strlen("{\"name\":");
        if (*p != '\"') continue;
        name_start = ++p;
        name_end = strchr(name_start, '\"');
        if (!name_end) break;
        value_key = strstr(name_end, "\"value\":");
        if (!value_key) break;
        value_start = value_key + strlen("\"value\":");
        if (*value_start != '\"') {
            p = name_end + 1;
            continue;
        }
        value_start++;
        value_end = strchr(value_start, '\"');
        if (!value_end) break;
        name = ducknng_dup_bytes((const uint8_t *)name_start, (size_t)(name_end - name_start));
        value = ducknng_dup_bytes((const uint8_t *)value_start, (size_t)(value_end - value_start));
        if (!name || !value) {
            if (name) duckdb_free(name);
            if (value) duckdb_free(value);
            break;
        }
        if (ducknng_ascii_ieq(name, wanted_name)) {
            if (last_value) duckdb_free(last_value);
            last_value = value;
            value = NULL;
        }
        duckdb_free(name);
        if (value) duckdb_free(value);
        p = value_end + 1;
    }
    return last_value;
}

static void ducknng_frame_decode_bind_data_reset(ducknng_frame_decode_bind_data *data) {
    if (!data) return;
    if (data->error) duckdb_free(data->error);
    if (data->type_name) duckdb_free(data->type_name);
    if (data->name) duckdb_free(data->name);
    if (data->payload_text) duckdb_free(data->payload_text);
    if (data->payload) duckdb_free(data->payload);
    memset(data, 0, sizeof(*data));
}

static void destroy_frame_decode_bind_data(void *ptr) {
    ducknng_frame_decode_bind_data *data = (ducknng_frame_decode_bind_data *)ptr;
    if (!data) return;
    ducknng_frame_decode_bind_data_reset(data);
    duckdb_free(data);
}

static void destroy_body_parse_bind_data(void *ptr) {
    ducknng_body_parse_bind_data *data = (ducknng_body_parse_bind_data *)ptr;
    if (!data) return;
    if (data->provider) duckdb_free(data->provider);
    if (data->media_type) duckdb_free(data->media_type);
    if (data->raw) duckdb_free(data->raw);
    if (data->text) duckdb_free(data->text);
    if (data->array.release) ArrowArrayRelease(&data->array);
    if (data->schema.release) ArrowSchemaRelease(&data->schema);
    ducknng_frame_decode_bind_data_reset(&data->frame);
    duckdb_free(data);
}

static void destroy_body_parse_init_data(void *ptr) {
    ducknng_body_parse_init_data *data = (ducknng_body_parse_init_data *)ptr;
    if (!data) return;
    duckdb_free(data);
}

static void destroy_codecs_bind_data(void *ptr) {
    ducknng_codecs_bind_data *data = (ducknng_codecs_bind_data *)ptr;
    if (data) duckdb_free(data);
}

static void destroy_codecs_init_data(void *ptr) {
    ducknng_codecs_init_data *data = (ducknng_codecs_init_data *)ptr;
    if (data) duckdb_free(data);
}

static int ducknng_lookup_tls_config_copy(ducknng_sql_context *ctx, uint64_t tls_config_id,
    uint64_t *out_id, char **out_source, ducknng_tls_opts *out_opts, char **errmsg) {
    size_t i;
    ducknng_tls_config *cfg = NULL;
    if (out_id) *out_id = 0;
    if (out_source) *out_source = NULL;
    if (out_opts) ducknng_tls_opts_init(out_opts);
    if (tls_config_id == 0) return 0;
    if (!ctx || !ctx->rt) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: missing runtime for TLS lookup");
        return -1;
    }
    ducknng_mutex_lock(&ctx->rt->mu);
    for (i = 0; i < ctx->rt->tls_config_count; i++) {
        if (ctx->rt->tls_configs[i] && ctx->rt->tls_configs[i]->tls_config_id == tls_config_id) {
            cfg = ctx->rt->tls_configs[i];
            break;
        }
    }
    if (!cfg) {
        ducknng_mutex_unlock(&ctx->rt->mu);
        if (errmsg) *errmsg = ducknng_strdup("ducknng: tls config not found");
        return -1;
    }
    if (out_id) *out_id = cfg->tls_config_id;
    if (out_source && cfg->source) {
        *out_source = ducknng_strdup(cfg->source);
        if (!*out_source) {
            ducknng_mutex_unlock(&ctx->rt->mu);
            if (errmsg) *errmsg = ducknng_strdup("ducknng: out of memory copying TLS source");
            return -1;
        }
    }
    if (out_opts && ducknng_tls_opts_copy(out_opts, &cfg->opts) != 0) {
        if (out_source && *out_source) {
            duckdb_free(*out_source);
            *out_source = NULL;
        }
        ducknng_mutex_unlock(&ctx->rt->mu);
        if (errmsg) *errmsg = ducknng_strdup("ducknng: out of memory copying TLS options");
        return -1;
    }
    ducknng_mutex_unlock(&ctx->rt->mu);
    return 0;
}

static int ducknng_lookup_tls_opts(ducknng_sql_context *ctx, uint64_t tls_config_id,
    const ducknng_tls_opts **out_opts, char **errmsg) {
    static _Thread_local ducknng_tls_opts tls_copy;
    static _Thread_local int tls_copy_valid = 0;
    if (out_opts) *out_opts = NULL;
    if (tls_copy_valid) {
        ducknng_tls_opts_reset(&tls_copy);
        tls_copy_valid = 0;
    }
    if (tls_config_id == 0) return 0;
    if (ducknng_lookup_tls_config_copy(ctx, tls_config_id, NULL, NULL, &tls_copy, errmsg) != 0) {
        return -1;
    }
    tls_copy_valid = 1;
    if (out_opts) *out_opts = &tls_copy;
    return 0;
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
    switch (schema_view.type) {
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
        case NANOARROW_TYPE_DATE32:
        case NANOARROW_TYPE_DATE64:
            *out_type = duckdb_create_logical_type(DUCKDB_TYPE_DATE);
            return 0;
        case NANOARROW_TYPE_TIME32:
        case NANOARROW_TYPE_TIME64:
            *out_type = duckdb_create_logical_type(schema_view.time_unit == NANOARROW_TIME_UNIT_NANO ? DUCKDB_TYPE_TIME_NS : DUCKDB_TYPE_TIME);
            return 0;
        case NANOARROW_TYPE_TIMESTAMP:
            if (schema_view.timezone && schema_view.timezone[0]) {
                if (errmsg) *errmsg = ducknng_strdup("ducknng: timezone-bearing Arrow timestamps are not supported yet");
                return -1;
            }
            if (schema_view.time_unit == NANOARROW_TIME_UNIT_SECOND) *out_type = duckdb_create_logical_type(DUCKDB_TYPE_TIMESTAMP_S);
            else if (schema_view.time_unit == NANOARROW_TIME_UNIT_MILLI) *out_type = duckdb_create_logical_type(DUCKDB_TYPE_TIMESTAMP_MS);
            else if (schema_view.time_unit == NANOARROW_TIME_UNIT_NANO) *out_type = duckdb_create_logical_type(DUCKDB_TYPE_TIMESTAMP_NS);
            else *out_type = duckdb_create_logical_type(DUCKDB_TYPE_TIMESTAMP);
            return 0;
        case NANOARROW_TYPE_DECIMAL32:
        case NANOARROW_TYPE_DECIMAL64:
        case NANOARROW_TYPE_DECIMAL128:
            *out_type = duckdb_create_decimal_type((uint8_t)schema_view.decimal_precision, (uint8_t)schema_view.decimal_scale);
            return *out_type ? 0 : -1;
        case NANOARROW_TYPE_LIST:
        case NANOARROW_TYPE_LARGE_LIST: {
            duckdb_logical_type child_type = NULL;
            int ok;
            if (!schema->children || !schema->children[0]) return -1;
            ok = ducknng_arrow_schema_to_logical_type(schema->children[0], &child_type, errmsg) == 0 && child_type;
            if (ok) *out_type = duckdb_create_list_type(child_type);
            if (child_type) duckdb_destroy_logical_type(&child_type);
            return ok && *out_type ? 0 : -1;
        }
        case NANOARROW_TYPE_STRUCT: {
            idx_t nchildren = (idx_t)schema->n_children;
            duckdb_logical_type *child_types = NULL;
            const char **child_names = NULL;
            idx_t i;
            int ok = 1;
            if (nchildren > 0) {
                child_types = (duckdb_logical_type *)duckdb_malloc(sizeof(*child_types) * (size_t)nchildren);
                child_names = (const char **)duckdb_malloc(sizeof(*child_names) * (size_t)nchildren);
                if (!child_types || !child_names) ok = 0;
                if (child_types) memset(child_types, 0, sizeof(*child_types) * (size_t)nchildren);
                if (child_names) memset(child_names, 0, sizeof(*child_names) * (size_t)nchildren);
            }
            for (i = 0; ok && i < nchildren; i++) {
                child_names[i] = schema->children[i] && schema->children[i]->name ? schema->children[i]->name : "";
                ok = schema->children[i] && ducknng_arrow_schema_to_logical_type(schema->children[i], &child_types[i], errmsg) == 0;
            }
            if (ok) *out_type = duckdb_create_struct_type(child_types, child_names, nchildren);
            ok = ok && *out_type;
            if (child_types) {
                for (i = 0; i < nchildren; i++) if (child_types[i]) duckdb_destroy_logical_type(&child_types[i]);
                duckdb_free(child_types);
            }
            if (child_names) duckdb_free(child_names);
            return ok ? 0 : -1;
        }
        default:
            if (errmsg) *errmsg = ducknng_strdup(
                "ducknng: remote unary row replies currently support BOOLEAN, numeric/date/time/timestamp/decimal scalars, VARCHAR, BLOB, LIST, and STRUCT");
            return -1;
    }
}

static int64_t ducknng_floor_div_i64(int64_t value, int64_t divisor) {
    int64_t q = value / divisor;
    int64_t r = value % divisor;
    return (r != 0 && ((r < 0) != (divisor < 0))) ? q - 1 : q;
}

static int ducknng_query_rpc_assign_column_at(duckdb_vector vec, struct ArrowArrayView *col_view,
    const struct ArrowSchema *col_schema, idx_t src_offset, idx_t dst_offset, idx_t count, char **errmsg);

static int ducknng_query_rpc_set_nested_null(duckdb_vector vec, const struct ArrowSchema *schema, idx_t index) {
    struct ArrowSchemaView schema_view;
    struct ArrowError error;
    idx_t child;
    memset(&schema_view, 0, sizeof(schema_view));
    memset(&error, 0, sizeof(error));
    set_null(vec, index);
    if (!schema || ArrowSchemaViewInit(&schema_view, schema, &error) != NANOARROW_OK) return 0;
    if (schema_view.type == NANOARROW_TYPE_STRUCT) {
        for (child = 0; child < (idx_t)schema->n_children; child++) {
            duckdb_vector child_vec = duckdb_struct_vector_get_child(vec, child);
            (void)ducknng_query_rpc_set_nested_null(child_vec, schema->children[child], index);
        }
    } else if (schema_view.type == NANOARROW_TYPE_LIST || schema_view.type == NANOARROW_TYPE_LARGE_LIST) {
        duckdb_list_entry *entries = (duckdb_list_entry *)duckdb_vector_get_data(vec);
        entries[index].offset = duckdb_list_vector_get_size(vec);
        entries[index].length = 0;
    }
    return 0;
}

static int ducknng_assign_arrow_decimal(duckdb_vector vec, idx_t dst_index,
    const struct ArrowSchemaView *schema_view, const struct ArrowArrayView *col_view, int64_t src_index) {
    duckdb_logical_type logical_type = duckdb_vector_get_column_type(vec);
    duckdb_type internal_type;
    struct ArrowDecimal decimal;
    if (!logical_type) return -1;
    internal_type = duckdb_decimal_internal_type(logical_type);
    duckdb_destroy_logical_type(&logical_type);
    ArrowDecimalInit(&decimal, schema_view->decimal_bitwidth, schema_view->decimal_precision, schema_view->decimal_scale);
    ArrowArrayViewGetDecimalUnsafe(col_view, src_index, &decimal);
    switch (internal_type) {
        case DUCKDB_TYPE_SMALLINT:
            ((int16_t *)duckdb_vector_get_data(vec))[dst_index] = (int16_t)ArrowDecimalGetIntUnsafe(&decimal);
            return 0;
        case DUCKDB_TYPE_INTEGER:
            ((int32_t *)duckdb_vector_get_data(vec))[dst_index] = (int32_t)ArrowDecimalGetIntUnsafe(&decimal);
            return 0;
        case DUCKDB_TYPE_BIGINT:
            ((int64_t *)duckdb_vector_get_data(vec))[dst_index] = (int64_t)ArrowDecimalGetIntUnsafe(&decimal);
            return 0;
        case DUCKDB_TYPE_HUGEINT: {
            duckdb_hugeint value;
            value.lower = decimal.words[decimal.low_word_index];
            value.upper = (int64_t)decimal.words[decimal.high_word_index];
            ((duckdb_hugeint *)duckdb_vector_get_data(vec))[dst_index] = value;
            return 0;
        }
        default:
            return -1;
    }
}

static int ducknng_query_rpc_assign_column_at(duckdb_vector vec, struct ArrowArrayView *col_view,
    const struct ArrowSchema *col_schema, idx_t src_offset, idx_t dst_offset, idx_t count, char **errmsg) {
    struct ArrowSchemaView schema_view;
    struct ArrowError error;
    idx_t i;
    memset(&schema_view, 0, sizeof(schema_view));
    memset(&error, 0, sizeof(error));
    if (ArrowSchemaViewInit(&schema_view, col_schema, &error) != NANOARROW_OK) {
        if (errmsg) *errmsg = ducknng_strdup(error.message);
        return -1;
    }
    switch (schema_view.type) {
        case NANOARROW_TYPE_BOOL: {
            bool *out = (bool *)duckdb_vector_get_data(vec);
            for (i = 0; i < count; i++) {
                idx_t dst = dst_offset + i;
                if (ArrowArrayViewIsNull(col_view, src_offset + i)) set_null(vec, dst);
                else out[dst] = ArrowArrayViewGetIntUnsafe(col_view, src_offset + i) != 0;
            }
            return 0;
        }
        case NANOARROW_TYPE_INT8: {
            int8_t *out = (int8_t *)duckdb_vector_get_data(vec);
            for (i = 0; i < count; i++) {
                idx_t dst = dst_offset + i;
                if (ArrowArrayViewIsNull(col_view, src_offset + i)) set_null(vec, dst);
                else out[dst] = (int8_t)ArrowArrayViewGetIntUnsafe(col_view, src_offset + i);
            }
            return 0;
        }
        case NANOARROW_TYPE_INT16: {
            int16_t *out = (int16_t *)duckdb_vector_get_data(vec);
            for (i = 0; i < count; i++) {
                idx_t dst = dst_offset + i;
                if (ArrowArrayViewIsNull(col_view, src_offset + i)) set_null(vec, dst);
                else out[dst] = (int16_t)ArrowArrayViewGetIntUnsafe(col_view, src_offset + i);
            }
            return 0;
        }
        case NANOARROW_TYPE_INT32: {
            int32_t *out = (int32_t *)duckdb_vector_get_data(vec);
            for (i = 0; i < count; i++) {
                idx_t dst = dst_offset + i;
                if (ArrowArrayViewIsNull(col_view, src_offset + i)) set_null(vec, dst);
                else out[dst] = (int32_t)ArrowArrayViewGetIntUnsafe(col_view, src_offset + i);
            }
            return 0;
        }
        case NANOARROW_TYPE_INT64: {
            int64_t *out = (int64_t *)duckdb_vector_get_data(vec);
            for (i = 0; i < count; i++) {
                idx_t dst = dst_offset + i;
                if (ArrowArrayViewIsNull(col_view, src_offset + i)) set_null(vec, dst);
                else out[dst] = (int64_t)ArrowArrayViewGetIntUnsafe(col_view, src_offset + i);
            }
            return 0;
        }
        case NANOARROW_TYPE_UINT8: {
            uint8_t *out = (uint8_t *)duckdb_vector_get_data(vec);
            for (i = 0; i < count; i++) {
                idx_t dst = dst_offset + i;
                if (ArrowArrayViewIsNull(col_view, src_offset + i)) set_null(vec, dst);
                else out[dst] = (uint8_t)ArrowArrayViewGetUIntUnsafe(col_view, src_offset + i);
            }
            return 0;
        }
        case NANOARROW_TYPE_UINT16: {
            uint16_t *out = (uint16_t *)duckdb_vector_get_data(vec);
            for (i = 0; i < count; i++) {
                idx_t dst = dst_offset + i;
                if (ArrowArrayViewIsNull(col_view, src_offset + i)) set_null(vec, dst);
                else out[dst] = (uint16_t)ArrowArrayViewGetUIntUnsafe(col_view, src_offset + i);
            }
            return 0;
        }
        case NANOARROW_TYPE_UINT32: {
            uint32_t *out = (uint32_t *)duckdb_vector_get_data(vec);
            for (i = 0; i < count; i++) {
                idx_t dst = dst_offset + i;
                if (ArrowArrayViewIsNull(col_view, src_offset + i)) set_null(vec, dst);
                else out[dst] = (uint32_t)ArrowArrayViewGetUIntUnsafe(col_view, src_offset + i);
            }
            return 0;
        }
        case NANOARROW_TYPE_UINT64: {
            uint64_t *out = (uint64_t *)duckdb_vector_get_data(vec);
            for (i = 0; i < count; i++) {
                idx_t dst = dst_offset + i;
                if (ArrowArrayViewIsNull(col_view, src_offset + i)) set_null(vec, dst);
                else out[dst] = (uint64_t)ArrowArrayViewGetUIntUnsafe(col_view, src_offset + i);
            }
            return 0;
        }
        case NANOARROW_TYPE_FLOAT: {
            float *out = (float *)duckdb_vector_get_data(vec);
            for (i = 0; i < count; i++) {
                idx_t dst = dst_offset + i;
                if (ArrowArrayViewIsNull(col_view, src_offset + i)) set_null(vec, dst);
                else out[dst] = (float)ArrowArrayViewGetDoubleUnsafe(col_view, src_offset + i);
            }
            return 0;
        }
        case NANOARROW_TYPE_DOUBLE: {
            double *out = (double *)duckdb_vector_get_data(vec);
            for (i = 0; i < count; i++) {
                idx_t dst = dst_offset + i;
                if (ArrowArrayViewIsNull(col_view, src_offset + i)) set_null(vec, dst);
                else out[dst] = ArrowArrayViewGetDoubleUnsafe(col_view, src_offset + i);
            }
            return 0;
        }
        case NANOARROW_TYPE_STRING: {
            for (i = 0; i < count; i++) {
                idx_t dst = dst_offset + i;
                struct ArrowStringView value;
                if (ArrowArrayViewIsNull(col_view, src_offset + i)) set_null(vec, dst);
                else {
                    value = ArrowArrayViewGetStringUnsafe(col_view, src_offset + i);
                    duckdb_vector_assign_string_element_len(vec, dst, value.data, (idx_t)value.size_bytes);
                }
            }
            return 0;
        }
        case NANOARROW_TYPE_BINARY: {
            for (i = 0; i < count; i++) {
                idx_t dst = dst_offset + i;
                struct ArrowBufferView value;
                if (ArrowArrayViewIsNull(col_view, src_offset + i)) set_null(vec, dst);
                else {
                    value = ArrowArrayViewGetBytesUnsafe(col_view, src_offset + i);
                    assign_blob(vec, dst, value.data.data, (idx_t)value.size_bytes);
                }
            }
            return 0;
        }
        case NANOARROW_TYPE_DATE32: {
            duckdb_date *out = (duckdb_date *)duckdb_vector_get_data(vec);
            for (i = 0; i < count; i++) {
                idx_t dst = dst_offset + i;
                if (ArrowArrayViewIsNull(col_view, src_offset + i)) set_null(vec, dst);
                else out[dst].days = (int32_t)ArrowArrayViewGetIntUnsafe(col_view, src_offset + i);
            }
            return 0;
        }
        case NANOARROW_TYPE_DATE64: {
            duckdb_date *out = (duckdb_date *)duckdb_vector_get_data(vec);
            for (i = 0; i < count; i++) {
                idx_t dst = dst_offset + i;
                if (ArrowArrayViewIsNull(col_view, src_offset + i)) set_null(vec, dst);
                else out[dst].days = (int32_t)ducknng_floor_div_i64(ArrowArrayViewGetIntUnsafe(col_view, src_offset + i), 86400000LL);
            }
            return 0;
        }
        case NANOARROW_TYPE_TIME32:
        case NANOARROW_TYPE_TIME64: {
            if (schema_view.time_unit == NANOARROW_TIME_UNIT_NANO) {
                duckdb_time_ns *out = (duckdb_time_ns *)duckdb_vector_get_data(vec);
                for (i = 0; i < count; i++) {
                    idx_t dst = dst_offset + i;
                    if (ArrowArrayViewIsNull(col_view, src_offset + i)) set_null(vec, dst);
                    else out[dst].nanos = (int64_t)ArrowArrayViewGetIntUnsafe(col_view, src_offset + i);
                }
            } else {
                duckdb_time *out = (duckdb_time *)duckdb_vector_get_data(vec);
                int64_t mul = schema_view.time_unit == NANOARROW_TIME_UNIT_SECOND ? 1000000LL :
                    (schema_view.time_unit == NANOARROW_TIME_UNIT_MILLI ? 1000LL : 1LL);
                for (i = 0; i < count; i++) {
                    idx_t dst = dst_offset + i;
                    if (ArrowArrayViewIsNull(col_view, src_offset + i)) set_null(vec, dst);
                    else out[dst].micros = (int64_t)ArrowArrayViewGetIntUnsafe(col_view, src_offset + i) * mul;
                }
            }
            return 0;
        }
        case NANOARROW_TYPE_TIMESTAMP: {
            int64_t mul = schema_view.time_unit == NANOARROW_TIME_UNIT_SECOND ? 1000000LL :
                (schema_view.time_unit == NANOARROW_TIME_UNIT_MILLI ? 1000LL : 1LL);
            if (schema_view.timezone && schema_view.timezone[0]) {
                if (errmsg) *errmsg = ducknng_strdup("ducknng: timezone-bearing Arrow timestamps are not supported yet");
                return -1;
            }
            if (schema_view.time_unit == NANOARROW_TIME_UNIT_SECOND) {
                duckdb_timestamp_s *out = (duckdb_timestamp_s *)duckdb_vector_get_data(vec);
                for (i = 0; i < count; i++) {
                    idx_t dst = dst_offset + i;
                    if (ArrowArrayViewIsNull(col_view, src_offset + i)) set_null(vec, dst);
                    else out[dst].seconds = (int64_t)ArrowArrayViewGetIntUnsafe(col_view, src_offset + i);
                }
            } else if (schema_view.time_unit == NANOARROW_TIME_UNIT_MILLI) {
                duckdb_timestamp_ms *out = (duckdb_timestamp_ms *)duckdb_vector_get_data(vec);
                for (i = 0; i < count; i++) {
                    idx_t dst = dst_offset + i;
                    if (ArrowArrayViewIsNull(col_view, src_offset + i)) set_null(vec, dst);
                    else out[dst].millis = (int64_t)ArrowArrayViewGetIntUnsafe(col_view, src_offset + i);
                }
            } else if (schema_view.time_unit == NANOARROW_TIME_UNIT_NANO) {
                duckdb_timestamp_ns *out = (duckdb_timestamp_ns *)duckdb_vector_get_data(vec);
                for (i = 0; i < count; i++) {
                    idx_t dst = dst_offset + i;
                    if (ArrowArrayViewIsNull(col_view, src_offset + i)) set_null(vec, dst);
                    else out[dst].nanos = (int64_t)ArrowArrayViewGetIntUnsafe(col_view, src_offset + i);
                }
            } else {
                duckdb_timestamp *out = (duckdb_timestamp *)duckdb_vector_get_data(vec);
                for (i = 0; i < count; i++) {
                    idx_t dst = dst_offset + i;
                    if (ArrowArrayViewIsNull(col_view, src_offset + i)) set_null(vec, dst);
                    else out[dst].micros = (int64_t)ArrowArrayViewGetIntUnsafe(col_view, src_offset + i) * mul;
                }
            }
            return 0;
        }
        case NANOARROW_TYPE_DECIMAL32:
        case NANOARROW_TYPE_DECIMAL64:
        case NANOARROW_TYPE_DECIMAL128: {
            for (i = 0; i < count; i++) {
                idx_t dst = dst_offset + i;
                if (ArrowArrayViewIsNull(col_view, src_offset + i)) set_null(vec, dst);
                else if (ducknng_assign_arrow_decimal(vec, dst, &schema_view, col_view, src_offset + i) != 0) return -1;
            }
            return 0;
        }
        case NANOARROW_TYPE_LIST:
        case NANOARROW_TYPE_LARGE_LIST: {
            duckdb_list_entry *entries = (duckdb_list_entry *)duckdb_vector_get_data(vec);
            duckdb_vector child_vec = duckdb_list_vector_get_child(vec);
            idx_t child_size = duckdb_list_vector_get_size(vec);
            for (i = 0; i < count; i++) {
                idx_t dst = dst_offset + i;
                int64_t list_index = (int64_t)(src_offset + i) + col_view->offset;
                int64_t child_start = ArrowArrayViewListChildOffset(col_view, list_index);
                int64_t child_end = ArrowArrayViewListChildOffset(col_view, list_index + 1);
                idx_t child_len = child_end >= child_start ? (idx_t)(child_end - child_start) : 0;
                entries[dst].offset = child_size;
                entries[dst].length = 0;
                if (ArrowArrayViewIsNull(col_view, src_offset + i)) {
                    set_null(vec, dst);
                    continue;
                }
                if (duckdb_list_vector_reserve(vec, child_size + child_len) == DuckDBError ||
                    duckdb_list_vector_set_size(vec, child_size + child_len) == DuckDBError) {
                    if (errmsg) *errmsg = ducknng_strdup("ducknng: failed to reserve DuckDB list child vector");
                    return -1;
                }
                entries[dst].offset = child_size;
                entries[dst].length = child_len;
                if (child_len > 0 && ducknng_query_rpc_assign_column_at(child_vec, col_view->children[0],
                        col_schema->children[0], (idx_t)child_start, child_size, child_len, errmsg) != 0) {
                    return -1;
                }
                child_size += child_len;
            }
            return 0;
        }
        case NANOARROW_TYPE_STRUCT: {
            idx_t nchildren = (idx_t)col_view->n_children;
            idx_t child;
            for (child = 0; child < nchildren; child++) {
                duckdb_vector child_vec = duckdb_struct_vector_get_child(vec, child);
                if (ducknng_query_rpc_assign_column_at(child_vec, col_view->children[child], col_schema->children[child],
                        src_offset, dst_offset, count, errmsg) != 0) return -1;
            }
            for (i = 0; i < count; i++) {
                if (ArrowArrayViewIsNull(col_view, src_offset + i)) {
                    (void)ducknng_query_rpc_set_nested_null(vec, col_schema, dst_offset + i);
                }
            }
            return 0;
        }
        default:
            if (errmsg) *errmsg = ducknng_strdup("ducknng: unsupported Arrow type in remote row reply");
            return -1;
    }
}

static int ducknng_query_rpc_assign_column(duckdb_vector vec, struct ArrowArrayView *col_view,
    const struct ArrowSchema *col_schema, idx_t src_offset, idx_t count, char **errmsg) {
    return ducknng_query_rpc_assign_column_at(vec, col_view, col_schema, src_offset, 0, count, errmsg);
}

static void ducknng_single_row_init(duckdb_init_info info) {
    ducknng_single_row_init_data *init = (ducknng_single_row_init_data *)duckdb_malloc(sizeof(*init));
    if (!init) {
        duckdb_init_set_error(info, "ducknng: out of memory");
        return;
    }
    init->emitted = 0;
    duckdb_init_set_max_threads(info, 1);
    duckdb_init_set_init_data(info, init, destroy_single_row_init_data);
}

static void ducknng_decode_frame_bind(duckdb_bind_info info) {
    ducknng_frame_decode_bind_data *bind;
    duckdb_logical_type type;
    duckdb_value frame_val;
    duckdb_blob blob;
    ducknng_frame frame;
    bind = (ducknng_frame_decode_bind_data *)duckdb_malloc(sizeof(*bind));
    if (!bind) {
        duckdb_bind_set_error(info, "ducknng: out of memory");
        return;
    }
    memset(bind, 0, sizeof(*bind));
    frame_val = duckdb_bind_get_parameter(info, 0);
    if (duckdb_is_null_value(frame_val)) {
        memset(&blob, 0, sizeof(blob));
    } else {
        blob = duckdb_get_blob(frame_val);
    }
    duckdb_destroy_value(&frame_val);
    if (!blob.data || blob.size == 0) {
        bind->ok = false;
        bind->error = ducknng_strdup("ducknng: frame blob must not be NULL or empty");
    } else if (ducknng_decode_frame_bytes((const uint8_t *)blob.data, (size_t)blob.size, &frame) != 0) {
        bind->ok = false;
        bind->error = ducknng_strdup("ducknng: invalid frame envelope");
    } else {
        bind->ok = true;
        bind->version = frame.version;
        bind->type = frame.type;
        bind->flags = frame.flags;
        bind->type_name = ducknng_strdup(ducknng_rpc_type_name(frame.type));
        if (!bind->type_name) {
            bind->ok = false;
            bind->error = ducknng_strdup("ducknng: out of memory copying frame type name");
        } else {
            if (frame.name_len > 0) bind->name = ducknng_dup_bytes(frame.name, (size_t)frame.name_len);
            bind->payload_len = (idx_t)frame.payload_len;
            if (frame.payload_len > 0) {
                bind->payload = (uint8_t *)duckdb_malloc((size_t)frame.payload_len);
                if (!bind->payload) {
                    bind->ok = false;
                    bind->error = ducknng_strdup("ducknng: out of memory copying frame payload");
                } else {
                    memcpy(bind->payload, frame.payload, (size_t)frame.payload_len);
                    if (ducknng_bytes_look_text(frame.payload, (size_t)frame.payload_len)) {
                        bind->payload_text = ducknng_dup_bytes(frame.payload, (size_t)frame.payload_len);
                    }
                }
            }
        }
    }
    if (blob.data) duckdb_free(blob.data);
    type = duckdb_create_logical_type(DUCKDB_TYPE_BOOLEAN);
    duckdb_bind_add_result_column(info, "ok", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
    duckdb_bind_add_result_column(info, "error", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_UTINYINT);
    duckdb_bind_add_result_column(info, "version", type);
    duckdb_bind_add_result_column(info, "type", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_UINTEGER);
    duckdb_bind_add_result_column(info, "flags", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
    duckdb_bind_add_result_column(info, "type_name", type);
    duckdb_bind_add_result_column(info, "name", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_BLOB);
    duckdb_bind_add_result_column(info, "payload", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
    duckdb_bind_add_result_column(info, "payload_text", type);
    duckdb_destroy_logical_type(&type);
    duckdb_bind_set_bind_data(info, bind, destroy_frame_decode_bind_data);
    duckdb_bind_set_cardinality(info, 1, true);
}

static void ducknng_decode_frame_scan(duckdb_function_info info, duckdb_data_chunk output) {
    ducknng_single_row_init_data *init = (ducknng_single_row_init_data *)duckdb_function_get_init_data(info);
    ducknng_frame_decode_bind_data *bind = (ducknng_frame_decode_bind_data *)duckdb_function_get_bind_data(info);
    bool *ok_data;
    uint8_t *version_data;
    uint8_t *type_data;
    uint32_t *flags_data;
    if (!init || !bind || init->emitted) {
        duckdb_data_chunk_set_size(output, 0);
        return;
    }
    ok_data = (bool *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 0));
    version_data = (uint8_t *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 2));
    type_data = (uint8_t *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 3));
    flags_data = (uint32_t *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 4));
    ok_data[0] = bind->ok;
    if (bind->error) duckdb_vector_assign_string_element(duckdb_data_chunk_get_vector(output, 1), 0, bind->error);
    else set_null(duckdb_data_chunk_get_vector(output, 1), 0);
    version_data[0] = bind->version;
    type_data[0] = bind->type;
    flags_data[0] = bind->flags;
    if (bind->type_name) duckdb_vector_assign_string_element(duckdb_data_chunk_get_vector(output, 5), 0, bind->type_name);
    else set_null(duckdb_data_chunk_get_vector(output, 5), 0);
    if (bind->name) duckdb_vector_assign_string_element(duckdb_data_chunk_get_vector(output, 6), 0, bind->name);
    else set_null(duckdb_data_chunk_get_vector(output, 6), 0);
    if (bind->payload) assign_blob(duckdb_data_chunk_get_vector(output, 7), 0, bind->payload, bind->payload_len);
    else set_null(duckdb_data_chunk_get_vector(output, 7), 0);
    if (bind->payload_text) duckdb_vector_assign_string_element(duckdb_data_chunk_get_vector(output, 8), 0, bind->payload_text);
    else set_null(duckdb_data_chunk_get_vector(output, 8), 0);
    duckdb_data_chunk_set_size(output, 1);
    init->emitted = 1;
}

static int ducknng_body_parse_frame_copy(const uint8_t *data, size_t len,
    ducknng_frame_decode_bind_data *out) {
    ducknng_frame frame;
    if (!out) return -1;
    memset(out, 0, sizeof(*out));
    if (!data || len == 0) {
        out->ok = false;
        out->error = ducknng_strdup("ducknng: frame blob must not be NULL or empty");
        return 0;
    }
    if (ducknng_decode_frame_bytes(data, len, &frame) != 0) {
        out->ok = false;
        out->error = ducknng_strdup("ducknng: invalid frame envelope");
        return 0;
    }
    out->ok = true;
    out->version = frame.version;
    out->type = frame.type;
    out->flags = frame.flags;
    out->type_name = ducknng_strdup(ducknng_rpc_type_name(frame.type));
    if (!out->type_name) {
        out->ok = false;
        out->error = ducknng_strdup("ducknng: out of memory copying frame type name");
        return -1;
    }
    if (frame.name_len > 0) out->name = ducknng_dup_bytes(frame.name, (size_t)frame.name_len);
    out->payload_len = (idx_t)frame.payload_len;
    if (frame.payload_len > 0) {
        out->payload = (uint8_t *)duckdb_malloc((size_t)frame.payload_len);
        if (!out->payload) {
            out->ok = false;
            out->error = ducknng_strdup("ducknng: out of memory copying frame payload");
            return -1;
        }
        memcpy(out->payload, frame.payload, (size_t)frame.payload_len);
        if (ducknng_bytes_look_text(frame.payload, (size_t)frame.payload_len)) {
            out->payload_text = ducknng_dup_bytes(frame.payload, (size_t)frame.payload_len);
        }
    }
    return 0;
}

static void ducknng_body_parse_add_frame_columns(duckdb_bind_info info) {
    duckdb_logical_type type;
    type = duckdb_create_logical_type(DUCKDB_TYPE_BOOLEAN);
    duckdb_bind_add_result_column(info, "ok", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
    duckdb_bind_add_result_column(info, "error", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_UTINYINT);
    duckdb_bind_add_result_column(info, "version", type);
    duckdb_bind_add_result_column(info, "type", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_UINTEGER);
    duckdb_bind_add_result_column(info, "flags", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
    duckdb_bind_add_result_column(info, "type_name", type);
    duckdb_bind_add_result_column(info, "name", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_BLOB);
    duckdb_bind_add_result_column(info, "payload", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
    duckdb_bind_add_result_column(info, "payload_text", type);
    duckdb_destroy_logical_type(&type);
}

static void ducknng_body_parse_emit_frame_row(duckdb_data_chunk output, const ducknng_frame_decode_bind_data *frame) {
    bool *ok_data = (bool *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 0));
    uint8_t *version_data = (uint8_t *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 2));
    uint8_t *type_data = (uint8_t *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 3));
    uint32_t *flags_data = (uint32_t *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 4));
    ok_data[0] = frame ? frame->ok : false;
    if (frame && frame->error) duckdb_vector_assign_string_element(duckdb_data_chunk_get_vector(output, 1), 0, frame->error);
    else set_null(duckdb_data_chunk_get_vector(output, 1), 0);
    version_data[0] = frame ? frame->version : 0;
    type_data[0] = frame ? frame->type : 0;
    flags_data[0] = frame ? frame->flags : 0;
    if (frame && frame->type_name) duckdb_vector_assign_string_element(duckdb_data_chunk_get_vector(output, 5), 0, frame->type_name);
    else set_null(duckdb_data_chunk_get_vector(output, 5), 0);
    if (frame && frame->name) duckdb_vector_assign_string_element(duckdb_data_chunk_get_vector(output, 6), 0, frame->name);
    else set_null(duckdb_data_chunk_get_vector(output, 6), 0);
    if (frame && frame->payload) assign_blob(duckdb_data_chunk_get_vector(output, 7), 0, frame->payload, frame->payload_len);
    else set_null(duckdb_data_chunk_get_vector(output, 7), 0);
    if (frame && frame->payload_text) duckdb_vector_assign_string_element(duckdb_data_chunk_get_vector(output, 8), 0, frame->payload_text);
    else set_null(duckdb_data_chunk_get_vector(output, 8), 0);
}

static int ducknng_body_parse_run_duckdb_reader(ducknng_sql_context *ctx,
    ducknng_body_parse_bind_data *bind, const uint8_t *body, size_t body_len,
    int codec_kind, char **errmsg) {
    char *json_text = NULL;
    char *quoted = NULL;
    char *sql = NULL;
    uint8_t *payload = NULL;
    size_t payload_len = 0;
    size_t sql_len;
    const char *template_array_struct =
        "WITH _parsed(v) AS (SELECT from_json(%s::JSON, json_structure(%s::JSON))) "
        "SELECT s.* FROM _parsed, unnest(v) AS t(s)";
    const char *template_array_value =
        "WITH _parsed(v) AS (SELECT from_json(%s::JSON, json_structure(%s::JSON))) "
        "SELECT unnest(v) AS value FROM _parsed";
    const char *template_object =
        "WITH _parsed(v) AS (SELECT from_json(%s::JSON, json_structure(%s::JSON))) "
        "SELECT v.* FROM _parsed";
    const char *template_scalar =
        "SELECT from_json(%s::JSON, json_structure(%s::JSON)) AS value";
    const char *chosen_template = NULL;
    const uint8_t *p;
    const uint8_t *end;
    int tried_array_struct = 0;
    int rc = -1;
    if (errmsg) *errmsg = NULL;
    if (!ctx || !ctx->rt || !ctx->rt->init_con || !bind) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: missing runtime for body parser");
        return -1;
    }
    if (codec_kind != DUCKNNG_BODY_CODEC_JSON) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: this body codec needs memory-backed DuckDB reader support and is not enabled");
        return -1;
    }
    if (!body || body_len == 0) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: JSON body must not be empty");
        return -1;
    }
    if (!ducknng_bytes_look_text(body, body_len)) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: JSON body is not valid UTF-8 text");
        return -1;
    }
    json_text = ducknng_dup_bytes(body, body_len);
    quoted = ducknng_sql_quote_literal(json_text ? json_text : "");
    if (!json_text || !quoted) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: out of memory preparing JSON body");
        goto cleanup;
    }
    p = body;
    end = body + body_len;
    while (p < end && (*p == ' ' || *p == '\t' || *p == '\r' || *p == '\n')) p++;
    if (p < end && *p == '[') {
        chosen_template = template_array_struct;
        tried_array_struct = 1;
    } else if (p < end && *p == '{') {
        chosen_template = template_object;
    } else {
        chosen_template = template_scalar;
    }
retry_build:
    sql_len = snprintf(NULL, 0, chosen_template, quoted, quoted) + 1;
    sql = (char *)duckdb_malloc(sql_len);
    if (!sql) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: out of memory building JSON parser query");
        goto cleanup;
    }
    snprintf(sql, sql_len, chosen_template, quoted, quoted);
    if (ducknng_runtime_current_thread_request_service_get(ctx->rt)) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: DuckDB-reader body codecs cannot run inside service-owned SQL yet");
        goto cleanup;
    }
    ducknng_runtime_init_con_lock(ctx->rt);
    if (ducknng_query_to_ipc_stream(ctx->rt->init_con, sql, &payload, &payload_len, errmsg) != 0) {
        ducknng_runtime_init_con_unlock(ctx->rt);
        if (tried_array_struct) {
            if (errmsg && *errmsg) { duckdb_free(*errmsg); *errmsg = NULL; }
            tried_array_struct = 0;
            chosen_template = template_array_value;
            duckdb_free(sql);
            sql = NULL;
            goto retry_build;
        }
        if (errmsg && !*errmsg) *errmsg = ducknng_strdup("ducknng: JSON body parser query failed");
        goto cleanup;
    }
    ducknng_runtime_init_con_unlock(ctx->rt);
    if (ducknng_decode_ipc_table_payload(payload, payload_len, &bind->schema, &bind->array, errmsg) != 0) goto cleanup;
    bind->result_kind = DUCKNNG_BODY_RESULT_ARROW;
    bind->row_count = (idx_t)bind->array.length;
    rc = 0;
cleanup:
    if (payload) duckdb_free(payload);
    if (sql) duckdb_free(sql);
    if (quoted) duckdb_free(quoted);
    if (json_text) duckdb_free(json_text);
    return rc;
}

static int ducknng_body_parse_prepare(duckdb_bind_info info, ducknng_sql_context *ctx,
    const uint8_t *body, size_t body_len, const char *content_type) {
    ducknng_body_parse_bind_data *bind;
    duckdb_logical_type type;
    char *errmsg = NULL;
    idx_t col;
    bind = (ducknng_body_parse_bind_data *)duckdb_malloc(sizeof(*bind));
    if (!bind) {
        duckdb_bind_set_error(info, "ducknng: out of memory");
        return -1;
    }
    memset(bind, 0, sizeof(*bind));
    bind->media_type = ducknng_normalize_media_type(content_type);
    if (!bind->media_type) {
        destroy_body_parse_bind_data(bind);
        duckdb_bind_set_error(info, "ducknng: out of memory normalizing content type");
        return -1;
    }
    bind->codec_kind = ducknng_body_codec_for_media_type(bind->media_type);
    bind->provider = ducknng_strdup(ducknng_body_codec_provider_name(bind->codec_kind));
    if (!bind->provider) {
        destroy_body_parse_bind_data(bind);
        duckdb_bind_set_error(info, "ducknng: out of memory copying body codec name");
        return -1;
    }
    switch (bind->codec_kind) {
        case DUCKNNG_BODY_CODEC_TEXT:
            if (!ducknng_bytes_look_text(body, body_len)) {
                destroy_body_parse_bind_data(bind);
                duckdb_bind_set_error(info, "ducknng: text body is not valid UTF-8 text");
                return -1;
            }
            bind->text = ducknng_dup_bytes(body, body_len);
            if (!bind->text) {
                destroy_body_parse_bind_data(bind);
                duckdb_bind_set_error(info, "ducknng: out of memory copying text body");
                return -1;
            }
            bind->result_kind = DUCKNNG_BODY_RESULT_SINGLE;
            type = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
            duckdb_bind_add_result_column(info, "body_text", type);
            duckdb_destroy_logical_type(&type);
            duckdb_bind_set_cardinality(info, 1, true);
            break;
        case DUCKNNG_BODY_CODEC_DUCKNNG_FRAME:
            if (ducknng_body_parse_frame_copy(body, body_len, &bind->frame) != 0) {
                destroy_body_parse_bind_data(bind);
                duckdb_bind_set_error(info, "ducknng: failed to decode frame body");
                return -1;
            }
            bind->result_kind = DUCKNNG_BODY_RESULT_FRAME;
            ducknng_body_parse_add_frame_columns(info);
            duckdb_bind_set_cardinality(info, 1, true);
            break;
        case DUCKNNG_BODY_CODEC_ARROW_IPC:
            if (ducknng_decode_ipc_table_payload(body, body_len, &bind->schema, &bind->array, &errmsg) != 0) {
                destroy_body_parse_bind_data(bind);
                duckdb_bind_set_error(info, errmsg ? errmsg : "ducknng: failed to decode Arrow IPC body");
                if (errmsg) duckdb_free(errmsg);
                return -1;
            }
            bind->result_kind = DUCKNNG_BODY_RESULT_ARROW;
            bind->row_count = (idx_t)bind->array.length;
            if (bind->schema.n_children < 0 || bind->schema.n_children != bind->array.n_children) {
                destroy_body_parse_bind_data(bind);
                duckdb_bind_set_error(info, "ducknng: invalid Arrow IPC body schema");
                return -1;
            }
            for (col = 0; col < (idx_t)bind->schema.n_children; col++) {
                duckdb_logical_type col_type;
                const char *name = bind->schema.children[col] && bind->schema.children[col]->name ? bind->schema.children[col]->name : "";
                if (ducknng_arrow_schema_to_logical_type(bind->schema.children[col], &col_type, &errmsg) != 0) {
                    destroy_body_parse_bind_data(bind);
                    duckdb_bind_set_error(info, errmsg ? errmsg : "ducknng: unsupported Arrow IPC body type");
                    if (errmsg) duckdb_free(errmsg);
                    return -1;
                }
                duckdb_bind_add_result_column(info, name, col_type);
                duckdb_destroy_logical_type(&col_type);
            }
            duckdb_bind_set_cardinality(info, bind->row_count, true);
            break;
        case DUCKNNG_BODY_CODEC_JSON:
            if (ducknng_body_parse_run_duckdb_reader(ctx, bind, body, body_len, bind->codec_kind, &errmsg) != 0) {
                destroy_body_parse_bind_data(bind);
                duckdb_bind_set_error(info, errmsg ? errmsg : "ducknng: failed to parse body with DuckDB reader");
                if (errmsg) duckdb_free(errmsg);
                return -1;
            }
            if (bind->schema.n_children < 0 || bind->schema.n_children != bind->array.n_children) {
                destroy_body_parse_bind_data(bind);
                duckdb_bind_set_error(info, "ducknng: invalid parsed body schema");
                return -1;
            }
            for (col = 0; col < (idx_t)bind->schema.n_children; col++) {
                duckdb_logical_type col_type;
                const char *name = bind->schema.children[col] && bind->schema.children[col]->name ? bind->schema.children[col]->name : "";
                if (ducknng_arrow_schema_to_logical_type(bind->schema.children[col], &col_type, &errmsg) != 0) {
                    destroy_body_parse_bind_data(bind);
                    duckdb_bind_set_error(info, errmsg ? errmsg : "ducknng: unsupported parsed body type");
                    if (errmsg) duckdb_free(errmsg);
                    return -1;
                }
                duckdb_bind_add_result_column(info, name, col_type);
                duckdb_destroy_logical_type(&col_type);
            }
            duckdb_bind_set_cardinality(info, bind->row_count, true);
            break;
        case DUCKNNG_BODY_CODEC_CSV:
        case DUCKNNG_BODY_CODEC_TSV:
        case DUCKNNG_BODY_CODEC_PARQUET:
        case DUCKNNG_BODY_CODEC_RAW:
        default:
            bind->raw_len = (idx_t)body_len;
            if (body_len > 0) {
                bind->raw = (uint8_t *)duckdb_malloc(body_len);
                if (!bind->raw) {
                    destroy_body_parse_bind_data(bind);
                    duckdb_bind_set_error(info, "ducknng: out of memory copying raw body");
                    return -1;
                }
                memcpy(bind->raw, body, body_len);
            }
            bind->result_kind = DUCKNNG_BODY_RESULT_SINGLE;
            type = duckdb_create_logical_type(DUCKDB_TYPE_BLOB);
            duckdb_bind_add_result_column(info, "body", type);
            duckdb_destroy_logical_type(&type);
            duckdb_bind_set_cardinality(info, 1, true);
            break;
    }
    duckdb_bind_set_bind_data(info, bind, destroy_body_parse_bind_data);
    return 0;
}

static void ducknng_body_parse_bind(duckdb_bind_info info) {
    duckdb_value body_val;
    duckdb_value content_type_val;
    duckdb_blob body_blob;
    char *content_type;
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_bind_get_extra_info(info);
    body_val = duckdb_bind_get_parameter(info, 0);
    content_type_val = duckdb_bind_get_parameter(info, 1);
    if (duckdb_is_null_value(body_val)) memset(&body_blob, 0, sizeof(body_blob));
    else body_blob = duckdb_get_blob(body_val);
    content_type = duckdb_is_null_value(content_type_val) ? NULL : duckdb_get_varchar(content_type_val);
    duckdb_destroy_value(&body_val);
    duckdb_destroy_value(&content_type_val);
    (void)ducknng_body_parse_prepare(info, ctx, (const uint8_t *)body_blob.data, (size_t)body_blob.size, content_type);
    if (content_type) duckdb_free(content_type);
    if (body_blob.data) duckdb_free(body_blob.data);
}

static void ducknng_ncurl_table_bind(duckdb_bind_info info) {
    duckdb_value url_val;
    duckdb_value method_val;
    duckdb_value headers_val;
    duckdb_value body_val;
    duckdb_value timeout_val;
    duckdb_value tls_val;
    duckdb_blob body_blob;
    char *url;
    char *method;
    char *headers_json;
    char *content_type = NULL;
    int32_t timeout_ms;
    uint64_t tls_config_id;
    const ducknng_tls_opts *tls_opts = NULL;
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_bind_get_extra_info(info);
    if (ducknng_reject_table_inside_authorizer(info, ctx)) return;
    char *errmsg = NULL;
    uint16_t status = 0;
    char *resp_headers_json = NULL;
    uint8_t *resp_body = NULL;
    size_t resp_body_len = 0;
    memset(&body_blob, 0, sizeof(body_blob));
    url_val = duckdb_bind_get_parameter(info, 0);
    method_val = duckdb_bind_get_parameter(info, 1);
    headers_val = duckdb_bind_get_parameter(info, 2);
    body_val = duckdb_bind_get_parameter(info, 3);
    timeout_val = duckdb_bind_get_parameter(info, 4);
    tls_val = duckdb_bind_get_parameter(info, 5);
    url = duckdb_is_null_value(url_val) ? NULL : duckdb_get_varchar(url_val);
    method = duckdb_is_null_value(method_val) ? NULL : duckdb_get_varchar(method_val);
    headers_json = duckdb_is_null_value(headers_val) ? NULL : duckdb_get_varchar(headers_val);
    if (!duckdb_is_null_value(body_val)) body_blob = duckdb_get_blob(body_val);
    timeout_ms = duckdb_get_int32(timeout_val);
    tls_config_id = (uint64_t)duckdb_get_uint64(tls_val);
    duckdb_destroy_value(&url_val);
    duckdb_destroy_value(&method_val);
    duckdb_destroy_value(&headers_val);
    duckdb_destroy_value(&body_val);
    duckdb_destroy_value(&timeout_val);
    duckdb_destroy_value(&tls_val);
    if (!url || !url[0]) {
        duckdb_bind_set_error(info, "ducknng: ducknng_ncurl_table URL must not be NULL or empty");
        goto cleanup;
    }
    if (ducknng_lookup_tls_opts(ctx, tls_config_id, &tls_opts, &errmsg) != 0) {
        duckdb_bind_set_error(info, errmsg ? errmsg : "ducknng: tls config not found");
        goto cleanup;
    }
    if (ducknng_http_transact(url, method, headers_json,
            (const uint8_t *)body_blob.data, (size_t)body_blob.size, timeout_ms, tls_opts,
            &status, &resp_headers_json, &resp_body, &resp_body_len, &errmsg) != 0) {
        duckdb_bind_set_error(info, errmsg ? errmsg : "ducknng: HTTP request failed");
        goto cleanup;
    }
    if (status < 200 || status >= 300) {
        char status_msg[128];
        snprintf(status_msg, sizeof(status_msg), "ducknng: HTTP status %u cannot be parsed as a table", (unsigned)status);
        duckdb_bind_set_error(info, status_msg);
        goto cleanup;
    }
    content_type = ducknng_headers_json_get_header(resp_headers_json, "Content-Type");
    (void)ducknng_body_parse_prepare(info, ctx, resp_body, resp_body_len, content_type);
cleanup:
    if (url) duckdb_free(url);
    if (method) duckdb_free(method);
    if (headers_json) duckdb_free(headers_json);
    if (body_blob.data) duckdb_free(body_blob.data);
    if (resp_headers_json) duckdb_free(resp_headers_json);
    if (resp_body) duckdb_free(resp_body);
    if (content_type) duckdb_free(content_type);
    if (errmsg) duckdb_free(errmsg);
}

static void ducknng_body_parse_init(duckdb_init_info info) {
    ducknng_body_parse_bind_data *bind = (ducknng_body_parse_bind_data *)duckdb_init_get_bind_data(info);
    ducknng_body_parse_init_data *init = (ducknng_body_parse_init_data *)duckdb_malloc(sizeof(*init));
    if (!init) {
        duckdb_init_set_error(info, "ducknng: out of memory");
        return;
    }
    memset(init, 0, sizeof(*init));
    init->bind = bind;
    duckdb_init_set_max_threads(info, 1);
    duckdb_init_set_init_data(info, init, destroy_body_parse_init_data);
}

static void ducknng_body_parse_scan(duckdb_function_info info, duckdb_data_chunk output) {
    ducknng_body_parse_init_data *init = (ducknng_body_parse_init_data *)duckdb_function_get_init_data(info);
    ducknng_body_parse_bind_data *bind = init ? init->bind : NULL;
    if (!init || !bind) {
        duckdb_data_chunk_set_size(output, 0);
        return;
    }
    if (bind->result_kind == DUCKNNG_BODY_RESULT_SINGLE) {
        if (init->emitted) {
            duckdb_data_chunk_set_size(output, 0);
            return;
        }
        if (bind->codec_kind == DUCKNNG_BODY_CODEC_TEXT) {
            if (bind->text) duckdb_vector_assign_string_element(duckdb_data_chunk_get_vector(output, 0), 0, bind->text);
            else set_null(duckdb_data_chunk_get_vector(output, 0), 0);
        } else {
            if (bind->raw) assign_blob(duckdb_data_chunk_get_vector(output, 0), 0, bind->raw, bind->raw_len);
            else assign_blob(duckdb_data_chunk_get_vector(output, 0), 0, (const uint8_t *)"", 0);
        }
        duckdb_data_chunk_set_size(output, 1);
        init->emitted = 1;
        return;
    }
    if (bind->result_kind == DUCKNNG_BODY_RESULT_FRAME) {
        if (init->emitted) {
            duckdb_data_chunk_set_size(output, 0);
            return;
        }
        ducknng_body_parse_emit_frame_row(output, &bind->frame);
        duckdb_data_chunk_set_size(output, 1);
        init->emitted = 1;
        return;
    }
    if (bind->result_kind == DUCKNNG_BODY_RESULT_ARROW) {
        struct ArrowArrayView view;
        struct ArrowError error;
        idx_t remaining;
        idx_t chunk_size;
        idx_t col;
        if (init->offset >= bind->row_count) {
            duckdb_data_chunk_set_size(output, 0);
            return;
        }
        memset(&view, 0, sizeof(view));
        memset(&error, 0, sizeof(error));
        if (ArrowArrayViewInitFromSchema(&view, &bind->schema, &error) != NANOARROW_OK ||
            ArrowArrayViewSetArray(&view, &bind->array, &error) != NANOARROW_OK) {
            duckdb_function_set_error(info, error.message[0] ? error.message : "ducknng: failed to view Arrow IPC body");
            ArrowArrayViewReset(&view);
            return;
        }
        remaining = bind->row_count - init->offset;
        chunk_size = remaining > duckdb_vector_size() ? duckdb_vector_size() : remaining;
        for (col = 0; col < (idx_t)bind->schema.n_children; col++) {
            duckdb_vector vec = duckdb_data_chunk_get_vector(output, col);
            if (ducknng_query_rpc_assign_column(vec, view.children[col], bind->schema.children[col], init->offset, chunk_size, NULL) != 0) {
                duckdb_function_set_error(info, "ducknng: failed to decode Arrow IPC body");
                ArrowArrayViewReset(&view);
                return;
            }
        }
        init->offset += chunk_size;
        duckdb_data_chunk_set_size(output, chunk_size);
        ArrowArrayViewReset(&view);
        return;
    }
    duckdb_data_chunk_set_size(output, 0);
}

static const ducknng_codec_static_row DUCKNNG_BUILTIN_CODECS[] = {
    {"raw", "application/octet-stream, */* fallback", "body BLOB", "No decoding; bytes are returned unchanged."},
    {"text", "text/*", "body_text VARCHAR", "Valid UTF-8 text bodies are exposed as VARCHAR."},
    {"json", "application/json, application/*+json, application/x-ndjson", "dynamic table", "Parsed in memory through DuckDB JSON functions."},
    {"csv", "text/csv, application/csv", "body BLOB fallback", "Recognized, but returned as raw bytes until a memory-backed DuckDB CSV reader path exists."},
    {"tsv", "text/tab-separated-values", "body BLOB fallback", "Recognized, but returned as raw bytes until a memory-backed DuckDB CSV reader path exists."},
    {"parquet", "application/vnd.apache.parquet, application/parquet", "body BLOB fallback", "Recognized, but returned as raw bytes until a memory-backed DuckDB Parquet reader path exists."},
    {"arrow_ipc", "application/vnd.apache.arrow.stream, application/vnd.apache.arrow.ipc, application/arrow", "dynamic table", "Decoded as Arrow IPC stream bytes with nanoarrow and mapped to DuckDB vectors."},
    {"ducknng_frame", "application/vnd.ducknng.frame", "decoded frame columns", "Decoded as one ducknng protocol frame, matching ducknng_decode_frame()."}
};

static void ducknng_codecs_bind(duckdb_bind_info info) {
    ducknng_codecs_bind_data *bind;
    duckdb_logical_type type;
    bind = (ducknng_codecs_bind_data *)duckdb_malloc(sizeof(*bind));
    if (!bind) {
        duckdb_bind_set_error(info, "ducknng: out of memory");
        return;
    }
    bind->rows = DUCKNNG_BUILTIN_CODECS;
    bind->row_count = (idx_t)(sizeof(DUCKNNG_BUILTIN_CODECS) / sizeof(DUCKNNG_BUILTIN_CODECS[0]));
    type = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
    duckdb_bind_add_result_column(info, "provider", type);
    duckdb_bind_add_result_column(info, "media_types", type);
    duckdb_bind_add_result_column(info, "output", type);
    duckdb_bind_add_result_column(info, "notes", type);
    duckdb_destroy_logical_type(&type);
    duckdb_bind_set_bind_data(info, bind, destroy_codecs_bind_data);
    duckdb_bind_set_cardinality(info, bind->row_count, true);
}

static void ducknng_codecs_init(duckdb_init_info info) {
    ducknng_codecs_bind_data *bind = (ducknng_codecs_bind_data *)duckdb_init_get_bind_data(info);
    ducknng_codecs_init_data *init = (ducknng_codecs_init_data *)duckdb_malloc(sizeof(*init));
    if (!init) {
        duckdb_init_set_error(info, "ducknng: out of memory");
        return;
    }
    init->bind = bind;
    init->offset = 0;
    duckdb_init_set_max_threads(info, 1);
    duckdb_init_set_init_data(info, init, destroy_codecs_init_data);
}

static void ducknng_codecs_scan(duckdb_function_info info, duckdb_data_chunk output) {
    ducknng_codecs_init_data *init = (ducknng_codecs_init_data *)duckdb_function_get_init_data(info);
    idx_t remaining;
    idx_t chunk_size;
    idx_t i;
    (void)info;
    if (!init || !init->bind || init->offset >= init->bind->row_count) {
        duckdb_data_chunk_set_size(output, 0);
        return;
    }
    remaining = init->bind->row_count - init->offset;
    chunk_size = remaining > duckdb_vector_size() ? duckdb_vector_size() : remaining;
    for (i = 0; i < chunk_size; i++) {
        const ducknng_codec_static_row *row = &init->bind->rows[init->offset + i];
        duckdb_vector_assign_string_element(duckdb_data_chunk_get_vector(output, 0), i, row->provider);
        duckdb_vector_assign_string_element(duckdb_data_chunk_get_vector(output, 1), i, row->media_types);
        duckdb_vector_assign_string_element(duckdb_data_chunk_get_vector(output, 2), i, row->output);
        duckdb_vector_assign_string_element(duckdb_data_chunk_get_vector(output, 3), i, row->notes);
    }
    init->offset += chunk_size;
    duckdb_data_chunk_set_size(output, chunk_size);
}

static int register_body_parse_table_named(duckdb_connection con, ducknng_sql_context *ctx, const char *name) {
    duckdb_table_function tf;
    duckdb_logical_type type_blob;
    duckdb_logical_type type_varchar;
    if (!ctx || !ctx->rt) return 0;
    tf = duckdb_create_table_function();
    if (!tf) return 0;
    duckdb_table_function_set_name(tf, name);
    type_blob = duckdb_create_logical_type(DUCKDB_TYPE_BLOB);
    type_varchar = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
    duckdb_table_function_add_parameter(tf, type_blob);
    duckdb_table_function_add_parameter(tf, type_varchar);
    duckdb_destroy_logical_type(&type_blob);
    duckdb_destroy_logical_type(&type_varchar);
    if (!ducknng_set_table_sql_context(tf, ctx)) { duckdb_destroy_table_function(&tf); return 0; }
    duckdb_table_function_set_bind(tf, ducknng_body_parse_bind);
    duckdb_table_function_set_init(tf, ducknng_body_parse_init);
    duckdb_table_function_set_function(tf, ducknng_body_parse_scan);
    if (duckdb_register_table_function(con, tf) == DuckDBError) {
        duckdb_destroy_table_function(&tf);
        return 0;
    }
    duckdb_destroy_table_function(&tf);
    return 1;
}

static int register_ncurl_table_named(duckdb_connection con, ducknng_sql_context *ctx, const char *name) {
    duckdb_table_function tf;
    duckdb_logical_type type_varchar;
    duckdb_logical_type type_blob;
    duckdb_logical_type type_int;
    duckdb_logical_type type_tls;
    if (!ctx || !ctx->rt) return 0;
    tf = duckdb_create_table_function();
    if (!tf) return 0;
    duckdb_table_function_set_name(tf, name);
    type_varchar = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
    type_blob = duckdb_create_logical_type(DUCKDB_TYPE_BLOB);
    type_int = duckdb_create_logical_type(DUCKDB_TYPE_INTEGER);
    type_tls = duckdb_create_logical_type(DUCKDB_TYPE_UBIGINT);
    duckdb_table_function_add_parameter(tf, type_varchar);
    duckdb_table_function_add_parameter(tf, type_varchar);
    duckdb_table_function_add_parameter(tf, type_varchar);
    duckdb_table_function_add_parameter(tf, type_blob);
    duckdb_table_function_add_parameter(tf, type_int);
    duckdb_table_function_add_parameter(tf, type_tls);
    duckdb_destroy_logical_type(&type_varchar);
    duckdb_destroy_logical_type(&type_blob);
    duckdb_destroy_logical_type(&type_int);
    duckdb_destroy_logical_type(&type_tls);
    if (!ducknng_set_table_sql_context(tf, ctx)) { duckdb_destroy_table_function(&tf); return 0; }
    duckdb_table_function_set_bind(tf, ducknng_ncurl_table_bind);
    duckdb_table_function_set_init(tf, ducknng_body_parse_init);
    duckdb_table_function_set_function(tf, ducknng_body_parse_scan);
    if (duckdb_register_table_function(con, tf) == DuckDBError) {
        duckdb_destroy_table_function(&tf);
        return 0;
    }
    duckdb_destroy_table_function(&tf);
    return 1;
}

static int register_codecs_table_named(duckdb_connection con, const char *name) {
    duckdb_table_function tf;
    tf = duckdb_create_table_function();
    if (!tf) return 0;
    duckdb_table_function_set_name(tf, name);
    duckdb_table_function_set_bind(tf, ducknng_codecs_bind);
    duckdb_table_function_set_init(tf, ducknng_codecs_init);
    duckdb_table_function_set_function(tf, ducknng_codecs_scan);
    if (duckdb_register_table_function(con, tf) == DuckDBError) {
        duckdb_destroy_table_function(&tf);
        return 0;
    }
    duckdb_destroy_table_function(&tf);
    return 1;
}

static int register_frame_decode_table_named(duckdb_connection con, const char *name) {
    duckdb_table_function tf;
    duckdb_logical_type type_blob;
    tf = duckdb_create_table_function();
    if (!tf) return 0;
    duckdb_table_function_set_name(tf, name);
    type_blob = duckdb_create_logical_type(DUCKDB_TYPE_BLOB);
    duckdb_table_function_add_parameter(tf, type_blob);
    duckdb_destroy_logical_type(&type_blob);
    duckdb_table_function_set_bind(tf, ducknng_decode_frame_bind);
    duckdb_table_function_set_init(tf, ducknng_single_row_init);
    duckdb_table_function_set_function(tf, ducknng_decode_frame_scan);
    if (duckdb_register_table_function(con, tf) == DuckDBError) {
        duckdb_destroy_table_function(&tf);
        return 0;
    }
    duckdb_destroy_table_function(&tf);
    return 1;
}


int ducknng_register_sql_body(duckdb_connection con, ducknng_sql_context *ctx) {
    if (!register_body_parse_table_named(con, ctx, "ducknng_parse_body")) return 0;
    if (!register_ncurl_table_named(con, ctx, "ducknng_ncurl_table")) return 0;
    if (!register_codecs_table_named(con, "ducknng_list_codecs")) return 0;
    if (!register_frame_decode_table_named(con, "ducknng_decode_frame")) return 0;
    return 1;
}
