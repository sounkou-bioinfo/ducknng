#include "ducknng_sql_api.h"
#include "ducknng_ipc_in.h"
#include "ducknng_ipc_out.h"
#include "ducknng_http_compat.h"
#include "ducknng_manifest.h"
#include "ducknng_nng_compat.h"
#include "ducknng_runtime.h"
#include "ducknng_transport.h"
#include "ducknng_service.h"
#include "ducknng_sql_shared.h"
#include "ducknng_util.h"
#include "ducknng_wire.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#ifdef _WIN32
#include <ws2tcpip.h>
#else
#include <arpa/inet.h>
#include <unistd.h>
#endif

DUCKDB_EXTENSION_EXTERN

static int ducknng_lookup_tls_config_copy(ducknng_sql_context *ctx, uint64_t tls_config_id,
    uint64_t *out_id, char **out_source, ducknng_tls_opts *out_opts, char **errmsg);

typedef struct {
    uint64_t service_id;
    char *name;
    char *listen;
    int32_t contexts;
    bool running;
    uint64_t sessions;
    uint64_t max_open_sessions;
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

typedef struct {
    uint64_t socket_id;
    char *protocol;
    char *url;
    bool open;
    bool connected;
    bool listening;
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
    uint64_t tls_config_id;
    char *source;
    bool enabled;
    bool has_cert_key_file;
    bool has_ca_file;
    bool has_cert_pem;
    bool has_key_pem;
    bool has_ca_pem;
    bool has_password;
    int32_t auth_mode;
    bool peer_allowlist_active;
    uint64_t peer_allowlist_count;
    char *peer_allowlist_json;
} ducknng_tls_config_row;

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

typedef struct {
    ducknng_tls_config_row *rows;
    idx_t row_count;
} ducknng_tls_configs_bind_data;

typedef struct {
    ducknng_tls_configs_bind_data *bind;
    idx_t offset;
} ducknng_tls_configs_init_data;

typedef struct {
    struct ArrowSchema schema;
    struct ArrowArray array;
    idx_t row_count;
} ducknng_query_rpc_bind_data;

typedef struct {
    ducknng_query_rpc_bind_data *bind;
    idx_t offset;
} ducknng_query_rpc_init_data;

typedef struct {
    bool ok;
    char *error;
    char *manifest;
} ducknng_manifest_result_bind_data;

typedef struct {
    bool ok;
    char *error;
    uint64_t rows_changed;
    int32_t statement_type;
    int32_t result_type;
} ducknng_exec_result_bind_data;

typedef struct {
    bool ok;
    char *error;
    uint8_t *payload;
    idx_t payload_len;
} ducknng_request_bind_data;

typedef struct {
    bool ok;
    int32_t status;
    char *error;
    char *headers_json;
    uint8_t *body;
    idx_t body_len;
    char *body_text;
} ducknng_http_result_bind_data;

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
    ducknng_sql_context *ctx;
    char *url;
    char *sql;
    char *method_name;
    uint64_t session_id;
    char *session_token;
    uint64_t batch_rows;
    uint64_t batch_bytes;
    uint64_t tls_config_id;
    int expect_json_only;
    int executed;
    bool ok;
    char *error;
    char *state;
    char *next_method;
    char *control_json;
    uint64_t idle_timeout_ms;
    int has_idle_timeout_ms;
    uint8_t *payload;
    idx_t payload_len;
    bool end_of_stream;
} ducknng_session_result_bind_data;

typedef struct {
    uint64_t aio_id;
    bool ok;
    char *error;
    uint8_t *frame;
    idx_t frame_len;
} ducknng_aio_collect_row;

typedef struct {
    ducknng_sql_context *ctx;
    uint64_t *aio_ids;
    idx_t aio_id_count;
    int32_t wait_ms;
    ducknng_aio_collect_row *rows;
    idx_t row_count;
    int materialized;
} ducknng_aio_collect_bind_data;

typedef struct {
    ducknng_aio_collect_bind_data *bind;
    idx_t offset;
} ducknng_aio_collect_init_data;

typedef struct {
    idx_t emitted;
} ducknng_single_row_init_data;

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
static bool arg_bool(duckdb_vector vec, idx_t row, bool dflt) {
    bool *data = (bool *)duckdb_vector_get_data(vec);
    if (arg_is_null(vec, row)) return dflt;
    return data[row];
}
static int ducknng_sql_inside_authorizer(ducknng_sql_context *ctx) {
    return ctx && ctx->rt && ducknng_runtime_current_thread_authorizer_context_get(ctx->rt) != NULL;
}
static int ducknng_reject_table_inside_authorizer(duckdb_bind_info info, ducknng_sql_context *ctx) {
    if (!ducknng_sql_inside_authorizer(ctx)) return 0;
    duckdb_bind_set_error(info, "ducknng: ducknng client and lifecycle functions cannot run inside a SQL authorizer callback");
    return 1;
}
static int ducknng_reject_scalar_inside_authorizer(duckdb_function_info info, ducknng_sql_context *ctx) {
    if (!ducknng_sql_inside_authorizer(ctx)) return 0;
    duckdb_scalar_function_set_error(info, "ducknng: ducknng client and lifecycle functions cannot run inside a SQL authorizer callback");
    return 1;
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

static int ducknng_cn_is_safe(const char *cn) {
    size_t i;
    if (!cn || !cn[0]) return 0;
    for (i = 0; cn[i]; i++) {
        char c = cn[i];
        if (!((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
              (c >= '0' && c <= '9') || c == '.' || c == ':' || c == '-' || c == '_')) {
            return 0;
        }
    }
    return 1;
}
static char *ducknng_read_text_file(const char *path) {
    FILE *fp;
    long len;
    char *buf;
    if (!path || !path[0]) return NULL;
    fp = fopen(path, "rb");
    if (!fp) return NULL;
    if (fseek(fp, 0, SEEK_END) != 0) {
        fclose(fp);
        return NULL;
    }
    len = ftell(fp);
    if (len < 0 || fseek(fp, 0, SEEK_SET) != 0) {
        fclose(fp);
        return NULL;
    }
    buf = (char *)duckdb_malloc((size_t)len + 1);
    if (!buf) {
        fclose(fp);
        return NULL;
    }
    if (len > 0 && fread(buf, 1, (size_t)len, fp) != (size_t)len) {
        fclose(fp);
        duckdb_free(buf);
        return NULL;
    }
    buf[len] = '\0';
    fclose(fp);
    return buf;
}
static int ducknng_generate_self_signed_material(const char *cn, int32_t valid_days,
    char **cert_pem, char **key_pem, char **ca_pem, char **errmsg) {
    char cmd[2048];
    char cert_path[512];
    char key_path[512];
    char *dir;
    int status;
#ifdef _WIN32
    const char *path_sep = "\\";
    const char *null_sink = "NUL";
#else
    const char *path_sep = "/";
    const char *null_sink = "/dev/null";
#endif
    if (cert_pem) *cert_pem = NULL;
    if (key_pem) *key_pem = NULL;
    if (ca_pem) *ca_pem = NULL;
    if (!ducknng_cn_is_safe(cn)) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: common name must use only letters, digits, dot, colon, dash, or underscore");
        return -1;
    }
    if (valid_days <= 0) valid_days = 365;
    dir = ducknng_make_temp_dir("ducknng-cert-");
    if (!dir) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: failed to create temp directory for certificate generation");
        return -1;
    }
    snprintf(cert_path, sizeof(cert_path), "%s%scert.pem", dir, path_sep);
    snprintf(key_path, sizeof(key_path), "%s%skey.pem", dir, path_sep);
    snprintf(cmd, sizeof(cmd), "openssl req -x509 -newkey rsa:2048 -keyout '%s' -out '%s' -sha256 -days %d -nodes -subj '/CN=%s' >%s 2>&1", key_path, cert_path, (int)valid_days, cn, null_sink);
    status = system(cmd);
    if (status != 0) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: openssl certificate generation failed");
        ducknng_remove_file(cert_path);
        ducknng_remove_file(key_path);
        ducknng_remove_dir(dir);
        duckdb_free(dir);
        return -1;
    }
    if (cert_pem) *cert_pem = ducknng_read_text_file(cert_path);
    if (key_pem) *key_pem = ducknng_read_text_file(key_path);
    if (ca_pem) *ca_pem = ducknng_read_text_file(cert_path);
    ducknng_remove_file(cert_path);
    ducknng_remove_file(key_path);
    ducknng_remove_dir(dir);
    duckdb_free(dir);
    if ((cert_pem && !*cert_pem) || (key_pem && !*key_pem) || (ca_pem && !*ca_pem)) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: failed to read generated certificate material");
        if (cert_pem && *cert_pem) duckdb_free(*cert_pem);
        if (key_pem && *key_pem) duckdb_free(*key_pem);
        if (ca_pem && *ca_pem) duckdb_free(*ca_pem);
        if (cert_pem) *cert_pem = NULL;
        if (key_pem) *key_pem = NULL;
        if (ca_pem) *ca_pem = NULL;
        return -1;
    }
    return 0;
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

static void destroy_tls_configs_bind_data(void *ptr) {
    ducknng_tls_configs_bind_data *data = (ducknng_tls_configs_bind_data *)ptr;
    idx_t i;
    if (!data) return;
    for (i = 0; i < data->row_count; i++) {
        if (data->rows[i].source) duckdb_free(data->rows[i].source);
        if (data->rows[i].peer_allowlist_json) duckdb_free(data->rows[i].peer_allowlist_json);
    }
    if (data->rows) duckdb_free(data->rows);
    duckdb_free(data);
}

static void destroy_tls_configs_init_data(void *ptr) {
    ducknng_tls_configs_init_data *data = (ducknng_tls_configs_init_data *)ptr;
    if (data) duckdb_free(data);
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

static void destroy_query_rpc_bind_data(void *ptr) {
    ducknng_query_rpc_bind_data *data = (ducknng_query_rpc_bind_data *)ptr;
    if (!data) return;
    if (data->array.release) ArrowArrayRelease(&data->array);
    if (data->schema.release) ArrowSchemaRelease(&data->schema);
    duckdb_free(data);
}

static void destroy_query_rpc_init_data(void *ptr) {
    ducknng_query_rpc_init_data *data = (ducknng_query_rpc_init_data *)ptr;
    if (data) duckdb_free(data);
}

static void destroy_manifest_result_bind_data(void *ptr) {
    ducknng_manifest_result_bind_data *data = (ducknng_manifest_result_bind_data *)ptr;
    if (!data) return;
    if (data->error) duckdb_free(data->error);
    if (data->manifest) duckdb_free(data->manifest);
    duckdb_free(data);
}

static void destroy_exec_result_bind_data(void *ptr) {
    ducknng_exec_result_bind_data *data = (ducknng_exec_result_bind_data *)ptr;
    if (!data) return;
    if (data->error) duckdb_free(data->error);
    duckdb_free(data);
}

static void destroy_request_bind_data(void *ptr) {
    ducknng_request_bind_data *data = (ducknng_request_bind_data *)ptr;
    if (!data) return;
    if (data->error) duckdb_free(data->error);
    if (data->payload) duckdb_free(data->payload);
    duckdb_free(data);
}

static void destroy_http_result_bind_data(void *ptr) {
    ducknng_http_result_bind_data *data = (ducknng_http_result_bind_data *)ptr;
    if (!data) return;
    if (data->error) duckdb_free(data->error);
    if (data->headers_json) duckdb_free(data->headers_json);
    if (data->body) duckdb_free(data->body);
    if (data->body_text) duckdb_free(data->body_text);
    duckdb_free(data);
}

static void destroy_session_result_bind_data(void *ptr) {
    ducknng_session_result_bind_data *data = (ducknng_session_result_bind_data *)ptr;
    if (!data) return;
    if (data->url) duckdb_free(data->url);
    if (data->sql) duckdb_free(data->sql);
    if (data->method_name) duckdb_free(data->method_name);
    if (data->session_token) duckdb_free(data->session_token);
    if (data->error) duckdb_free(data->error);
    if (data->state) duckdb_free(data->state);
    if (data->next_method) duckdb_free(data->next_method);
    if (data->control_json) duckdb_free(data->control_json);
    if (data->payload) duckdb_free(data->payload);
    duckdb_free(data);
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

static void destroy_aio_collect_bind_data(void *ptr) {
    ducknng_aio_collect_bind_data *data = (ducknng_aio_collect_bind_data *)ptr;
    idx_t i;
    if (!data) return;
    if (data->aio_ids) duckdb_free(data->aio_ids);
    if (data->rows) {
        for (i = 0; i < data->row_count; i++) {
            if (data->rows[i].error) duckdb_free(data->rows[i].error);
            if (data->rows[i].frame) duckdb_free(data->rows[i].frame);
        }
        duckdb_free(data->rows);
    }
    duckdb_free(data);
}

static void destroy_aio_collect_init_data(void *ptr) {
    ducknng_aio_collect_init_data *data = (ducknng_aio_collect_init_data *)ptr;
    if (data) duckdb_free(data);
}

static void destroy_single_row_init_data(void *ptr) {
    ducknng_single_row_init_data *data = (ducknng_single_row_init_data *)ptr;
    if (data) duckdb_free(data);
}

static int ducknng_start_service_with_tls_opts(ducknng_sql_context *ctx, const char *name, const char *listen,
    int contexts, uint64_t recv_max, uint64_t idle_ms, uint64_t tls_config_id,
    const char *tls_config_source, const ducknng_tls_opts *tls_opts,
    const char *ip_allowlist_json, char **errmsg) {
    ducknng_service *svc;
    if (!ctx || !ctx->rt || !name || !listen || !name[0] || !listen[0]) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: service name and listen URL must be non-empty");
        return -1;
    }
    if (contexts < 1) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: contexts must be >= 1");
        return -1;
    }
    if (recv_max == 0 || idle_ms == 0) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: recv_max_bytes and session_idle_ms must be > 0");
        return -1;
    }
    if (tls_opts && (tls_opts->auth_mode < 0 || tls_opts->auth_mode > 2)) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: tls_auth_mode must be 0, 1, or 2");
        return -1;
    }
    svc = ducknng_service_create(ctx->rt, name, listen, contexts, (size_t)recv_max, idle_ms,
        tls_config_id, tls_config_source, tls_opts);
    if (!svc) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: failed to allocate service");
        return -1;
    }
    if (ip_allowlist_json && ip_allowlist_json[0] &&
        ducknng_service_set_ip_allowlist(svc, ip_allowlist_json, errmsg) != 0) {
        ducknng_service_destroy(svc);
        return -1;
    }
    if (ducknng_runtime_add_service(ctx->rt, svc, errmsg) != 0) {
        ducknng_service_destroy(svc);
        return -1;
    }
    if (ducknng_service_start(svc, errmsg) != 0) {
        ducknng_runtime_remove_service(ctx->rt, svc->name);
        ducknng_service_destroy(svc);
        return -1;
    }
    return 0;
}

static void ducknng_server_start_tls_config_scalar(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
    idx_t count = duckdb_data_chunk_get_size(input);
    idx_t ncols = duckdb_data_chunk_get_column_count(input);
    idx_t row;
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_scalar_function_get_extra_info(info);
    if (ducknng_reject_scalar_inside_authorizer(info, ctx)) return;
    bool *out = (bool *)duckdb_vector_get_data(output);
    for (row = 0; row < count; row++) {
        char *name = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 0), row);
        char *listen = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 1), row);
        int contexts = arg_int32(duckdb_data_chunk_get_vector(input, 2), row, 1);
        uint64_t recv_max = arg_u64(duckdb_data_chunk_get_vector(input, 3), row, 134217728ULL);
        uint64_t idle_ms = arg_u64(duckdb_data_chunk_get_vector(input, 4), row, 300000ULL);
        uint64_t tls_config_id = arg_u64(duckdb_data_chunk_get_vector(input, 5), row, 0);
        char *ip_allowlist_json = ncols > 6 ? arg_varchar_dup(duckdb_data_chunk_get_vector(input, 6), row) : NULL;
        uint64_t copied_tls_id = 0;
        char *tls_source = NULL;
        ducknng_tls_opts tls_opts_copy;
        char *errmsg = NULL;
        ducknng_tls_opts_init(&tls_opts_copy);
        if (tls_config_id != 0 && ducknng_lookup_tls_config_copy(ctx, tls_config_id, &copied_tls_id, &tls_source, &tls_opts_copy, &errmsg) != 0) {
            if (name) duckdb_free(name);
            if (listen) duckdb_free(listen);
            if (ip_allowlist_json) duckdb_free(ip_allowlist_json);
            ducknng_tls_opts_reset(&tls_opts_copy);
            if (tls_source) duckdb_free(tls_source);
            duckdb_scalar_function_set_error(info, errmsg ? errmsg : "ducknng: tls config not found");
            if (errmsg) duckdb_free(errmsg);
            return;
        }
        if (ducknng_validate_nng_url(listen, &errmsg) != 0) {
            if (name) duckdb_free(name);
            if (listen) duckdb_free(listen);
            if (ip_allowlist_json) duckdb_free(ip_allowlist_json);
            ducknng_tls_opts_reset(&tls_opts_copy);
            if (tls_source) duckdb_free(tls_source);
            duckdb_scalar_function_set_error(info, errmsg ? errmsg : "ducknng: invalid listen URL");
            if (errmsg) duckdb_free(errmsg);
            return;
        }
        if (ducknng_start_service_with_tls_opts(ctx, name, listen, contexts, recv_max, idle_ms,
                copied_tls_id, tls_source, tls_config_id != 0 ? &tls_opts_copy : NULL,
                ip_allowlist_json, &errmsg) != 0) {
            if (name) duckdb_free(name);
            if (listen) duckdb_free(listen);
            if (ip_allowlist_json) duckdb_free(ip_allowlist_json);
            ducknng_tls_opts_reset(&tls_opts_copy);
            if (tls_source) duckdb_free(tls_source);
            duckdb_scalar_function_set_error(info, errmsg ? errmsg : "ducknng: failed to start service");
            if (errmsg) duckdb_free(errmsg);
            return;
        }
        ducknng_tls_opts_reset(&tls_opts_copy);
        if (tls_source) duckdb_free(tls_source);
        if (name) duckdb_free(name);
        if (listen) duckdb_free(listen);
        if (ip_allowlist_json) duckdb_free(ip_allowlist_json);
        out[row] = true;
    }
}

static void ducknng_http_server_start_scalar(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
    idx_t count = duckdb_data_chunk_get_size(input);
    idx_t ncols = duckdb_data_chunk_get_column_count(input);
    idx_t row;
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_scalar_function_get_extra_info(info);
    if (ducknng_reject_scalar_inside_authorizer(info, ctx)) return;
    bool *out = (bool *)duckdb_vector_get_data(output);
    for (row = 0; row < count; row++) {
        char *name = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 0), row);
        char *listen = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 1), row);
        uint64_t recv_max = arg_u64(duckdb_data_chunk_get_vector(input, 2), row, 134217728ULL);
        uint64_t idle_ms = arg_u64(duckdb_data_chunk_get_vector(input, 3), row, 300000ULL);
        uint64_t tls_config_id = arg_u64(duckdb_data_chunk_get_vector(input, 4), row, 0);
        char *ip_allowlist_json = ncols > 5 ? arg_varchar_dup(duckdb_data_chunk_get_vector(input, 5), row) : NULL;
        uint64_t copied_tls_id = 0;
        char *tls_source = NULL;
        ducknng_tls_opts tls_opts_copy;
        char *errmsg = NULL;
        ducknng_tls_opts_init(&tls_opts_copy);
        if (tls_config_id != 0 && ducknng_lookup_tls_config_copy(ctx, tls_config_id, &copied_tls_id, &tls_source, &tls_opts_copy, &errmsg) != 0) {
            if (name) duckdb_free(name);
            if (listen) duckdb_free(listen);
            if (ip_allowlist_json) duckdb_free(ip_allowlist_json);
            ducknng_tls_opts_reset(&tls_opts_copy);
            if (tls_source) duckdb_free(tls_source);
            duckdb_scalar_function_set_error(info, errmsg ? errmsg : "ducknng: tls config not found");
            if (errmsg) duckdb_free(errmsg);
            return;
        }
        if (ducknng_validate_http_server_url(listen, tls_config_id != 0 ? &tls_opts_copy : NULL, &errmsg) != 0) {
            if (name) duckdb_free(name);
            if (listen) duckdb_free(listen);
            if (ip_allowlist_json) duckdb_free(ip_allowlist_json);
            ducknng_tls_opts_reset(&tls_opts_copy);
            if (tls_source) duckdb_free(tls_source);
            duckdb_scalar_function_set_error(info, errmsg ? errmsg : "ducknng: invalid HTTP listen URL");
            if (errmsg) duckdb_free(errmsg);
            return;
        }
        if (ducknng_start_service_with_tls_opts(ctx, name, listen, 1, recv_max, idle_ms,
                copied_tls_id, tls_source, tls_config_id != 0 ? &tls_opts_copy : NULL,
                ip_allowlist_json, &errmsg) != 0) {
            if (name) duckdb_free(name);
            if (listen) duckdb_free(listen);
            if (ip_allowlist_json) duckdb_free(ip_allowlist_json);
            ducknng_tls_opts_reset(&tls_opts_copy);
            if (tls_source) duckdb_free(tls_source);
            duckdb_scalar_function_set_error(info, errmsg ? errmsg : "ducknng: failed to start HTTP service");
            if (errmsg) duckdb_free(errmsg);
            return;
        }
        ducknng_tls_opts_reset(&tls_opts_copy);
        if (tls_source) duckdb_free(tls_source);
        if (name) duckdb_free(name);
        if (listen) duckdb_free(listen);
        if (ip_allowlist_json) duckdb_free(ip_allowlist_json);
        out[row] = true;
    }
}

static void ducknng_server_stop_scalar(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
    idx_t count = duckdb_data_chunk_get_size(input);
    idx_t row;
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_scalar_function_get_extra_info(info);
    if (ducknng_reject_scalar_inside_authorizer(info, ctx)) return;
    bool *out = (bool *)duckdb_vector_get_data(output);
    for (row = 0; row < count; row++) {
        char *name = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 0), row);
        ducknng_service *svc;
        if (!ctx || !ctx->rt || !name) {
            if (name) duckdb_free(name);
            duckdb_scalar_function_set_error(info, "ducknng: invalid stop arguments");
            return;
        }
        svc = ducknng_runtime_find_service(ctx->rt, name);
        if (!svc) {
            duckdb_free(name);
            duckdb_scalar_function_set_error(info, "ducknng: service not found");
            return;
        }
        if (ctx->is_init_connection && ducknng_runtime_current_request_service_get(ctx->rt) == svc) {
            duckdb_free(name);
            duckdb_scalar_function_set_error(info, "ducknng: cannot stop a service from its own request handler");
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

static void ducknng_tls_config_from_files_scalar(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
    idx_t count = duckdb_data_chunk_get_size(input);
    idx_t row;
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_scalar_function_get_extra_info(info);
    uint64_t *out = (uint64_t *)duckdb_vector_get_data(output);
    for (row = 0; row < count; row++) {
        ducknng_tls_config *cfg;
        char *cert_key_file = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 0), row);
        char *ca_file = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 1), row);
        char *password = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 2), row);
        int auth_mode = arg_int32(duckdb_data_chunk_get_vector(input, 3), row, 0);
        char *errmsg = NULL;
        if (!ctx || !ctx->rt || auth_mode < 0 || auth_mode > 2 || ((!cert_key_file || !cert_key_file[0]) && (!ca_file || !ca_file[0]) && auth_mode == 0)) {
            if (cert_key_file) duckdb_free(cert_key_file);
            if (ca_file) duckdb_free(ca_file);
            if (password) duckdb_free(password);
            duckdb_scalar_function_set_error(info, "ducknng: invalid tls config file arguments");
            return;
        }
        cfg = (ducknng_tls_config *)duckdb_malloc(sizeof(*cfg));
        if (!cfg) {
            if (cert_key_file) duckdb_free(cert_key_file);
            if (ca_file) duckdb_free(ca_file);
            if (password) duckdb_free(password);
            duckdb_scalar_function_set_error(info, "ducknng: out of memory allocating tls config");
            return;
        }
        memset(cfg, 0, sizeof(*cfg));
        cfg->source = ducknng_strdup("files");
        ducknng_tls_opts_init(&cfg->opts);
        cfg->opts.enabled = 1;
        cfg->opts.cert_key_file = cert_key_file;
        cfg->opts.ca_file = ca_file;
        cfg->opts.password = password;
        cfg->opts.auth_mode = auth_mode;
        if (!cfg->source || ducknng_runtime_add_tls_config(ctx->rt, cfg, &errmsg) != 0) {
            if (cfg->source) duckdb_free(cfg->source);
            ducknng_tls_opts_reset(&cfg->opts);
            duckdb_free(cfg);
            duckdb_scalar_function_set_error(info, errmsg ? errmsg : "ducknng: failed to register tls config");
            if (errmsg) duckdb_free(errmsg);
            return;
        }
        out[row] = cfg->tls_config_id;
    }
}

static void ducknng_tls_config_from_pem_scalar(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
    idx_t count = duckdb_data_chunk_get_size(input);
    idx_t row;
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_scalar_function_get_extra_info(info);
    uint64_t *out = (uint64_t *)duckdb_vector_get_data(output);
    for (row = 0; row < count; row++) {
        ducknng_tls_config *cfg;
        char *cert_pem = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 0), row);
        char *key_pem = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 1), row);
        char *ca_pem = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 2), row);
        char *password = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 3), row);
        int auth_mode = arg_int32(duckdb_data_chunk_get_vector(input, 4), row, 0);
        char *errmsg = NULL;
        if (!ctx || !ctx->rt || auth_mode < 0 || auth_mode > 2 || ((!cert_pem || !cert_pem[0]) && (!key_pem || !key_pem[0]) && (!ca_pem || !ca_pem[0]) && auth_mode == 0)) {
            if (cert_pem) duckdb_free(cert_pem);
            if (key_pem) duckdb_free(key_pem);
            if (ca_pem) duckdb_free(ca_pem);
            if (password) duckdb_free(password);
            duckdb_scalar_function_set_error(info, "ducknng: invalid tls pem arguments");
            return;
        }
        cfg = (ducknng_tls_config *)duckdb_malloc(sizeof(*cfg));
        if (!cfg) {
            if (cert_pem) duckdb_free(cert_pem);
            if (key_pem) duckdb_free(key_pem);
            if (ca_pem) duckdb_free(ca_pem);
            if (password) duckdb_free(password);
            duckdb_scalar_function_set_error(info, "ducknng: out of memory allocating tls config");
            return;
        }
        memset(cfg, 0, sizeof(*cfg));
        cfg->source = ducknng_strdup("pem");
        ducknng_tls_opts_init(&cfg->opts);
        cfg->opts.enabled = 1;
        cfg->opts.cert_pem = cert_pem;
        cfg->opts.key_pem = key_pem;
        cfg->opts.ca_pem = ca_pem;
        cfg->opts.password = password;
        cfg->opts.auth_mode = auth_mode;
        if (!cfg->source || ducknng_runtime_add_tls_config(ctx->rt, cfg, &errmsg) != 0) {
            if (cfg->source) duckdb_free(cfg->source);
            ducknng_tls_opts_reset(&cfg->opts);
            duckdb_free(cfg);
            duckdb_scalar_function_set_error(info, errmsg ? errmsg : "ducknng: failed to register tls config");
            if (errmsg) duckdb_free(errmsg);
            return;
        }
        out[row] = cfg->tls_config_id;
    }
}

static void ducknng_self_signed_tls_config_scalar(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
    idx_t count = duckdb_data_chunk_get_size(input);
    idx_t row;
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_scalar_function_get_extra_info(info);
    uint64_t *out = (uint64_t *)duckdb_vector_get_data(output);
    for (row = 0; row < count; row++) {
        ducknng_tls_config *cfg;
        char *cn = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 0), row);
        int32_t valid_days = arg_int32(duckdb_data_chunk_get_vector(input, 1), row, 365);
        int auth_mode = arg_int32(duckdb_data_chunk_get_vector(input, 2), row, 0);
        char *cert_pem = NULL, *key_pem = NULL, *ca_pem = NULL, *errmsg = NULL;
        if (!ctx || !ctx->rt || !cn || auth_mode < 0 || auth_mode > 2) {
            if (cn) duckdb_free(cn);
            duckdb_scalar_function_set_error(info, "ducknng: invalid self-signed tls config arguments");
            return;
        }
        if (ducknng_generate_self_signed_material(cn, valid_days, &cert_pem, &key_pem, &ca_pem, &errmsg) != 0) {
            duckdb_free(cn);
            duckdb_scalar_function_set_error(info, errmsg ? errmsg : "ducknng: failed to generate self-signed certificate");
            if (errmsg) duckdb_free(errmsg);
            return;
        }
        duckdb_free(cn);
        cfg = (ducknng_tls_config *)duckdb_malloc(sizeof(*cfg));
        if (!cfg) {
            if (cert_pem) duckdb_free(cert_pem);
            if (key_pem) duckdb_free(key_pem);
            if (ca_pem) duckdb_free(ca_pem);
            duckdb_scalar_function_set_error(info, "ducknng: out of memory allocating tls config");
            return;
        }
        memset(cfg, 0, sizeof(*cfg));
        cfg->source = ducknng_strdup("self_signed");
        ducknng_tls_opts_init(&cfg->opts);
        cfg->opts.enabled = 1;
        cfg->opts.cert_pem = cert_pem;
        cfg->opts.key_pem = key_pem;
        cfg->opts.ca_pem = ca_pem;
        cfg->opts.auth_mode = auth_mode;
        if (!cfg->source || ducknng_runtime_add_tls_config(ctx->rt, cfg, &errmsg) != 0) {
            if (cfg->source) duckdb_free(cfg->source);
            ducknng_tls_opts_reset(&cfg->opts);
            duckdb_free(cfg);
            duckdb_scalar_function_set_error(info, errmsg ? errmsg : "ducknng: failed to register tls config");
            if (errmsg) duckdb_free(errmsg);
            return;
        }
        out[row] = cfg->tls_config_id;
    }
}

static void ducknng_drop_tls_config_scalar(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
    idx_t count = duckdb_data_chunk_get_size(input);
    idx_t row;
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_scalar_function_get_extra_info(info);
    if (ducknng_reject_scalar_inside_authorizer(info, ctx)) return;
    bool *out = (bool *)duckdb_vector_get_data(output);
    for (row = 0; row < count; row++) {
        uint64_t tls_config_id = arg_u64(duckdb_data_chunk_get_vector(input, 0), row, 0);
        ducknng_tls_config *cfg;
        if (!ctx || !ctx->rt || tls_config_id == 0) {
            duckdb_scalar_function_set_error(info, "ducknng: tls config id is required");
            return;
        }
        cfg = ducknng_runtime_remove_tls_config(ctx->rt, tls_config_id);
        if (!cfg) {
            duckdb_scalar_function_set_error(info, "ducknng: tls config not found");
            return;
        }
        if (cfg->source) duckdb_free(cfg->source);
        ducknng_tls_opts_reset(&cfg->opts);
        duckdb_free(cfg);
        out[row] = true;
    }
}

static void ducknng_set_tls_peer_allowlist_scalar(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
    idx_t count = duckdb_data_chunk_get_size(input);
    idx_t row;
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_scalar_function_get_extra_info(info);
    if (ducknng_reject_scalar_inside_authorizer(info, ctx)) return;
    bool *out = (bool *)duckdb_vector_get_data(output);
    for (row = 0; row < count; row++) {
        uint64_t tls_config_id = arg_u64(duckdb_data_chunk_get_vector(input, 0), row, 0);
        char *identities_json = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 1), row);
        ducknng_tls_config *cfg = NULL;
        char *errmsg = NULL;
        size_t i;
        if (!ctx || !ctx->rt || tls_config_id == 0) {
            if (identities_json) duckdb_free(identities_json);
            duckdb_scalar_function_set_error(info, "ducknng: tls config id is required");
            return;
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
            if (identities_json) duckdb_free(identities_json);
            duckdb_scalar_function_set_error(info, "ducknng: tls config not found");
            return;
        }
        if (ducknng_tls_opts_set_peer_allowlist(&cfg->opts, identities_json, &errmsg) != 0) {
            ducknng_mutex_unlock(&ctx->rt->mu);
            if (identities_json) duckdb_free(identities_json);
            duckdb_scalar_function_set_error(info, errmsg ? errmsg : "ducknng: failed to set TLS peer allowlist");
            if (errmsg) duckdb_free(errmsg);
            return;
        }
        ducknng_mutex_unlock(&ctx->rt->mu);
        if (identities_json) duckdb_free(identities_json);
        out[row] = true;
    }
}

static void ducknng_set_service_peer_allowlist_scalar(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
    idx_t count = duckdb_data_chunk_get_size(input);
    idx_t row;
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_scalar_function_get_extra_info(info);
    if (ducknng_reject_scalar_inside_authorizer(info, ctx)) return;
    bool *out = (bool *)duckdb_vector_get_data(output);
    for (row = 0; row < count; row++) {
        char *name = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 0), row);
        char *identities_json = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 1), row);
        ducknng_service *svc = NULL;
        char *errmsg = NULL;
        size_t i;
        if (!ctx || !ctx->rt || !name || !name[0]) {
            if (name) duckdb_free(name);
            if (identities_json) duckdb_free(identities_json);
            duckdb_scalar_function_set_error(info, "ducknng: service name is required");
            return;
        }
        ducknng_mutex_lock(&ctx->rt->mu);
        for (i = 0; i < ctx->rt->service_count; i++) {
            if (ctx->rt->services[i] && ctx->rt->services[i]->name && strcmp(ctx->rt->services[i]->name, name) == 0) {
                svc = ctx->rt->services[i];
                break;
            }
        }
        if (!svc) {
            ducknng_mutex_unlock(&ctx->rt->mu);
            duckdb_free(name);
            if (identities_json) duckdb_free(identities_json);
            duckdb_scalar_function_set_error(info, "ducknng: service not found");
            return;
        }
        if (ducknng_service_set_peer_allowlist(svc, identities_json, &errmsg) != 0) {
            ducknng_mutex_unlock(&ctx->rt->mu);
            duckdb_free(name);
            if (identities_json) duckdb_free(identities_json);
            duckdb_scalar_function_set_error(info, errmsg ? errmsg : "ducknng: failed to set service peer allowlist");
            if (errmsg) duckdb_free(errmsg);
            return;
        }
        ducknng_mutex_unlock(&ctx->rt->mu);
        duckdb_free(name);
        if (identities_json) duckdb_free(identities_json);
        out[row] = true;
    }
}

static void ducknng_set_service_ip_allowlist_scalar(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
    idx_t count = duckdb_data_chunk_get_size(input);
    idx_t row;
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_scalar_function_get_extra_info(info);
    if (ducknng_reject_scalar_inside_authorizer(info, ctx)) return;
    bool *out = (bool *)duckdb_vector_get_data(output);
    for (row = 0; row < count; row++) {
        char *name = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 0), row);
        char *cidrs_json = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 1), row);
        ducknng_service *svc = NULL;
        char *errmsg = NULL;
        size_t i;
        if (!ctx || !ctx->rt || !name || !name[0]) {
            if (name) duckdb_free(name);
            if (cidrs_json) duckdb_free(cidrs_json);
            duckdb_scalar_function_set_error(info, "ducknng: service name is required");
            return;
        }
        ducknng_mutex_lock(&ctx->rt->mu);
        for (i = 0; i < ctx->rt->service_count; i++) {
            if (ctx->rt->services[i] && ctx->rt->services[i]->name && strcmp(ctx->rt->services[i]->name, name) == 0) {
                svc = ctx->rt->services[i];
                break;
            }
        }
        if (!svc) {
            ducknng_mutex_unlock(&ctx->rt->mu);
            duckdb_free(name);
            if (cidrs_json) duckdb_free(cidrs_json);
            duckdb_scalar_function_set_error(info, "ducknng: service not found");
            return;
        }
        if (ducknng_service_set_ip_allowlist(svc, cidrs_json, &errmsg) != 0) {
            ducknng_mutex_unlock(&ctx->rt->mu);
            duckdb_free(name);
            if (cidrs_json) duckdb_free(cidrs_json);
            duckdb_scalar_function_set_error(info, errmsg ? errmsg : "ducknng: failed to set service IP allowlist");
            if (errmsg) duckdb_free(errmsg);
            return;
        }
        ducknng_mutex_unlock(&ctx->rt->mu);
        duckdb_free(name);
        if (cidrs_json) duckdb_free(cidrs_json);
        out[row] = true;
    }
}

static void ducknng_set_service_limits_scalar(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
    idx_t count = duckdb_data_chunk_get_size(input);
    idx_t row;
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_scalar_function_get_extra_info(info);
    if (ducknng_reject_scalar_inside_authorizer(info, ctx)) return;
    bool *out = (bool *)duckdb_vector_get_data(output);
    for (row = 0; row < count; row++) {
        char *name = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 0), row);
        uint64_t max_open_sessions = arg_u64(duckdb_data_chunk_get_vector(input, 1), row, 0);
        ducknng_service *svc = NULL;
        char *errmsg = NULL;
        size_t i;
        if (!ctx || !ctx->rt || !name || !name[0]) {
            if (name) duckdb_free(name);
            duckdb_scalar_function_set_error(info, "ducknng: service name is required");
            return;
        }
        ducknng_mutex_lock(&ctx->rt->mu);
        for (i = 0; i < ctx->rt->service_count; i++) {
            if (ctx->rt->services[i] && ctx->rt->services[i]->name && strcmp(ctx->rt->services[i]->name, name) == 0) {
                svc = ctx->rt->services[i];
                break;
            }
        }
        if (!svc) {
            ducknng_mutex_unlock(&ctx->rt->mu);
            duckdb_free(name);
            duckdb_scalar_function_set_error(info, "ducknng: service not found");
            return;
        }
        if (ducknng_service_set_limits(svc, max_open_sessions, &errmsg) != 0) {
            ducknng_mutex_unlock(&ctx->rt->mu);
            duckdb_free(name);
            duckdb_scalar_function_set_error(info, errmsg ? errmsg : "ducknng: failed to set service limits");
            if (errmsg) duckdb_free(errmsg);
            return;
        }
        ducknng_mutex_unlock(&ctx->rt->mu);
        duckdb_free(name);
        out[row] = true;
    }
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

static int ducknng_client_open_req_socket_tls(const char *url, int timeout_ms, const ducknng_tls_opts *tls_opts, nng_socket *out, char **errmsg);
static int ducknng_lookup_tls_opts(ducknng_sql_context *ctx, uint64_t tls_config_id, const ducknng_tls_opts **out_opts, char **errmsg);
static int ducknng_wait_any_for_ids(ducknng_runtime *rt, const uint64_t *aio_ids, idx_t aio_id_count, int32_t wait_ms);
static int ducknng_socket_is_active(const ducknng_client_socket *sock);
static int ducknng_socket_is_req_protocol(const ducknng_client_socket *sock);

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

static ducknng_client_aio *ducknng_find_client_aio_locked(ducknng_runtime *rt, uint64_t aio_id) {
    size_t i;
    if (!rt || aio_id == 0) return NULL;
    for (i = 0; i < rt->client_aio_count; i++) {
        if (rt->client_aios[i] && rt->client_aios[i]->aio_id == aio_id) return rt->client_aios[i];
    }
    return NULL;
}

static nng_msg *ducknng_client_raw_request_message(const uint8_t *payload, size_t payload_len, char **errmsg) {
    nng_msg *req = NULL;
    int rv = nng_msg_alloc(&req, payload_len);
    if (rv != 0) {
        if (errmsg && !*errmsg) *errmsg = ducknng_strdup(ducknng_nng_strerror(rv));
        return NULL;
    }
    if (payload_len) memcpy(nng_msg_body(req), payload, payload_len);
    return req;
}

static void ducknng_client_aio_clear_http_handles(ducknng_client_aio *slot) {
    if (!slot) return;
    if (slot->http_res) {
        nng_http_res_free(slot->http_res);
        slot->http_res = NULL;
    }
    if (slot->http_req) {
        nng_http_req_free(slot->http_req);
        slot->http_req = NULL;
    }
    if (slot->http_client) {
        nng_http_client_free(slot->http_client);
        slot->http_client = NULL;
    }
    if (slot->http_url) {
        nng_url_free(slot->http_url);
        slot->http_url = NULL;
    }
}

static void ducknng_client_aio_cb(void *arg) {
    ducknng_client_aio *slot = (ducknng_client_aio *)arg;
    ducknng_runtime *rt;
    int rv;
    if (!slot || !(rt = slot->rt) || !slot->aio) return;
    rv = ducknng_aio_result(slot->aio);
    ducknng_mutex_lock(&rt->mu);
    if (slot->state != DUCKNNG_CLIENT_AIO_PENDING) {
        ducknng_mutex_unlock(&rt->mu);
        return;
    }
    if (slot->kind == DUCKNNG_CLIENT_AIO_KIND_NCURL) {
        slot->send_done = 1;
        slot->recv_done = 1;
        slot->send_result = rv;
        slot->recv_result = rv;
    } else if (slot->phase == DUCKNNG_CLIENT_AIO_PHASE_SEND) {
        slot->send_done = 1;
        slot->send_result = rv;
    } else if (slot->phase == DUCKNNG_CLIENT_AIO_PHASE_RECV) {
        slot->recv_done = 1;
        slot->recv_result = rv;
    }
    if (rv != 0) {
        nng_msg *pending_msg = ducknng_aio_get_msg(slot->aio);
        if (pending_msg) {
            nng_msg_free(pending_msg);
            ducknng_aio_set_msg(slot->aio, NULL);
        }
        if (slot->error) duckdb_free(slot->error);
        slot->error = ducknng_strdup(ducknng_nng_strerror(rv));
        slot->state = rv == NNG_ECANCELED ? DUCKNNG_CLIENT_AIO_CANCELLED : DUCKNNG_CLIENT_AIO_ERROR;
        slot->finished_ms = ducknng_now_ms();
        if (slot->socket_ref) {
            ducknng_runtime_release_client_socket(slot->socket_ref);
            slot->socket_ref = NULL;
        }
        if (slot->kind == DUCKNNG_CLIENT_AIO_KIND_NCURL) ducknng_client_aio_clear_http_handles(slot);
        ducknng_cond_broadcast(&rt->aio_cv);
        ducknng_mutex_unlock(&rt->mu);
        return;
    }
    if (slot->kind == DUCKNNG_CLIENT_AIO_KIND_NCURL) {
        uint16_t status = 0;
        char *headers_json = NULL;
        uint8_t *body = NULL;
        size_t body_len = 0;
        char *errmsg = NULL;
        if (ducknng_http_response_copy(slot->http_res, &status, &headers_json, &body, &body_len, &errmsg) != 0) {
            if (slot->error) duckdb_free(slot->error);
            slot->error = errmsg ? errmsg : ducknng_strdup("ducknng: failed to copy HTTP response");
            slot->state = DUCKNNG_CLIENT_AIO_ERROR;
        } else {
            slot->http_status = status;
            slot->http_headers_json = headers_json;
            slot->http_body = body;
            slot->http_body_len = body_len;
            if (body && body_len > 0 && ducknng_bytes_look_text(body, body_len)) {
                slot->http_body_text = ducknng_dup_bytes(body, body_len);
                if (!slot->http_body_text) {
                    slot->error = ducknng_strdup("ducknng: out of memory copying HTTP response text");
                    slot->state = DUCKNNG_CLIENT_AIO_ERROR;
                } else {
                    slot->state = DUCKNNG_CLIENT_AIO_READY;
                }
            } else {
                slot->state = DUCKNNG_CLIENT_AIO_READY;
            }
        }
        ducknng_client_aio_clear_http_handles(slot);
        slot->finished_ms = ducknng_now_ms();
        ducknng_cond_broadcast(&rt->aio_cv);
        ducknng_mutex_unlock(&rt->mu);
        return;
    }
    if (slot->kind == DUCKNNG_CLIENT_AIO_KIND_REQUEST && slot->phase == DUCKNNG_CLIENT_AIO_PHASE_SEND) {
        slot->phase = DUCKNNG_CLIENT_AIO_PHASE_RECV;
        ducknng_mutex_unlock(&rt->mu);
        ducknng_ctx_recv_aio(slot->ctx, slot->aio);
        return;
    }
    if (slot->phase == DUCKNNG_CLIENT_AIO_PHASE_RECV) {
        slot->reply_msg = ducknng_aio_get_msg(slot->aio);
        ducknng_aio_set_msg(slot->aio, NULL);
    }
    slot->state = DUCKNNG_CLIENT_AIO_READY;
    slot->finished_ms = ducknng_now_ms();
    if (slot->socket_ref) {
        ducknng_runtime_release_client_socket(slot->socket_ref);
        slot->socket_ref = NULL;
    }
    ducknng_cond_broadcast(&rt->aio_cv);
    ducknng_mutex_unlock(&rt->mu);
}

static ducknng_client_aio *ducknng_client_aio_alloc_slot(ducknng_runtime *rt, int timeout_ms, char **errmsg) {
    ducknng_client_aio *slot;
    if (!rt) return NULL;
    slot = (ducknng_client_aio *)duckdb_malloc(sizeof(*slot));
    if (!slot) {
        if (errmsg && !*errmsg) *errmsg = ducknng_strdup("ducknng: out of memory allocating aio slot");
        return NULL;
    }
    memset(slot, 0, sizeof(*slot));
    slot->rt = rt;
    slot->timeout_ms = timeout_ms;
    slot->state = DUCKNNG_CLIENT_AIO_PENDING;
    slot->send_result = -1;
    slot->recv_result = -1;
    slot->started_ms = ducknng_now_ms();
    if (ducknng_aio_alloc(&slot->aio, ducknng_client_aio_cb, slot, timeout_ms) != 0) {
        if (errmsg && !*errmsg) *errmsg = ducknng_strdup("ducknng: failed to allocate nng aio");
        duckdb_free(slot);
        return NULL;
    }
    return slot;
}

static ducknng_client_aio *ducknng_client_prepare_url_aio(ducknng_runtime *rt, const char *url,
    int timeout_ms, const ducknng_tls_opts *tls_opts, char **errmsg) {
    ducknng_client_aio *slot = ducknng_client_aio_alloc_slot(rt, timeout_ms, errmsg);
    int rv;
    if (!slot) return NULL;
    if (ducknng_client_open_req_socket_tls(url, timeout_ms, tls_opts, &slot->sock, errmsg) != 0) {
        ducknng_client_aio_destroy(slot);
        return NULL;
    }
    slot->owns_socket = 1;
    slot->open = 1;
    rv = ducknng_ctx_open(&slot->ctx, slot->sock);
    if (rv != 0) {
        if (errmsg && !*errmsg) *errmsg = ducknng_strdup(ducknng_nng_strerror(rv));
        ducknng_client_aio_destroy(slot);
        return NULL;
    }
    slot->has_ctx = 1;
    return slot;
}

static ducknng_client_aio *ducknng_client_prepare_socket_request_aio(ducknng_runtime *rt, uint64_t socket_id,
    int timeout_ms, char **errmsg) {
    ducknng_client_socket *sock;
    ducknng_client_aio *slot;
    int rv;
    if (!rt || socket_id == 0) {
        if (errmsg && !*errmsg) *errmsg = ducknng_strdup("ducknng: connected req client socket is required for aio request");
        return NULL;
    }
    sock = ducknng_runtime_acquire_client_socket(rt, socket_id);
    if (!sock || !sock->open || !sock->connected || !ducknng_socket_is_req_protocol(sock)) {
        if (sock) ducknng_runtime_release_client_socket(sock);
        if (errmsg && !*errmsg) *errmsg = ducknng_strdup("ducknng: connected req client socket is required for aio request");
        return NULL;
    }
    slot = ducknng_client_aio_alloc_slot(rt, timeout_ms, errmsg);
    if (!slot) {
        ducknng_runtime_release_client_socket(sock);
        return NULL;
    }
    slot->socket_ref = sock;
    slot->sock = sock->sock;
    slot->socket_id = sock->socket_id;
    rv = ducknng_ctx_open(&slot->ctx, sock->sock);
    if (rv != 0) {
        if (errmsg && !*errmsg) *errmsg = ducknng_strdup(ducknng_nng_strerror(rv));
        ducknng_client_aio_destroy(slot);
        return NULL;
    }
    slot->has_ctx = 1;
    return slot;
}

static ducknng_client_aio *ducknng_client_prepare_socket_raw_aio(ducknng_runtime *rt, uint64_t socket_id,
    int timeout_ms, char **errmsg) {
    ducknng_client_socket *sock;
    ducknng_client_aio *slot;
    if (!rt || socket_id == 0) {
        if (errmsg && !*errmsg) *errmsg = ducknng_strdup("ducknng: active client socket is required for aio socket operation");
        return NULL;
    }
    sock = ducknng_runtime_acquire_client_socket(rt, socket_id);
    if (!ducknng_socket_is_active(sock)) {
        if (sock) ducknng_runtime_release_client_socket(sock);
        if (errmsg && !*errmsg) *errmsg = ducknng_strdup("ducknng: active client socket is required for aio socket operation");
        return NULL;
    }
    slot = ducknng_client_aio_alloc_slot(rt, timeout_ms, errmsg);
    if (!slot) {
        ducknng_runtime_release_client_socket(sock);
        return NULL;
    }
    slot->socket_ref = sock;
    slot->sock = sock->sock;
    slot->socket_id = sock->socket_id;
    return slot;
}

static int ducknng_client_launch_request_aio(ducknng_runtime *rt, ducknng_client_aio *slot, nng_msg *req, char **errmsg) {
    if (!rt || !slot || !req) {
        if (req) nng_msg_free(req);
        if (errmsg && !*errmsg) *errmsg = ducknng_strdup("ducknng: missing aio launch state");
        return -1;
    }
    if (ducknng_runtime_add_client_aio(rt, slot, errmsg) != 0) {
        nng_msg_free(req);
        ducknng_client_aio_destroy(slot);
        return -1;
    }
    slot->kind = DUCKNNG_CLIENT_AIO_KIND_REQUEST;
    slot->phase = DUCKNNG_CLIENT_AIO_PHASE_SEND;
    ducknng_aio_set_msg(slot->aio, req);
    ducknng_ctx_send_aio(slot->ctx, slot->aio);
    return 0;
}

static int ducknng_client_launch_socket_send_aio(ducknng_runtime *rt, ducknng_client_aio *slot, nng_msg *msg, char **errmsg) {
    if (!rt || !slot || !msg) {
        if (msg) nng_msg_free(msg);
        if (errmsg && !*errmsg) *errmsg = ducknng_strdup("ducknng: missing aio send state");
        return -1;
    }
    if (ducknng_runtime_add_client_aio(rt, slot, errmsg) != 0) {
        nng_msg_free(msg);
        ducknng_client_aio_destroy(slot);
        return -1;
    }
    slot->kind = DUCKNNG_CLIENT_AIO_KIND_SEND;
    slot->phase = DUCKNNG_CLIENT_AIO_PHASE_SEND;
    ducknng_aio_set_msg(slot->aio, msg);
    ducknng_socket_send_aio(slot->sock, slot->aio);
    return 0;
}

static int ducknng_client_launch_socket_recv_aio(ducknng_runtime *rt, ducknng_client_aio *slot, char **errmsg) {
    if (!rt || !slot) {
        if (errmsg && !*errmsg) *errmsg = ducknng_strdup("ducknng: missing aio recv state");
        return -1;
    }
    if (ducknng_runtime_add_client_aio(rt, slot, errmsg) != 0) {
        ducknng_client_aio_destroy(slot);
        return -1;
    }
    slot->kind = DUCKNNG_CLIENT_AIO_KIND_RECV;
    slot->phase = DUCKNNG_CLIENT_AIO_PHASE_RECV;
    ducknng_socket_recv_aio(slot->sock, slot->aio);
    return 0;
}

static int ducknng_client_launch_url_request_aio(ducknng_sql_context *ctx, const char *url,
    int32_t timeout_ms, uint64_t tls_config_id, nng_msg *req, uint64_t *out_aio_id, char **errmsg) {
    const ducknng_tls_opts *tls_opts = NULL;
    ducknng_client_aio *slot = NULL;
    if (out_aio_id) *out_aio_id = 0;
    if (!ctx || !ctx->rt || !url || !req) {
        if (req) nng_msg_free(req);
        if (errmsg && !*errmsg) *errmsg = ducknng_strdup("ducknng: missing async RPC request state");
        return -1;
    }
    if (ducknng_lookup_tls_opts(ctx, tls_config_id, &tls_opts, errmsg) != 0) {
        nng_msg_free(req);
        return -1;
    }
    slot = ducknng_client_prepare_url_aio(ctx->rt, url, timeout_ms, tls_opts, errmsg);
    if (!slot) {
        nng_msg_free(req);
        return -1;
    }
    if (ducknng_client_launch_request_aio(ctx->rt, slot, req, errmsg) != 0) {
        return -1;
    }
    if (out_aio_id) *out_aio_id = slot->aio_id;
    return 0;
}

static int ducknng_client_launch_ncurl_aio(ducknng_sql_context *ctx, const char *url,
    const char *method, const char *headers_json, const uint8_t *body, size_t body_len,
    int32_t timeout_ms, uint64_t tls_config_id, uint64_t *out_aio_id, char **errmsg) {
    const ducknng_tls_opts *tls_opts = NULL;
    ducknng_client_aio *slot = NULL;
    if (out_aio_id) *out_aio_id = 0;
    if (!ctx || !ctx->rt || !url || !url[0]) {
        if (errmsg && !*errmsg) *errmsg = ducknng_strdup("ducknng: ncurl_aio URL must not be NULL or empty");
        return -1;
    }
    if (ducknng_lookup_tls_opts(ctx, tls_config_id, &tls_opts, errmsg) != 0) return -1;
    slot = ducknng_client_aio_alloc_slot(ctx->rt, timeout_ms, errmsg);
    if (!slot) return -1;
    slot->kind = DUCKNNG_CLIENT_AIO_KIND_NCURL;
    slot->phase = DUCKNNG_CLIENT_AIO_PHASE_HTTP;
    if (ducknng_http_transact_aio_prepare(url, method, headers_json, body, body_len, tls_opts,
            &slot->http_url, &slot->http_client, &slot->http_req, &slot->http_res, errmsg) != 0) {
        ducknng_client_aio_destroy(slot);
        return -1;
    }
    if (ducknng_runtime_add_client_aio(ctx->rt, slot, errmsg) != 0) {
        ducknng_client_aio_destroy(slot);
        return -1;
    }
    nng_http_client_transact(slot->http_client, slot->http_req, slot->http_res, slot->aio);
    if (out_aio_id) *out_aio_id = slot->aio_id;
    return 0;
}

static const char *ducknng_json_find_key(const char *json, const char *key) {
    char needle[128];
    const char *p;
    if (!json || !key) return NULL;
    if (snprintf(needle, sizeof(needle), "\"%s\"", key) >= (int)sizeof(needle)) return NULL;
    p = strstr(json, needle);
    if (!p) return NULL;
    p = strchr(p + strlen(needle), ':');
    if (!p) return NULL;
    p++;
    while (*p == ' ' || *p == '\t' || *p == '\n' || *p == '\r') p++;
    return p;
}

static int ducknng_json_extract_u64_value(const char *json, const char *key, uint64_t *out) {
    const char *p = ducknng_json_find_key(json, key);
    char *end = NULL;
    if (out) *out = 0;
    if (!p || !out) return -1;
    if (*p == '"') p++;
    *out = (uint64_t)strtoull(p, &end, 10);
    return end == p ? -1 : 0;
}

static char *ducknng_json_extract_string_dup(const char *json, const char *key) {
    const char *p = ducknng_json_find_key(json, key);
    const char *end;
    char *out;
    size_t len;
    if (!p || *p != '"') return NULL;
    p++;
    end = strchr(p, '"');
    if (!end) return NULL;
    len = (size_t)(end - p);
    out = (char *)duckdb_malloc(len + 1);
    if (!out) return NULL;
    if (len) memcpy(out, p, len);
    out[len] = '\0';
    return out;
}

static char *ducknng_session_json_request(uint64_t session_id, const char *session_token,
    uint64_t batch_rows, uint64_t batch_bytes) {
    char buf[320];
    int n;
    if (!session_token || !session_token[0]) return NULL;
    n = snprintf(buf, sizeof(buf), "{\"session_id\":%llu,\"session_token\":\"%s\"",
        (unsigned long long)session_id, session_token);
    if (n < 0 || (size_t)n >= sizeof(buf)) return NULL;
    if (batch_rows > 0) {
        n += snprintf(buf + n, sizeof(buf) - (size_t)n, ",\"batch_rows\":%llu", (unsigned long long)batch_rows);
        if ((size_t)n >= sizeof(buf)) return NULL;
    }
    if (batch_bytes > 0) {
        n += snprintf(buf + n, sizeof(buf) - (size_t)n, ",\"batch_bytes\":%llu", (unsigned long long)batch_bytes);
        if ((size_t)n >= sizeof(buf)) return NULL;
    }
    n += snprintf(buf + n, sizeof(buf) - (size_t)n, "}");
    if ((size_t)n >= sizeof(buf)) return NULL;
    return ducknng_strdup(buf);
}

static nng_msg *ducknng_client_method_request(const char *method_name, const void *payload,
    size_t payload_len, char **errmsg) {
    nng_msg *msg;
    msg = ducknng_build_reply(DUCKNNG_RPC_CALL, method_name, 0, NULL, payload, (uint64_t)payload_len);
    if (!msg && errmsg && !*errmsg) {
        *errmsg = ducknng_strdup("ducknng: failed to allocate RPC request message");
    }
    return msg;
}

static nng_msg *ducknng_client_roundtrip_tls(const char *url, nng_msg *req, int timeout_ms,
    const ducknng_tls_opts *tls_opts, char **errmsg);

static nng_msg *ducknng_client_method_roundtrip_tls(const char *url, const char *method_name,
    const void *payload, size_t payload_len, int timeout_ms, const ducknng_tls_opts *tls_opts,
    char **errmsg) {
    nng_msg *req = ducknng_client_method_request(method_name, payload, payload_len, errmsg);
    if (!req) return NULL;
    return ducknng_client_roundtrip_tls(url, req, timeout_ms, tls_opts, errmsg);
}

static void ducknng_session_result_fill_from_response(ducknng_session_result_bind_data *bind,
    uint64_t fallback_session_id, nng_msg *resp_msg) {
    ducknng_frame frame;
    if (!bind) return;
    bind->session_id = fallback_session_id;
    if (!resp_msg) {
        bind->ok = false;
        bind->error = ducknng_strdup("ducknng: missing session response");
        return;
    }
    if (ducknng_decode_request(resp_msg, &frame) != 0) {
        bind->ok = false;
        bind->error = ducknng_strdup("ducknng: invalid session response envelope");
        return;
    }
    if (frame.type == DUCKNNG_RPC_ERROR) {
        bind->ok = false;
        bind->error = ducknng_frame_error_detail(&frame, "ducknng: session request failed");
        return;
    }
    if (frame.type != DUCKNNG_RPC_RESULT) {
        bind->ok = false;
        bind->error = ducknng_strdup("ducknng: session response was not an RPC result");
        return;
    }
    bind->end_of_stream = (frame.flags & DUCKNNG_RPC_FLAG_END_OF_STREAM) != 0;
    if (frame.flags & DUCKNNG_RPC_FLAG_PAYLOAD_JSON) {
        bind->control_json = (char *)duckdb_malloc((size_t)frame.payload_len + 1);
        if (!bind->control_json) {
            bind->ok = false;
            bind->error = ducknng_strdup("ducknng: out of memory copying session control payload");
            return;
        }
        if (frame.payload_len) memcpy(bind->control_json, frame.payload, (size_t)frame.payload_len);
        bind->control_json[frame.payload_len] = '\0';
        (void)ducknng_json_extract_u64_value(bind->control_json, "session_id", &bind->session_id);
        if (!bind->session_token) bind->session_token = ducknng_json_extract_string_dup(bind->control_json, "session_token");
        bind->state = ducknng_json_extract_string_dup(bind->control_json, "state");
        bind->next_method = ducknng_json_extract_string_dup(bind->control_json, "next_method");
        if (ducknng_json_extract_u64_value(bind->control_json, "idle_timeout_ms", &bind->idle_timeout_ms) == 0) {
            bind->has_idle_timeout_ms = 1;
        }
        bind->ok = true;
        return;
    }
    if ((frame.flags & DUCKNNG_RPC_FLAG_PAYLOAD_ARROW_STREAM) && (frame.flags & DUCKNNG_RPC_FLAG_RESULT_ROWS)) {
        bind->payload_len = (idx_t)frame.payload_len;
        bind->payload = (uint8_t *)duckdb_malloc((size_t)bind->payload_len);
        if (!bind->payload && bind->payload_len > 0) {
            bind->ok = false;
            bind->error = ducknng_strdup("ducknng: out of memory copying session row payload");
            return;
        }
        if (bind->payload_len) memcpy(bind->payload, frame.payload, (size_t)bind->payload_len);
        bind->ok = true;
        return;
    }
    bind->ok = false;
    bind->error = ducknng_strdup("ducknng: unexpected session response payload kind");
}

static int ducknng_client_open_req_socket_tls(const char *url, int timeout_ms, const ducknng_tls_opts *tls_opts, nng_socket *out, char **errmsg) {
    int rv;
    if (!url || !url[0] || !out) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: client URL is required");
        return -1;
    }
    if (ducknng_validate_nng_url(url, errmsg) != 0) return -1;
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
    rv = ducknng_socket_apply_tls(*out, url, tls_opts);
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

static int ducknng_client_open_req_socket(const char *url, int timeout_ms, nng_socket *out, char **errmsg) {
    return ducknng_client_open_req_socket_tls(url, timeout_ms, NULL, out, errmsg);
}

static nng_msg *ducknng_client_roundtrip_tls(const char *url, nng_msg *req, int timeout_ms, const ducknng_tls_opts *tls_opts, char **errmsg) {
    ducknng_transport_url parsed;
    nng_socket sock;
    nng_msg *resp = NULL;
    uint8_t *reply_frame = NULL;
    size_t reply_frame_len = 0;
    int rv;
    memset(&sock, 0, sizeof(sock));
    if (!req) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: request message is required");
        return NULL;
    }
    if (ducknng_transport_url_parse(url, &parsed, errmsg) != 0) {
        nng_msg_free(req);
        return NULL;
    }
    if (ducknng_transport_url_is_http(&parsed)) {
        if (ducknng_http_frame_transact(url, (const uint8_t *)nng_msg_body(req), nng_msg_len(req),
                timeout_ms, tls_opts, &reply_frame, &reply_frame_len, errmsg) != 0) {
            nng_msg_free(req);
            return NULL;
        }
        nng_msg_free(req);
        rv = nng_msg_alloc(&resp, reply_frame_len);
        if (rv != 0) {
            if (reply_frame) duckdb_free(reply_frame);
            if (errmsg && !*errmsg) *errmsg = ducknng_strdup(ducknng_nng_strerror(rv));
            return NULL;
        }
        if (reply_frame_len) memcpy(nng_msg_body(resp), reply_frame, reply_frame_len);
        if (reply_frame) duckdb_free(reply_frame);
        return resp;
    }
    if (ducknng_client_open_req_socket_tls(url, timeout_ms, tls_opts, &sock, errmsg) != 0) {
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

static nng_msg *ducknng_client_roundtrip(const char *url, nng_msg *req, int timeout_ms, char **errmsg) {
    return ducknng_client_roundtrip_tls(url, req, timeout_ms, NULL, errmsg);
}

static nng_msg *ducknng_client_roundtrip_raw_tls(const char *url, const uint8_t *payload, size_t payload_len,
    int timeout_ms, const ducknng_tls_opts *tls_opts, char **errmsg) {
    nng_msg *req = NULL;
    int rv = nng_msg_alloc(&req, payload_len);
    if (rv != 0) {
        if (errmsg) *errmsg = ducknng_strdup(ducknng_nng_strerror(rv));
        return NULL;
    }
    if (payload_len) memcpy(nng_msg_body(req), payload, payload_len);
    return ducknng_client_roundtrip_tls(url, req, timeout_ms, tls_opts, errmsg);
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

static void ducknng_get_rpc_manifest_raw_scalar(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
    idx_t count = duckdb_data_chunk_get_size(input);
    idx_t row;
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_scalar_function_get_extra_info(info);
    if (ducknng_reject_scalar_inside_authorizer(info, ctx)) return;
    for (row = 0; row < count; row++) {
        char *url = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 0), row);
        uint64_t tls_config_id = arg_u64(duckdb_data_chunk_get_vector(input, 1), row, 0);
        const ducknng_tls_opts *tls_opts = NULL;
        char *errmsg = NULL;
        nng_msg *resp_msg;
        if (!url || ducknng_lookup_tls_opts(ctx, tls_config_id, &tls_opts, &errmsg) != 0) {
            if (url) duckdb_free(url);
            if (errmsg) duckdb_free(errmsg);
            set_null(output, row);
            continue;
        }
        resp_msg = ducknng_client_roundtrip_tls(url, ducknng_client_manifest_request(), 5000, tls_opts, &errmsg);
        duckdb_free(url);
        if (!resp_msg) {
            if (errmsg) duckdb_free(errmsg);
            set_null(output, row);
            continue;
        }
        assign_blob(output, row, (const uint8_t *)nng_msg_body(resp_msg), (idx_t)nng_msg_len(resp_msg));
        nng_msg_free(resp_msg);
    }
}

static void ducknng_run_rpc_raw_scalar(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
    idx_t count = duckdb_data_chunk_get_size(input);
    idx_t row;
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_scalar_function_get_extra_info(info);
    if (ducknng_reject_scalar_inside_authorizer(info, ctx)) return;
    for (row = 0; row < count; row++) {
        char *url = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 0), row);
        char *sql = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 1), row);
        uint64_t tls_config_id = arg_u64(duckdb_data_chunk_get_vector(input, 2), row, 0);
        const ducknng_tls_opts *tls_opts = NULL;
        char *errmsg = NULL;
        nng_msg *req = NULL;
        nng_msg *resp_msg;
        if (!url || !sql || ducknng_lookup_tls_opts(ctx, tls_config_id, &tls_opts, &errmsg) != 0) {
            if (url) duckdb_free(url);
            if (sql) duckdb_free(sql);
            if (errmsg) duckdb_free(errmsg);
            set_null(output, row);
            continue;
        }
        req = ducknng_client_exec_request(sql, 0, &errmsg);
        duckdb_free(sql);
        if (!req) {
            duckdb_free(url);
            if (errmsg) duckdb_free(errmsg);
            set_null(output, row);
            continue;
        }
        resp_msg = ducknng_client_roundtrip_tls(url, req, 5000, tls_opts, &errmsg);
        duckdb_free(url);
        if (!resp_msg) {
            if (errmsg) duckdb_free(errmsg);
            set_null(output, row);
            continue;
        }
        assign_blob(output, row, (const uint8_t *)nng_msg_body(resp_msg), (idx_t)nng_msg_len(resp_msg));
        nng_msg_free(resp_msg);
    }
}

static int ducknng_socket_is_active(const ducknng_client_socket *sock) {
    return sock && sock->open && (sock->connected || sock->has_listener);
}

static int ducknng_socket_is_req_protocol(const ducknng_client_socket *sock) {
    return sock && sock->protocol && strcmp(sock->protocol, "req") == 0;
}

static void ducknng_socket_scalar(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
    idx_t count = duckdb_data_chunk_get_size(input);
    idx_t row;
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_scalar_function_get_extra_info(info);
    if (ducknng_reject_scalar_inside_authorizer(info, ctx)) return;
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
        if (ducknng_mutex_init(&sock->mu) != 0) {
            duckdb_free(sock->protocol);
            duckdb_free(sock);
            duckdb_scalar_function_set_error(info, "ducknng: failed to initialize client socket mutex");
            return;
        }
        sock->mu_initialized = 1;
        if (ducknng_cond_init(&sock->cv) != 0) {
            ducknng_mutex_destroy(&sock->mu);
            sock->mu_initialized = 0;
            duckdb_free(sock->protocol);
            duckdb_free(sock);
            duckdb_scalar_function_set_error(info, "ducknng: failed to initialize client socket condition variable");
            return;
        }
        sock->cv_initialized = 1;
        if (ducknng_socket_open_protocol(protocol, &sock->sock, &errmsg) != 0) {
            ducknng_client_socket_destroy(sock);
            duckdb_scalar_function_set_error(info, errmsg ? errmsg : "ducknng: failed to open socket protocol");
            if (errmsg) duckdb_free(errmsg);
            return;
        }
        rv = ducknng_ctx_open(&sock->ctx, sock->sock);
        if (rv == 0) {
            sock->has_ctx = 1;
        } else if (rv != NNG_ENOTSUP) {
            ducknng_socket_close(sock->sock);
            sock->open = 0;
            ducknng_client_socket_destroy(sock);
            duckdb_scalar_function_set_error(info, ducknng_nng_strerror(rv));
            return;
        }
        sock->open = 1;
        if (ducknng_runtime_add_client_socket(ctx->rt, sock, &errmsg) != 0) {
            if (sock->has_ctx) {
                ducknng_ctx_close(sock->ctx);
                sock->has_ctx = 0;
            }
            if (sock->open) {
                ducknng_socket_close(sock->sock);
                sock->open = 0;
            }
            ducknng_client_socket_destroy(sock);
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
    if (ducknng_reject_scalar_inside_authorizer(info, ctx)) return;
    bool *out = (bool *)duckdb_vector_get_data(output);
    for (row = 0; row < count; row++) {
        uint64_t socket_id = arg_u64(duckdb_data_chunk_get_vector(input, 0), row, 0);
        char *url = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 1), row);
        int32_t timeout_ms = arg_int32(duckdb_data_chunk_get_vector(input, 2), row, 5000);
        uint64_t tls_config_id = arg_u64(duckdb_data_chunk_get_vector(input, 3), row, 0);
        const ducknng_tls_opts *tls_opts = NULL;
        ducknng_client_socket *sock;
        char *errmsg = NULL;
        int rv;
        out[row] = false;
        if (!ctx || !ctx->rt || socket_id == 0 || !url) {
            if (url) duckdb_free(url);
            duckdb_scalar_function_set_error(info, "ducknng: socket id and URL are required");
            return;
        }
        if (ducknng_lookup_tls_opts(ctx, tls_config_id, &tls_opts, &errmsg) != 0) {
            duckdb_free(url);
            duckdb_scalar_function_set_error(info, errmsg ? errmsg : "ducknng: tls config not found");
            if (errmsg) duckdb_free(errmsg);
            return;
        }
        if (ducknng_validate_nng_url(url, &errmsg) != 0) {
            duckdb_free(url);
            duckdb_scalar_function_set_error(info, errmsg ? errmsg : "ducknng: invalid transport URL");
            if (errmsg) duckdb_free(errmsg);
            return;
        }
        sock = ducknng_runtime_acquire_client_socket(ctx->rt, socket_id);
        if (!sock) {
            duckdb_free(url);
            duckdb_scalar_function_set_error(info, "ducknng: client socket not found");
            return;
        }
        ducknng_mutex_lock(&sock->mu);
        if (!sock->open) {
            ducknng_mutex_unlock(&sock->mu);
            ducknng_runtime_release_client_socket(sock);
            duckdb_free(url);
            duckdb_scalar_function_set_error(info, "ducknng: client socket not found");
            return;
        }
        if (sock->connected) {
            ducknng_mutex_unlock(&sock->mu);
            ducknng_runtime_release_client_socket(sock);
            duckdb_free(url);
            duckdb_scalar_function_set_error(info, "ducknng: socket is already dialed");
            return;
        }
        rv = ducknng_socket_set_timeout_ms(sock->sock, timeout_ms, timeout_ms);
        if (rv == 0) rv = ducknng_socket_apply_tls(sock->sock, url, tls_opts);
        if (rv == 0) rv = ducknng_socket_dial(sock->sock, url);
        if (rv == 0) {
            if (sock->url) duckdb_free(sock->url);
            sock->url = url;
            sock->connected = 1;
            sock->send_timeout_ms = timeout_ms;
            sock->recv_timeout_ms = timeout_ms;
            out[row] = true;
            url = NULL;
        }
        ducknng_mutex_unlock(&sock->mu);
        ducknng_runtime_release_client_socket(sock);
        if (url) duckdb_free(url);
        if (rv != 0) {
            duckdb_scalar_function_set_error(info, ducknng_nng_strerror(rv));
            return;
        }
    }
}

static void ducknng_listen_scalar(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
    idx_t count = duckdb_data_chunk_get_size(input);
    idx_t row;
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_scalar_function_get_extra_info(info);
    if (ducknng_reject_scalar_inside_authorizer(info, ctx)) return;
    bool *out = (bool *)duckdb_vector_get_data(output);
    for (row = 0; row < count; row++) {
        uint64_t socket_id = arg_u64(duckdb_data_chunk_get_vector(input, 0), row, 0);
        char *url = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 1), row);
        uint64_t recv_max_bytes = arg_u64(duckdb_data_chunk_get_vector(input, 2), row, 0);
        uint64_t tls_config_id = arg_u64(duckdb_data_chunk_get_vector(input, 3), row, 0);
        const ducknng_tls_opts *tls_opts = NULL;
        ducknng_client_socket *sock;
        char *errmsg = NULL;
        nng_listener lst;
        char *resolved_url = NULL;
        int rv;
        out[row] = false;
        memset(&lst, 0, sizeof(lst));
        if (!ctx || !ctx->rt || socket_id == 0 || !url) {
            if (url) duckdb_free(url);
            duckdb_scalar_function_set_error(info, "ducknng: socket id and URL are required");
            return;
        }
        if (ducknng_lookup_tls_opts(ctx, tls_config_id, &tls_opts, &errmsg) != 0) {
            duckdb_free(url);
            duckdb_scalar_function_set_error(info, errmsg ? errmsg : "ducknng: tls config not found");
            if (errmsg) duckdb_free(errmsg);
            return;
        }
        if (ducknng_listener_validate_startup_url(url, tls_opts, &errmsg) != 0) {
            duckdb_free(url);
            duckdb_scalar_function_set_error(info, errmsg ? errmsg : "ducknng: invalid listen URL");
            if (errmsg) duckdb_free(errmsg);
            return;
        }
        sock = ducknng_runtime_acquire_client_socket(ctx->rt, socket_id);
        if (!sock) {
            duckdb_free(url);
            duckdb_scalar_function_set_error(info, "ducknng: client socket not found");
            return;
        }
        ducknng_mutex_lock(&sock->mu);
        if (!sock->open) {
            ducknng_mutex_unlock(&sock->mu);
            ducknng_runtime_release_client_socket(sock);
            duckdb_free(url);
            duckdb_scalar_function_set_error(info, "ducknng: client socket not found");
            return;
        }
        if (sock->has_listener) {
            ducknng_mutex_unlock(&sock->mu);
            ducknng_runtime_release_client_socket(sock);
            duckdb_free(url);
            duckdb_scalar_function_set_error(info, "ducknng: socket is already listening");
            return;
        }
        rv = ducknng_listener_create(&lst, sock->sock, url);
        if (rv == 0 && recv_max_bytes > 0) rv = ducknng_listener_set_recvmaxsz(lst, (size_t)recv_max_bytes);
        if (rv == 0) rv = ducknng_listener_apply_tls(lst, tls_opts);
        if (rv == 0) rv = ducknng_listener_start(lst);
        if (rv == 0) {
            resolved_url = ducknng_listener_resolve_url(lst, url);
            if (sock->listen_url) duckdb_free(sock->listen_url);
            sock->listen_url = resolved_url ? resolved_url : url;
            if (resolved_url) duckdb_free(url);
            url = NULL;
            sock->listener = lst;
            sock->has_listener = 1;
            out[row] = true;
        }
        ducknng_mutex_unlock(&sock->mu);
        ducknng_runtime_release_client_socket(sock);
        if (url) duckdb_free(url);
        if (rv != 0) {
            duckdb_scalar_function_set_error(info, ducknng_nng_strerror(rv));
            if (errmsg) duckdb_free(errmsg);
            return;
        }
    }
}

static void ducknng_close_scalar(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
    idx_t count = duckdb_data_chunk_get_size(input);
    idx_t row;
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_scalar_function_get_extra_info(info);
    if (ducknng_reject_scalar_inside_authorizer(info, ctx)) return;
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
        ducknng_client_socket_destroy(sock);
        out[row] = true;
    }
}

static void ducknng_send_scalar(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
    idx_t count = duckdb_data_chunk_get_size(input);
    idx_t row;
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_scalar_function_get_extra_info(info);
    if (ducknng_reject_scalar_inside_authorizer(info, ctx)) return;
    bool *out = (bool *)duckdb_vector_get_data(output);
    for (row = 0; row < count; row++) {
        uint64_t socket_id = arg_u64(duckdb_data_chunk_get_vector(input, 0), row, 0);
        idx_t payload_len = 0;
        uint8_t *payload = arg_blob_dup(duckdb_data_chunk_get_vector(input, 1), row, &payload_len);
        int32_t timeout_ms = arg_int32(duckdb_data_chunk_get_vector(input, 2), row, 5000);
        ducknng_client_socket *sock;
        nng_msg *msg = NULL;
        char *errmsg = NULL;
        int rv;
        out[row] = false;
        if (!ctx || !ctx->rt || socket_id == 0 || (!payload && payload_len > 0)) {
            if (payload) duckdb_free(payload);
            duckdb_scalar_function_set_error(info, "ducknng: socket id, payload, and timeout are required");
            return;
        }
        sock = ducknng_runtime_acquire_client_socket(ctx->rt, socket_id);
        if (!ducknng_socket_is_active(sock)) {
            if (payload) duckdb_free(payload);
            if (sock) ducknng_runtime_release_client_socket(sock);
            duckdb_scalar_function_set_error(info, "ducknng: active client socket not found");
            return;
        }
        msg = ducknng_client_raw_request_message(payload, (size_t)payload_len, &errmsg);
        if (payload) duckdb_free(payload);
        if (!msg) {
            ducknng_runtime_release_client_socket(sock);
            duckdb_scalar_function_set_error(info, errmsg ? errmsg : "ducknng: failed to build socket send message");
            if (errmsg) duckdb_free(errmsg);
            return;
        }
        ducknng_mutex_lock(&sock->mu);
        rv = ducknng_socket_set_timeout_ms(sock->sock, timeout_ms, sock->recv_timeout_ms);
        if (rv == 0) rv = ducknng_socket_send(sock->sock, msg);
        if (rv == 0) {
            sock->send_timeout_ms = timeout_ms;
            out[row] = true;
        }
        ducknng_mutex_unlock(&sock->mu);
        ducknng_runtime_release_client_socket(sock);
        if (rv != 0) {
            duckdb_scalar_function_set_error(info, ducknng_nng_strerror(rv));
            return;
        }
    }
}

static void ducknng_recv_scalar(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
    idx_t count = duckdb_data_chunk_get_size(input);
    idx_t row;
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_scalar_function_get_extra_info(info);
    if (ducknng_reject_scalar_inside_authorizer(info, ctx)) return;
    for (row = 0; row < count; row++) {
        uint64_t socket_id = arg_u64(duckdb_data_chunk_get_vector(input, 0), row, 0);
        int32_t timeout_ms = arg_int32(duckdb_data_chunk_get_vector(input, 1), row, 5000);
        ducknng_client_socket *sock;
        nng_msg *msg = NULL;
        int rv;
        if (!ctx || !ctx->rt || socket_id == 0) {
            duckdb_scalar_function_set_error(info, "ducknng: socket id is required");
            return;
        }
        sock = ducknng_runtime_acquire_client_socket(ctx->rt, socket_id);
        if (!ducknng_socket_is_active(sock)) {
            if (sock) ducknng_runtime_release_client_socket(sock);
            duckdb_scalar_function_set_error(info, "ducknng: active client socket not found");
            return;
        }
        ducknng_mutex_lock(&sock->mu);
        rv = ducknng_socket_set_timeout_ms(sock->sock, sock->send_timeout_ms, timeout_ms);
        if (rv == 0) rv = ducknng_socket_recv(sock->sock, &msg);
        if (rv == 0) sock->recv_timeout_ms = timeout_ms;
        ducknng_mutex_unlock(&sock->mu);
        ducknng_runtime_release_client_socket(sock);
        if (rv != 0) {
            duckdb_scalar_function_set_error(info, ducknng_nng_strerror(rv));
            return;
        }
        assign_blob(output, row, (const uint8_t *)nng_msg_body(msg), (idx_t)nng_msg_len(msg));
        nng_msg_free(msg);
    }
}

static void ducknng_subscribe_scalar(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
    idx_t count = duckdb_data_chunk_get_size(input);
    idx_t row;
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_scalar_function_get_extra_info(info);
    if (ducknng_reject_scalar_inside_authorizer(info, ctx)) return;
    bool *out = (bool *)duckdb_vector_get_data(output);
    for (row = 0; row < count; row++) {
        uint64_t socket_id = arg_u64(duckdb_data_chunk_get_vector(input, 0), row, 0);
        idx_t topic_len = 0;
        uint8_t *topic = arg_blob_dup(duckdb_data_chunk_get_vector(input, 1), row, &topic_len);
        ducknng_client_socket *sock;
        int rv;
        out[row] = false;
        if (!ctx || !ctx->rt || socket_id == 0 || (!topic && topic_len > 0)) {
            if (topic) duckdb_free(topic);
            duckdb_scalar_function_set_error(info, "ducknng: subscribe_socket requires socket id and topic blob");
            return;
        }
        sock = ducknng_runtime_acquire_client_socket(ctx->rt, socket_id);
        if (!sock || !sock->open || !sock->protocol || strcmp(sock->protocol, "sub") != 0) {
            if (topic) duckdb_free(topic);
            if (sock) ducknng_runtime_release_client_socket(sock);
            duckdb_scalar_function_set_error(info, "ducknng: sub socket not found");
            return;
        }
        ducknng_mutex_lock(&sock->mu);
        rv = ducknng_socket_subscribe(sock->sock, topic, (size_t)topic_len);
        ducknng_mutex_unlock(&sock->mu);
        if (topic) duckdb_free(topic);
        ducknng_runtime_release_client_socket(sock);
        if (rv != 0) {
            duckdb_scalar_function_set_error(info, ducknng_nng_strerror(rv));
            return;
        }
        out[row] = true;
    }
}

static void ducknng_unsubscribe_scalar(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
    idx_t count = duckdb_data_chunk_get_size(input);
    idx_t row;
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_scalar_function_get_extra_info(info);
    if (ducknng_reject_scalar_inside_authorizer(info, ctx)) return;
    bool *out = (bool *)duckdb_vector_get_data(output);
    for (row = 0; row < count; row++) {
        uint64_t socket_id = arg_u64(duckdb_data_chunk_get_vector(input, 0), row, 0);
        idx_t topic_len = 0;
        uint8_t *topic = arg_blob_dup(duckdb_data_chunk_get_vector(input, 1), row, &topic_len);
        ducknng_client_socket *sock;
        int rv;
        out[row] = false;
        if (!ctx || !ctx->rt || socket_id == 0 || (!topic && topic_len > 0)) {
            if (topic) duckdb_free(topic);
            duckdb_scalar_function_set_error(info, "ducknng: unsubscribe_socket requires socket id and topic blob");
            return;
        }
        sock = ducknng_runtime_acquire_client_socket(ctx->rt, socket_id);
        if (!sock || !sock->open || !sock->protocol || strcmp(sock->protocol, "sub") != 0) {
            if (topic) duckdb_free(topic);
            if (sock) ducknng_runtime_release_client_socket(sock);
            duckdb_scalar_function_set_error(info, "ducknng: sub socket not found");
            return;
        }
        ducknng_mutex_lock(&sock->mu);
        rv = ducknng_socket_unsubscribe(sock->sock, topic, (size_t)topic_len);
        ducknng_mutex_unlock(&sock->mu);
        if (topic) duckdb_free(topic);
        ducknng_runtime_release_client_socket(sock);
        if (rv != 0) {
            duckdb_scalar_function_set_error(info, ducknng_nng_strerror(rv));
            return;
        }
        out[row] = true;
    }
}

static void ducknng_request_raw_scalar(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
    idx_t count = duckdb_data_chunk_get_size(input);
    idx_t row;
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_scalar_function_get_extra_info(info);
    if (ducknng_reject_scalar_inside_authorizer(info, ctx)) return;
    for (row = 0; row < count; row++) {
        char *url = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 0), row);
        idx_t payload_len = 0;
        uint8_t *payload = arg_blob_dup(duckdb_data_chunk_get_vector(input, 1), row, &payload_len);
        int32_t timeout_ms = arg_int32(duckdb_data_chunk_get_vector(input, 2), row, 5000);
        uint64_t tls_config_id = arg_u64(duckdb_data_chunk_get_vector(input, 3), row, 0);
        const ducknng_tls_opts *tls_opts = NULL;
        nng_msg *resp = NULL;
        char *errmsg = NULL;
        if (!url || (!payload && payload_len > 0) || ducknng_lookup_tls_opts(ctx, tls_config_id, &tls_opts, &errmsg) != 0) {
            if (url) duckdb_free(url);
            if (payload) duckdb_free(payload);
            if (errmsg) duckdb_free(errmsg);
            set_null(output, row);
            continue;
        }
        resp = ducknng_client_roundtrip_raw_tls(url, payload, (size_t)payload_len, timeout_ms, tls_opts, &errmsg);
        duckdb_free(url);
        if (payload) duckdb_free(payload);
        if (!resp) {
            if (errmsg) duckdb_free(errmsg);
            set_null(output, row);
            continue;
        }
        assign_blob(output, row, (const uint8_t *)nng_msg_body(resp), (idx_t)nng_msg_len(resp));
        nng_msg_free(resp);
    }
}

static void ducknng_request_socket_scalar(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
    idx_t count = duckdb_data_chunk_get_size(input);
    idx_t row;
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_scalar_function_get_extra_info(info);
    if (ducknng_reject_scalar_inside_authorizer(info, ctx)) return;
    for (row = 0; row < count; row++) {
        uint64_t socket_id = arg_u64(duckdb_data_chunk_get_vector(input, 0), row, 0);
        idx_t payload_len = 0;
        uint8_t *payload = arg_blob_dup(duckdb_data_chunk_get_vector(input, 1), row, &payload_len);
        int32_t timeout_ms = arg_int32(duckdb_data_chunk_get_vector(input, 2), row, 5000);
        ducknng_client_socket *sock;
        nng_msg *resp = NULL;
        char *errmsg = NULL;
        int rv;
        if (!ctx || !ctx->rt || socket_id == 0 || (!payload && payload_len > 0)) {
            if (payload) duckdb_free(payload);
            set_null(output, row);
            continue;
        }
        sock = ducknng_runtime_acquire_client_socket(ctx->rt, socket_id);
        if (!sock || !sock->open || !sock->connected || !ducknng_socket_is_req_protocol(sock)) {
            if (payload) duckdb_free(payload);
            if (sock) ducknng_runtime_release_client_socket(sock);
            set_null(output, row);
            continue;
        }
        {
            nng_msg *req = ducknng_client_raw_request_message(payload, (size_t)payload_len, &errmsg);
            if (payload) duckdb_free(payload);
            payload = NULL;
            if (!req) {
                if (sock) ducknng_runtime_release_client_socket(sock);
                if (errmsg) duckdb_free(errmsg);
                set_null(output, row);
                continue;
            }
            ducknng_mutex_lock(&sock->mu);
            rv = ducknng_socket_set_timeout_ms(sock->sock, timeout_ms, timeout_ms);
            if (rv == 0) rv = ducknng_req_transact(sock->sock, req, &resp);
            if (rv == 0) {
                sock->send_timeout_ms = timeout_ms;
                sock->recv_timeout_ms = timeout_ms;
            }
            ducknng_mutex_unlock(&sock->mu);
            ducknng_runtime_release_client_socket(sock);
            if (rv != 0) {
                set_null(output, row);
                continue;
            }
        }
        if (!resp) {
            if (errmsg) duckdb_free(errmsg);
            set_null(output, row);
            continue;
        }
        assign_blob(output, row, (const uint8_t *)nng_msg_body(resp), (idx_t)nng_msg_len(resp));
        nng_msg_free(resp);
    }
}

static void ducknng_request_raw_aio_scalar(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
    idx_t count = duckdb_data_chunk_get_size(input);
    idx_t row;
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_scalar_function_get_extra_info(info);
    if (ducknng_reject_scalar_inside_authorizer(info, ctx)) return;
    uint64_t *out = (uint64_t *)duckdb_vector_get_data(output);
    for (row = 0; row < count; row++) {
        char *url = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 0), row);
        idx_t payload_len = 0;
        uint8_t *payload = arg_blob_dup(duckdb_data_chunk_get_vector(input, 1), row, &payload_len);
        int32_t timeout_ms = arg_int32(duckdb_data_chunk_get_vector(input, 2), row, 5000);
        uint64_t tls_config_id = arg_u64(duckdb_data_chunk_get_vector(input, 3), row, 0);
        nng_msg *req = NULL;
        char *errmsg = NULL;
        if (!ctx || !ctx->rt || !url || (!payload && payload_len > 0)) {
            if (url) duckdb_free(url);
            if (payload) duckdb_free(payload);
            duckdb_scalar_function_set_error(info, "ducknng: request_raw_aio requires url and payload");
            return;
        }
        req = ducknng_client_raw_request_message(payload, (size_t)payload_len, &errmsg);
        if (payload) duckdb_free(payload);
        payload = NULL;
        if (!req) {
            duckdb_free(url);
            duckdb_scalar_function_set_error(info, errmsg ? errmsg : "ducknng: failed to build request frame");
            if (errmsg) duckdb_free(errmsg);
            return;
        }
        if (ducknng_client_launch_url_request_aio(ctx, url, timeout_ms, tls_config_id, req, &out[row], &errmsg) != 0) {
            duckdb_free(url);
            duckdb_scalar_function_set_error(info, errmsg ? errmsg : "ducknng: failed to launch aio request");
            if (errmsg) duckdb_free(errmsg);
            return;
        }
        duckdb_free(url);
    }
}

static void ducknng_get_rpc_manifest_raw_aio_scalar(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
    idx_t count = duckdb_data_chunk_get_size(input);
    idx_t row;
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_scalar_function_get_extra_info(info);
    if (ducknng_reject_scalar_inside_authorizer(info, ctx)) return;
    uint64_t *out = (uint64_t *)duckdb_vector_get_data(output);
    for (row = 0; row < count; row++) {
        char *url = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 0), row);
        int32_t timeout_ms = arg_int32(duckdb_data_chunk_get_vector(input, 1), row, 5000);
        uint64_t tls_config_id = arg_u64(duckdb_data_chunk_get_vector(input, 2), row, 0);
        nng_msg *req = NULL;
        char *errmsg = NULL;
        if (!ctx || !ctx->rt || !url) {
            if (url) duckdb_free(url);
            duckdb_scalar_function_set_error(info, "ducknng: get_rpc_manifest_raw_aio requires url");
            return;
        }
        req = ducknng_client_manifest_request();
        if (!req) {
            duckdb_free(url);
            duckdb_scalar_function_set_error(info, "ducknng: failed to build manifest request frame");
            return;
        }
        if (ducknng_client_launch_url_request_aio(ctx, url, timeout_ms, tls_config_id, req, &out[row], &errmsg) != 0) {
            duckdb_free(url);
            duckdb_scalar_function_set_error(info, errmsg ? errmsg : "ducknng: failed to launch manifest aio request");
            if (errmsg) duckdb_free(errmsg);
            return;
        }
        duckdb_free(url);
    }
}

static void ducknng_run_rpc_raw_aio_scalar(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
    idx_t count = duckdb_data_chunk_get_size(input);
    idx_t row;
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_scalar_function_get_extra_info(info);
    if (ducknng_reject_scalar_inside_authorizer(info, ctx)) return;
    uint64_t *out = (uint64_t *)duckdb_vector_get_data(output);
    for (row = 0; row < count; row++) {
        char *url = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 0), row);
        char *sql = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 1), row);
        int32_t timeout_ms = arg_int32(duckdb_data_chunk_get_vector(input, 2), row, 5000);
        uint64_t tls_config_id = arg_u64(duckdb_data_chunk_get_vector(input, 3), row, 0);
        nng_msg *req = NULL;
        char *errmsg = NULL;
        if (!ctx || !ctx->rt || !url || !sql) {
            if (url) duckdb_free(url);
            if (sql) duckdb_free(sql);
            duckdb_scalar_function_set_error(info, "ducknng: run_rpc_raw_aio requires url and sql");
            return;
        }
        req = ducknng_client_exec_request(sql, 0, &errmsg);
        duckdb_free(sql);
        if (!req) {
            duckdb_free(url);
            duckdb_scalar_function_set_error(info, errmsg ? errmsg : "ducknng: failed to build exec request frame");
            if (errmsg) duckdb_free(errmsg);
            return;
        }
        if (ducknng_client_launch_url_request_aio(ctx, url, timeout_ms, tls_config_id, req, &out[row], &errmsg) != 0) {
            duckdb_free(url);
            duckdb_scalar_function_set_error(info, errmsg ? errmsg : "ducknng: failed to launch exec aio request");
            if (errmsg) duckdb_free(errmsg);
            return;
        }
        duckdb_free(url);
    }
}

static void ducknng_ncurl_aio_scalar(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
    idx_t count = duckdb_data_chunk_get_size(input);
    idx_t row;
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_scalar_function_get_extra_info(info);
    if (ducknng_reject_scalar_inside_authorizer(info, ctx)) return;
    uint64_t *out = (uint64_t *)duckdb_vector_get_data(output);
    for (row = 0; row < count; row++) {
        char *url = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 0), row);
        char *method = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 1), row);
        char *headers_json = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 2), row);
        idx_t body_len = 0;
        uint8_t *body = arg_blob_dup(duckdb_data_chunk_get_vector(input, 3), row, &body_len);
        int32_t timeout_ms = arg_int32(duckdb_data_chunk_get_vector(input, 4), row, 5000);
        uint64_t tls_config_id = arg_u64(duckdb_data_chunk_get_vector(input, 5), row, 0);
        char *errmsg = NULL;
        if (!ctx || !ctx->rt || !url || (!body && body_len > 0)) {
            if (url) duckdb_free(url);
            if (method) duckdb_free(method);
            if (headers_json) duckdb_free(headers_json);
            if (body) duckdb_free(body);
            duckdb_scalar_function_set_error(info, "ducknng: ncurl_aio requires url");
            return;
        }
        if (ducknng_client_launch_ncurl_aio(ctx, url, method, headers_json,
                body, (size_t)body_len, timeout_ms, tls_config_id, &out[row], &errmsg) != 0) {
            duckdb_free(url);
            if (method) duckdb_free(method);
            if (headers_json) duckdb_free(headers_json);
            if (body) duckdb_free(body);
            duckdb_scalar_function_set_error(info, errmsg ? errmsg : "ducknng: failed to launch ncurl aio request");
            if (errmsg) duckdb_free(errmsg);
            return;
        }
        duckdb_free(url);
        if (method) duckdb_free(method);
        if (headers_json) duckdb_free(headers_json);
        if (body) duckdb_free(body);
    }
}

static void ducknng_request_socket_raw_aio_scalar(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
    idx_t count = duckdb_data_chunk_get_size(input);
    idx_t row;
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_scalar_function_get_extra_info(info);
    if (ducknng_reject_scalar_inside_authorizer(info, ctx)) return;
    uint64_t *out = (uint64_t *)duckdb_vector_get_data(output);
    for (row = 0; row < count; row++) {
        uint64_t socket_id = arg_u64(duckdb_data_chunk_get_vector(input, 0), row, 0);
        idx_t payload_len = 0;
        uint8_t *payload = arg_blob_dup(duckdb_data_chunk_get_vector(input, 1), row, &payload_len);
        int32_t timeout_ms = arg_int32(duckdb_data_chunk_get_vector(input, 2), row, 5000);
        ducknng_client_aio *slot = NULL;
        nng_msg *req = NULL;
        char *errmsg = NULL;
        if (!ctx || !ctx->rt || socket_id == 0 || (!payload && payload_len > 0)) {
            if (payload) duckdb_free(payload);
            duckdb_scalar_function_set_error(info, "ducknng: request_socket_raw_aio requires socket id and payload");
            return;
        }
        req = ducknng_client_raw_request_message(payload, (size_t)payload_len, &errmsg);
        if (payload) duckdb_free(payload);
        payload = NULL;
        if (!req) {
            duckdb_scalar_function_set_error(info, errmsg ? errmsg : "ducknng: failed to build request frame");
            if (errmsg) duckdb_free(errmsg);
            return;
        }
        slot = ducknng_client_prepare_socket_request_aio(ctx->rt, socket_id, timeout_ms, &errmsg);
        if (!slot) {
            nng_msg_free(req);
            duckdb_scalar_function_set_error(info, errmsg ? errmsg : "ducknng: failed to prepare socket aio request");
            if (errmsg) duckdb_free(errmsg);
            return;
        }
        if (ducknng_client_launch_request_aio(ctx->rt, slot, req, &errmsg) != 0) {
            duckdb_scalar_function_set_error(info, errmsg ? errmsg : "ducknng: failed to launch socket aio request");
            if (errmsg) duckdb_free(errmsg);
            return;
        }
        out[row] = slot->aio_id;
    }
}

static void ducknng_send_socket_raw_aio_scalar(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
    idx_t count = duckdb_data_chunk_get_size(input);
    idx_t row;
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_scalar_function_get_extra_info(info);
    if (ducknng_reject_scalar_inside_authorizer(info, ctx)) return;
    uint64_t *out = (uint64_t *)duckdb_vector_get_data(output);
    for (row = 0; row < count; row++) {
        uint64_t socket_id = arg_u64(duckdb_data_chunk_get_vector(input, 0), row, 0);
        idx_t payload_len = 0;
        uint8_t *payload = arg_blob_dup(duckdb_data_chunk_get_vector(input, 1), row, &payload_len);
        int32_t timeout_ms = arg_int32(duckdb_data_chunk_get_vector(input, 2), row, 5000);
        ducknng_client_aio *slot = NULL;
        nng_msg *msg = NULL;
        char *errmsg = NULL;
        if (!ctx || !ctx->rt || socket_id == 0 || (!payload && payload_len > 0)) {
            if (payload) duckdb_free(payload);
            duckdb_scalar_function_set_error(info, "ducknng: send_socket_raw_aio requires socket id and payload");
            return;
        }
        msg = ducknng_client_raw_request_message(payload, (size_t)payload_len, &errmsg);
        if (payload) duckdb_free(payload);
        if (!msg) {
            duckdb_scalar_function_set_error(info, errmsg ? errmsg : "ducknng: failed to build send message");
            if (errmsg) duckdb_free(errmsg);
            return;
        }
        slot = ducknng_client_prepare_socket_raw_aio(ctx->rt, socket_id, timeout_ms, &errmsg);
        if (!slot) {
            nng_msg_free(msg);
            duckdb_scalar_function_set_error(info, errmsg ? errmsg : "ducknng: failed to prepare socket send aio");
            if (errmsg) duckdb_free(errmsg);
            return;
        }
        if (ducknng_client_launch_socket_send_aio(ctx->rt, slot, msg, &errmsg) != 0) {
            duckdb_scalar_function_set_error(info, errmsg ? errmsg : "ducknng: failed to launch socket send aio");
            if (errmsg) duckdb_free(errmsg);
            return;
        }
        out[row] = slot->aio_id;
    }
}

static void ducknng_recv_socket_raw_aio_scalar(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
    idx_t count = duckdb_data_chunk_get_size(input);
    idx_t row;
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_scalar_function_get_extra_info(info);
    if (ducknng_reject_scalar_inside_authorizer(info, ctx)) return;
    uint64_t *out = (uint64_t *)duckdb_vector_get_data(output);
    for (row = 0; row < count; row++) {
        uint64_t socket_id = arg_u64(duckdb_data_chunk_get_vector(input, 0), row, 0);
        int32_t timeout_ms = arg_int32(duckdb_data_chunk_get_vector(input, 1), row, 5000);
        ducknng_client_aio *slot = NULL;
        char *errmsg = NULL;
        if (!ctx || !ctx->rt || socket_id == 0) {
            duckdb_scalar_function_set_error(info, "ducknng: recv_socket_raw_aio requires socket id");
            return;
        }
        slot = ducknng_client_prepare_socket_raw_aio(ctx->rt, socket_id, timeout_ms, &errmsg);
        if (!slot) {
            duckdb_scalar_function_set_error(info, errmsg ? errmsg : "ducknng: failed to prepare socket recv aio");
            if (errmsg) duckdb_free(errmsg);
            return;
        }
        if (ducknng_client_launch_socket_recv_aio(ctx->rt, slot, &errmsg) != 0) {
            duckdb_scalar_function_set_error(info, errmsg ? errmsg : "ducknng: failed to launch socket recv aio");
            if (errmsg) duckdb_free(errmsg);
            return;
        }
        out[row] = slot->aio_id;
    }
}

static void ducknng_aio_ready_scalar(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
    idx_t count = duckdb_data_chunk_get_size(input);
    idx_t row;
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_scalar_function_get_extra_info(info);
    bool *out = (bool *)duckdb_vector_get_data(output);
    for (row = 0; row < count; row++) {
        uint64_t aio_id = arg_u64(duckdb_data_chunk_get_vector(input, 0), row, 0);
        ducknng_client_aio *slot;
        out[row] = false;
        if (!ctx || !ctx->rt || aio_id == 0) continue;
        ducknng_mutex_lock(&ctx->rt->mu);
        slot = ducknng_find_client_aio_locked(ctx->rt, aio_id);
        if (slot && slot->state != DUCKNNG_CLIENT_AIO_PENDING) out[row] = true;
        ducknng_mutex_unlock(&ctx->rt->mu);
    }
}

static void ducknng_aio_cancel_scalar(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
    idx_t count = duckdb_data_chunk_get_size(input);
    idx_t row;
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_scalar_function_get_extra_info(info);
    if (ducknng_reject_scalar_inside_authorizer(info, ctx)) return;
    bool *out = (bool *)duckdb_vector_get_data(output);
    for (row = 0; row < count; row++) {
        uint64_t aio_id = arg_u64(duckdb_data_chunk_get_vector(input, 0), row, 0);
        ducknng_client_aio *slot;
        out[row] = false;
        if (!ctx || !ctx->rt || aio_id == 0) continue;
        ducknng_mutex_lock(&ctx->rt->mu);
        slot = ducknng_find_client_aio_locked(ctx->rt, aio_id);
        if (slot && slot->state == DUCKNNG_CLIENT_AIO_PENDING && slot->aio) {
            ducknng_aio_cancel(slot->aio);
            out[row] = true;
        }
        ducknng_mutex_unlock(&ctx->rt->mu);
    }
}

static void ducknng_aio_drop_scalar(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
    idx_t count = duckdb_data_chunk_get_size(input);
    idx_t row;
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_scalar_function_get_extra_info(info);
    if (ducknng_reject_scalar_inside_authorizer(info, ctx)) return;
    bool *out = (bool *)duckdb_vector_get_data(output);
    for (row = 0; row < count; row++) {
        uint64_t aio_id = arg_u64(duckdb_data_chunk_get_vector(input, 0), row, 0);
        ducknng_client_aio *slot = NULL;
        int droppable = 0;
        out[row] = false;
        if (!ctx || !ctx->rt || aio_id == 0) continue;
        ducknng_mutex_lock(&ctx->rt->mu);
        slot = ducknng_find_client_aio_locked(ctx->rt, aio_id);
        droppable = slot && slot->state != DUCKNNG_CLIENT_AIO_PENDING;
        ducknng_mutex_unlock(&ctx->rt->mu);
        if (!droppable) continue;
        slot = ducknng_runtime_remove_client_aio(ctx->rt, aio_id);
        if (!slot) continue;
        ducknng_client_aio_destroy(slot);
        out[row] = true;
    }
}

static const char *ducknng_aio_kind_name(int kind) {
    switch (kind) {
        case DUCKNNG_CLIENT_AIO_KIND_REQUEST: return "request";
        case DUCKNNG_CLIENT_AIO_KIND_SEND: return "send";
        case DUCKNNG_CLIENT_AIO_KIND_RECV: return "recv";
        case DUCKNNG_CLIENT_AIO_KIND_NCURL: return "ncurl";
        default: return NULL;
    }
}

static const char *ducknng_aio_state_name(int state) {
    switch (state) {
        case DUCKNNG_CLIENT_AIO_PENDING: return "pending";
        case DUCKNNG_CLIENT_AIO_READY: return "ready";
        case DUCKNNG_CLIENT_AIO_ERROR: return "error";
        case DUCKNNG_CLIENT_AIO_CANCELLED: return "cancelled";
        case DUCKNNG_CLIENT_AIO_COLLECTED: return "collected";
        default: return NULL;
    }
}

static const char *ducknng_aio_phase_name(int phase) {
    switch (phase) {
        case DUCKNNG_CLIENT_AIO_PHASE_SEND: return "send";
        case DUCKNNG_CLIENT_AIO_PHASE_RECV: return "recv";
        case DUCKNNG_CLIENT_AIO_PHASE_HTTP: return "http";
        default: return NULL;
    }
}

static void ducknng_aio_status_scalar(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_scalar_function_get_extra_info(info);
    duckdb_vector aio_id_vec = duckdb_data_chunk_get_vector(input, 0);
    duckdb_vector child_vecs[12];
    uint64_t *out_aio_id;
    bool *out_exists;
    bool *out_terminal;
    bool *out_send_done;
    bool *out_recv_done;
    bool *out_has_reply_frame;
    idx_t row_count = duckdb_data_chunk_get_size(input);
    for (int i = 0; i < 12; i++) child_vecs[i] = duckdb_struct_vector_get_child(output, (idx_t)i);
    out_aio_id = (uint64_t *)duckdb_vector_get_data(child_vecs[0]);
    out_exists = (bool *)duckdb_vector_get_data(child_vecs[1]);
    out_terminal = (bool *)duckdb_vector_get_data(child_vecs[5]);
    out_send_done = (bool *)duckdb_vector_get_data(child_vecs[6]);
    out_recv_done = (bool *)duckdb_vector_get_data(child_vecs[8]);
    out_has_reply_frame = (bool *)duckdb_vector_get_data(child_vecs[10]);
    for (idx_t row = 0; row < row_count; row++) {
        uint64_t aio_id = arg_u64(aio_id_vec, row, 0);
        ducknng_client_aio snapshot;
        char *error_copy = NULL;
        const char *kind_name = NULL;
        const char *state_name = NULL;
        const char *phase_name = NULL;
        int found = 0;
        memset(&snapshot, 0, sizeof(snapshot));
        out_aio_id[row] = aio_id;
        out_exists[row] = false;
        out_terminal[row] = false;
        out_send_done[row] = false;
        out_recv_done[row] = false;
        out_has_reply_frame[row] = false;
        if (!ctx || !ctx->rt || arg_is_null(aio_id_vec, row) || aio_id == 0) {
            set_null(child_vecs[2], row);
            set_null(child_vecs[3], row);
            set_null(child_vecs[4], row);
            set_null(child_vecs[7], row);
            set_null(child_vecs[9], row);
            set_null(child_vecs[11], row);
            continue;
        }
        ducknng_mutex_lock(&ctx->rt->mu);
        {
            ducknng_client_aio *slot = ducknng_find_client_aio_locked(ctx->rt, aio_id);
            if (slot) {
                snapshot = *slot;
                if (slot->error) error_copy = ducknng_strdup(slot->error);
                found = 1;
            }
        }
        ducknng_mutex_unlock(&ctx->rt->mu);
        if (!found) {
            set_null(child_vecs[2], row);
            set_null(child_vecs[3], row);
            set_null(child_vecs[4], row);
            set_null(child_vecs[7], row);
            set_null(child_vecs[9], row);
            set_null(child_vecs[11], row);
            continue;
        }
        out_exists[row] = true;
        out_terminal[row] = snapshot.state != DUCKNNG_CLIENT_AIO_PENDING;
        out_send_done[row] = snapshot.send_done != 0;
        out_recv_done[row] = snapshot.recv_done != 0;
        out_has_reply_frame[row] = snapshot.reply_msg != NULL;
        kind_name = ducknng_aio_kind_name(snapshot.kind);
        state_name = ducknng_aio_state_name(snapshot.state);
        phase_name = ducknng_aio_phase_name(snapshot.phase);
        if (kind_name) duckdb_vector_assign_string_element(child_vecs[2], row, kind_name); else set_null(child_vecs[2], row);
        if (state_name) duckdb_vector_assign_string_element(child_vecs[3], row, state_name); else set_null(child_vecs[3], row);
        if (phase_name) duckdb_vector_assign_string_element(child_vecs[4], row, phase_name); else set_null(child_vecs[4], row);
        if (snapshot.send_done) {
            bool *send_ok = (bool *)duckdb_vector_get_data(child_vecs[7]);
            send_ok[row] = snapshot.send_result == 0;
        } else {
            set_null(child_vecs[7], row);
        }
        if (snapshot.recv_done) {
            bool *recv_ok = (bool *)duckdb_vector_get_data(child_vecs[9]);
            recv_ok[row] = snapshot.recv_result == 0;
        } else {
            set_null(child_vecs[9], row);
        }
        if (error_copy) duckdb_vector_assign_string_element(child_vecs[11], row, error_copy);
        else set_null(child_vecs[11], row);
        if (error_copy) duckdb_free(error_copy);
    }
}

static void ducknng_aio_wait_any_scalar(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_scalar_function_get_extra_info(info);
    duckdb_vector aio_ids_vec = duckdb_data_chunk_get_vector(input, 0);
    duckdb_vector wait_vec = duckdb_data_chunk_get_vector(input, 1);
    duckdb_vector child_vec = duckdb_list_vector_get_child(aio_ids_vec);
    duckdb_list_entry *entries = (duckdb_list_entry *)duckdb_vector_get_data(aio_ids_vec);
    uint64_t *child_ids = (uint64_t *)duckdb_vector_get_data(child_vec);
    bool *out = (bool *)duckdb_vector_get_data(output);
    idx_t row_count = duckdb_data_chunk_get_size(input);
    for (idx_t row = 0; row < row_count; row++) {
        duckdb_list_entry entry;
        uint64_t *ids = NULL;
        idx_t count = 0;
        idx_t write_idx = 0;
        out[row] = false;
        if (!ctx || !ctx->rt || arg_is_null(aio_ids_vec, row) || arg_is_null(wait_vec, row)) continue;
        entry = entries[row];
        if (entry.length == 0) continue;
        ids = (uint64_t *)duckdb_malloc(sizeof(*ids) * (size_t)entry.length);
        if (!ids) continue;
        for (idx_t i = 0; i < (idx_t)entry.length; i++) {
            idx_t child_index = (idx_t)entry.offset + i;
            if (arg_is_null(child_vec, child_index)) continue;
            ids[write_idx++] = child_ids[child_index];
        }
        count = write_idx;
        if (count > 0) out[row] = ducknng_wait_any_for_ids(ctx->rt, ids, count, arg_int32(wait_vec, row, 0)) != 0;
        duckdb_free(ids);
    }
}

static void ducknng_aio_collect_row_scalar(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_scalar_function_get_extra_info(info);
    duckdb_vector aio_id_vec = duckdb_data_chunk_get_vector(input, 0);
    duckdb_vector wait_vec = duckdb_data_chunk_get_vector(input, 1);
    duckdb_vector child_vecs[4];
    uint64_t *out_aio_id;
    bool *out_ok;
    idx_t row_count = duckdb_data_chunk_get_size(input);
    for (int i = 0; i < 4; i++) child_vecs[i] = duckdb_struct_vector_get_child(output, (idx_t)i);
    out_aio_id = (uint64_t *)duckdb_vector_get_data(child_vecs[0]);
    out_ok = (bool *)duckdb_vector_get_data(child_vecs[1]);
    for (idx_t row = 0; row < row_count; row++) {
        uint64_t aio_id;
        int32_t wait_ms;
        ducknng_client_aio *slot;
        if (!ctx || !ctx->rt || arg_is_null(aio_id_vec, row) || arg_is_null(wait_vec, row)) {
            set_null(output, row);
            for (int i = 0; i < 4; i++) set_null(child_vecs[i], row);
            continue;
        }
        aio_id = arg_u64(aio_id_vec, row, 0);
        wait_ms = arg_int32(wait_vec, row, 0);
        if (aio_id == 0 || wait_ms < 0) {
            set_null(output, row);
            for (int i = 0; i < 4; i++) set_null(child_vecs[i], row);
            continue;
        }
        if (!ducknng_wait_any_for_ids(ctx->rt, &aio_id, 1, wait_ms) && wait_ms > 0) {
            set_null(output, row);
            for (int i = 0; i < 4; i++) set_null(child_vecs[i], row);
            continue;
        }
        ducknng_mutex_lock(&ctx->rt->mu);
        slot = ducknng_find_client_aio_locked(ctx->rt, aio_id);
        if (!slot || !(slot->state == DUCKNNG_CLIENT_AIO_READY || slot->state == DUCKNNG_CLIENT_AIO_ERROR ||
                slot->state == DUCKNNG_CLIENT_AIO_CANCELLED || slot->state == DUCKNNG_CLIENT_AIO_COLLECTED)) {
            ducknng_mutex_unlock(&ctx->rt->mu);
            set_null(output, row);
            for (int i = 0; i < 4; i++) set_null(child_vecs[i], row);
            continue;
        }
        out_aio_id[row] = slot->aio_id;
        out_ok[row] = slot->state == DUCKNNG_CLIENT_AIO_READY ||
            (slot->state == DUCKNNG_CLIENT_AIO_COLLECTED && !slot->error);
        if (slot->error) duckdb_vector_assign_string_element(child_vecs[2], row, slot->error);
        else set_null(child_vecs[2], row);
        if ((slot->state == DUCKNNG_CLIENT_AIO_READY || slot->state == DUCKNNG_CLIENT_AIO_COLLECTED) && slot->reply_msg) {
            assign_blob(child_vecs[3], row, (const uint8_t *)nng_msg_body(slot->reply_msg), (idx_t)nng_msg_len(slot->reply_msg));
        } else {
            set_null(child_vecs[3], row);
        }
        ducknng_mutex_unlock(&ctx->rt->mu);
    }
}

static void ducknng_ncurl_aio_collect_row_scalar(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_scalar_function_get_extra_info(info);
    duckdb_vector aio_id_vec = duckdb_data_chunk_get_vector(input, 0);
    duckdb_vector wait_vec = duckdb_data_chunk_get_vector(input, 1);
    duckdb_vector child_vecs[7];
    uint64_t *out_aio_id;
    bool *out_ok;
    int32_t *out_status;
    idx_t row_count = duckdb_data_chunk_get_size(input);
    for (int i = 0; i < 7; i++) child_vecs[i] = duckdb_struct_vector_get_child(output, (idx_t)i);
    out_aio_id = (uint64_t *)duckdb_vector_get_data(child_vecs[0]);
    out_ok = (bool *)duckdb_vector_get_data(child_vecs[1]);
    out_status = (int32_t *)duckdb_vector_get_data(child_vecs[2]);
    for (idx_t row = 0; row < row_count; row++) {
        uint64_t aio_id;
        int32_t wait_ms;
        ducknng_client_aio *slot;
        if (!ctx || !ctx->rt || arg_is_null(aio_id_vec, row) || arg_is_null(wait_vec, row)) {
            set_null(output, row);
            for (int i = 0; i < 7; i++) set_null(child_vecs[i], row);
            continue;
        }
        aio_id = arg_u64(aio_id_vec, row, 0);
        wait_ms = arg_int32(wait_vec, row, 0);
        if (aio_id == 0 || wait_ms < 0) {
            set_null(output, row);
            for (int i = 0; i < 7; i++) set_null(child_vecs[i], row);
            continue;
        }
        if (!ducknng_wait_any_for_ids(ctx->rt, &aio_id, 1, wait_ms) && wait_ms > 0) {
            set_null(output, row);
            for (int i = 0; i < 7; i++) set_null(child_vecs[i], row);
            continue;
        }
        ducknng_mutex_lock(&ctx->rt->mu);
        slot = ducknng_find_client_aio_locked(ctx->rt, aio_id);
        if (!slot || slot->kind != DUCKNNG_CLIENT_AIO_KIND_NCURL ||
                !(slot->state == DUCKNNG_CLIENT_AIO_READY || slot->state == DUCKNNG_CLIENT_AIO_ERROR ||
                slot->state == DUCKNNG_CLIENT_AIO_CANCELLED || slot->state == DUCKNNG_CLIENT_AIO_COLLECTED)) {
            ducknng_mutex_unlock(&ctx->rt->mu);
            set_null(output, row);
            for (int i = 0; i < 7; i++) set_null(child_vecs[i], row);
            continue;
        }
        out_aio_id[row] = slot->aio_id;
        out_ok[row] = slot->state == DUCKNNG_CLIENT_AIO_READY ||
            (slot->state == DUCKNNG_CLIENT_AIO_COLLECTED && !slot->error);
        if (out_ok[row]) out_status[row] = (int32_t)slot->http_status; else set_null(child_vecs[2], row);
        if (slot->error) duckdb_vector_assign_string_element(child_vecs[3], row, slot->error);
        else set_null(child_vecs[3], row);
        if (slot->http_headers_json) duckdb_vector_assign_string_element(child_vecs[4], row, slot->http_headers_json);
        else set_null(child_vecs[4], row);
        if (slot->http_body) assign_blob(child_vecs[5], row, slot->http_body, (idx_t)slot->http_body_len);
        else set_null(child_vecs[5], row);
        if (slot->http_body_text) duckdb_vector_assign_string_element(child_vecs[6], row, slot->http_body_text);
        else set_null(child_vecs[6], row);
        ducknng_mutex_unlock(&ctx->rt->mu);
    }
}

static void ducknng_aio_collectable_scalar(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
    idx_t count = duckdb_data_chunk_get_size(input);
    idx_t row;
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_scalar_function_get_extra_info(info);
    bool *out = (bool *)duckdb_vector_get_data(output);
    for (row = 0; row < count; row++) {
        uint64_t aio_id = arg_u64(duckdb_data_chunk_get_vector(input, 0), row, 0);
        ducknng_client_aio *slot;
        out[row] = false;
        if (!ctx || !ctx->rt || aio_id == 0) continue;
        ducknng_mutex_lock(&ctx->rt->mu);
        slot = ducknng_find_client_aio_locked(ctx->rt, aio_id);
        if (slot && slot->kind != DUCKNNG_CLIENT_AIO_KIND_NCURL &&
                (slot->state == DUCKNNG_CLIENT_AIO_READY || slot->state == DUCKNNG_CLIENT_AIO_ERROR ||
                slot->state == DUCKNNG_CLIENT_AIO_CANCELLED)) {
            out[row] = true;
        }
        ducknng_mutex_unlock(&ctx->rt->mu);
    }
}

static void ducknng_ncurl_aio_collectable_scalar(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
    idx_t count = duckdb_data_chunk_get_size(input);
    idx_t row;
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_scalar_function_get_extra_info(info);
    bool *out = (bool *)duckdb_vector_get_data(output);
    for (row = 0; row < count; row++) {
        uint64_t aio_id = arg_u64(duckdb_data_chunk_get_vector(input, 0), row, 0);
        ducknng_client_aio *slot;
        out[row] = false;
        if (!ctx || !ctx->rt || aio_id == 0) continue;
        ducknng_mutex_lock(&ctx->rt->mu);
        slot = ducknng_find_client_aio_locked(ctx->rt, aio_id);
        if (slot && slot->kind == DUCKNNG_CLIENT_AIO_KIND_NCURL &&
                (slot->state == DUCKNNG_CLIENT_AIO_READY || slot->state == DUCKNNG_CLIENT_AIO_ERROR ||
                slot->state == DUCKNNG_CLIENT_AIO_CANCELLED)) {
            out[row] = true;
        }
        ducknng_mutex_unlock(&ctx->rt->mu);
    }
}

static void ducknng_aio_mark_collected_scalar(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
    idx_t count = duckdb_data_chunk_get_size(input);
    idx_t row;
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_scalar_function_get_extra_info(info);
    bool *out = (bool *)duckdb_vector_get_data(output);
    for (row = 0; row < count; row++) {
        uint64_t aio_id = arg_u64(duckdb_data_chunk_get_vector(input, 0), row, 0);
        ducknng_client_aio *slot;
        out[row] = false;
        if (!ctx || !ctx->rt || aio_id == 0) continue;
        ducknng_mutex_lock(&ctx->rt->mu);
        slot = ducknng_find_client_aio_locked(ctx->rt, aio_id);
        if (slot && (slot->state == DUCKNNG_CLIENT_AIO_READY || slot->state == DUCKNNG_CLIENT_AIO_ERROR ||
                slot->state == DUCKNNG_CLIENT_AIO_CANCELLED)) {
            slot->state = DUCKNNG_CLIENT_AIO_COLLECTED;
            out[row] = true;
        }
        ducknng_mutex_unlock(&ctx->rt->mu);
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

static void ducknng_query_rpc_bind(duckdb_bind_info info) {
    ducknng_query_rpc_bind_data *bind;
    duckdb_value url_val;
    duckdb_value sql_val;
    duckdb_value tls_val;
    char *url;
    char *sql;
    char *errmsg = NULL;
    nng_msg *resp_msg;
    ducknng_frame frame;
    idx_t col;
    uint64_t tls_config_id;
    const ducknng_tls_opts *tls_opts = NULL;
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_bind_get_extra_info(info);
    if (ducknng_reject_table_inside_authorizer(info, ctx)) return;
    if (duckdb_bind_get_parameter_count(info) != 3) {
        duckdb_bind_set_error(info, "ducknng: ducknng_query_rpc(url, sql, tls_config_id) requires exactly three parameters");
        return;
    }
    url_val = duckdb_bind_get_parameter(info, 0);
    sql_val = duckdb_bind_get_parameter(info, 1);
    tls_val = duckdb_bind_get_parameter(info, 2);
    url = duckdb_get_varchar(url_val);
    sql = duckdb_get_varchar(sql_val);
    tls_config_id = (uint64_t)duckdb_get_uint64(tls_val);
    duckdb_destroy_value(&url_val);
    duckdb_destroy_value(&sql_val);
    duckdb_destroy_value(&tls_val);
    if (!url || !sql || !url[0] || !sql[0]) {
        if (url) duckdb_free(url);
        if (sql) duckdb_free(sql);
        duckdb_bind_set_error(info, "ducknng: ducknng_query_rpc(url, sql, tls_config_id) requires non-empty url and sql");
        return;
    }
    if (ducknng_lookup_tls_opts(ctx, tls_config_id, &tls_opts, &errmsg) != 0) {
        duckdb_free(url);
        duckdb_free(sql);
        duckdb_bind_set_error(info, errmsg ? errmsg : "ducknng: tls config not found");
        if (errmsg) duckdb_free(errmsg);
        return;
    }
    resp_msg = ducknng_client_roundtrip_tls(url, ducknng_client_exec_request(sql, 1, &errmsg), 5000, tls_opts, &errmsg);
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
    bind = (ducknng_query_rpc_bind_data *)duckdb_malloc(sizeof(*bind));
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
        destroy_query_rpc_bind_data(bind);
        duckdb_bind_set_error(info, "ducknng: invalid remote Arrow row schema");
        return;
    }
    for (col = 0; col < (idx_t)bind->schema.n_children; col++) {
        duckdb_logical_type type;
        const char *name = bind->schema.children[col] && bind->schema.children[col]->name ? bind->schema.children[col]->name : "";
        if (ducknng_arrow_schema_to_logical_type(bind->schema.children[col], &type, &errmsg) != 0) {
            destroy_query_rpc_bind_data(bind);
            duckdb_bind_set_error(info, errmsg ? errmsg : "ducknng: unsupported remote Arrow type");
            if (errmsg) duckdb_free(errmsg);
            return;
        }
        duckdb_bind_add_result_column(info, name, type);
        duckdb_destroy_logical_type(&type);
    }
    duckdb_bind_set_bind_data(info, bind, destroy_query_rpc_bind_data);
    duckdb_bind_set_cardinality(info, bind->row_count, true);
}

static void ducknng_query_rpc_init(duckdb_init_info info) {
    ducknng_query_rpc_bind_data *bind = (ducknng_query_rpc_bind_data *)duckdb_init_get_bind_data(info);
    ducknng_query_rpc_init_data *init = (ducknng_query_rpc_init_data *)duckdb_malloc(sizeof(*init));
    if (!init) {
        duckdb_init_set_error(info, "ducknng: out of memory");
        return;
    }
    init->bind = bind;
    init->offset = 0;
    duckdb_init_set_max_threads(info, 1);
    duckdb_init_set_init_data(info, init, destroy_query_rpc_init_data);
}

static void ducknng_query_rpc_scan(duckdb_function_info info, duckdb_data_chunk output) {
    ducknng_query_rpc_init_data *init = (ducknng_query_rpc_init_data *)duckdb_function_get_init_data(info);
    ducknng_query_rpc_bind_data *bind;
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
        if (ducknng_query_rpc_assign_column(vec, view.children[col], bind->schema.children[col], init->offset, chunk_size, NULL) != 0) {
            duckdb_function_set_error(info, "ducknng: failed to decode remote Arrow row payload");
            ArrowArrayViewReset(&view);
            return;
        }
    }
    init->offset += chunk_size;
    duckdb_data_chunk_set_size(output, chunk_size);
    ArrowArrayViewReset(&view);
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

static void ducknng_get_rpc_manifest_bind(duckdb_bind_info info) {
    ducknng_manifest_result_bind_data *bind;
    duckdb_logical_type type;
    duckdb_value url_val;
    duckdb_value tls_val;
    char *url;
    uint64_t tls_config_id;
    const ducknng_tls_opts *tls_opts = NULL;
    char *errmsg = NULL;
    nng_msg *resp_msg = NULL;
    ducknng_frame frame;
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_bind_get_extra_info(info);
    if (ducknng_reject_table_inside_authorizer(info, ctx)) return;
    bind = (ducknng_manifest_result_bind_data *)duckdb_malloc(sizeof(*bind));
    if (!bind) {
        duckdb_bind_set_error(info, "ducknng: out of memory");
        return;
    }
    memset(bind, 0, sizeof(*bind));
    url_val = duckdb_bind_get_parameter(info, 0);
    tls_val = duckdb_bind_get_parameter(info, 1);
    url = duckdb_get_varchar(url_val);
    tls_config_id = (uint64_t)duckdb_get_uint64(tls_val);
    duckdb_destroy_value(&url_val);
    duckdb_destroy_value(&tls_val);
    if (!url || !url[0]) {
        bind->ok = false;
        bind->error = ducknng_strdup("ducknng: remote manifest URL must not be NULL or empty");
    } else if (ducknng_lookup_tls_opts(ctx, tls_config_id, &tls_opts, &errmsg) != 0) {
        bind->ok = false;
        bind->error = errmsg ? errmsg : ducknng_strdup("ducknng: tls config not found");
        errmsg = NULL;
    } else {
        resp_msg = ducknng_client_roundtrip_tls(url, ducknng_client_manifest_request(), 5000, tls_opts, &errmsg);
        if (!resp_msg) {
            bind->ok = false;
            bind->error = errmsg ? errmsg : ducknng_strdup("ducknng: manifest request failed");
            errmsg = NULL;
        } else if (ducknng_decode_request(resp_msg, &frame) != 0) {
            bind->ok = false;
            bind->error = ducknng_strdup("ducknng: invalid manifest response envelope");
        } else if (frame.type == DUCKNNG_RPC_ERROR) {
            bind->ok = false;
            bind->error = ducknng_frame_error_detail(&frame, "ducknng: manifest request failed");
        } else if (frame.type != DUCKNNG_RPC_RESULT || !(frame.flags & DUCKNNG_RPC_FLAG_PAYLOAD_JSON)) {
            bind->ok = false;
            bind->error = ducknng_strdup("ducknng: manifest response was not JSON result payload");
        } else {
            bind->ok = true;
            bind->manifest = (char *)duckdb_malloc((size_t)frame.payload_len + 1);
            if (!bind->manifest) {
                bind->ok = false;
                bind->error = ducknng_strdup("ducknng: out of memory copying manifest payload");
            } else {
                memcpy(bind->manifest, frame.payload, (size_t)frame.payload_len);
                bind->manifest[frame.payload_len] = '\0';
            }
        }
    }
    if (resp_msg) nng_msg_free(resp_msg);
    if (url) duckdb_free(url);
    type = duckdb_create_logical_type(DUCKDB_TYPE_BOOLEAN);
    duckdb_bind_add_result_column(info, "ok", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
    duckdb_bind_add_result_column(info, "error", type);
    duckdb_bind_add_result_column(info, "manifest", type);
    duckdb_destroy_logical_type(&type);
    duckdb_bind_set_bind_data(info, bind, destroy_manifest_result_bind_data);
    duckdb_bind_set_cardinality(info, 1, true);
}

static void ducknng_get_rpc_manifest_scan(duckdb_function_info info, duckdb_data_chunk output) {
    ducknng_single_row_init_data *init = (ducknng_single_row_init_data *)duckdb_function_get_init_data(info);
    ducknng_manifest_result_bind_data *bind = (ducknng_manifest_result_bind_data *)duckdb_function_get_bind_data(info);
    bool *ok_data;
    if (!init || !bind || init->emitted) {
        duckdb_data_chunk_set_size(output, 0);
        return;
    }
    ok_data = (bool *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 0));
    ok_data[0] = bind->ok;
    if (bind->error) duckdb_vector_assign_string_element(duckdb_data_chunk_get_vector(output, 1), 0, bind->error);
    else set_null(duckdb_data_chunk_get_vector(output, 1), 0);
    if (bind->manifest) duckdb_vector_assign_string_element(duckdb_data_chunk_get_vector(output, 2), 0, bind->manifest);
    else set_null(duckdb_data_chunk_get_vector(output, 2), 0);
    duckdb_data_chunk_set_size(output, 1);
    init->emitted = 1;
}

static void ducknng_run_rpc_bind(duckdb_bind_info info) {
    ducknng_exec_result_bind_data *bind;
    duckdb_logical_type type;
    duckdb_value url_val;
    duckdb_value sql_val;
    duckdb_value tls_val;
    char *url;
    char *sql;
    uint64_t tls_config_id;
    const ducknng_tls_opts *tls_opts = NULL;
    char *errmsg = NULL;
    nng_msg *resp_msg = NULL;
    ducknng_frame frame;
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_bind_get_extra_info(info);
    if (ducknng_reject_table_inside_authorizer(info, ctx)) return;
    bind = (ducknng_exec_result_bind_data *)duckdb_malloc(sizeof(*bind));
    if (!bind) {
        duckdb_bind_set_error(info, "ducknng: out of memory");
        return;
    }
    memset(bind, 0, sizeof(*bind));
    url_val = duckdb_bind_get_parameter(info, 0);
    sql_val = duckdb_bind_get_parameter(info, 1);
    tls_val = duckdb_bind_get_parameter(info, 2);
    url = duckdb_get_varchar(url_val);
    sql = duckdb_get_varchar(sql_val);
    tls_config_id = (uint64_t)duckdb_get_uint64(tls_val);
    duckdb_destroy_value(&url_val);
    duckdb_destroy_value(&sql_val);
    duckdb_destroy_value(&tls_val);
    if (!url || !url[0] || !sql || !sql[0]) {
        bind->ok = false;
        bind->error = ducknng_strdup("ducknng: remote exec URL and SQL must not be NULL or empty");
    } else if (ducknng_lookup_tls_opts(ctx, tls_config_id, &tls_opts, &errmsg) != 0) {
        bind->ok = false;
        bind->error = errmsg ? errmsg : ducknng_strdup("ducknng: tls config not found");
        errmsg = NULL;
    } else {
        resp_msg = ducknng_client_roundtrip_tls(url, ducknng_client_exec_request(sql, 0, &errmsg), 5000, tls_opts, &errmsg);
        if (!resp_msg) {
            bind->ok = false;
            bind->error = errmsg ? errmsg : ducknng_strdup("ducknng: remote exec request failed");
            errmsg = NULL;
        } else if (ducknng_decode_request(resp_msg, &frame) != 0) {
            bind->ok = false;
            bind->error = ducknng_strdup("ducknng: invalid remote exec response envelope");
        } else if (frame.type == DUCKNNG_RPC_ERROR) {
            bind->ok = false;
            bind->error = ducknng_frame_error_detail(&frame, "ducknng: remote exec request failed");
        } else if (frame.type != DUCKNNG_RPC_RESULT || !(frame.flags & DUCKNNG_RPC_FLAG_RESULT_METADATA)) {
            bind->ok = false;
            bind->error = ducknng_strdup("ducknng: remote exec expected metadata reply");
        } else if (ducknng_decode_exec_metadata_payload(frame.payload, (size_t)frame.payload_len,
                &bind->rows_changed, (uint32_t *)&bind->statement_type, (uint32_t *)&bind->result_type, &errmsg) != 0) {
            bind->ok = false;
            bind->error = errmsg ? errmsg : ducknng_strdup("ducknng: failed to decode remote exec metadata");
            errmsg = NULL;
        } else {
            bind->ok = true;
        }
    }
    if (resp_msg) nng_msg_free(resp_msg);
    if (url) duckdb_free(url);
    if (sql) duckdb_free(sql);
    type = duckdb_create_logical_type(DUCKDB_TYPE_BOOLEAN);
    duckdb_bind_add_result_column(info, "ok", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
    duckdb_bind_add_result_column(info, "error", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_UBIGINT);
    duckdb_bind_add_result_column(info, "rows_changed", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_INTEGER);
    duckdb_bind_add_result_column(info, "statement_type", type);
    duckdb_bind_add_result_column(info, "result_type", type);
    duckdb_destroy_logical_type(&type);
    duckdb_bind_set_bind_data(info, bind, destroy_exec_result_bind_data);
    duckdb_bind_set_cardinality(info, 1, true);
}

static void ducknng_run_rpc_scan(duckdb_function_info info, duckdb_data_chunk output) {
    ducknng_single_row_init_data *init = (ducknng_single_row_init_data *)duckdb_function_get_init_data(info);
    ducknng_exec_result_bind_data *bind = (ducknng_exec_result_bind_data *)duckdb_function_get_bind_data(info);
    bool *ok_data;
    uint64_t *rows_changed;
    int32_t *statement_type;
    int32_t *result_type;
    if (!init || !bind || init->emitted) {
        duckdb_data_chunk_set_size(output, 0);
        return;
    }
    ok_data = (bool *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 0));
    rows_changed = (uint64_t *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 2));
    statement_type = (int32_t *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 3));
    result_type = (int32_t *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 4));
    ok_data[0] = bind->ok;
    if (bind->error) duckdb_vector_assign_string_element(duckdb_data_chunk_get_vector(output, 1), 0, bind->error);
    else set_null(duckdb_data_chunk_get_vector(output, 1), 0);
    rows_changed[0] = bind->rows_changed;
    statement_type[0] = bind->statement_type;
    result_type[0] = bind->result_type;
    duckdb_data_chunk_set_size(output, 1);
    init->emitted = 1;
}

static void ducknng_session_bind_add_control_columns(duckdb_bind_info info) {
    duckdb_logical_type type;
    type = duckdb_create_logical_type(DUCKDB_TYPE_BOOLEAN);
    duckdb_bind_add_result_column(info, "ok", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
    duckdb_bind_add_result_column(info, "error", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_UBIGINT);
    duckdb_bind_add_result_column(info, "session_id", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
    duckdb_bind_add_result_column(info, "session_token", type);
    duckdb_bind_add_result_column(info, "state", type);
    duckdb_bind_add_result_column(info, "next_method", type);
    duckdb_bind_add_result_column(info, "control_json", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_UBIGINT);
    duckdb_bind_add_result_column(info, "idle_timeout_ms", type);
    duckdb_destroy_logical_type(&type);
}

static void ducknng_session_bind_common(ducknng_session_result_bind_data *bind,
    ducknng_sql_context *ctx, const char *url, const char *method_name,
    const uint8_t *payload, size_t payload_len, uint64_t tls_config_id,
    uint64_t fallback_session_id, int expect_json_only) {
    const ducknng_tls_opts *tls_opts = NULL;
    char *errmsg = NULL;
    nng_msg *resp_msg = NULL;
    if (!bind) return;
    if (!url || !url[0]) {
        bind->ok = false;
        bind->error = ducknng_strdup("ducknng: session URL must not be NULL or empty");
        return;
    }
    if (ducknng_lookup_tls_opts(ctx, tls_config_id, &tls_opts, &errmsg) != 0) {
        bind->ok = false;
        bind->error = errmsg ? errmsg : ducknng_strdup("ducknng: tls config not found");
        return;
    }
    resp_msg = ducknng_client_method_roundtrip_tls(url, method_name, payload, payload_len, 5000, tls_opts, &errmsg);
    if (!resp_msg) {
        bind->ok = false;
        bind->error = errmsg ? errmsg : ducknng_strdup("ducknng: session request failed");
        return;
    }
    ducknng_session_result_fill_from_response(bind, fallback_session_id, resp_msg);
    nng_msg_free(resp_msg);
    if (expect_json_only && bind->ok && !bind->control_json) {
        if (bind->payload) {
            duckdb_free(bind->payload);
            bind->payload = NULL;
            bind->payload_len = 0;
        }
        bind->ok = false;
        bind->error = ducknng_strdup("ducknng: session control request expected a JSON control reply");
    }
}

static void ducknng_open_query_bind(duckdb_bind_info info) {
    ducknng_session_result_bind_data *bind;
    duckdb_value url_val;
    duckdb_value sql_val;
    duckdb_value batch_rows_val;
    duckdb_value batch_bytes_val;
    duckdb_value tls_val;
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_bind_get_extra_info(info);
    if (ducknng_reject_table_inside_authorizer(info, ctx)) return;
    bind = (ducknng_session_result_bind_data *)duckdb_malloc(sizeof(*bind));
    if (!bind) {
        duckdb_bind_set_error(info, "ducknng: out of memory");
        return;
    }
    memset(bind, 0, sizeof(*bind));
    bind->ctx = ctx;
    url_val = duckdb_bind_get_parameter(info, 0);
    sql_val = duckdb_bind_get_parameter(info, 1);
    batch_rows_val = duckdb_bind_get_parameter(info, 2);
    batch_bytes_val = duckdb_bind_get_parameter(info, 3);
    tls_val = duckdb_bind_get_parameter(info, 4);
    bind->url = duckdb_get_varchar(url_val);
    bind->sql = duckdb_get_varchar(sql_val);
    bind->method_name = ducknng_strdup("query_open");
    bind->batch_rows = (uint64_t)duckdb_get_uint64(batch_rows_val);
    bind->batch_bytes = (uint64_t)duckdb_get_uint64(batch_bytes_val);
    bind->tls_config_id = (uint64_t)duckdb_get_uint64(tls_val);
    bind->expect_json_only = 1;
    duckdb_destroy_value(&url_val);
    duckdb_destroy_value(&sql_val);
    duckdb_destroy_value(&batch_rows_val);
    duckdb_destroy_value(&batch_bytes_val);
    duckdb_destroy_value(&tls_val);
    if (!bind->url || !bind->url[0] || !bind->sql || !bind->sql[0]) {
        bind->error = ducknng_strdup("ducknng: open_query requires non-empty url and sql");
    } else if (!bind->method_name) {
        bind->error = ducknng_strdup("ducknng: out of memory preparing open_query call");
    }
    ducknng_session_bind_add_control_columns(info);
    duckdb_bind_set_bind_data(info, bind, destroy_session_result_bind_data);
    duckdb_bind_set_cardinality(info, 1, true);
}

static void ducknng_close_query_bind(duckdb_bind_info info) {
    ducknng_session_result_bind_data *bind;
    duckdb_value url_val;
    duckdb_value session_val;
    duckdb_value token_val;
    duckdb_value tls_val;
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_bind_get_extra_info(info);
    if (ducknng_reject_table_inside_authorizer(info, ctx)) return;
    bind = (ducknng_session_result_bind_data *)duckdb_malloc(sizeof(*bind));
    if (!bind) {
        duckdb_bind_set_error(info, "ducknng: out of memory");
        return;
    }
    memset(bind, 0, sizeof(*bind));
    bind->ctx = ctx;
    url_val = duckdb_bind_get_parameter(info, 0);
    session_val = duckdb_bind_get_parameter(info, 1);
    token_val = duckdb_bind_get_parameter(info, 2);
    tls_val = duckdb_bind_get_parameter(info, 3);
    bind->url = duckdb_get_varchar(url_val);
    bind->session_id = (uint64_t)duckdb_get_uint64(session_val);
    bind->session_token = duckdb_get_varchar(token_val);
    bind->tls_config_id = (uint64_t)duckdb_get_uint64(tls_val);
    bind->method_name = ducknng_strdup("close");
    bind->expect_json_only = 1;
    duckdb_destroy_value(&url_val);
    duckdb_destroy_value(&session_val);
    duckdb_destroy_value(&token_val);
    duckdb_destroy_value(&tls_val);
    if (!bind->url || !bind->url[0] || bind->session_id == 0 || !bind->session_token || !bind->session_token[0]) {
        bind->error = ducknng_strdup("ducknng: close_query requires non-empty url, session_id, and session_token");
    } else if (!bind->method_name) {
        bind->error = ducknng_strdup("ducknng: out of memory preparing close_query call");
    }
    ducknng_session_bind_add_control_columns(info);
    duckdb_bind_set_bind_data(info, bind, destroy_session_result_bind_data);
    duckdb_bind_set_cardinality(info, 1, true);
}

static void ducknng_cancel_query_bind(duckdb_bind_info info) {
    ducknng_session_result_bind_data *bind;
    duckdb_value url_val;
    duckdb_value session_val;
    duckdb_value token_val;
    duckdb_value tls_val;
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_bind_get_extra_info(info);
    if (ducknng_reject_table_inside_authorizer(info, ctx)) return;
    bind = (ducknng_session_result_bind_data *)duckdb_malloc(sizeof(*bind));
    if (!bind) {
        duckdb_bind_set_error(info, "ducknng: out of memory");
        return;
    }
    memset(bind, 0, sizeof(*bind));
    bind->ctx = ctx;
    url_val = duckdb_bind_get_parameter(info, 0);
    session_val = duckdb_bind_get_parameter(info, 1);
    token_val = duckdb_bind_get_parameter(info, 2);
    tls_val = duckdb_bind_get_parameter(info, 3);
    bind->url = duckdb_get_varchar(url_val);
    bind->session_id = (uint64_t)duckdb_get_uint64(session_val);
    bind->session_token = duckdb_get_varchar(token_val);
    bind->tls_config_id = (uint64_t)duckdb_get_uint64(tls_val);
    bind->method_name = ducknng_strdup("cancel");
    bind->expect_json_only = 1;
    duckdb_destroy_value(&url_val);
    duckdb_destroy_value(&session_val);
    duckdb_destroy_value(&token_val);
    duckdb_destroy_value(&tls_val);
    if (!bind->url || !bind->url[0] || bind->session_id == 0 || !bind->session_token || !bind->session_token[0]) {
        bind->error = ducknng_strdup("ducknng: cancel_query requires non-empty url, session_id, and session_token");
    } else if (!bind->method_name) {
        bind->error = ducknng_strdup("ducknng: out of memory preparing cancel_query call");
    }
    ducknng_session_bind_add_control_columns(info);
    duckdb_bind_set_bind_data(info, bind, destroy_session_result_bind_data);
    duckdb_bind_set_cardinality(info, 1, true);
}

static void ducknng_fetch_query_bind(duckdb_bind_info info) {
    ducknng_session_result_bind_data *bind;
    duckdb_logical_type type;
    duckdb_value url_val;
    duckdb_value session_val;
    duckdb_value token_val;
    duckdb_value batch_rows_val;
    duckdb_value batch_bytes_val;
    duckdb_value tls_val;
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_bind_get_extra_info(info);
    if (ducknng_reject_table_inside_authorizer(info, ctx)) return;
    bind = (ducknng_session_result_bind_data *)duckdb_malloc(sizeof(*bind));
    if (!bind) {
        duckdb_bind_set_error(info, "ducknng: out of memory");
        return;
    }
    memset(bind, 0, sizeof(*bind));
    bind->ctx = ctx;
    url_val = duckdb_bind_get_parameter(info, 0);
    session_val = duckdb_bind_get_parameter(info, 1);
    token_val = duckdb_bind_get_parameter(info, 2);
    batch_rows_val = duckdb_bind_get_parameter(info, 3);
    batch_bytes_val = duckdb_bind_get_parameter(info, 4);
    tls_val = duckdb_bind_get_parameter(info, 5);
    bind->url = duckdb_get_varchar(url_val);
    bind->session_id = (uint64_t)duckdb_get_uint64(session_val);
    bind->session_token = duckdb_get_varchar(token_val);
    bind->batch_rows = (uint64_t)duckdb_get_uint64(batch_rows_val);
    bind->batch_bytes = (uint64_t)duckdb_get_uint64(batch_bytes_val);
    bind->tls_config_id = (uint64_t)duckdb_get_uint64(tls_val);
    bind->method_name = ducknng_strdup("fetch");
    bind->expect_json_only = 0;
    duckdb_destroy_value(&url_val);
    duckdb_destroy_value(&session_val);
    duckdb_destroy_value(&token_val);
    duckdb_destroy_value(&batch_rows_val);
    duckdb_destroy_value(&batch_bytes_val);
    duckdb_destroy_value(&tls_val);
    if (!bind->url || !bind->url[0] || bind->session_id == 0 || !bind->session_token || !bind->session_token[0]) {
        bind->error = ducknng_strdup("ducknng: fetch_query requires non-empty url, session_id, and session_token");
    } else if (!bind->method_name) {
        bind->error = ducknng_strdup("ducknng: out of memory preparing fetch_query call");
    }
    ducknng_session_bind_add_control_columns(info);
    type = duckdb_create_logical_type(DUCKDB_TYPE_BLOB);
    duckdb_bind_add_result_column(info, "payload", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_BOOLEAN);
    duckdb_bind_add_result_column(info, "end_of_stream", type);
    duckdb_destroy_logical_type(&type);
    duckdb_bind_set_bind_data(info, bind, destroy_session_result_bind_data);
    duckdb_bind_set_cardinality(info, 1, true);
}

static void ducknng_session_result_init(duckdb_init_info info) {
    ducknng_session_result_bind_data *bind = (ducknng_session_result_bind_data *)duckdb_init_get_bind_data(info);
    ducknng_single_row_init_data *init = (ducknng_single_row_init_data *)duckdb_malloc(sizeof(*init));
    uint8_t *payload = NULL;
    size_t payload_len = 0;
    char *json = NULL;
    char *errmsg = NULL;
    if (!bind) {
        duckdb_init_set_error(info, "ducknng: missing session bind data");
        return;
    }
    if (!bind->executed && !bind->error) {
        if (bind->method_name && strcmp(bind->method_name, "query_open") == 0) {
            if (ducknng_query_open_request_to_ipc(bind->sql, bind->batch_rows, bind->batch_bytes, &payload, &payload_len, &errmsg) != 0) {
                bind->error = errmsg ? errmsg : ducknng_strdup("ducknng: failed to encode query_open payload");
            } else {
                ducknng_session_bind_common(bind, bind->ctx, bind->url, bind->method_name, payload, payload_len, bind->tls_config_id, 0, bind->expect_json_only);
                if (bind->ok && (bind->session_id == 0 || !bind->session_token || !bind->session_token[0])) {
                    bind->ok = false;
                    if (!bind->error) bind->error = ducknng_strdup("ducknng: query_open reply did not include session_id and session_token");
                }
            }
        } else {
            json = ducknng_session_json_request(bind->session_id, bind->session_token, bind->batch_rows, bind->batch_bytes);
            if (!json) {
                bind->error = ducknng_strdup("ducknng: failed to build session request payload");
            } else {
                ducknng_session_bind_common(bind, bind->ctx, bind->url, bind->method_name, (const uint8_t *)json, strlen(json), bind->tls_config_id, bind->session_id, bind->expect_json_only);
            }
        }
        if (payload) duckdb_free(payload);
        if (json) duckdb_free(json);
        bind->executed = 1;
    }
    if (bind->error) bind->ok = false;
    if (!init) {
        duckdb_init_set_error(info, "ducknng: out of memory");
        return;
    }
    init->emitted = 0;
    duckdb_init_set_max_threads(info, 1);
    duckdb_init_set_init_data(info, init, destroy_single_row_init_data);
}

static void ducknng_session_control_scan(duckdb_function_info info, duckdb_data_chunk output) {
    ducknng_single_row_init_data *init = (ducknng_single_row_init_data *)duckdb_function_get_init_data(info);
    ducknng_session_result_bind_data *bind = (ducknng_session_result_bind_data *)duckdb_function_get_bind_data(info);
    bool *ok_data;
    uint64_t *session_ids;
    uint64_t *idle_timeout_ms;
    if (!init || !bind || init->emitted) {
        duckdb_data_chunk_set_size(output, 0);
        return;
    }
    ok_data = (bool *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 0));
    session_ids = (uint64_t *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 2));
    idle_timeout_ms = (uint64_t *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 7));
    ok_data[0] = bind->ok;
    if (bind->error) duckdb_vector_assign_string_element(duckdb_data_chunk_get_vector(output, 1), 0, bind->error);
    else set_null(duckdb_data_chunk_get_vector(output, 1), 0);
    session_ids[0] = bind->session_id;
    if (bind->session_token) duckdb_vector_assign_string_element(duckdb_data_chunk_get_vector(output, 3), 0, bind->session_token);
    else set_null(duckdb_data_chunk_get_vector(output, 3), 0);
    if (bind->state) duckdb_vector_assign_string_element(duckdb_data_chunk_get_vector(output, 4), 0, bind->state);
    else set_null(duckdb_data_chunk_get_vector(output, 4), 0);
    if (bind->next_method) duckdb_vector_assign_string_element(duckdb_data_chunk_get_vector(output, 5), 0, bind->next_method);
    else set_null(duckdb_data_chunk_get_vector(output, 5), 0);
    if (bind->control_json) duckdb_vector_assign_string_element(duckdb_data_chunk_get_vector(output, 6), 0, bind->control_json);
    else set_null(duckdb_data_chunk_get_vector(output, 6), 0);
    if (bind->has_idle_timeout_ms) idle_timeout_ms[0] = bind->idle_timeout_ms;
    else set_null(duckdb_data_chunk_get_vector(output, 7), 0);
    duckdb_data_chunk_set_size(output, 1);
    init->emitted = 1;
}

static void ducknng_fetch_query_scan(duckdb_function_info info, duckdb_data_chunk output) {
    ducknng_single_row_init_data *init = (ducknng_single_row_init_data *)duckdb_function_get_init_data(info);
    ducknng_session_result_bind_data *bind = (ducknng_session_result_bind_data *)duckdb_function_get_bind_data(info);
    bool *ok_data;
    uint64_t *session_ids;
    uint64_t *idle_timeout_ms;
    bool *end_of_stream;
    if (!init || !bind || init->emitted) {
        duckdb_data_chunk_set_size(output, 0);
        return;
    }
    ok_data = (bool *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 0));
    session_ids = (uint64_t *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 2));
    idle_timeout_ms = (uint64_t *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 7));
    end_of_stream = (bool *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 9));
    ok_data[0] = bind->ok;
    if (bind->error) duckdb_vector_assign_string_element(duckdb_data_chunk_get_vector(output, 1), 0, bind->error);
    else set_null(duckdb_data_chunk_get_vector(output, 1), 0);
    session_ids[0] = bind->session_id;
    if (bind->session_token) duckdb_vector_assign_string_element(duckdb_data_chunk_get_vector(output, 3), 0, bind->session_token);
    else set_null(duckdb_data_chunk_get_vector(output, 3), 0);
    if (bind->state) duckdb_vector_assign_string_element(duckdb_data_chunk_get_vector(output, 4), 0, bind->state);
    else set_null(duckdb_data_chunk_get_vector(output, 4), 0);
    if (bind->next_method) duckdb_vector_assign_string_element(duckdb_data_chunk_get_vector(output, 5), 0, bind->next_method);
    else set_null(duckdb_data_chunk_get_vector(output, 5), 0);
    if (bind->control_json) duckdb_vector_assign_string_element(duckdb_data_chunk_get_vector(output, 6), 0, bind->control_json);
    else set_null(duckdb_data_chunk_get_vector(output, 6), 0);
    if (bind->has_idle_timeout_ms) idle_timeout_ms[0] = bind->idle_timeout_ms;
    else set_null(duckdb_data_chunk_get_vector(output, 7), 0);
    if (bind->payload) assign_blob(duckdb_data_chunk_get_vector(output, 8), 0, bind->payload, bind->payload_len);
    else set_null(duckdb_data_chunk_get_vector(output, 8), 0);
    end_of_stream[0] = bind->end_of_stream;
    duckdb_data_chunk_set_size(output, 1);
    init->emitted = 1;
}

static void ducknng_request_bind_common(ducknng_request_bind_data *bind, const char *url,
    const uint8_t *payload, size_t payload_len, int32_t timeout_ms, const ducknng_tls_opts *tls_opts) {
    char *errmsg = NULL;
    nng_msg *resp_msg = NULL;
    if (!url || !url[0]) {
        bind->ok = false;
        bind->error = ducknng_strdup("ducknng: request URL must not be NULL or empty");
        return;
    }
    resp_msg = ducknng_client_roundtrip_raw_tls(url, payload, payload_len, timeout_ms, tls_opts, &errmsg);
    if (!resp_msg) {
        bind->ok = false;
        bind->error = errmsg ? errmsg : ducknng_strdup("ducknng: request failed");
        return;
    }
    bind->payload_len = (idx_t)nng_msg_len(resp_msg);
    bind->payload = (uint8_t *)duckdb_malloc((size_t)bind->payload_len);
    if (!bind->payload && bind->payload_len > 0) {
        bind->ok = false;
        bind->error = ducknng_strdup("ducknng: out of memory copying reply payload");
        nng_msg_free(resp_msg);
        return;
    }
    if (bind->payload_len) memcpy(bind->payload, nng_msg_body(resp_msg), (size_t)bind->payload_len);
    bind->ok = true;
    nng_msg_free(resp_msg);
}

static void ducknng_request_bind_common_socket(ducknng_request_bind_data *bind, ducknng_client_socket *sock,
    const uint8_t *payload, size_t payload_len, int32_t timeout_ms) {
    char *errmsg = NULL;
    nng_msg *req_msg = NULL;
    nng_msg *resp_msg = NULL;
    int rv;
    if (!bind || !sock || !sock->open || !sock->connected || !ducknng_socket_is_req_protocol(sock)) {
        if (bind) {
            bind->ok = false;
            bind->error = ducknng_strdup("ducknng: connected req client socket not found");
        }
        return;
    }
    req_msg = ducknng_client_raw_request_message(payload, payload_len, &errmsg);
    if (!req_msg) {
        bind->ok = false;
        bind->error = errmsg ? errmsg : ducknng_strdup("ducknng: failed to build socket request frame");
        return;
    }
    rv = ducknng_socket_set_timeout_ms(sock->sock, timeout_ms, timeout_ms);
    if (rv != 0) {
        nng_msg_free(req_msg);
        bind->ok = false;
        bind->error = ducknng_strdup(ducknng_nng_strerror(rv));
        return;
    }
    rv = ducknng_req_transact(sock->sock, req_msg, &resp_msg);
    if (rv != 0) {
        bind->ok = false;
        bind->error = ducknng_strdup(ducknng_nng_strerror(rv));
        return;
    }
    bind->payload_len = (idx_t)nng_msg_len(resp_msg);
    bind->payload = (uint8_t *)duckdb_malloc((size_t)bind->payload_len);
    if (!bind->payload && bind->payload_len > 0) {
        bind->ok = false;
        bind->error = ducknng_strdup("ducknng: out of memory copying reply payload");
        nng_msg_free(resp_msg);
        return;
    }
    if (bind->payload_len) memcpy(bind->payload, nng_msg_body(resp_msg), (size_t)bind->payload_len);
    sock->send_timeout_ms = timeout_ms;
    sock->recv_timeout_ms = timeout_ms;
    bind->ok = true;
    nng_msg_free(resp_msg);
}

static void ducknng_request_bind(duckdb_bind_info info) {
    ducknng_request_bind_data *bind;
    duckdb_logical_type type;
    duckdb_value url_val;
    duckdb_value payload_val;
    duckdb_value timeout_val;
    duckdb_value tls_val;
    duckdb_blob blob;
    char *url;
    int32_t timeout_ms;
    uint64_t tls_config_id;
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_bind_get_extra_info(info);
    if (ducknng_reject_table_inside_authorizer(info, ctx)) return;
    const ducknng_tls_opts *tls_opts = NULL;
    char *errmsg = NULL;
    bind = (ducknng_request_bind_data *)duckdb_malloc(sizeof(*bind));
    if (!bind) {
        duckdb_bind_set_error(info, "ducknng: out of memory");
        return;
    }
    memset(bind, 0, sizeof(*bind));
    url_val = duckdb_bind_get_parameter(info, 0);
    payload_val = duckdb_bind_get_parameter(info, 1);
    timeout_val = duckdb_bind_get_parameter(info, 2);
    tls_val = duckdb_bind_get_parameter(info, 3);
    url = duckdb_get_varchar(url_val);
    blob = duckdb_get_blob(payload_val);
    timeout_ms = duckdb_get_int32(timeout_val);
    tls_config_id = (uint64_t)duckdb_get_uint64(tls_val);
    duckdb_destroy_value(&url_val);
    duckdb_destroy_value(&payload_val);
    duckdb_destroy_value(&timeout_val);
    duckdb_destroy_value(&tls_val);
    if (ducknng_lookup_tls_opts(ctx, tls_config_id, &tls_opts, &errmsg) != 0) {
        bind->ok = false;
        bind->error = errmsg ? errmsg : ducknng_strdup("ducknng: tls config not found");
    } else {
        ducknng_request_bind_common(bind, url, (const uint8_t *)blob.data, (size_t)blob.size, timeout_ms, tls_opts);
    }
    if (url) duckdb_free(url);
    if (blob.data) duckdb_free(blob.data);
    type = duckdb_create_logical_type(DUCKDB_TYPE_BOOLEAN);
    duckdb_bind_add_result_column(info, "ok", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
    duckdb_bind_add_result_column(info, "error", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_BLOB);
    duckdb_bind_add_result_column(info, "payload", type);
    duckdb_destroy_logical_type(&type);
    duckdb_bind_set_bind_data(info, bind, destroy_request_bind_data);
    duckdb_bind_set_cardinality(info, 1, true);
}

static void ducknng_request_socket_bind(duckdb_bind_info info) {
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_bind_get_extra_info(info);
    if (ducknng_reject_table_inside_authorizer(info, ctx)) return;
    ducknng_request_bind_data *bind;
    duckdb_logical_type type;
    duckdb_value socket_val;
    duckdb_value payload_val;
    duckdb_value timeout_val;
    duckdb_blob blob;
    uint64_t socket_id;
    int32_t timeout_ms;
    ducknng_client_socket *sock;
    bind = (ducknng_request_bind_data *)duckdb_malloc(sizeof(*bind));
    if (!bind) {
        duckdb_bind_set_error(info, "ducknng: out of memory");
        return;
    }
    memset(bind, 0, sizeof(*bind));
    socket_val = duckdb_bind_get_parameter(info, 0);
    payload_val = duckdb_bind_get_parameter(info, 1);
    timeout_val = duckdb_bind_get_parameter(info, 2);
    socket_id = duckdb_get_int64(socket_val);
    blob = duckdb_get_blob(payload_val);
    timeout_ms = duckdb_get_int32(timeout_val);
    duckdb_destroy_value(&socket_val);
    duckdb_destroy_value(&payload_val);
    duckdb_destroy_value(&timeout_val);
    sock = ctx && ctx->rt ? ducknng_runtime_acquire_client_socket(ctx->rt, socket_id) : NULL;
    if (!sock || !sock->open || !sock->connected) {
        bind->ok = false;
        bind->error = ducknng_strdup("ducknng: connected client socket not found");
    } else {
        ducknng_request_bind_common_socket(bind, sock, (const uint8_t *)blob.data, (size_t)blob.size, timeout_ms);
    }
    if (sock) ducknng_runtime_release_client_socket(sock);
    if (blob.data) duckdb_free(blob.data);
    type = duckdb_create_logical_type(DUCKDB_TYPE_BOOLEAN);
    duckdb_bind_add_result_column(info, "ok", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
    duckdb_bind_add_result_column(info, "error", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_BLOB);
    duckdb_bind_add_result_column(info, "payload", type);
    duckdb_destroy_logical_type(&type);
    duckdb_bind_set_bind_data(info, bind, destroy_request_bind_data);
    duckdb_bind_set_cardinality(info, 1, true);
}

static void ducknng_request_scan(duckdb_function_info info, duckdb_data_chunk output) {
    ducknng_single_row_init_data *init = (ducknng_single_row_init_data *)duckdb_function_get_init_data(info);
    ducknng_request_bind_data *bind = (ducknng_request_bind_data *)duckdb_function_get_bind_data(info);
    bool *ok_data;
    if (!init || !bind || init->emitted) {
        duckdb_data_chunk_set_size(output, 0);
        return;
    }
    ok_data = (bool *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 0));
    ok_data[0] = bind->ok;
    if (bind->error) duckdb_vector_assign_string_element(duckdb_data_chunk_get_vector(output, 1), 0, bind->error);
    else set_null(duckdb_data_chunk_get_vector(output, 1), 0);
    if (bind->payload) assign_blob(duckdb_data_chunk_get_vector(output, 2), 0, bind->payload, bind->payload_len);
    else set_null(duckdb_data_chunk_get_vector(output, 2), 0);
    duckdb_data_chunk_set_size(output, 1);
    init->emitted = 1;
}

static void ducknng_ncurl_bind(duckdb_bind_info info) {
    ducknng_http_result_bind_data *bind;
    duckdb_logical_type type;
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
    int32_t timeout_ms;
    uint64_t tls_config_id;
    const ducknng_tls_opts *tls_opts = NULL;
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_bind_get_extra_info(info);
    if (ducknng_reject_table_inside_authorizer(info, ctx)) return;
    char *errmsg = NULL;
    uint16_t status = 0;
    size_t body_len = 0;
    bind = (ducknng_http_result_bind_data *)duckdb_malloc(sizeof(*bind));
    if (!bind) {
        duckdb_bind_set_error(info, "ducknng: out of memory");
        return;
    }
    memset(bind, 0, sizeof(*bind));
    url_val = duckdb_bind_get_parameter(info, 0);
    method_val = duckdb_bind_get_parameter(info, 1);
    headers_val = duckdb_bind_get_parameter(info, 2);
    body_val = duckdb_bind_get_parameter(info, 3);
    timeout_val = duckdb_bind_get_parameter(info, 4);
    tls_val = duckdb_bind_get_parameter(info, 5);
    url = duckdb_is_null_value(url_val) ? NULL : duckdb_get_varchar(url_val);
    method = duckdb_is_null_value(method_val) ? NULL : duckdb_get_varchar(method_val);
    headers_json = duckdb_is_null_value(headers_val) ? NULL : duckdb_get_varchar(headers_val);
    if (duckdb_is_null_value(body_val)) {
        memset(&body_blob, 0, sizeof(body_blob));
    } else {
        body_blob = duckdb_get_blob(body_val);
    }
    timeout_ms = duckdb_get_int32(timeout_val);
    tls_config_id = (uint64_t)duckdb_get_uint64(tls_val);
    duckdb_destroy_value(&url_val);
    duckdb_destroy_value(&method_val);
    duckdb_destroy_value(&headers_val);
    duckdb_destroy_value(&body_val);
    duckdb_destroy_value(&timeout_val);
    duckdb_destroy_value(&tls_val);
    if (ducknng_lookup_tls_opts(ctx, tls_config_id, &tls_opts, &errmsg) != 0) {
        bind->ok = false;
        bind->error = errmsg ? errmsg : ducknng_strdup("ducknng: tls config not found");
        errmsg = NULL;
    } else if (ducknng_http_transact(url, method, headers_json,
            (const uint8_t *)body_blob.data, (size_t)body_blob.size, timeout_ms, tls_opts,
            &status, &bind->headers_json, &bind->body, &body_len, &errmsg) != 0) {
        bind->ok = false;
        bind->error = errmsg ? errmsg : ducknng_strdup("ducknng: HTTP request failed");
        errmsg = NULL;
    } else {
        bind->ok = true;
        bind->status = (int32_t)status;
        bind->body_len = (idx_t)body_len;
        if (bind->body && bind->body_len > 0 && ducknng_bytes_look_text(bind->body, (size_t)bind->body_len)) {
            bind->body_text = ducknng_dup_bytes(bind->body, (size_t)bind->body_len);
            if (!bind->body_text) {
                bind->ok = false;
                bind->error = ducknng_strdup("ducknng: out of memory copying HTTP response text");
            }
        }
    }
    if (url) duckdb_free(url);
    if (method) duckdb_free(method);
    if (headers_json) duckdb_free(headers_json);
    if (body_blob.data) duckdb_free(body_blob.data);
    type = duckdb_create_logical_type(DUCKDB_TYPE_BOOLEAN);
    duckdb_bind_add_result_column(info, "ok", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_INTEGER);
    duckdb_bind_add_result_column(info, "status", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
    duckdb_bind_add_result_column(info, "error", type);
    duckdb_bind_add_result_column(info, "headers_json", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_BLOB);
    duckdb_bind_add_result_column(info, "body", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
    duckdb_bind_add_result_column(info, "body_text", type);
    duckdb_destroy_logical_type(&type);
    duckdb_bind_set_bind_data(info, bind, destroy_http_result_bind_data);
    duckdb_bind_set_cardinality(info, 1, true);
}

static void ducknng_ncurl_scan(duckdb_function_info info, duckdb_data_chunk output) {
    ducknng_single_row_init_data *init = (ducknng_single_row_init_data *)duckdb_function_get_init_data(info);
    ducknng_http_result_bind_data *bind = (ducknng_http_result_bind_data *)duckdb_function_get_bind_data(info);
    bool *ok_data;
    int32_t *status_data;
    if (!init || !bind || init->emitted) {
        duckdb_data_chunk_set_size(output, 0);
        return;
    }
    ok_data = (bool *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 0));
    status_data = (int32_t *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 1));
    ok_data[0] = bind->ok;
    if (bind->ok) status_data[0] = bind->status;
    else set_null(duckdb_data_chunk_get_vector(output, 1), 0);
    if (bind->error) duckdb_vector_assign_string_element(duckdb_data_chunk_get_vector(output, 2), 0, bind->error);
    else set_null(duckdb_data_chunk_get_vector(output, 2), 0);
    if (bind->headers_json) duckdb_vector_assign_string_element(duckdb_data_chunk_get_vector(output, 3), 0, bind->headers_json);
    else set_null(duckdb_data_chunk_get_vector(output, 3), 0);
    if (bind->body) assign_blob(duckdb_data_chunk_get_vector(output, 4), 0, bind->body, bind->body_len);
    else set_null(duckdb_data_chunk_get_vector(output, 4), 0);
    if (bind->body_text) duckdb_vector_assign_string_element(duckdb_data_chunk_get_vector(output, 5), 0, bind->body_text);
    else set_null(duckdb_data_chunk_get_vector(output, 5), 0);
    duckdb_data_chunk_set_size(output, 1);
    init->emitted = 1;
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

static idx_t ducknng_count_collectable_aios_locked(ducknng_runtime *rt, const uint64_t *aio_ids, idx_t aio_id_count) {
    idx_t i;
    idx_t count = 0;
    if (!rt || !aio_ids) return 0;
    for (i = 0; i < aio_id_count; i++) {
        ducknng_client_aio *slot = ducknng_find_client_aio_locked(rt, aio_ids[i]);
        if (!slot) continue;
        if (slot->state == DUCKNNG_CLIENT_AIO_READY || slot->state == DUCKNNG_CLIENT_AIO_ERROR ||
            slot->state == DUCKNNG_CLIENT_AIO_CANCELLED) {
            count++;
        }
    }
    return count;
}

static int ducknng_wait_any_for_ids(ducknng_runtime *rt, const uint64_t *aio_ids, idx_t aio_id_count, int32_t wait_ms) {
    uint64_t deadline_ms;
    idx_t ready_count;
    if (!rt || !aio_ids || aio_id_count == 0) return 0;
    deadline_ms = ducknng_now_ms() + (wait_ms > 0 ? (uint64_t)wait_ms : 0ULL);
    ducknng_mutex_lock(&rt->mu);
    for (;;) {
        ready_count = ducknng_count_collectable_aios_locked(rt, aio_ids, aio_id_count);
        if (ready_count > 0) {
            ducknng_mutex_unlock(&rt->mu);
            return 1;
        }
        if (wait_ms == 0 || ducknng_now_ms() >= deadline_ms) {
            ducknng_mutex_unlock(&rt->mu);
            return 0;
        }
        if (ducknng_cond_timedwait_ms(&rt->aio_cv, &rt->mu, deadline_ms - ducknng_now_ms()) != 0) {
            ducknng_mutex_unlock(&rt->mu);
            return 0;
        }
    }
}

static void ducknng_aio_collect_materialize(ducknng_aio_collect_bind_data *bind) {
    ducknng_runtime *rt;
    idx_t ready_count;
    idx_t row_index = 0;
    idx_t out_index = 0;
    if (!bind || bind->materialized || !bind->ctx || !(rt = bind->ctx->rt)) return;
    bind->materialized = 1;
    if (!ducknng_wait_any_for_ids(rt, bind->aio_ids, bind->aio_id_count, bind->wait_ms) && bind->wait_ms > 0) {
        return;
    }
    ducknng_mutex_lock(&rt->mu);
    ready_count = ducknng_count_collectable_aios_locked(rt, bind->aio_ids, bind->aio_id_count);
    if (ready_count > 0) {
        bind->rows = (ducknng_aio_collect_row *)duckdb_malloc(sizeof(*bind->rows) * (size_t)ready_count);
        if (bind->rows) memset(bind->rows, 0, sizeof(*bind->rows) * (size_t)ready_count);
    }
    if (ready_count > 0 && !bind->rows) {
        ducknng_mutex_unlock(&rt->mu);
        return;
    }
    bind->row_count = ready_count;
    for (row_index = 0; row_index < bind->aio_id_count && bind->rows; row_index++) {
        ducknng_client_aio *slot = ducknng_find_client_aio_locked(rt, bind->aio_ids[row_index]);
        ducknng_aio_collect_row *out_row;
        nng_msg *reply_msg;
        size_t frame_len;
        if (!slot) continue;
        if (!(slot->state == DUCKNNG_CLIENT_AIO_READY || slot->state == DUCKNNG_CLIENT_AIO_ERROR ||
                slot->state == DUCKNNG_CLIENT_AIO_CANCELLED)) {
            continue;
        }
        out_row = &bind->rows[out_index++];
        out_row->aio_id = slot->aio_id;
        out_row->ok = slot->state == DUCKNNG_CLIENT_AIO_READY;
        if (slot->error) out_row->error = ducknng_strdup(slot->error);
        if (slot->state == DUCKNNG_CLIENT_AIO_READY && (reply_msg = slot->reply_msg) != NULL) {
            frame_len = nng_msg_len(reply_msg);
            out_row->frame = (uint8_t *)duckdb_malloc(frame_len);
            if (out_row->frame || frame_len == 0) {
                out_row->frame_len = (idx_t)frame_len;
                if (frame_len) memcpy(out_row->frame, nng_msg_body(reply_msg), frame_len);
            }
            nng_msg_free(reply_msg);
            slot->reply_msg = NULL;
        }
        slot->state = DUCKNNG_CLIENT_AIO_COLLECTED;
    }
    ducknng_mutex_unlock(&rt->mu);
}

static void ducknng_aio_collect_bind(duckdb_bind_info info) {
    ducknng_aio_collect_bind_data *bind;
    duckdb_logical_type type;
    duckdb_value aio_ids_val;
    duckdb_value wait_val;
    idx_t i;
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_bind_get_extra_info(info);
    if (ducknng_reject_table_inside_authorizer(info, ctx)) return;
    bind = (ducknng_aio_collect_bind_data *)duckdb_malloc(sizeof(*bind));
    if (!bind) {
        duckdb_bind_set_error(info, "ducknng: out of memory");
        return;
    }
    memset(bind, 0, sizeof(*bind));
    bind->ctx = ctx;
    aio_ids_val = duckdb_bind_get_parameter(info, 0);
    wait_val = duckdb_bind_get_parameter(info, 1);
    bind->wait_ms = duckdb_get_int32(wait_val);
    if (bind->wait_ms < 0) {
        duckdb_destroy_value(&aio_ids_val);
        duckdb_destroy_value(&wait_val);
        duckdb_free(bind);
        duckdb_bind_set_error(info, "ducknng: aio_collect wait_ms must be >= 0");
        return;
    }
    bind->aio_id_count = duckdb_get_list_size(aio_ids_val);
    if (bind->aio_id_count > 0) {
        bind->aio_ids = (uint64_t *)duckdb_malloc(sizeof(*bind->aio_ids) * (size_t)bind->aio_id_count);
        if (!bind->aio_ids) {
            duckdb_destroy_value(&aio_ids_val);
            duckdb_destroy_value(&wait_val);
            duckdb_free(bind);
            duckdb_bind_set_error(info, "ducknng: out of memory");
            return;
        }
        for (i = 0; i < bind->aio_id_count; i++) {
            duckdb_value child = duckdb_get_list_child(aio_ids_val, i);
            bind->aio_ids[i] = (uint64_t)duckdb_get_uint64(child);
            duckdb_destroy_value(&child);
        }
    }
    duckdb_destroy_value(&aio_ids_val);
    duckdb_destroy_value(&wait_val);
    type = duckdb_create_logical_type(DUCKDB_TYPE_UBIGINT);
    duckdb_bind_add_result_column(info, "aio_id", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_BOOLEAN);
    duckdb_bind_add_result_column(info, "ok", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
    duckdb_bind_add_result_column(info, "error", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_BLOB);
    duckdb_bind_add_result_column(info, "frame", type);
    duckdb_destroy_logical_type(&type);
    duckdb_bind_set_bind_data(info, bind, destroy_aio_collect_bind_data);
}

static void ducknng_aio_collect_init(duckdb_init_info info) {
    ducknng_aio_collect_bind_data *bind = (ducknng_aio_collect_bind_data *)duckdb_init_get_bind_data(info);
    ducknng_aio_collect_init_data *init = (ducknng_aio_collect_init_data *)duckdb_malloc(sizeof(*init));
    if (!bind) {
        duckdb_init_set_error(info, "ducknng: missing aio collect bind data");
        return;
    }
    ducknng_aio_collect_materialize(bind);
    if (!init) {
        duckdb_init_set_error(info, "ducknng: out of memory");
        return;
    }
    init->bind = bind;
    init->offset = 0;
    duckdb_init_set_max_threads(info, 1);
    duckdb_init_set_init_data(info, init, destroy_aio_collect_init_data);
}

static void ducknng_aio_collect_scan(duckdb_function_info info, duckdb_data_chunk output) {
    ducknng_aio_collect_init_data *init = (ducknng_aio_collect_init_data *)duckdb_function_get_init_data(info);
    ducknng_aio_collect_bind_data *bind;
    idx_t remaining;
    idx_t chunk_size;
    idx_t i;
    uint64_t *aio_ids;
    bool *ok;
    if (!init || !init->bind || init->offset >= init->bind->row_count) {
        duckdb_data_chunk_set_size(output, 0);
        return;
    }
    bind = init->bind;
    remaining = bind->row_count - init->offset;
    chunk_size = remaining > duckdb_vector_size() ? duckdb_vector_size() : remaining;
    aio_ids = (uint64_t *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 0));
    ok = (bool *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 1));
    for (i = 0; i < chunk_size; i++) {
        ducknng_aio_collect_row *row = &bind->rows[init->offset + i];
        aio_ids[i] = row->aio_id;
        ok[i] = row->ok;
        if (row->error) duckdb_vector_assign_string_element(duckdb_data_chunk_get_vector(output, 2), i, row->error);
        else set_null(duckdb_data_chunk_get_vector(output, 2), i);
        if (row->frame) assign_blob(duckdb_data_chunk_get_vector(output, 3), i, row->frame, row->frame_len);
        else set_null(duckdb_data_chunk_get_vector(output, 3), i);
    }
    init->offset += chunk_size;
    duckdb_data_chunk_set_size(output, chunk_size);
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

static int register_scalar_ex(duckdb_connection con, const char *name, idx_t nparams,
    duckdb_scalar_function_t fn, ducknng_sql_context *ctx, duckdb_type *param_types,
    duckdb_type return_type_id, bool is_volatile) {
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
    if (is_volatile) duckdb_scalar_function_set_volatile(f);
    if (!ducknng_set_scalar_sql_context(f, ctx)) { duckdb_destroy_scalar_function(&f); return 0; }
    if (duckdb_register_scalar_function(con, f) == DuckDBError) { duckdb_destroy_scalar_function(&f); return 0; }
    duckdb_destroy_scalar_function(&f);
    return 1;
}

static int register_scalar(duckdb_connection con, const char *name, idx_t nparams,
    duckdb_scalar_function_t fn, ducknng_sql_context *ctx, duckdb_type *param_types,
    duckdb_type return_type_id) {
    return register_scalar_ex(con, name, nparams, fn, ctx, param_types, return_type_id, false);
}

static int register_volatile_scalar(duckdb_connection con, const char *name, idx_t nparams,
    duckdb_scalar_function_t fn, ducknng_sql_context *ctx, duckdb_type *param_types,
    duckdb_type return_type_id) {
    return register_scalar_ex(con, name, nparams, fn, ctx, param_types, return_type_id, true);
}

static int register_aio_wait_any_scalar_named(duckdb_connection con, ducknng_sql_context *ctx, const char *name) {
    duckdb_scalar_function fn;
    duckdb_logical_type u64_type;
    duckdb_logical_type list_u64_type;
    duckdb_logical_type int_type;
    duckdb_logical_type bool_type;
    if (!ctx || !ctx->rt) return 0;
    fn = duckdb_create_scalar_function();
    if (!fn) return 0;
    duckdb_scalar_function_set_name(fn, name);
    u64_type = duckdb_create_logical_type(DUCKDB_TYPE_UBIGINT);
    list_u64_type = duckdb_create_list_type(u64_type);
    int_type = duckdb_create_logical_type(DUCKDB_TYPE_INTEGER);
    bool_type = duckdb_create_logical_type(DUCKDB_TYPE_BOOLEAN);
    duckdb_scalar_function_add_parameter(fn, list_u64_type);
    duckdb_scalar_function_add_parameter(fn, int_type);
    duckdb_scalar_function_set_return_type(fn, bool_type);
    duckdb_scalar_function_set_function(fn, ducknng_aio_wait_any_scalar);
    duckdb_scalar_function_set_special_handling(fn);
    duckdb_scalar_function_set_volatile(fn);
    if (!ducknng_set_scalar_sql_context(fn, ctx)) { duckdb_destroy_scalar_function(&fn); return 0; }
    if (duckdb_register_scalar_function(con, fn) == DuckDBError) {
        duckdb_destroy_scalar_function(&fn);
        duckdb_destroy_logical_type(&u64_type);
        duckdb_destroy_logical_type(&list_u64_type);
        duckdb_destroy_logical_type(&int_type);
        duckdb_destroy_logical_type(&bool_type);
        return 0;
    }
    duckdb_destroy_scalar_function(&fn);
    duckdb_destroy_logical_type(&u64_type);
    duckdb_destroy_logical_type(&list_u64_type);
    duckdb_destroy_logical_type(&int_type);
    duckdb_destroy_logical_type(&bool_type);
    return 1;
}

static int register_aio_collect_row_scalar_named(duckdb_connection con, ducknng_sql_context *ctx, const char *name) {
    duckdb_scalar_function fn;
    duckdb_logical_type u64_type;
    duckdb_logical_type int_type;
    duckdb_logical_type bool_type;
    duckdb_logical_type varchar_type;
    duckdb_logical_type blob_type;
    duckdb_logical_type fields[4];
    const char *field_names[4] = {"aio_id", "ok", "error", "frame"};
    duckdb_logical_type struct_type;
    if (!ctx || !ctx->rt) return 0;
    fn = duckdb_create_scalar_function();
    if (!fn) return 0;
    duckdb_scalar_function_set_name(fn, name);
    u64_type = duckdb_create_logical_type(DUCKDB_TYPE_UBIGINT);
    int_type = duckdb_create_logical_type(DUCKDB_TYPE_INTEGER);
    bool_type = duckdb_create_logical_type(DUCKDB_TYPE_BOOLEAN);
    varchar_type = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
    blob_type = duckdb_create_logical_type(DUCKDB_TYPE_BLOB);
    fields[0] = duckdb_create_logical_type(DUCKDB_TYPE_UBIGINT);
    fields[1] = duckdb_create_logical_type(DUCKDB_TYPE_BOOLEAN);
    fields[2] = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
    fields[3] = duckdb_create_logical_type(DUCKDB_TYPE_BLOB);
    struct_type = duckdb_create_struct_type(fields, field_names, 4);
    duckdb_scalar_function_add_parameter(fn, u64_type);
    duckdb_scalar_function_add_parameter(fn, int_type);
    duckdb_scalar_function_set_return_type(fn, struct_type);
    duckdb_scalar_function_set_function(fn, ducknng_aio_collect_row_scalar);
    duckdb_scalar_function_set_special_handling(fn);
    duckdb_scalar_function_set_volatile(fn);
    if (!ducknng_set_scalar_sql_context(fn, ctx)) { duckdb_destroy_scalar_function(&fn); return 0; }
    if (duckdb_register_scalar_function(con, fn) == DuckDBError) {
        duckdb_destroy_scalar_function(&fn);
        duckdb_destroy_logical_type(&u64_type);
        duckdb_destroy_logical_type(&int_type);
        duckdb_destroy_logical_type(&bool_type);
        duckdb_destroy_logical_type(&varchar_type);
        duckdb_destroy_logical_type(&blob_type);
        for (int i = 0; i < 4; i++) duckdb_destroy_logical_type(&fields[i]);
        duckdb_destroy_logical_type(&struct_type);
        return 0;
    }
    duckdb_destroy_scalar_function(&fn);
    duckdb_destroy_logical_type(&u64_type);
    duckdb_destroy_logical_type(&int_type);
    duckdb_destroy_logical_type(&bool_type);
    duckdb_destroy_logical_type(&varchar_type);
    duckdb_destroy_logical_type(&blob_type);
    for (int i = 0; i < 4; i++) duckdb_destroy_logical_type(&fields[i]);
    duckdb_destroy_logical_type(&struct_type);
    return 1;
}

static int register_ncurl_aio_collect_row_scalar_named(duckdb_connection con, ducknng_sql_context *ctx, const char *name) {
    duckdb_scalar_function fn;
    duckdb_logical_type u64_type;
    duckdb_logical_type int_type;
    duckdb_logical_type bool_type;
    duckdb_logical_type varchar_type;
    duckdb_logical_type blob_type;
    duckdb_logical_type fields[7];
    const char *field_names[7] = {"aio_id", "ok", "status", "error", "headers_json", "body", "body_text"};
    duckdb_logical_type struct_type;
    if (!ctx || !ctx->rt) return 0;
    fn = duckdb_create_scalar_function();
    if (!fn) return 0;
    duckdb_scalar_function_set_name(fn, name);
    u64_type = duckdb_create_logical_type(DUCKDB_TYPE_UBIGINT);
    int_type = duckdb_create_logical_type(DUCKDB_TYPE_INTEGER);
    bool_type = duckdb_create_logical_type(DUCKDB_TYPE_BOOLEAN);
    varchar_type = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
    blob_type = duckdb_create_logical_type(DUCKDB_TYPE_BLOB);
    fields[0] = duckdb_create_logical_type(DUCKDB_TYPE_UBIGINT);
    fields[1] = duckdb_create_logical_type(DUCKDB_TYPE_BOOLEAN);
    fields[2] = duckdb_create_logical_type(DUCKDB_TYPE_INTEGER);
    fields[3] = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
    fields[4] = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
    fields[5] = duckdb_create_logical_type(DUCKDB_TYPE_BLOB);
    fields[6] = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
    struct_type = duckdb_create_struct_type(fields, field_names, 7);
    duckdb_scalar_function_add_parameter(fn, u64_type);
    duckdb_scalar_function_add_parameter(fn, int_type);
    duckdb_scalar_function_set_return_type(fn, struct_type);
    duckdb_scalar_function_set_function(fn, ducknng_ncurl_aio_collect_row_scalar);
    duckdb_scalar_function_set_special_handling(fn);
    duckdb_scalar_function_set_volatile(fn);
    if (!ducknng_set_scalar_sql_context(fn, ctx)) { duckdb_destroy_scalar_function(&fn); return 0; }
    if (duckdb_register_scalar_function(con, fn) == DuckDBError) {
        duckdb_destroy_scalar_function(&fn);
        duckdb_destroy_logical_type(&u64_type);
        duckdb_destroy_logical_type(&int_type);
        duckdb_destroy_logical_type(&bool_type);
        duckdb_destroy_logical_type(&varchar_type);
        duckdb_destroy_logical_type(&blob_type);
        for (int i = 0; i < 7; i++) duckdb_destroy_logical_type(&fields[i]);
        duckdb_destroy_logical_type(&struct_type);
        return 0;
    }
    duckdb_destroy_scalar_function(&fn);
    duckdb_destroy_logical_type(&u64_type);
    duckdb_destroy_logical_type(&int_type);
    duckdb_destroy_logical_type(&bool_type);
    duckdb_destroy_logical_type(&varchar_type);
    duckdb_destroy_logical_type(&blob_type);
    for (int i = 0; i < 7; i++) duckdb_destroy_logical_type(&fields[i]);
    duckdb_destroy_logical_type(&struct_type);
    return 1;
}

static int register_aio_status_scalar_named(duckdb_connection con, ducknng_sql_context *ctx, const char *name) {
    duckdb_scalar_function fn;
    duckdb_logical_type u64_type;
    duckdb_logical_type bool_type;
    duckdb_logical_type varchar_type;
    duckdb_logical_type fields[12];
    const char *field_names[12] = {
        "aio_id", "exists", "kind", "state", "phase", "terminal",
        "send_done", "send_ok", "recv_done", "recv_ok", "has_reply_frame", "error"
    };
    duckdb_logical_type struct_type;
    if (!ctx || !ctx->rt) return 0;
    fn = duckdb_create_scalar_function();
    if (!fn) return 0;
    duckdb_scalar_function_set_name(fn, name);
    u64_type = duckdb_create_logical_type(DUCKDB_TYPE_UBIGINT);
    bool_type = duckdb_create_logical_type(DUCKDB_TYPE_BOOLEAN);
    varchar_type = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
    fields[0] = duckdb_create_logical_type(DUCKDB_TYPE_UBIGINT);
    fields[1] = duckdb_create_logical_type(DUCKDB_TYPE_BOOLEAN);
    fields[2] = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
    fields[3] = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
    fields[4] = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
    fields[5] = duckdb_create_logical_type(DUCKDB_TYPE_BOOLEAN);
    fields[6] = duckdb_create_logical_type(DUCKDB_TYPE_BOOLEAN);
    fields[7] = duckdb_create_logical_type(DUCKDB_TYPE_BOOLEAN);
    fields[8] = duckdb_create_logical_type(DUCKDB_TYPE_BOOLEAN);
    fields[9] = duckdb_create_logical_type(DUCKDB_TYPE_BOOLEAN);
    fields[10] = duckdb_create_logical_type(DUCKDB_TYPE_BOOLEAN);
    fields[11] = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
    struct_type = duckdb_create_struct_type(fields, field_names, 12);
    duckdb_scalar_function_add_parameter(fn, u64_type);
    duckdb_scalar_function_set_return_type(fn, struct_type);
    duckdb_scalar_function_set_function(fn, ducknng_aio_status_scalar);
    duckdb_scalar_function_set_special_handling(fn);
    duckdb_scalar_function_set_volatile(fn);
    if (!ducknng_set_scalar_sql_context(fn, ctx)) { duckdb_destroy_scalar_function(&fn); return 0; }
    if (duckdb_register_scalar_function(con, fn) == DuckDBError) {
        duckdb_destroy_scalar_function(&fn);
        duckdb_destroy_logical_type(&u64_type);
        duckdb_destroy_logical_type(&bool_type);
        duckdb_destroy_logical_type(&varchar_type);
        for (int i = 0; i < 12; i++) duckdb_destroy_logical_type(&fields[i]);
        duckdb_destroy_logical_type(&struct_type);
        return 0;
    }
    duckdb_destroy_scalar_function(&fn);
    duckdb_destroy_logical_type(&u64_type);
    duckdb_destroy_logical_type(&bool_type);
    duckdb_destroy_logical_type(&varchar_type);
    for (int i = 0; i < 12; i++) duckdb_destroy_logical_type(&fields[i]);
    duckdb_destroy_logical_type(&struct_type);
    return 1;
}

static int register_aio_status_macro(duckdb_connection con) {
    duckdb_result result;
    const char *sql =
        "CREATE OR REPLACE MACRO ducknng_aio_status(aio_id) AS TABLE "
        "SELECT struct_extract(s, 'aio_id') AS aio_id, "
        "       struct_extract(s, 'exists') AS exists, "
        "       struct_extract(s, 'kind') AS kind, "
        "       struct_extract(s, 'state') AS state, "
        "       struct_extract(s, 'phase') AS phase, "
        "       struct_extract(s, 'terminal') AS terminal, "
        "       struct_extract(s, 'send_done') AS send_done, "
        "       struct_extract(s, 'send_ok') AS send_ok, "
        "       struct_extract(s, 'recv_done') AS recv_done, "
        "       struct_extract(s, 'recv_ok') AS recv_ok, "
        "       struct_extract(s, 'has_reply_frame') AS has_reply_frame, "
        "       struct_extract(s, 'error') AS error "
        "FROM (SELECT ducknng__aio_status_row(aio_id) AS s)";
    memset(&result, 0, sizeof(result));
    if (duckdb_query(con, sql, &result) == DuckDBError) {
        duckdb_destroy_result(&result);
        return 0;
    }
    duckdb_destroy_result(&result);
    return 1;
}

static int register_aio_collect_macro(duckdb_connection con) {
    duckdb_result result;
    const char *sql =
        "CREATE OR REPLACE MACRO ducknng_aio_collect(aio_ids, wait_ms) AS TABLE "
        "WITH _input AS (SELECT aio_ids AS aio_ids, ducknng__aio_wait_any(aio_ids, wait_ms) AS have_ready) "
        "SELECT struct_extract(r, 'aio_id') AS aio_id, "
        "       struct_extract(r, 'ok') AS ok, "
        "       struct_extract(r, 'error') AS error, "
        "       struct_extract(r, 'frame') AS frame "
        "FROM _input, "
        "UNNEST(list_transform("
        "  list_filter(aio_ids, lambda x: ducknng__aio_collectable(x) AND ducknng__aio_mark_collected(x)), "
        "  lambda x: ducknng__aio_collect_row(x, 0)" 
        ")) AS t(r) "
        "WHERE have_ready";
    memset(&result, 0, sizeof(result));
    if (duckdb_query(con, sql, &result) == DuckDBError) {
        duckdb_destroy_result(&result);
        return 0;
    }
    duckdb_destroy_result(&result);
    return 1;
}

static int register_ncurl_aio_collect_macro(duckdb_connection con) {
    duckdb_result result;
    const char *sql =
        "CREATE OR REPLACE MACRO ducknng_ncurl_aio_collect(aio_ids, wait_ms) AS TABLE "
        "WITH _input AS (SELECT aio_ids AS aio_ids, ducknng__aio_wait_any(aio_ids, wait_ms) AS have_ready) "
        "SELECT struct_extract(r, 'aio_id') AS aio_id, "
        "       struct_extract(r, 'ok') AS ok, "
        "       struct_extract(r, 'status') AS status, "
        "       struct_extract(r, 'error') AS error, "
        "       struct_extract(r, 'headers_json') AS headers_json, "
        "       struct_extract(r, 'body') AS body, "
        "       struct_extract(r, 'body_text') AS body_text "
        "FROM _input, "
        "UNNEST(list_transform("
        "  list_filter(aio_ids, lambda x: ducknng__ncurl_aio_collectable(x) AND ducknng__aio_mark_collected(x)), "
        "  lambda x: ducknng__ncurl_aio_collect_row(x, 0)"
        ")) AS t(r) "
        "WHERE have_ready";
    memset(&result, 0, sizeof(result));
    if (duckdb_query(con, sql, &result) == DuckDBError) {
        duckdb_destroy_result(&result);
        return 0;
    }
    duckdb_destroy_result(&result);
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
            bind->rows[i].sessions = svc ? (uint64_t)atomic_load_explicit(&svc->session_count_visible, memory_order_acquire) : 0;
            bind->rows[i].max_open_sessions = svc ? ducknng_service_max_open_sessions(svc) : 0;
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
    duckdb_bind_add_result_column(info, "max_open_sessions", type);
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
    duckdb_vector vec_max_open_sessions;
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
    uint64_t *max_open_sessions;
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
    vec_max_open_sessions = duckdb_data_chunk_get_vector(output, 6);
    vec_tls_enabled = duckdb_data_chunk_get_vector(output, 7);
    vec_tls_auth_mode = duckdb_data_chunk_get_vector(output, 8);
    vec_peer_identity_required = duckdb_data_chunk_get_vector(output, 9);
    vec_peer_allowlist_active = duckdb_data_chunk_get_vector(output, 10);
    vec_ip_allowlist_active = duckdb_data_chunk_get_vector(output, 11);
    vec_sql_authorizer_active = duckdb_data_chunk_get_vector(output, 12);
    vec_peer_allowlist_count = duckdb_data_chunk_get_vector(output, 13);
    vec_ip_allowlist_count = duckdb_data_chunk_get_vector(output, 14);

    service_ids = (uint64_t *)duckdb_vector_get_data(vec_service_id);
    contexts = (int32_t *)duckdb_vector_get_data(vec_contexts);
    running = (bool *)duckdb_vector_get_data(vec_running);
    sessions = (uint64_t *)duckdb_vector_get_data(vec_sessions);
    max_open_sessions = (uint64_t *)duckdb_vector_get_data(vec_max_open_sessions);
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
        max_open_sessions[i] = row->max_open_sessions;
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
            bind->rows[i].url = sock ? ducknng_strdup(sock->url ? sock->url : sock->listen_url) : NULL;
            bind->rows[i].open = sock ? (bool)sock->open : false;
            bind->rows[i].connected = sock ? (bool)sock->connected : false;
            bind->rows[i].listening = sock ? (bool)sock->has_listener : false;
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
    duckdb_bind_add_result_column(info, "listening", type);
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
    duckdb_vector vec_listening;
    duckdb_vector vec_send_timeout_ms;
    duckdb_vector vec_recv_timeout_ms;
    uint64_t *socket_ids;
    bool *open;
    bool *connected;
    bool *listening;
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
    vec_listening = duckdb_data_chunk_get_vector(output, 5);
    vec_send_timeout_ms = duckdb_data_chunk_get_vector(output, 6);
    vec_recv_timeout_ms = duckdb_data_chunk_get_vector(output, 7);
    socket_ids = (uint64_t *)duckdb_vector_get_data(vec_socket_id);
    open = (bool *)duckdb_vector_get_data(vec_open);
    connected = (bool *)duckdb_vector_get_data(vec_connected);
    listening = (bool *)duckdb_vector_get_data(vec_listening);
    send_timeout_ms = (int32_t *)duckdb_vector_get_data(vec_send_timeout_ms);
    recv_timeout_ms = (int32_t *)duckdb_vector_get_data(vec_recv_timeout_ms);
    for (i = 0; i < chunk_size; i++) {
        ducknng_socket_row *row = &bind->rows[init->offset + i];
        socket_ids[i] = row->socket_id;
        open[i] = row->open;
        connected[i] = row->connected;
        listening[i] = row->listening;
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

static void ducknng_tls_configs_bind(duckdb_bind_info info) {
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_bind_get_extra_info(info);
    ducknng_tls_configs_bind_data *bind;
    duckdb_logical_type type;
    size_t i;
    if (!ctx || !ctx->rt) {
        duckdb_bind_set_error(info, "ducknng: missing runtime");
        return;
    }
    bind = (ducknng_tls_configs_bind_data *)duckdb_malloc(sizeof(*bind));
    if (!bind) {
        duckdb_bind_set_error(info, "ducknng: out of memory");
        return;
    }
    memset(bind, 0, sizeof(*bind));
    ducknng_mutex_lock(&ctx->rt->mu);
    bind->row_count = (idx_t)ctx->rt->tls_config_count;
    if (bind->row_count > 0) {
        bind->rows = (ducknng_tls_config_row *)duckdb_malloc(sizeof(*bind->rows) * (size_t)bind->row_count);
        if (!bind->rows) {
            ducknng_mutex_unlock(&ctx->rt->mu);
            duckdb_free(bind);
            duckdb_bind_set_error(info, "ducknng: out of memory");
            return;
        }
        memset(bind->rows, 0, sizeof(*bind->rows) * (size_t)bind->row_count);
        for (i = 0; i < (size_t)bind->row_count; i++) {
            ducknng_tls_config *cfg = ctx->rt->tls_configs[i];
            bind->rows[i].tls_config_id = cfg ? cfg->tls_config_id : 0;
            bind->rows[i].source = cfg && cfg->source ? ducknng_strdup(cfg->source) : NULL;
            bind->rows[i].enabled = cfg ? (bool)cfg->opts.enabled : false;
            bind->rows[i].has_cert_key_file = cfg && cfg->opts.cert_key_file && cfg->opts.cert_key_file[0];
            bind->rows[i].has_ca_file = cfg && cfg->opts.ca_file && cfg->opts.ca_file[0];
            bind->rows[i].has_cert_pem = cfg && cfg->opts.cert_pem && cfg->opts.cert_pem[0];
            bind->rows[i].has_key_pem = cfg && cfg->opts.key_pem && cfg->opts.key_pem[0];
            bind->rows[i].has_ca_pem = cfg && cfg->opts.ca_pem && cfg->opts.ca_pem[0];
            bind->rows[i].has_password = cfg && cfg->opts.password && cfg->opts.password[0];
            bind->rows[i].auth_mode = cfg ? cfg->opts.auth_mode : 0;
            bind->rows[i].peer_allowlist_active = cfg ? (bool)cfg->opts.peer_allowlist_active : false;
            bind->rows[i].peer_allowlist_count = cfg ? (uint64_t)cfg->opts.peer_allowlist_count : 0;
            bind->rows[i].peer_allowlist_json = cfg && cfg->opts.peer_allowlist_json ? ducknng_strdup(cfg->opts.peer_allowlist_json) : NULL;
        }
    }
    ducknng_mutex_unlock(&ctx->rt->mu);
    type = duckdb_create_logical_type(DUCKDB_TYPE_UBIGINT);
    duckdb_bind_add_result_column(info, "tls_config_id", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
    duckdb_bind_add_result_column(info, "source", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_BOOLEAN);
    duckdb_bind_add_result_column(info, "enabled", type);
    duckdb_bind_add_result_column(info, "has_cert_key_file", type);
    duckdb_bind_add_result_column(info, "has_ca_file", type);
    duckdb_bind_add_result_column(info, "has_cert_pem", type);
    duckdb_bind_add_result_column(info, "has_key_pem", type);
    duckdb_bind_add_result_column(info, "has_ca_pem", type);
    duckdb_bind_add_result_column(info, "has_password", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_INTEGER);
    duckdb_bind_add_result_column(info, "auth_mode", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_BOOLEAN);
    duckdb_bind_add_result_column(info, "peer_allowlist_active", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_UBIGINT);
    duckdb_bind_add_result_column(info, "peer_allowlist_count", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
    duckdb_bind_add_result_column(info, "peer_allowlist_json", type);
    duckdb_destroy_logical_type(&type);
    duckdb_bind_set_bind_data(info, bind, destroy_tls_configs_bind_data);
    duckdb_bind_set_cardinality(info, bind->row_count, true);
}

static void ducknng_tls_configs_init(duckdb_init_info info) {
    ducknng_tls_configs_bind_data *bind = (ducknng_tls_configs_bind_data *)duckdb_init_get_bind_data(info);
    ducknng_tls_configs_init_data *init = (ducknng_tls_configs_init_data *)duckdb_malloc(sizeof(*init));
    if (!init) {
        duckdb_init_set_error(info, "ducknng: out of memory");
        return;
    }
    init->bind = bind;
    init->offset = 0;
    duckdb_init_set_max_threads(info, 1);
    duckdb_init_set_init_data(info, init, destroy_tls_configs_init_data);
}

static void ducknng_tls_configs_scan(duckdb_function_info info, duckdb_data_chunk output) {
    ducknng_tls_configs_init_data *init = (ducknng_tls_configs_init_data *)duckdb_function_get_init_data(info);
    ducknng_tls_configs_bind_data *bind;
    idx_t remaining;
    idx_t chunk_size;
    idx_t i;
    uint64_t *ids;
    bool *enabled;
    bool *has_cert_key_file;
    bool *has_ca_file;
    bool *has_cert_pem;
    bool *has_key_pem;
    bool *has_ca_pem;
    bool *has_password;
    int32_t *auth_mode;
    bool *peer_allowlist_active;
    uint64_t *peer_allowlist_count;
    if (!init || !init->bind || init->offset >= init->bind->row_count) {
        duckdb_data_chunk_set_size(output, 0);
        return;
    }
    bind = init->bind;
    remaining = bind->row_count - init->offset;
    chunk_size = remaining > duckdb_vector_size() ? duckdb_vector_size() : remaining;
    ids = (uint64_t *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 0));
    enabled = (bool *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 2));
    has_cert_key_file = (bool *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 3));
    has_ca_file = (bool *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 4));
    has_cert_pem = (bool *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 5));
    has_key_pem = (bool *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 6));
    has_ca_pem = (bool *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 7));
    has_password = (bool *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 8));
    auth_mode = (int32_t *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 9));
    peer_allowlist_active = (bool *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 10));
    peer_allowlist_count = (uint64_t *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 11));
    for (i = 0; i < chunk_size; i++) {
        ducknng_tls_config_row *row = &bind->rows[init->offset + i];
        ids[i] = row->tls_config_id;
        enabled[i] = row->enabled;
        has_cert_key_file[i] = row->has_cert_key_file;
        has_ca_file[i] = row->has_ca_file;
        has_cert_pem[i] = row->has_cert_pem;
        has_key_pem[i] = row->has_key_pem;
        has_ca_pem[i] = row->has_ca_pem;
        has_password[i] = row->has_password;
        auth_mode[i] = row->auth_mode;
        peer_allowlist_active[i] = row->peer_allowlist_active;
        peer_allowlist_count[i] = row->peer_allowlist_count;
        if (row->source) duckdb_vector_assign_string_element(duckdb_data_chunk_get_vector(output, 1), i, row->source);
        else set_null(duckdb_data_chunk_get_vector(output, 1), i);
        if (row->peer_allowlist_json) duckdb_vector_assign_string_element(duckdb_data_chunk_get_vector(output, 12), i, row->peer_allowlist_json);
        else set_null(duckdb_data_chunk_get_vector(output, 12), i);
    }
    init->offset += chunk_size;
    duckdb_data_chunk_set_size(output, chunk_size);
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

static int register_named_servers_table(duckdb_connection con, ducknng_sql_context *ctx, const char *name) {
    duckdb_table_function tf;
    if (!ctx || !ctx->rt) return 0;
    tf = duckdb_create_table_function();
    if (!tf) return 0;
    duckdb_table_function_set_name(tf, name);
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

static int register_named_sockets_table(duckdb_connection con, ducknng_sql_context *ctx, const char *name) {
    duckdb_table_function tf;
    if (!ctx || !ctx->rt) return 0;
    tf = duckdb_create_table_function();
    if (!tf) return 0;
    duckdb_table_function_set_name(tf, name);
    if (!ducknng_set_table_sql_context(tf, ctx)) { duckdb_destroy_table_function(&tf); return 0; }
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

static int register_named_tls_configs_table(duckdb_connection con, ducknng_sql_context *ctx, const char *name) {
    duckdb_table_function tf;
    if (!ctx || !ctx->rt) return 0;
    tf = duckdb_create_table_function();
    if (!tf) return 0;
    duckdb_table_function_set_name(tf, name);
    if (!ducknng_set_table_sql_context(tf, ctx)) { duckdb_destroy_table_function(&tf); return 0; }
    duckdb_table_function_set_bind(tf, ducknng_tls_configs_bind);
    duckdb_table_function_set_init(tf, ducknng_tls_configs_init);
    duckdb_table_function_set_function(tf, ducknng_tls_configs_scan);
    if (duckdb_register_table_function(con, tf) == DuckDBError) {
        duckdb_destroy_table_function(&tf);
        return 0;
    }
    duckdb_destroy_table_function(&tf);
    return 1;
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

static int register_remote_table_named(duckdb_connection con, ducknng_sql_context *ctx, const char *name) {
    duckdb_table_function tf;
    duckdb_logical_type type;
    duckdb_logical_type type_tls;
    if (!ctx || !ctx->rt) return 0;
    tf = duckdb_create_table_function();
    if (!tf) return 0;
    duckdb_table_function_set_name(tf, name);
    type = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
    type_tls = duckdb_create_logical_type(DUCKDB_TYPE_UBIGINT);
    duckdb_table_function_add_parameter(tf, type);
    duckdb_table_function_add_parameter(tf, type);
    duckdb_table_function_add_parameter(tf, type_tls);
    duckdb_destroy_logical_type(&type);
    duckdb_destroy_logical_type(&type_tls);
    if (!ducknng_set_table_sql_context(tf, ctx)) { duckdb_destroy_table_function(&tf); return 0; }
    duckdb_table_function_set_bind(tf, ducknng_query_rpc_bind);
    duckdb_table_function_set_init(tf, ducknng_query_rpc_init);
    duckdb_table_function_set_function(tf, ducknng_query_rpc_scan);
    if (duckdb_register_table_function(con, tf) == DuckDBError) {
        duckdb_destroy_table_function(&tf);
        return 0;
    }
    duckdb_destroy_table_function(&tf);
    return 1;
}

static int register_manifest_result_table_named(duckdb_connection con, ducknng_sql_context *ctx, const char *name) {
    duckdb_table_function tf;
    duckdb_logical_type type;
    duckdb_logical_type type_tls;
    if (!ctx || !ctx->rt) return 0;
    tf = duckdb_create_table_function();
    if (!tf) return 0;
    duckdb_table_function_set_name(tf, name);
    type = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
    type_tls = duckdb_create_logical_type(DUCKDB_TYPE_UBIGINT);
    duckdb_table_function_add_parameter(tf, type);
    duckdb_table_function_add_parameter(tf, type_tls);
    duckdb_destroy_logical_type(&type);
    duckdb_destroy_logical_type(&type_tls);
    if (!ducknng_set_table_sql_context(tf, ctx)) { duckdb_destroy_table_function(&tf); return 0; }
    duckdb_table_function_set_bind(tf, ducknng_get_rpc_manifest_bind);
    duckdb_table_function_set_init(tf, ducknng_single_row_init);
    duckdb_table_function_set_function(tf, ducknng_get_rpc_manifest_scan);
    if (duckdb_register_table_function(con, tf) == DuckDBError) {
        duckdb_destroy_table_function(&tf);
        return 0;
    }
    duckdb_destroy_table_function(&tf);
    return 1;
}

static int register_exec_result_table_named(duckdb_connection con, ducknng_sql_context *ctx, const char *name) {
    duckdb_table_function tf;
    duckdb_logical_type type;
    duckdb_logical_type type_tls;
    if (!ctx || !ctx->rt) return 0;
    tf = duckdb_create_table_function();
    if (!tf) return 0;
    duckdb_table_function_set_name(tf, name);
    type = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
    type_tls = duckdb_create_logical_type(DUCKDB_TYPE_UBIGINT);
    duckdb_table_function_add_parameter(tf, type);
    duckdb_table_function_add_parameter(tf, type);
    duckdb_table_function_add_parameter(tf, type_tls);
    duckdb_destroy_logical_type(&type);
    duckdb_destroy_logical_type(&type_tls);
    if (!ducknng_set_table_sql_context(tf, ctx)) { duckdb_destroy_table_function(&tf); return 0; }
    duckdb_table_function_set_bind(tf, ducknng_run_rpc_bind);
    duckdb_table_function_set_init(tf, ducknng_single_row_init);
    duckdb_table_function_set_function(tf, ducknng_run_rpc_scan);
    if (duckdb_register_table_function(con, tf) == DuckDBError) {
        duckdb_destroy_table_function(&tf);
        return 0;
    }
    duckdb_destroy_table_function(&tf);
    return 1;
}

static int register_request_result_table_named(duckdb_connection con, ducknng_sql_context *ctx, const char *name) {
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
    duckdb_table_function_add_parameter(tf, type_blob);
    duckdb_table_function_add_parameter(tf, type_int);
    duckdb_table_function_add_parameter(tf, type_tls);
    duckdb_destroy_logical_type(&type_varchar);
    duckdb_destroy_logical_type(&type_blob);
    duckdb_destroy_logical_type(&type_int);
    duckdb_destroy_logical_type(&type_tls);
    if (!ducknng_set_table_sql_context(tf, ctx)) { duckdb_destroy_table_function(&tf); return 0; }
    duckdb_table_function_set_bind(tf, ducknng_request_bind);
    duckdb_table_function_set_init(tf, ducknng_single_row_init);
    duckdb_table_function_set_function(tf, ducknng_request_scan);
    if (duckdb_register_table_function(con, tf) == DuckDBError) {
        duckdb_destroy_table_function(&tf);
        return 0;
    }
    duckdb_destroy_table_function(&tf);
    return 1;
}

static int register_request_socket_result_table_named(duckdb_connection con, ducknng_sql_context *ctx, const char *name) {
    duckdb_table_function tf;
    duckdb_logical_type type_u64;
    duckdb_logical_type type_blob;
    duckdb_logical_type type_int;
    if (!ctx || !ctx->rt) return 0;
    tf = duckdb_create_table_function();
    if (!tf) return 0;
    duckdb_table_function_set_name(tf, name);
    type_u64 = duckdb_create_logical_type(DUCKDB_TYPE_UBIGINT);
    type_blob = duckdb_create_logical_type(DUCKDB_TYPE_BLOB);
    type_int = duckdb_create_logical_type(DUCKDB_TYPE_INTEGER);
    duckdb_table_function_add_parameter(tf, type_u64);
    duckdb_table_function_add_parameter(tf, type_blob);
    duckdb_table_function_add_parameter(tf, type_int);
    duckdb_destroy_logical_type(&type_u64);
    duckdb_destroy_logical_type(&type_blob);
    duckdb_destroy_logical_type(&type_int);
    if (!ducknng_set_table_sql_context(tf, ctx)) { duckdb_destroy_table_function(&tf); return 0; }
    duckdb_table_function_set_bind(tf, ducknng_request_socket_bind);
    duckdb_table_function_set_init(tf, ducknng_single_row_init);
    duckdb_table_function_set_function(tf, ducknng_request_scan);
    if (duckdb_register_table_function(con, tf) == DuckDBError) {
        duckdb_destroy_table_function(&tf);
        return 0;
    }
    duckdb_destroy_table_function(&tf);
    return 1;
}

static int register_http_result_table_named(duckdb_connection con, ducknng_sql_context *ctx, const char *name) {
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
    duckdb_table_function_set_bind(tf, ducknng_ncurl_bind);
    duckdb_table_function_set_init(tf, ducknng_single_row_init);
    duckdb_table_function_set_function(tf, ducknng_ncurl_scan);
    if (duckdb_register_table_function(con, tf) == DuckDBError) {
        duckdb_destroy_table_function(&tf);
        return 0;
    }
    duckdb_destroy_table_function(&tf);
    return 1;
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

static int register_open_query_table_named(duckdb_connection con, ducknng_sql_context *ctx, const char *name) {
    duckdb_table_function tf;
    duckdb_logical_type type_varchar;
    duckdb_logical_type type_u64;
    if (!ctx || !ctx->rt) return 0;
    tf = duckdb_create_table_function();
    if (!tf) return 0;
    duckdb_table_function_set_name(tf, name);
    type_varchar = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
    type_u64 = duckdb_create_logical_type(DUCKDB_TYPE_UBIGINT);
    duckdb_table_function_add_parameter(tf, type_varchar);
    duckdb_table_function_add_parameter(tf, type_varchar);
    duckdb_table_function_add_parameter(tf, type_u64);
    duckdb_table_function_add_parameter(tf, type_u64);
    duckdb_table_function_add_parameter(tf, type_u64);
    duckdb_destroy_logical_type(&type_varchar);
    duckdb_destroy_logical_type(&type_u64);
    if (!ducknng_set_table_sql_context(tf, ctx)) { duckdb_destroy_table_function(&tf); return 0; }
    duckdb_table_function_set_bind(tf, ducknng_open_query_bind);
    duckdb_table_function_set_init(tf, ducknng_session_result_init);
    duckdb_table_function_set_function(tf, ducknng_session_control_scan);
    if (duckdb_register_table_function(con, tf) == DuckDBError) {
        duckdb_destroy_table_function(&tf);
        return 0;
    }
    duckdb_destroy_table_function(&tf);
    return 1;
}

static int register_session_control_table_named(duckdb_connection con, ducknng_sql_context *ctx,
    const char *name, duckdb_table_function_bind_t bind_fn) {
    duckdb_table_function tf;
    duckdb_logical_type type_varchar;
    duckdb_logical_type type_u64;
    if (!ctx || !ctx->rt) return 0;
    tf = duckdb_create_table_function();
    if (!tf) return 0;
    duckdb_table_function_set_name(tf, name);
    type_varchar = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
    type_u64 = duckdb_create_logical_type(DUCKDB_TYPE_UBIGINT);
    duckdb_table_function_add_parameter(tf, type_varchar);
    duckdb_table_function_add_parameter(tf, type_u64);
    duckdb_table_function_add_parameter(tf, type_varchar);
    duckdb_table_function_add_parameter(tf, type_u64);
    duckdb_destroy_logical_type(&type_varchar);
    duckdb_destroy_logical_type(&type_u64);
    if (!ducknng_set_table_sql_context(tf, ctx)) { duckdb_destroy_table_function(&tf); return 0; }
    duckdb_table_function_set_bind(tf, bind_fn);
    duckdb_table_function_set_init(tf, ducknng_session_result_init);
    duckdb_table_function_set_function(tf, ducknng_session_control_scan);
    if (duckdb_register_table_function(con, tf) == DuckDBError) {
        duckdb_destroy_table_function(&tf);
        return 0;
    }
    duckdb_destroy_table_function(&tf);
    return 1;
}

static int register_fetch_query_table_named(duckdb_connection con, ducknng_sql_context *ctx, const char *name) {
    duckdb_table_function tf;
    duckdb_logical_type type_varchar;
    duckdb_logical_type type_u64;
    if (!ctx || !ctx->rt) return 0;
    tf = duckdb_create_table_function();
    if (!tf) return 0;
    duckdb_table_function_set_name(tf, name);
    type_varchar = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
    type_u64 = duckdb_create_logical_type(DUCKDB_TYPE_UBIGINT);
    duckdb_table_function_add_parameter(tf, type_varchar);
    duckdb_table_function_add_parameter(tf, type_u64);
    duckdb_table_function_add_parameter(tf, type_varchar);
    duckdb_table_function_add_parameter(tf, type_u64);
    duckdb_table_function_add_parameter(tf, type_u64);
    duckdb_table_function_add_parameter(tf, type_u64);
    duckdb_destroy_logical_type(&type_varchar);
    duckdb_destroy_logical_type(&type_u64);
    if (!ducknng_set_table_sql_context(tf, ctx)) { duckdb_destroy_table_function(&tf); return 0; }
    duckdb_table_function_set_bind(tf, ducknng_fetch_query_bind);
    duckdb_table_function_set_init(tf, ducknng_session_result_init);
    duckdb_table_function_set_function(tf, ducknng_fetch_query_scan);
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

static int register_aio_collect_table_named(duckdb_connection con, ducknng_sql_context *ctx, const char *name) {
    duckdb_table_function tf;
    duckdb_logical_type type_u64;
    duckdb_logical_type type_list_u64;
    duckdb_logical_type type_int;
    if (!ctx || !ctx->rt) return 0;
    tf = duckdb_create_table_function();
    if (!tf) return 0;
    duckdb_table_function_set_name(tf, name);
    type_u64 = duckdb_create_logical_type(DUCKDB_TYPE_UBIGINT);
    type_list_u64 = duckdb_create_list_type(type_u64);
    type_int = duckdb_create_logical_type(DUCKDB_TYPE_INTEGER);
    duckdb_table_function_add_parameter(tf, type_list_u64);
    duckdb_table_function_add_parameter(tf, type_int);
    duckdb_destroy_logical_type(&type_u64);
    duckdb_destroy_logical_type(&type_list_u64);
    duckdb_destroy_logical_type(&type_int);
    if (!ducknng_set_table_sql_context(tf, ctx)) { duckdb_destroy_table_function(&tf); return 0; }
    duckdb_table_function_set_bind(tf, ducknng_aio_collect_bind);
    duckdb_table_function_set_init(tf, ducknng_aio_collect_init);
    duckdb_table_function_set_function(tf, ducknng_aio_collect_scan);
    if (duckdb_register_table_function(con, tf) == DuckDBError) {
        duckdb_destroy_table_function(&tf);
        return 0;
    }
    duckdb_destroy_table_function(&tf);
    return 1;
}

int ducknng_register_sql_api(duckdb_connection connection, ducknng_runtime *rt) {
    ducknng_sql_context ctx;
    duckdb_type start_tls_config_types[6] = {DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_INTEGER, DUCKDB_TYPE_UBIGINT,
        DUCKDB_TYPE_UBIGINT, DUCKDB_TYPE_UBIGINT};
    duckdb_type start_tls_config_ip_types[7] = {DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_INTEGER, DUCKDB_TYPE_UBIGINT,
        DUCKDB_TYPE_UBIGINT, DUCKDB_TYPE_UBIGINT, DUCKDB_TYPE_VARCHAR};
    duckdb_type start_http_types[5] = {DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_UBIGINT,
        DUCKDB_TYPE_UBIGINT, DUCKDB_TYPE_UBIGINT};
    duckdb_type start_http_ip_types[6] = {DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_UBIGINT,
        DUCKDB_TYPE_UBIGINT, DUCKDB_TYPE_UBIGINT, DUCKDB_TYPE_VARCHAR};
    duckdb_type stop_types[1] = {DUCKDB_TYPE_VARCHAR};
    duckdb_type rpc_exec_raw_types[3] = {DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_UBIGINT};
    duckdb_type rpc_exec_raw_aio_types[4] = {DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_INTEGER, DUCKDB_TYPE_UBIGINT};
    duckdb_type rpc_manifest_raw_types[2] = {DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_UBIGINT};
    duckdb_type rpc_manifest_raw_aio_types[3] = {DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_INTEGER, DUCKDB_TYPE_UBIGINT};
    duckdb_type ncurl_aio_types[6] = {DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_VARCHAR,
        DUCKDB_TYPE_BLOB, DUCKDB_TYPE_INTEGER, DUCKDB_TYPE_UBIGINT};
    duckdb_type socket_types[1] = {DUCKDB_TYPE_VARCHAR};
    duckdb_type dial_types[4] = {DUCKDB_TYPE_UBIGINT, DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_INTEGER, DUCKDB_TYPE_UBIGINT};
    duckdb_type listen_types[4] = {DUCKDB_TYPE_UBIGINT, DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_UBIGINT, DUCKDB_TYPE_UBIGINT};
    duckdb_type close_types[1] = {DUCKDB_TYPE_UBIGINT};
    duckdb_type request_socket_types[3] = {DUCKDB_TYPE_UBIGINT, DUCKDB_TYPE_BLOB, DUCKDB_TYPE_INTEGER};
    duckdb_type recv_socket_types[2] = {DUCKDB_TYPE_UBIGINT, DUCKDB_TYPE_INTEGER};
    duckdb_type subscribe_types[2] = {DUCKDB_TYPE_UBIGINT, DUCKDB_TYPE_BLOB};
    duckdb_type request_tls_types[4] = {DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_BLOB, DUCKDB_TYPE_INTEGER, DUCKDB_TYPE_UBIGINT};
    duckdb_type aio_id_types[1] = {DUCKDB_TYPE_UBIGINT};
    duckdb_type tls_files_types[4] = {DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_INTEGER};
    duckdb_type tls_pem_types[5] = {DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_INTEGER};
    duckdb_type tls_self_signed_types[3] = {DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_INTEGER, DUCKDB_TYPE_INTEGER};
    duckdb_type tls_id_types[1] = {DUCKDB_TYPE_UBIGINT};
    duckdb_type tls_allowlist_types[2] = {DUCKDB_TYPE_UBIGINT, DUCKDB_TYPE_VARCHAR};
    duckdb_type service_allowlist_types[2] = {DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_VARCHAR};
    duckdb_type service_limits_types[2] = {DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_UBIGINT};
    duckdb_type method_name_types[1] = {DUCKDB_TYPE_VARCHAR};
    duckdb_type method_auth_types[2] = {DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_BOOLEAN};
    duckdb_type method_family_types[1] = {DUCKDB_TYPE_VARCHAR};
    duckdb_type register_exec_auth_types[1] = {DUCKDB_TYPE_BOOLEAN};
    ctx.rt = rt;
    ctx.is_init_connection = connection == rt->init_con;
    if (!register_scalar(connection, "ducknng_start_server", 6, ducknng_server_start_tls_config_scalar, &ctx, start_tls_config_types, DUCKDB_TYPE_BOOLEAN)) return 0;
    if (!register_scalar(connection, "ducknng_start_server", 7, ducknng_server_start_tls_config_scalar, &ctx, start_tls_config_ip_types, DUCKDB_TYPE_BOOLEAN)) return 0;
    if (!register_scalar(connection, "ducknng_start_http_server", 5, ducknng_http_server_start_scalar, &ctx, start_http_types, DUCKDB_TYPE_BOOLEAN)) return 0;
    if (!register_scalar(connection, "ducknng_start_http_server", 6, ducknng_http_server_start_scalar, &ctx, start_http_ip_types, DUCKDB_TYPE_BOOLEAN)) return 0;
    if (!register_scalar(connection, "ducknng_stop_server", 1, ducknng_server_stop_scalar, &ctx, stop_types, DUCKDB_TYPE_BOOLEAN)) return 0;
    if (!register_scalar(connection, "ducknng_tls_config_from_files", 4, ducknng_tls_config_from_files_scalar, &ctx, tls_files_types, DUCKDB_TYPE_UBIGINT)) return 0;
    if (!register_scalar(connection, "ducknng_tls_config_from_pem", 5, ducknng_tls_config_from_pem_scalar, &ctx, tls_pem_types, DUCKDB_TYPE_UBIGINT)) return 0;
    if (!register_scalar(connection, "ducknng_self_signed_tls_config", 3, ducknng_self_signed_tls_config_scalar, &ctx, tls_self_signed_types, DUCKDB_TYPE_UBIGINT)) return 0;
    if (!register_scalar(connection, "ducknng_drop_tls_config", 1, ducknng_drop_tls_config_scalar, &ctx, tls_id_types, DUCKDB_TYPE_BOOLEAN)) return 0;
    if (!register_scalar(connection, "ducknng_set_tls_peer_allowlist", 2, ducknng_set_tls_peer_allowlist_scalar, &ctx, tls_allowlist_types, DUCKDB_TYPE_BOOLEAN)) return 0;
    if (!register_scalar(connection, "ducknng_set_service_peer_allowlist", 2, ducknng_set_service_peer_allowlist_scalar, &ctx, service_allowlist_types, DUCKDB_TYPE_BOOLEAN)) return 0;
    if (!register_scalar(connection, "ducknng_set_service_ip_allowlist", 2, ducknng_set_service_ip_allowlist_scalar, &ctx, service_allowlist_types, DUCKDB_TYPE_BOOLEAN)) return 0;
    if (!register_scalar(connection, "ducknng_set_service_limits", 2, ducknng_set_service_limits_scalar, &ctx, service_limits_types, DUCKDB_TYPE_BOOLEAN)) return 0;
    if (!register_scalar(connection, "ducknng_register_exec_method", 0, ducknng_register_exec_method_scalar, &ctx, NULL, DUCKDB_TYPE_BOOLEAN)) return 0;
    if (!register_scalar(connection, "ducknng_register_exec_method", 1, ducknng_register_exec_method_scalar, &ctx, register_exec_auth_types, DUCKDB_TYPE_BOOLEAN)) return 0;
    if (!register_scalar(connection, "ducknng_set_method_auth", 2, ducknng_set_method_auth_scalar, &ctx, method_auth_types, DUCKDB_TYPE_BOOLEAN)) return 0;
    if (!register_scalar(connection, "ducknng_unregister_method", 1, ducknng_unregister_method_scalar, &ctx, method_name_types, DUCKDB_TYPE_BOOLEAN)) return 0;
    if (!register_scalar(connection, "ducknng_unregister_family", 1, ducknng_unregister_family_scalar, &ctx, method_family_types, DUCKDB_TYPE_UBIGINT)) return 0;
    if (!register_scalar(connection, "ducknng_run_rpc_raw", 3, ducknng_run_rpc_raw_scalar, &ctx, rpc_exec_raw_types, DUCKDB_TYPE_BLOB)) return 0;
    if (!register_scalar(connection, "ducknng_run_rpc_raw_aio", 4, ducknng_run_rpc_raw_aio_scalar, &ctx, rpc_exec_raw_aio_types, DUCKDB_TYPE_UBIGINT)) return 0;
    if (!register_scalar(connection, "ducknng_get_rpc_manifest_raw", 2, ducknng_get_rpc_manifest_raw_scalar, &ctx, rpc_manifest_raw_types, DUCKDB_TYPE_BLOB)) return 0;
    if (!register_scalar(connection, "ducknng_get_rpc_manifest_raw_aio", 3, ducknng_get_rpc_manifest_raw_aio_scalar, &ctx, rpc_manifest_raw_aio_types, DUCKDB_TYPE_UBIGINT)) return 0;
    if (!register_scalar(connection, "ducknng_ncurl_aio", 6, ducknng_ncurl_aio_scalar, &ctx, ncurl_aio_types, DUCKDB_TYPE_UBIGINT)) return 0;
    if (!register_scalar(connection, "ducknng_open_socket", 1, ducknng_socket_scalar, &ctx, socket_types, DUCKDB_TYPE_UBIGINT)) return 0;
    if (!register_scalar(connection, "ducknng_dial_socket", 4, ducknng_dial_scalar, &ctx, dial_types, DUCKDB_TYPE_BOOLEAN)) return 0;
    if (!register_scalar(connection, "ducknng_listen_socket", 4, ducknng_listen_scalar, &ctx, listen_types, DUCKDB_TYPE_BOOLEAN)) return 0;
    if (!register_scalar(connection, "ducknng_close_socket", 1, ducknng_close_scalar, &ctx, close_types, DUCKDB_TYPE_BOOLEAN)) return 0;
    if (!register_scalar(connection, "ducknng_send_socket_raw", 3, ducknng_send_scalar, &ctx, request_socket_types, DUCKDB_TYPE_BOOLEAN)) return 0;
    if (!register_scalar(connection, "ducknng_recv_socket_raw", 2, ducknng_recv_scalar, &ctx, recv_socket_types, DUCKDB_TYPE_BLOB)) return 0;
    if (!register_scalar(connection, "ducknng_subscribe_socket", 2, ducknng_subscribe_scalar, &ctx, subscribe_types, DUCKDB_TYPE_BOOLEAN)) return 0;
    if (!register_scalar(connection, "ducknng_unsubscribe_socket", 2, ducknng_unsubscribe_scalar, &ctx, subscribe_types, DUCKDB_TYPE_BOOLEAN)) return 0;
    if (!register_scalar(connection, "ducknng_request_socket_raw", 3, ducknng_request_socket_scalar, &ctx, request_socket_types, DUCKDB_TYPE_BLOB)) return 0;
    if (!register_scalar(connection, "ducknng_request_raw", 4, ducknng_request_raw_scalar, &ctx, request_tls_types, DUCKDB_TYPE_BLOB)) return 0;
    if (!register_scalar(connection, "ducknng_send_socket_raw_aio", 3, ducknng_send_socket_raw_aio_scalar, &ctx, request_socket_types, DUCKDB_TYPE_UBIGINT)) return 0;
    if (!register_scalar(connection, "ducknng_recv_socket_raw_aio", 2, ducknng_recv_socket_raw_aio_scalar, &ctx, recv_socket_types, DUCKDB_TYPE_UBIGINT)) return 0;
    if (!register_scalar(connection, "ducknng_request_socket_raw_aio", 3, ducknng_request_socket_raw_aio_scalar, &ctx, request_socket_types, DUCKDB_TYPE_UBIGINT)) return 0;
    if (!register_scalar(connection, "ducknng_request_raw_aio", 4, ducknng_request_raw_aio_scalar, &ctx, request_tls_types, DUCKDB_TYPE_UBIGINT)) return 0;
    if (!register_scalar(connection, "ducknng_aio_ready", 1, ducknng_aio_ready_scalar, &ctx, aio_id_types, DUCKDB_TYPE_BOOLEAN)) return 0;
    if (!register_scalar(connection, "ducknng_aio_cancel", 1, ducknng_aio_cancel_scalar, &ctx, aio_id_types, DUCKDB_TYPE_BOOLEAN)) return 0;
    if (!register_scalar(connection, "ducknng_aio_drop", 1, ducknng_aio_drop_scalar, &ctx, aio_id_types, DUCKDB_TYPE_BOOLEAN)) return 0;
    if (!register_volatile_scalar(connection, "ducknng__aio_collectable", 1, ducknng_aio_collectable_scalar, &ctx, aio_id_types, DUCKDB_TYPE_BOOLEAN)) return 0;
    if (!register_volatile_scalar(connection, "ducknng__ncurl_aio_collectable", 1, ducknng_ncurl_aio_collectable_scalar, &ctx, aio_id_types, DUCKDB_TYPE_BOOLEAN)) return 0;
    if (!register_volatile_scalar(connection, "ducknng__aio_mark_collected", 1, ducknng_aio_mark_collected_scalar, &ctx, aio_id_types, DUCKDB_TYPE_BOOLEAN)) return 0;
    if (!register_aio_wait_any_scalar_named(connection, &ctx, "ducknng__aio_wait_any")) return 0;
    if (!register_aio_collect_row_scalar_named(connection, &ctx, "ducknng__aio_collect_row")) return 0;
    if (!register_ncurl_aio_collect_row_scalar_named(connection, &ctx, "ducknng__ncurl_aio_collect_row")) return 0;
    if (!register_aio_status_scalar_named(connection, &ctx, "ducknng__aio_status_row")) return 0;
    if (!register_named_servers_table(connection, &ctx, "ducknng_list_servers")) return 0;
    if (!register_named_sockets_table(connection, &ctx, "ducknng_list_sockets")) return 0;
    if (!register_named_tls_configs_table(connection, &ctx, "ducknng_list_tls_configs")) return 0;
    if (!register_named_methods_table(connection, &ctx, "ducknng_list_methods")) return 0;
    if (!ducknng_register_sql_auth(connection, &ctx)) return 0;
    if (!register_remote_table_named(connection, &ctx, "ducknng_query_rpc")) return 0;
    if (!register_manifest_result_table_named(connection, &ctx, "ducknng_get_rpc_manifest")) return 0;
    if (!register_exec_result_table_named(connection, &ctx, "ducknng_run_rpc")) return 0;
    if (!register_request_result_table_named(connection, &ctx, "ducknng_request")) return 0;
    if (!register_request_socket_result_table_named(connection, &ctx, "ducknng_request_socket")) return 0;
    if (!register_http_result_table_named(connection, &ctx, "ducknng_ncurl")) return 0;
    if (!register_body_parse_table_named(connection, &ctx, "ducknng_parse_body")) return 0;
    if (!register_ncurl_table_named(connection, &ctx, "ducknng_ncurl_table")) return 0;
    if (!register_codecs_table_named(connection, "ducknng_list_codecs")) return 0;
    if (!register_open_query_table_named(connection, &ctx, "ducknng_open_query")) return 0;
    if (!register_fetch_query_table_named(connection, &ctx, "ducknng_fetch_query")) return 0;
    if (!register_session_control_table_named(connection, &ctx, "ducknng_close_query", ducknng_close_query_bind)) return 0;
    if (!register_session_control_table_named(connection, &ctx, "ducknng_cancel_query", ducknng_cancel_query_bind)) return 0;
    if (!register_frame_decode_table_named(connection, "ducknng_decode_frame")) return 0;
    if (!register_aio_status_macro(connection)) return 0;
    if (!register_aio_collect_macro(connection)) return 0;
    if (!register_ncurl_aio_collect_macro(connection)) return 0;
    return 1;
}
